use std::{io, num::ParseIntError, path::Path, path::PathBuf, str::FromStr};

use futures::future::BoxFuture;
use snafu::{ResultExt, Snafu};
use tokio::{
    fs::{self, File},
    io::AsyncReadExt,
};
use vector_lib::metric_tags;

use super::{filter_result_sync, CGroupsConfig, HostMetrics, MetricsBuffer};
use crate::event::MetricTags;

const MICROSECONDS: f64 = 1.0 / 1_000_000.0;

#[derive(Debug, Snafu)]
enum CGroupsError {
    #[snafu(display("Could not open cgroup data file {:?}.", filename))]
    Opening {
        filename: PathBuf,
        source: io::Error,
    },
    #[snafu(display("Could not read cgroup data file {:?}.", filename))]
    Reading {
        filename: PathBuf,
        source: io::Error,
    },
    #[snafu(display("Could not parse cgroup data file {:?}.", filename))]
    Parsing {
        filename: PathBuf,
        source: ParseIntError,
    },
}

type CGroupsResult<T> = Result<T, CGroupsError>;

impl HostMetrics {
    pub(super) async fn cgroups_metrics(&self, output: &mut MetricsBuffer) {
        if let Some(root) = &self.root_cgroup {
            output.name = "cgroups";
            let mut recurser = CGroupRecurser::new(self, output);
            match &root.mode {
                Mode::Modern(base) => recurser.scan_modern(root, base).await,
                Mode::Legacy(base) => recurser.scan_legacy(root, base).await,
                Mode::Hybrid(v1base, v2base) => {
                    // Hybrid cgroups contain both legacy and modern cgroups, so scan them both
                    // for the data files. The `cpu` controller is usually found in the modern
                    // groups, but the top-level stats are found under the legacy controller in
                    // some setups. Similarly, the `memory` controller can be found in either
                    // location. As such, detecting exactly where to scan for the controllers
                    // doesn't work, so opportunistically scan for any controller files in all
                    // subdirectories of the given root.
                    recurser.scan_legacy(root, v1base).await;
                    recurser.scan_modern(root, v2base).await;
                }
            }
        }
    }
}

struct CGroupRecurser<'a> {
    output: &'a mut MetricsBuffer,
    buffer: String,
    load_cpu: bool,
    load_memory: bool,
    config: CGroupsConfig,
}

impl<'a> CGroupRecurser<'a> {
    fn new(host: &'a HostMetrics, output: &'a mut MetricsBuffer) -> Self {
        let cgroups = host.config.cgroups.clone().unwrap_or_default();

        Self {
            output,
            buffer: String::new(),
            load_cpu: true,
            load_memory: true,
            config: cgroups,
        }
    }

    async fn scan_modern(&mut self, root: &CGroupRoot, base: &Path) {
        let cgroup = CGroup {
            path: join_path(base, &root.path),
            name: root.name.clone(),
        };
        self.load_cpu = true;
        self.load_memory = true;
        self.recurse(cgroup, 1).await;
    }

    async fn scan_legacy(&mut self, root: &CGroupRoot, base: &Path) {
        let memory_base = join_path(base, "memory");
        let cgroup = CGroup {
            path: join_path(memory_base, &root.path),
            name: root.name.clone(),
        };
        self.load_cpu = false;
        self.load_memory = true;
        self.recurse(cgroup, 1).await;

        let cpu_base = join_path(base, "cpu");
        let cgroup = CGroup {
            path: join_path(cpu_base, &root.path),
            name: root.name.clone(),
        };
        self.load_cpu = true;
        self.load_memory = false;
        self.recurse(cgroup, 1).await;
    }

    fn recurse(&mut self, cgroup: CGroup, level: usize) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let tags = cgroup.tags();

            if self.load_cpu {
                self.load_cpu(&cgroup, &tags).await;
            }
            if self.load_memory {
                self.load_memory(&cgroup, &tags).await;
            }

            if level < self.config.levels {
                let groups = self.config.groups.clone();
                if let Some(children) =
                    filter_result_sync(cgroup.children().await, "Failed to load cgroups children.")
                {
                    for child in children {
                        if groups.contains_path(Some(&child.name)) {
                            self.recurse(child, level + 1).await;
                        }
                    }
                }
            }
        })
    }

    /// Try to load the `cpu` controller data file and emit metrics if it is found.
    async fn load_cpu(&mut self, cgroup: &CGroup, tags: &MetricTags) {
        if let Some(Some(cpu)) = filter_result_sync(
            cgroup.load_cpu(&mut self.buffer).await,
            "Failed to load cgroups CPU statistics.",
        ) {
            self.output.counter(
                "cgroup_cpu_usage_seconds_total",
                cpu.usage_usec as f64 * MICROSECONDS,
                tags.clone(),
            );
            self.output.counter(
                "cgroup_cpu_user_seconds_total",
                cpu.user_usec as f64 * MICROSECONDS,
                tags.clone(),
            );
            self.output.counter(
                "cgroup_cpu_system_seconds_total",
                cpu.system_usec as f64 * MICROSECONDS,
                tags.clone(),
            );
        }
    }

    /// Try to load the `memory` controller data files and emit metrics if they are found.
    async fn load_memory(&mut self, cgroup: &CGroup, tags: &MetricTags) {
        if let Some(Some(current)) = filter_result_sync(
            cgroup.load_memory_current(&mut self.buffer).await,
            "Failed to load cgroups current memory.",
        ) {
            self.output
                .gauge("cgroup_memory_current_bytes", current as f64, tags.clone());
        }

        if let Some(Some(stat)) = filter_result_sync(
            cgroup.load_memory_stat(&mut self.buffer).await,
            "Failed to load cgroups memory statistics.",
        ) {
            self.output
                .gauge("cgroup_memory_anon_bytes", stat.anon as f64, tags.clone());
            self.output
                .gauge("cgroup_memory_file_bytes", stat.file as f64, tags.clone());
            self.output.gauge(
                "cgroup_memory_anon_active_bytes",
                stat.active_anon as f64,
                tags.clone(),
            );
            self.output.gauge(
                "cgroup_memory_anon_inactive_bytes",
                stat.inactive_anon as f64,
                tags.clone(),
            );
            self.output.gauge(
                "cgroup_memory_file_active_bytes",
                stat.active_file as f64,
                tags.clone(),
            );
            self.output.gauge(
                "cgroup_memory_file_inactive_bytes",
                stat.inactive_file as f64,
                tags.clone(),
            );
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct CGroupRoot {
    name: PathBuf,
    path: PathBuf,
    mode: Mode,
}

#[derive(Clone, Debug)]
enum Mode {
    Legacy(PathBuf),
    Hybrid(PathBuf, PathBuf),
    Modern(PathBuf),
}

const CGROUP_CONTROLLERS: &str = "cgroup.controllers";

impl CGroupRoot {
    pub(super) fn new(config: &CGroupsConfig) -> Option<Self> {
        // There are three standard possibilities for cgroups setups
        // (`BASE` below is normally `/sys/fs/cgroup`, but containers
        // sometimes have `/sys` mounted elsewhere):
        // 1. Legacy v1 cgroups mounted at `BASE`
        // 2. Modern v2 cgroups mounted at `BASE`
        // 3. Hybrid cgroups, with v1 mounted at `BASE` and v2 mounted at `BASE/unified`.
        //
        // The `unified` directory only exists if cgroups is operating
        // in "hybrid" mode. Similarly, v2 cgroups will always have a
        // file named `cgroup.procs` in the base directory, and that
        // file is never present in v1 cgroups. By testing for either
        // the hybrid directory or the base file, we can uniquely
        // identify the current operating mode and, critically, the
        // location of the v2 cgroups root directory.
        //
        // Within that v2 root directory, each cgroup is a subdirectory
        // named for the cgroup identifier. Each group, including the
        // root, contains a set of files representing the controllers
        // for that group.

        let base_dir = config
            .base_dir
            .clone()
            .unwrap_or_else(|| join_path(heim::os::linux::sysfs_root(), "fs/cgroup"));

        let mode = {
            let hybrid_root = join_path(&base_dir, "unified");
            let hybrid_test_file = join_path(&hybrid_root, CGROUP_CONTROLLERS);
            let modern_test_file = join_path(&base_dir, CGROUP_CONTROLLERS);
            let cpu_dir = join_path(&base_dir, "cpu");
            if is_file(hybrid_test_file) {
                debug!(
                    message = "Detected hybrid cgroup base directory.",
                    ?base_dir
                );
                Mode::Hybrid(base_dir, hybrid_root)
            } else if is_file(modern_test_file) {
                debug!(
                    message = "Detected modern cgroup base directory.",
                    ?base_dir
                );
                Mode::Modern(base_dir)
            } else if is_dir(cpu_dir) {
                debug!(
                    message = "Detected legacy cgroup base directory.",
                    ?base_dir
                );
                Mode::Legacy(base_dir)
            } else {
                warn!(
                    message = "Could not detect cgroup base directory.",
                    ?base_dir
                );
                return None;
            }
        };

        let (path, name) = match &config.base {
            Some(base) => (base.to_path_buf(), base.to_path_buf()),
            None => ("/".into(), "/".into()),
        };
        Some(Self { name, path, mode })
    }
}

#[derive(Clone, Debug)]
struct CGroup {
    path: PathBuf,
    name: PathBuf,
}

impl CGroup {
    fn tags(&self) -> MetricTags {
        metric_tags! {
            "cgroup" => self.name.to_string_lossy(),
            "collector" => "cgroups",
        }
    }

    fn make_path(&self, filename: impl AsRef<Path>) -> PathBuf {
        join_path(&self.path, filename)
    }

    /// Open the file and read its contents. Returns `Ok(Some(filename))` if the file was read
    /// successfully, `Ok(None)` if it didn't exist, and `Err(…)` if an error happened during the
    /// process.
    async fn open_read(
        &self,
        filename: impl AsRef<Path>,
        buffer: &mut String,
    ) -> CGroupsResult<Option<PathBuf>> {
        buffer.clear();
        let filename = self.make_path(filename);
        match File::open(&filename).await {
            Ok(mut file) => {
                file.read_to_string(buffer)
                    .await
                    .with_context(|_| ReadingSnafu {
                        filename: filename.clone(),
                    })?;
                Ok(Some(filename))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(CGroupsError::Opening { source, filename }),
        }
    }

    /// Open the file, read its contents, and parse the contents using the `FromStr` trait on the
    /// desired type. Returns `Ok(Some(parsed_data))` on success, otherwise see `CGroup::open_read`.
    async fn open_read_parse<T: FromStr<Err = ParseIntError>>(
        &self,
        filename: impl AsRef<Path>,
        buffer: &mut String,
    ) -> CGroupsResult<Option<T>> {
        self.open_read(filename, buffer)
            .await?
            .map(|filename| {
                buffer
                    .trim()
                    .parse()
                    .with_context(|_| ParsingSnafu { filename })
            })
            .transpose()
    }

    async fn load_cpu(&self, buffer: &mut String) -> CGroupsResult<Option<CpuStat>> {
        self.open_read_parse("cpu.stat", buffer).await
    }

    async fn load_memory_current(&self, buffer: &mut String) -> CGroupsResult<Option<u64>> {
        self.open_read_parse("memory.current", buffer).await
    }

    async fn load_memory_stat(&self, buffer: &mut String) -> CGroupsResult<Option<MemoryStat>> {
        self.open_read_parse("memory.stat", buffer).await
    }

    async fn children(&self) -> io::Result<Vec<CGroup>> {
        let mut result = Vec::new();
        let mut dir = fs::read_dir(&self.path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if is_dir(&path) {
                let name = join_name(&self.name, entry.file_name());
                result.push(CGroup { path, name });
            }
        }
        Ok(result)
    }
}

macro_rules! define_stat_struct {
    ($name:ident ( $( $field:ident, )* )) => {
        #[derive(Clone, Copy, Debug, Default)]
        struct $name {
            $( $field: u64, )*
        }

        impl FromStr for $name {
            type Err = ParseIntError;
            fn from_str(text:&str)->Result<Self,Self::Err>{
                let mut result = Self::default();
                for line in text.lines(){
                    if false {}
                    $(
                        else if line.starts_with(concat!(stringify!($field), ' ')) {
                            result.$field = line[stringify!($field).len()+1..].parse()?;
                        }
                    )*
                }
                Ok(result)
            }
        }
    };
}

define_stat_struct! { CpuStat(
    usage_usec,
    user_usec,
    system_usec,
)}

define_stat_struct! { MemoryStat(
    // This file contains *many* more fields than defined here, these are
    // just the ones used to provide the metrics here. See the
    // documentation on `memory.stat` at
    // https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory
    // for more details.
    anon,
    file,
    active_anon,
    inactive_anon,
    active_file,
    inactive_file,
)}

fn is_dir(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path.as_ref()).is_ok_and(|metadata| metadata.is_dir())
}

fn is_file(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path.as_ref()).is_ok_and(|metadata| metadata.is_file())
}

/// Join a base directory path with a cgroup name.
fn join_path(base_path: impl AsRef<Path>, filename: impl AsRef<Path>) -> PathBuf {
    let filename = filename.as_ref();
    let base_path = base_path.as_ref();
    if filename == Path::new("/") {
        // `/` is the base cgroup name, no changes to the base path
        base_path.into()
    } else {
        [base_path, filename].iter().collect()
    }
}

/// Join a base cgroup name with another cgroup name.
fn join_name(base_name: &Path, filename: impl AsRef<Path>) -> PathBuf {
    let filename = filename.as_ref();
    // Joining cgroups names works a little differently than path
    // names. All names are relative paths except for the base, which is
    // the literal `/`. So, we have to check for the literal before joining.
    if base_name == Path::new("/") {
        filename.into()
    } else {
        [base_name, filename].iter().collect()
    }
}
