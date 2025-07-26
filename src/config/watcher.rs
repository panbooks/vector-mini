use crate::config::ComponentConfig;
use std::collections::HashSet;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use std::{
    sync::mpsc::{channel, Receiver},
    thread,
};

use notify::{recommended_watcher, EventKind, RecursiveMode};

use crate::Error;

/// Per notify own documentation, it's advised to have delay of more than 30 sec,
/// so to avoid receiving repetitions of previous events on macOS.
///
/// But, config and topology reload logic can handle:
///  - Invalid config, caused either by user or by data race.
///  - Frequent changes, caused by user/editor modifying/saving file in small chunks.
///    so we can use smaller, more responsive delay.
const CONFIG_WATCH_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Refer to [`crate::cli::WatchConfigMethod`] for details.
pub enum WatcherConfig {
    /// Recommended watcher for the current OS.
    RecommendedWatcher,
    /// A poll-based watcher that checks for file changes at regular intervals.
    PollWatcher(u64),
}

enum Watcher {
    /// recommended watcher for os, usually inotify for linux based systems
    RecommendedWatcher(notify::RecommendedWatcher),
    /// poll based watcher. for watching files from NFS.
    PollWatcher(notify::PollWatcher),
}

impl Watcher {
    fn add_paths(&mut self, config_paths: &[PathBuf]) -> Result<(), Error> {
        for path in config_paths {
            self.watch(path, RecursiveMode::Recursive)?;
        }
        Ok(())
    }

    fn watch(&mut self, path: &Path, recursive_mode: RecursiveMode) -> Result<(), Error> {
        use notify::Watcher as NotifyWatcher;
        match self {
            Watcher::RecommendedWatcher(watcher) => {
                watcher.watch(path, recursive_mode)?;
            }
            Watcher::PollWatcher(watcher) => {
                watcher.watch(path, recursive_mode)?;
            }
        }
        Ok(())
    }
}

/// Sends a ReloadFromDisk on config_path changes.
/// Accumulates file changes until no change for given duration has occurred.
/// Has best effort guarantee of detecting all file changes from the end of
/// this function until the main thread stops.
pub fn spawn_thread<'a>(
    watcher_conf: WatcherConfig,
    signal_tx: crate::signal::SignalTx,
    config_paths: impl IntoIterator<Item = &'a PathBuf> + 'a,
    component_configs: Vec<ComponentConfig>,
    delay: impl Into<Option<Duration>>,
) -> Result<(), Error> {
    let mut config_paths: Vec<_> = config_paths.into_iter().cloned().collect();
    let mut component_config_paths: Vec<_> = component_configs
        .clone()
        .into_iter()
        .flat_map(|p| p.config_paths.clone())
        .collect();

    config_paths.append(&mut component_config_paths);

    let delay = delay.into().unwrap_or(CONFIG_WATCH_DELAY);

    // Create watcher now so not to miss any changes happening between
    // returning from this function and the thread starting.
    let mut watcher = Some(create_watcher(&watcher_conf, &config_paths)?);

    info!("Watching configuration files.");

    thread::spawn(move || loop {
        if let Some((mut watcher, receiver)) = watcher.take() {
            while let Ok(Ok(event)) = receiver.recv() {
                if matches!(
                    event.kind,
                    EventKind::Create(_) | EventKind::Remove(_) | EventKind::Modify(_)
                ) {
                    debug!(message = "Configuration file change detected.", event = ?event);

                    // Consume events until delay amount of time has passed since the latest event.
                    while receiver.recv_timeout(delay).is_ok() {}

                    debug!(message = "Consumed file change events for delay.", delay = ?delay);

                    let component_keys: HashSet<_> = component_configs
                        .clone()
                        .into_iter()
                        .flat_map(|p| p.contains(&event.paths))
                        .collect();

                    // We need to read paths to resolve any inode changes that may have happened.
                    // And we need to do it before raising sighup to avoid missing any change.
                    if let Err(error) = watcher.add_paths(&config_paths) {
                        error!(message = "Failed to read files to watch.", %error);
                        break;
                    }

                    debug!(message = "Reloaded paths.");

                    info!("Configuration file changed.");
                    if !component_keys.is_empty() {
                        info!("Component {:?} configuration changed.", component_keys);
                        _ = signal_tx.send(crate::signal::SignalTo::ReloadComponents(component_keys)).map_err(|error| {
                            error!(message = "Unable to reload component configuration. Restart Vector to reload it.", cause = %error)
                        });
                    } else {
                        _ = signal_tx.send(crate::signal::SignalTo::ReloadFromDisk)
                            .map_err(|error| {
                                error!(message = "Unable to reload configuration file. Restart Vector to reload it.", cause = %error)
                            });
                    }
                } else {
                    debug!(message = "Ignoring event.", event = ?event)
                }
            }
        }

        thread::sleep(RETRY_TIMEOUT);

        watcher = create_watcher(&watcher_conf, &config_paths)
            .map_err(|error| error!(message = "Failed to create file watcher.", %error))
            .ok();

        if watcher.is_some() {
            // Config files could have changed while we weren't watching,
            // so for a good measure raise SIGHUP and let reload logic
            // determine if anything changed.
            info!("Speculating that configuration files have changed.");
            _ = signal_tx.send(crate::signal::SignalTo::ReloadFromDisk).map_err(|error| {
                error!(message = "Unable to reload configuration file. Restart Vector to reload it.", cause = %error)
            });
        }
    });

    Ok(())
}

fn create_watcher(
    watcher_conf: &WatcherConfig,
    config_paths: &[PathBuf],
) -> Result<(Watcher, Receiver<Result<notify::Event, notify::Error>>), Error> {
    info!("Creating configuration file watcher.");

    let (sender, receiver) = channel();
    let mut watcher = match watcher_conf {
        WatcherConfig::RecommendedWatcher => {
            let recommended_watcher = recommended_watcher(sender)?;
            Watcher::RecommendedWatcher(recommended_watcher)
        }
        WatcherConfig::PollWatcher(interval) => {
            let config =
                notify::Config::default().with_poll_interval(Duration::from_secs(*interval));
            let poll_watcher = notify::PollWatcher::new(sender, config)?;
            Watcher::PollWatcher(poll_watcher)
        }
    };
    watcher.add_paths(config_paths)?;
    Ok((watcher, receiver))
}
