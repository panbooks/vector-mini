use std::{
    cmp::{self, Ordering},
    mem,
};

use ordered_float::OrderedFloat;
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, SerializeAs};
use snafu::Snafu;
use vector_common::byte_size_of::ByteSizeOf;
use vector_config::configurable_component;

use crate::{
    event::{metric::Bucket, Metric, MetricValue},
    float_eq,
};

const AGENT_DEFAULT_BIN_LIMIT: u16 = 4096;
const AGENT_DEFAULT_EPS: f64 = 1.0 / 128.0;
const AGENT_DEFAULT_MIN_VALUE: f64 = 1.0e-9;

const UV_INF: i16 = i16::MAX;
const MAX_KEY: i16 = UV_INF;

const INITIAL_BINS: u16 = 128;
const MAX_BIN_WIDTH: u16 = u16::MAX;

#[inline]
fn log_gamma(gamma_ln: f64, v: f64) -> f64 {
    v.ln() / gamma_ln
}

#[inline]
fn pow_gamma(gamma_v: f64, y: f64) -> f64 {
    gamma_v.powf(y)
}

#[inline]
fn lower_bound(gamma_v: f64, bias: i32, k: i16) -> f64 {
    if k < 0 {
        return -lower_bound(gamma_v, bias, -k);
    }

    if k == MAX_KEY {
        return f64::INFINITY;
    }

    if k == 0 {
        return 0.0;
    }

    pow_gamma(gamma_v, f64::from(i32::from(k) - bias))
}

#[derive(Debug, Snafu)]
pub enum MergeError {
    #[snafu(display("cannot merge two sketches with mismatched configuration parameters"))]
    MismatchedConfigs,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Config {
    bin_limit: u16,
    // gamma_ln is the natural log of gamma_v, used to speed up calculating log base gamma.
    gamma_v: f64,
    gamma_ln: f64,
    // Min and max values representable by a sketch with these params.
    //
    // key(x) =
    //    0 : -min > x < min
    //    1 : x == min
    //   -1 : x == -min
    // +Inf : x > max
    // -Inf : x < -max.
    norm_min: f64,
    // Bias of the exponent, used to ensure key(x) >= 1.
    norm_bias: i32,
}

impl Config {
    #[allow(clippy::cast_possible_truncation)]
    pub(self) fn new(mut eps: f64, min_value: f64, bin_limit: u16) -> Self {
        assert!(eps > 0.0 && eps < 1.0, "eps must be between 0.0 and 1.0");
        assert!(min_value > 0.0, "min value must be greater than 0.0");
        assert!(bin_limit > 0, "bin limit must be greater than 0");

        eps *= 2.0;
        let gamma_v = 1.0 + eps;
        let gamma_ln = eps.ln_1p();

        // SAFETY: We expect `log_gamma` to return a value between -2^16 and 2^16, so it will always
        // fit in an i32.
        let norm_eff_min = log_gamma(gamma_ln, min_value).floor() as i32;
        let norm_bias = -norm_eff_min + 1;

        let norm_min = lower_bound(gamma_v, norm_bias, 1);

        assert!(
            norm_min <= min_value,
            "norm min should not exceed min_value"
        );

        Self {
            bin_limit,
            gamma_v,
            gamma_ln,
            norm_min,
            norm_bias,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn relative_accuracy(&self) -> f64 {
        // Only used for unit tests, hence the allow.
        (self.gamma_v - 1.0) / 2.0
    }

    /// Gets the value lower bound of the bin at the given key.
    pub fn bin_lower_bound(&self, k: i16) -> f64 {
        lower_bound(self.gamma_v, self.norm_bias, k)
    }

    /// Gets the key for the given value.
    ///
    /// The key corresponds to the bin where this value would be represented. The value returned here
    /// is such that: γ^k <= v < γ^(k+1).
    #[allow(clippy::cast_possible_truncation)]
    pub fn key(&self, v: f64) -> i16 {
        if v < 0.0 {
            return -self.key(-v);
        }

        if v == 0.0 || (v > 0.0 && v < self.norm_min) || (v < 0.0 && v > -self.norm_min) {
            return 0;
        }

        // SAFETY: `rounded` is intentionally meant to be a whole integer, and additionally, based
        // on our target gamma ln, we expect `log_gamma` to return a value between -2^16 and 2^16,
        // so it will always fit in an i32.
        let rounded = round_to_even(self.log_gamma(v)) as i32;
        let key = rounded.wrapping_add(self.norm_bias);

        // SAFETY: Our upper bound of POS_INF_KEY is i16, and our lower bound is simply one, so
        // there is no risk of truncation via conversion.
        key.clamp(1, i32::from(MAX_KEY)) as i16
    }

    pub fn log_gamma(&self, v: f64) -> f64 {
        log_gamma(self.gamma_ln, v)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config::new(
            AGENT_DEFAULT_EPS,
            AGENT_DEFAULT_MIN_VALUE,
            AGENT_DEFAULT_BIN_LIMIT,
        )
    }
}

/// A sketch bin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Bin {
    /// The bin index.
    k: i16,

    /// The number of observations within the bin.
    n: u16,
}

impl Bin {
    #[allow(clippy::cast_possible_truncation)]
    fn increment(&mut self, n: u32) -> u32 {
        let next = n + u32::from(self.n);
        if next > u32::from(MAX_BIN_WIDTH) {
            self.n = MAX_BIN_WIDTH;
            return next - u32::from(MAX_BIN_WIDTH);
        }

        // SAFETY: We already know `next` is less than or equal to `MAX_BIN_WIDTH` if we got here, and
        // `MAX_BIN_WIDTH` is u16, so next can't possibly be larger than a u16.
        self.n = next as u16;
        0
    }
}

/// [DDSketch][ddsketch] implementation based on the [Datadog Agent][ddagent].
///
/// This implementation is subtly different from the open-source implementations of `DDSketch`, as
/// Datadog made some slight tweaks to configuration values and in-memory layout to optimize it for
/// insertion performance within the agent.
///
/// We've mimicked the agent version of `DDSketch` here in order to support a future where we can
/// take sketches shipped by the agent, handle them internally, merge them, and so on, without any
/// loss of accuracy, eventually forwarding them to Datadog ourselves.
///
/// As such, this implementation is constrained in the same ways: the configuration parameters
/// cannot be changed, the collapsing strategy is fixed, and we support a limited number of methods
/// for inserting into the sketch.
///
/// Importantly, we have a special function, again taken from the agent version, to allow us to
/// interpolate histograms, specifically our own aggregated histograms, into a sketch so that we can
/// emit useful default quantiles, rather than having to ship the buckets -- upper bound and count
/// -- to a downstream system that might have no native way to do the same thing, basically
/// providing no value as they have no way to render useful data from them.
///
/// [ddsketch]: https://www.vldb.org/pvldb/vol12/p2195-masson.pdf
/// [ddagent]: https://github.com/DataDog/datadog-agent
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug)]
pub struct AgentDDSketch {
    #[serde(skip)]
    config: Config,

    /// The bins within the sketch.
    #[serde_as(as = "BinMap")]
    bins: Vec<Bin>,

    /// The number of observations within the sketch.
    count: u32,

    /// The minimum value of all observations within the sketch.
    min: f64,

    /// The maximum value of all observations within the sketch.
    max: f64,

    /// The sum of all observations within the sketch.
    sum: f64,

    /// The average value of all observations within the sketch.
    avg: f64,
}

impl AgentDDSketch {
    /// Creates a new `AgentDDSketch` based on a configuration that is identical to the one used by
    /// the Datadog agent itself.
    pub fn with_agent_defaults() -> Self {
        let config = Config::default();
        let initial_bins = cmp::max(INITIAL_BINS, config.bin_limit) as usize;

        Self {
            config,
            bins: Vec::with_capacity(initial_bins),
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.0,
            avg: 0.0,
        }
    }

    /// Creates a new `AgentDDSketch` based on the given inputs.
    ///
    /// This is _only_ useful for constructing a sketch from the raw components when the sketch has
    /// passed through the transform boundary into Lua/VRL and needs to be reconstructed.
    ///
    /// This is a light code smell, as our intention is to rigorously mediate access and mutation of
    /// a sketch through `AgentDDSketch` and the provided methods.
    pub fn from_raw(
        count: u32,
        min: f64,
        max: f64,
        sum: f64,
        avg: f64,
        keys: &[i16],
        counts: &[u16],
    ) -> Option<AgentDDSketch> {
        let bin_map = BinMap {
            keys: keys.into(),
            counts: counts.into(),
        };
        bin_map.into_bins().map(|bins| Self {
            config: Config::default(),
            bins,
            count,
            min,
            max,
            sum,
            avg,
        })
    }

    pub fn gamma(&self) -> f64 {
        self.config.gamma_v
    }

    pub fn bin_index_offset(&self) -> i32 {
        self.config.norm_bias
    }

    #[allow(dead_code)]
    fn bin_count(&self) -> usize {
        self.bins.len()
    }

    pub fn bins(&self) -> &[Bin] {
        &self.bins
    }

    pub fn bin_map(&self) -> BinMap {
        BinMap::from_bins(&self.bins)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Whether or not this sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Number of samples currently represented by this sketch.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Minimum value seen by this sketch.
    ///
    /// Returns `None` if the sketch is empty.
    pub fn min(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.min)
        }
    }

    /// Maximum value seen by this sketch.
    ///
    /// Returns `None` if the sketch is empty.
    pub fn max(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.max)
        }
    }

    /// Sum of all values seen by this sketch.
    ///
    /// Returns `None` if the sketch is empty.
    pub fn sum(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.sum)
        }
    }

    /// Average value seen by this sketch.
    ///
    /// Returns `None` if the sketch is empty.
    pub fn avg(&self) -> Option<f64> {
        if self.is_empty() {
            None
        } else {
            Some(self.avg)
        }
    }

    /// Clears the sketch, removing all bins and resetting all statistics.
    pub fn clear(&mut self) {
        self.count = 0;
        self.min = f64::MAX;
        self.max = f64::MIN;
        self.avg = 0.0;
        self.sum = 0.0;
        self.bins.clear();
    }

    fn adjust_basic_stats(&mut self, v: f64, n: u32) {
        if v < self.min {
            self.min = v;
        }

        if v > self.max {
            self.max = v;
        }

        self.count += n;
        self.sum += v * f64::from(n);

        if n == 1 {
            self.avg += (v - self.avg) / f64::from(self.count);
        } else {
            // TODO: From the Agent source code, this method apparently loses precision when the
            // two averages -- v and self.avg -- are close.  Is there a better approach?
            self.avg = self.avg + (v - self.avg) * f64::from(n) / f64::from(self.count);
        }
    }

    fn insert_key_counts(&mut self, mut counts: Vec<(i16, u32)>) {
        // Counts need to be sorted by key.
        counts.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2));

        let mut temp = Vec::new();

        let mut bins_idx = 0;
        let mut key_idx = 0;
        let bins_len = self.bins.len();
        let counts_len = counts.len();

        // PERF TODO: there's probably a fast path to be had where could check if all if the counts
        // have existing bins that aren't yet full, and we just update them directly, although we'd
        // still be doing a linear scan to find them since keys aren't 1:1 with their position in
        // `self.bins` but using this method just to update one or two bins is clearly suboptimal
        // and we wouldn't really want to scan them all just to have to back out and actually do the
        // non-fast path.. maybe a first pass could be checking if the first/last key falls within
        // our known min/max key, and if it doesn't, then we know we have to go through the non-fast
        // path, and if it passes, we do the scan to see if we can just update bins directly?
        while bins_idx < bins_len && key_idx < counts_len {
            let bin = self.bins[bins_idx];
            let vk = counts[key_idx].0;
            let kn = counts[key_idx].1;

            match bin.k.cmp(&vk) {
                Ordering::Greater => {
                    generate_bins(&mut temp, vk, kn);
                    key_idx += 1;
                }
                Ordering::Less => {
                    temp.push(bin);
                    bins_idx += 1;
                }
                Ordering::Equal => {
                    generate_bins(&mut temp, bin.k, u32::from(bin.n) + kn);
                    bins_idx += 1;
                    key_idx += 1;
                }
            }
        }

        temp.extend_from_slice(&self.bins[bins_idx..]);

        while key_idx < counts_len {
            let vk = counts[key_idx].0;
            let kn = counts[key_idx].1;
            generate_bins(&mut temp, vk, kn);
            key_idx += 1;
        }

        trim_left(&mut temp, self.config.bin_limit);

        // PERF TODO: This is where we might do a mem::swap instead so that we could shove the
        // bin vector into an object pool but I'm not sure this actually matters at the moment.
        self.bins = temp;
    }

    fn insert_keys(&mut self, mut keys: Vec<i16>) {
        // Updating more than 4 billion keys would be very very weird and likely indicative of
        // something horribly broken.
        assert!(
            keys.len()
                <= TryInto::<usize>::try_into(u32::MAX).expect("we don't support 16-bit systems")
        );

        keys.sort_unstable();

        let mut temp = Vec::new();

        let mut bins_idx = 0;
        let mut key_idx = 0;
        let bins_len = self.bins.len();
        let keys_len = keys.len();

        // PERF TODO: there's probably a fast path to be had where could check if all if the counts
        // have existing bins that aren't yet full, and we just update them directly, although we'd
        // still be doing a linear scan to find them since keys aren't 1:1 with their position in
        // `self.bins` but using this method just to update one or two bins is clearly suboptimal
        // and we wouldn't really want to scan them all just to have to back out and actually do the
        // non-fast path.. maybe a first pass could be checking if the first/last key falls within
        // our known min/max key, and if it doesn't, then we know we have to go through the non-fast
        // path, and if it passes, we do the scan to see if we can just update bins directly?
        while bins_idx < bins_len && key_idx < keys_len {
            let bin = self.bins[bins_idx];
            let vk = keys[key_idx];

            match bin.k.cmp(&vk) {
                Ordering::Greater => {
                    let kn = buf_count_leading_equal(&keys, key_idx);
                    generate_bins(&mut temp, vk, kn);
                    key_idx += kn as usize;
                }
                Ordering::Less => {
                    temp.push(bin);
                    bins_idx += 1;
                }
                Ordering::Equal => {
                    let kn = buf_count_leading_equal(&keys, key_idx);
                    generate_bins(&mut temp, bin.k, u32::from(bin.n) + kn);
                    bins_idx += 1;
                    key_idx += kn as usize;
                }
            }
        }

        temp.extend_from_slice(&self.bins[bins_idx..]);

        while key_idx < keys_len {
            let vk = keys[key_idx];
            let kn = buf_count_leading_equal(&keys, key_idx);
            generate_bins(&mut temp, vk, kn);
            key_idx += kn as usize;
        }

        trim_left(&mut temp, self.config.bin_limit);

        // PERF TODO: This is where we might do a mem::swap instead so that we could shove the
        // bin vector into an object pool but I'm not sure this actually matters at the moment.
        self.bins = temp;
    }

    pub fn insert(&mut self, v: f64) {
        // TODO: this should return a result that makes sure we have enough room to actually add 1
        // more sample without hitting `self.config.max_count()`
        self.adjust_basic_stats(v, 1);

        let key = self.config.key(v);
        self.insert_keys(vec![key]);
    }

    pub fn insert_many(&mut self, vs: &[f64]) {
        // TODO: this should return a result that makes sure we have enough room to actually add 1
        // more sample without hitting `self.config.max_count()`
        let mut keys = Vec::with_capacity(vs.len());
        for v in vs {
            self.adjust_basic_stats(*v, 1);
            keys.push(self.config.key(*v));
        }
        self.insert_keys(keys);
    }

    pub fn insert_n(&mut self, v: f64, n: u32) {
        // TODO: this should return a result that makes sure we have enough room to actually add N
        // more samples without hitting `self.config.max_count()`
        self.adjust_basic_stats(v, n);

        let key = self.config.key(v);
        self.insert_key_counts(vec![(key, n)]);
    }

    fn insert_interpolate_bucket(&mut self, lower: f64, upper: f64, count: u32) {
        // Find the keys for the bins where the lower bound and upper bound would end up, and
        // collect all of the keys in between, inclusive.
        let lower_key = self.config.key(lower);
        let upper_key = self.config.key(upper);
        let keys = (lower_key..=upper_key).collect::<Vec<_>>();

        let mut key_counts = Vec::new();
        let mut remaining_count = count;
        let distance = upper - lower;
        let mut start_idx = 0;
        let mut end_idx = 1;
        let mut lower_bound = self.config.bin_lower_bound(keys[start_idx]);
        let mut remainder = 0.0;

        while end_idx < keys.len() && remaining_count > 0 {
            // For each key, map the total distance between the input lower/upper bound against the sketch
            // lower/upper bound for the current sketch bin, which tells us how much of the input
            // count to apply to the current sketch bin.
            let upper_bound = self.config.bin_lower_bound(keys[end_idx]);
            let fkn = ((upper_bound - lower_bound) / distance) * f64::from(count);
            if fkn > 1.0 {
                remainder += fkn - fkn.trunc();
            }

            // SAFETY: This integer cast is intentional: we want to get the non-fractional part, as
            // we've captured the fractional part in the above conditional.
            #[allow(clippy::cast_possible_truncation)]
            let mut kn = fkn as u32;
            if remainder > 1.0 {
                kn += 1;
                remainder -= 1.0;
            }

            if kn > 0 {
                if kn > remaining_count {
                    kn = remaining_count;
                }

                self.adjust_basic_stats(lower_bound, kn);
                key_counts.push((keys[start_idx], kn));

                remaining_count -= kn;
                start_idx = end_idx;
                lower_bound = upper_bound;
            }

            end_idx += 1;
        }

        if remaining_count > 0 {
            let last_key = keys[start_idx];
            lower_bound = self.config.bin_lower_bound(last_key);
            self.adjust_basic_stats(lower_bound, remaining_count);
            key_counts.push((last_key, remaining_count));
        }

        self.insert_key_counts(key_counts);
    }

    /// ## Errors
    ///
    /// Returns an error if a bucket size is greater that `u32::MAX`.
    pub fn insert_interpolate_buckets(
        &mut self,
        mut buckets: Vec<Bucket>,
    ) -> Result<(), &'static str> {
        // Buckets need to be sorted from lowest to highest so that we can properly calculate the
        // rolling lower/upper bounds.
        buckets.sort_by(|a, b| {
            let oa = OrderedFloat(a.upper_limit);
            let ob = OrderedFloat(b.upper_limit);

            oa.cmp(&ob)
        });

        let mut lower = f64::NEG_INFINITY;

        if buckets
            .iter()
            .any(|bucket| bucket.count > u64::from(u32::MAX))
        {
            return Err("bucket size greater than u32::MAX");
        }

        for bucket in buckets {
            let mut upper = bucket.upper_limit;
            if upper.is_sign_positive() && upper.is_infinite() {
                upper = lower;
            } else if lower.is_sign_negative() && lower.is_infinite() {
                lower = upper;
            }

            // Each bucket should only have the values that fit within that bucket, which is
            // generally enforced at the source level by converting from cumulative buckets, or
            // enforced by the internal structures that hold bucketed data i.e. Vector's internal
            // `Histogram` data structure used for collecting histograms from `metrics`.
            let count = u32::try_from(bucket.count)
                .unwrap_or_else(|_| unreachable!("count range has already been checked."));

            self.insert_interpolate_bucket(lower, upper, count);
            lower = bucket.upper_limit;
        }

        Ok(())
    }

    /// Adds a bin directly into the sketch.
    ///
    /// Used only for unit testing so that we can create a sketch with an exact layout, which allows
    /// testing around the resulting bins when feeding in specific values, as well as generating
    /// explicitly bad layouts for testing.
    #[allow(dead_code)]
    pub(crate) fn insert_raw_bin(&mut self, k: i16, n: u16) {
        let v = self.config.bin_lower_bound(k);
        self.adjust_basic_stats(v, u32::from(n));
        self.bins.push(Bin { k, n });
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }

        if q <= 0.0 {
            return Some(self.min);
        }

        if q >= 1.0 {
            return Some(self.max);
        }

        let mut n = 0.0;
        let mut estimated = None;
        let wanted_rank = rank(self.count, q);

        for (i, bin) in self.bins.iter().enumerate() {
            n += f64::from(bin.n);
            if n <= wanted_rank {
                continue;
            }

            let weight = (n - wanted_rank) / f64::from(bin.n);
            let mut v_low = self.config.bin_lower_bound(bin.k);
            let mut v_high = v_low * self.config.gamma_v;

            if i == self.bins.len() {
                v_high = self.max;
            } else if i == 0 {
                v_low = self.min;
            }

            estimated = Some(v_low * weight + v_high * (1.0 - weight));
            break;
        }

        estimated
            .map(|v| v.clamp(self.min, self.max))
            .or(Some(f64::NAN))
    }

    /// Merges another sketch into this sketch, without a loss of accuracy.
    ///
    /// All samples present in the other sketch will be correctly represented in this sketch, and
    /// summary statistics such as the sum, average, count, min, and max, will represent the sum of
    /// samples from both sketches.
    ///
    /// ## Errors
    ///
    /// If there is an error while merging the two sketches together, an error variant will be
    /// returned that describes the issue.
    pub fn merge(&mut self, other: &AgentDDSketch) -> Result<(), MergeError> {
        if self.config != other.config {
            return Err(MergeError::MismatchedConfigs);
        }

        // Merge the basic statistics together.
        self.count += other.count;
        if other.max > self.max {
            self.max = other.max;
        }
        if other.min < self.min {
            self.min = other.min;
        }
        self.sum += other.sum;
        self.avg =
            self.avg + (other.avg - self.avg) * f64::from(other.count) / f64::from(self.count);

        // Now merge the bins.
        let mut temp = Vec::new();

        let mut bins_idx = 0;
        for other_bin in &other.bins {
            let start = bins_idx;
            while bins_idx < self.bins.len() && self.bins[bins_idx].k < other_bin.k {
                bins_idx += 1;
            }

            temp.extend_from_slice(&self.bins[start..bins_idx]);

            if bins_idx >= self.bins.len() || self.bins[bins_idx].k > other_bin.k {
                temp.push(*other_bin);
            } else if self.bins[bins_idx].k == other_bin.k {
                generate_bins(
                    &mut temp,
                    other_bin.k,
                    u32::from(other_bin.n) + u32::from(self.bins[bins_idx].n),
                );
                bins_idx += 1;
            }
        }

        temp.extend_from_slice(&self.bins[bins_idx..]);
        trim_left(&mut temp, self.config.bin_limit);

        self.bins = temp;

        Ok(())
    }

    /// Converts a `Metric` to a sketch representation, if possible, using `AgentDDSketch`.
    ///
    /// For certain types of metric values, such as distributions or aggregated histograms, we can
    /// easily convert them to a sketch-based representation.  Rather than push the logic of how to
    /// do that up to callers that wish to use a sketch-based representation, we bundle it here as a
    /// free function on `AgentDDSketch` itself.
    ///
    /// If the metric value cannot be represented as a sketch -- essentially, everything that isn't
    /// a distribution or aggregated histogram -- then the metric is passed back unmodified.  All
    /// existing metadata -- series name, tags, timestamp, etc -- is left unmodified, even if the
    /// metric is converted to a sketch internally.
    ///
    /// ## Errors
    ///
    /// Returns an error if a bucket size is greater that `u32::MAX`.
    pub fn transform_to_sketch(mut metric: Metric) -> Result<Metric, &'static str> {
        let sketch = match metric.data_mut().value_mut() {
            MetricValue::Distribution { samples, .. } => {
                let mut sketch = AgentDDSketch::with_agent_defaults();
                for sample in samples {
                    sketch.insert_n(sample.value, sample.rate);
                }
                Some(sketch)
            }
            MetricValue::AggregatedHistogram { buckets, .. } => {
                let delta_buckets = mem::take(buckets);
                let mut sketch = AgentDDSketch::with_agent_defaults();
                sketch.insert_interpolate_buckets(delta_buckets)?;
                Some(sketch)
            }
            // We can't convert from any other metric value.
            _ => None,
        };

        match sketch {
            // Metric was not able to be converted to a sketch, so pass it back.
            None => Ok(metric),
            // Metric was able to be converted to a sketch, so adjust the value.
            Some(sketch) => Ok(metric.with_value(sketch.into())),
        }
    }
}

impl PartialEq for AgentDDSketch {
    fn eq(&self, other: &Self) -> bool {
        // We skip checking the configuration because we don't allow creating configurations by
        // hand, and it's always locked to the constants used by the Datadog Agent.  We only check
        // the configuration equality manually in `AgentDDSketch::merge`, to protect ourselves in
        // the future if different configurations become allowed.
        //
        // Additionally, we also use floating-point-specific relative comparisons for sum/avg
        // because they can be minimally different between sketches purely due to floating-point
        // behavior, despite being fed the same exact data in terms of recorded samples.
        self.count == other.count
            && float_eq(self.min, other.min)
            && float_eq(self.max, other.max)
            && float_eq(self.sum, other.sum)
            && float_eq(self.avg, other.avg)
            && self.bins == other.bins
    }
}

impl Eq for AgentDDSketch {}

impl ByteSizeOf for AgentDDSketch {
    fn allocated_bytes(&self) -> usize {
        self.bins.len() * mem::size_of::<Bin>()
    }
}

/// A split representation of sketch bins.
///
/// While internally we depend on a vector of bins, the serialized form used in Protocol Buffers
/// splits the bins -- which contain their own key and bin count -- into two separate vectors, one
/// for the keys and one for the bin counts.
///
/// This type provides the glue to go from our internal representation to the Protocol
/// Buffers-specific representation.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct BinMap {
    /// The bin keys.
    #[serde(rename = "k")]
    pub keys: Vec<i16>,

    /// The bin counts.
    #[serde(rename = "n")]
    pub counts: Vec<u16>,
}

impl BinMap {
    pub fn from_bins<B>(bins: B) -> BinMap
    where
        B: AsRef<[Bin]>,
    {
        let (keys, counts) =
            bins.as_ref()
                .iter()
                .fold((Vec::new(), Vec::new()), |(mut keys, mut counts), bin| {
                    keys.push(bin.k);
                    counts.push(bin.n);

                    (keys, counts)
                });

        BinMap { keys, counts }
    }

    pub fn into_parts(self) -> (Vec<i16>, Vec<u16>) {
        (self.keys, self.counts)
    }

    pub fn into_bins(self) -> Option<Vec<Bin>> {
        if self.keys.len() == self.counts.len() {
            Some(
                self.keys
                    .into_iter()
                    .zip(self.counts)
                    .map(|(k, n)| Bin { k, n })
                    .collect(),
            )
        } else {
            None
        }
    }
}

impl SerializeAs<Vec<Bin>> for BinMap {
    fn serialize_as<S>(source: &Vec<Bin>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bin_map = BinMap::from_bins(source);
        bin_map.serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, Vec<Bin>> for BinMap {
    fn deserialize_as<D>(deserializer: D) -> Result<Vec<Bin>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bin_map: BinMap = Deserialize::deserialize(deserializer)?;
        bin_map
            .into_bins()
            .ok_or("keys and counts must match in length")
            .map_err(D::Error::custom)
    }
}

fn rank(count: u32, q: f64) -> f64 {
    round_to_even(q * f64::from(count - 1))
}

#[allow(clippy::cast_possible_truncation)]
fn buf_count_leading_equal(keys: &[i16], start_idx: usize) -> u32 {
    if start_idx == keys.len() - 1 {
        return 1;
    }

    let mut idx = start_idx;
    while idx < keys.len() && keys[idx] == keys[start_idx] {
        idx += 1;
    }

    // SAFETY: We limit the size of the vector (used to provide the slice given to us here) to be no
    // larger than 2^32, so we can't exceed u32 here.
    (idx - start_idx) as u32
}

fn trim_left(bins: &mut Vec<Bin>, bin_limit: u16) {
    // We won't ever support Vector running on anything other than a 32-bit platform and above, I
    // imagine, so this should always be safe.
    let bin_limit = bin_limit as usize;
    if bin_limit == 0 || bins.len() < bin_limit {
        return;
    }

    let num_to_remove = bins.len() - bin_limit;
    let mut missing = 0;
    let mut overflow = Vec::new();

    for bin in bins.iter().take(num_to_remove) {
        missing += u32::from(bin.n);

        if missing > u32::from(MAX_BIN_WIDTH) {
            overflow.push(Bin {
                k: bin.k,
                n: MAX_BIN_WIDTH,
            });

            missing -= u32::from(MAX_BIN_WIDTH);
        }
    }

    let bin_remove = &mut bins[num_to_remove];
    missing = bin_remove.increment(missing);
    if missing > 0 {
        generate_bins(&mut overflow, bin_remove.k, missing);
    }

    let overflow_len = overflow.len();
    let (_, bins_end) = bins.split_at(num_to_remove);
    overflow.extend_from_slice(bins_end);

    // I still don't yet understand how this works, since you'd think bin limit should be the
    // overall limit of the number of bins, but we're allowing more than that.. :thinkies:
    overflow.truncate(bin_limit + overflow_len);

    mem::swap(bins, &mut overflow);
}

#[allow(clippy::cast_possible_truncation)]
fn generate_bins(bins: &mut Vec<Bin>, k: i16, n: u32) {
    if n < u32::from(MAX_BIN_WIDTH) {
        // SAFETY: Cannot truncate `n`, as it's less than a u16 value.
        bins.push(Bin { k, n: n as u16 });
    } else {
        let overflow = n % u32::from(MAX_BIN_WIDTH);
        if overflow != 0 {
            bins.push(Bin {
                k,
                // SAFETY: Cannot truncate `overflow`, as it's modulo'd by a u16 value.
                n: overflow as u16,
            });
        }

        for _ in 0..(n / u32::from(MAX_BIN_WIDTH)) {
            bins.push(Bin {
                k,
                n: MAX_BIN_WIDTH,
            });
        }
    }
}

#[allow(clippy::cast_possible_truncation)]
#[inline]
fn capped_u64_shift(shift: u64) -> u32 {
    if shift >= 64 {
        u32::MAX
    } else {
        // SAFETY: There's no way that we end up truncating `shift`, since we cap it to 64 above.
        shift as u32
    }
}

fn round_to_even(v: f64) -> f64 {
    // Taken from Go: src/math/floor.go
    //
    // Copyright (c) 2009 The Go Authors. All rights reserved.
    //
    // Redistribution and use in source and binary forms, with or without
    // modification, are permitted provided that the following conditions are
    // met:
    //
    //    * Redistributions of source code must retain the above copyright
    // notice, this list of conditions and the following disclaimer.
    //    * Redistributions in binary form must reproduce the above
    // copyright notice, this list of conditions and the following disclaimer
    // in the documentation and/or other materials provided with the
    // distribution.
    //    * Neither the name of Google Inc. nor the names of its
    // contributors may be used to endorse or promote products derived from
    // this software without specific prior written permission.
    //
    // THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    // "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    // LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    // A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    // OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    // SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    // LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    // DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    // THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    // (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    // OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    // HEAR YE: There's a non-zero chance that we could rewrite this function in a way that is far
    // more Rust like, rather than porting over the particulars of how Go works, but we're
    // aiming for compatibility with a Go implementation, and so we've ported this over as
    // faithfully as possible.  With that said, here are the specifics we're dealing with:
    // - in Go, subtraction of unsigned numbers implicitly wraps around i.e. 1u64 - u64::MAX == 2
    // - in Go, right shifts are capped at the bitwidth of the left operand, which means that if
    //   you shift a 64-bit unsigned integers by 65 or above (some_u64 >> 65), instead of being told that
    //   you're doing something wrong, Go just caps the shift amount, and you end up with 0,
    //   whether you shift by 64 or by 9 million
    // - in Rust, it's not possible to directly do `some_u64 >> 64` even, as this would be
    //   considered an overflow
    // - in Rust, there are methods on unsigned integer primitives that allow for safely
    //   shifting by amounts greater than the bitwidth of the primitive type, although they mask off
    //   the bits in the shift amount that are higher than the bitwidth i.e. shift values above 64,
    //   for shifting a u64, are masked such that the resulting shift amount is 0, effectively
    //   not shifting the operand at all
    //
    // With all of this in mind, the code below compensates for that by doing wrapped
    // subtraction and shifting, which is straightforward, but also by utilizing
    // `capped_u64_shift` to approximate the behavior Go has when shifting by amounts larger
    // than the bitwidth of the left operand.
    //
    // I'm really proud of myself for reverse engineering this, but I sincerely hope we can
    // flush it down the toilet in the near future for something vastly simpler.
    const MASK: u64 = 0x7ff;
    const BIAS: u64 = 1023;
    const SHIFT: u64 = 64 - 11 - 1;
    const SIGN_MASK: u64 = 1 << 63;
    const FRAC_MASK: u64 = (1 << SHIFT) - 1;
    #[allow(clippy::unreadable_literal)]
    const UV_ONE: u64 = 0x3FF0000000000000;
    const HALF_MINUS_ULP: u64 = (1 << (SHIFT - 1)) - 1;

    let mut bits = v.to_bits();
    let mut e = (bits >> SHIFT) & MASK;
    if e >= BIAS {
        e = e.wrapping_sub(BIAS);
        let shift_amount = SHIFT.wrapping_sub(e);
        let shifted = bits.wrapping_shr(capped_u64_shift(shift_amount));
        let plus_ulp = HALF_MINUS_ULP + (shifted & 1);
        bits += plus_ulp.wrapping_shr(capped_u64_shift(e));
        bits &= !(FRAC_MASK.wrapping_shr(capped_u64_shift(e)));
    } else if e == BIAS - 1 && bits & FRAC_MASK != 0 {
        // Round 0.5 < abs(x) < 1.
        bits = bits & SIGN_MASK | UV_ONE; // +-1
    } else {
        // Round abs(x) <= 0.5 including denormals.
        bits &= SIGN_MASK; // +-0
    }
    f64::from_bits(bits)
}

