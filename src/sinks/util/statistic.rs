use std::cmp::Ordering;

use snafu::Snafu;

use crate::event::metric::Sample;

#[derive(Debug, Snafu)]
pub enum ValidationError {
    #[snafu(display("Quantiles must be in range [0.0,1.0]"))]
    QuantileOutOfRange,
}

#[derive(Debug)]
pub struct DistributionStatistic {
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub avg: f64,
    pub sum: f64,
    pub count: u64,
    /// (quantile, value)
    pub quantiles: Vec<(f64, f64)>,
}

impl DistributionStatistic {
    pub fn from_samples(source: &[Sample], quantiles: &[f64]) -> Option<Self> {
        let mut bins = source
            .iter()
            .filter(|sample| sample.rate > 0)
            .copied()
            .collect::<Vec<_>>();

        match bins.len() {
            0 => None,
            1 => Some({
                let val = bins[0].value;
                let count = bins[0].rate;
                Self {
                    min: val,
                    max: val,
                    median: val,
                    avg: val,
                    sum: val * count as f64,
                    count: count as u64,
                    quantiles: quantiles.iter().map(|&p| (p, val)).collect(),
                }
            }),
            _ => Some({
                bins.sort_unstable_by(|a, b| {
                    a.value.partial_cmp(&b.value).unwrap_or(Ordering::Equal)
                });

                let min = bins.first().unwrap().value;
                let max = bins.last().unwrap().value;
                let sum = bins
                    .iter()
                    .map(|sample| sample.value * sample.rate as f64)
                    .sum::<f64>();

                for i in 1..bins.len() {
                    bins[i].rate += bins[i - 1].rate;
                }

                let count = bins.last().unwrap().rate;
                let avg = sum / count as f64;

                let median = find_quantile(&bins, 0.5);
                let quantiles = quantiles
                    .iter()
                    .map(|&p| (p, find_quantile(&bins, p)))
                    .collect();

                Self {
                    min,
                    max,
                    median,
                    avg,
                    sum,
                    count: count as u64,
                    quantiles,
                }
            }),
        }
    }
}

/// `bins` is a cumulative histogram
/// We are using R-3 (without choosing the even integer in the case of a tie),
/// it might be preferable to use a more common function, such as R-7.
///
/// List of quantile functions:
/// <https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample>
fn find_quantile(bins: &[Sample], p: f64) -> f64 {
    let count = bins.last().expect("bins is empty").rate;
    find_sample(bins, (p * count as f64).round() as u32)
}

/// `bins` is a cumulative histogram
/// Return the i-th smallest value,
/// i starts from 1 (i == 1 mean the smallest value).
/// i == 0 is equivalent to i == 1.
fn find_sample(bins: &[Sample], i: u32) -> f64 {
    let index = match bins.binary_search_by_key(&i, |sample| sample.rate) {
        Ok(index) => index,
        Err(index) => index,
    };
    bins[index].value
}

pub fn validate_quantiles(quantiles: &[f64]) -> Result<(), ValidationError> {
    if quantiles
        .iter()
        .all(|&quantile| (0.0..=1.0).contains(&quantile))
    {
        Ok(())
    } else {
        Err(ValidationError::QuantileOutOfRange)
    }
}

