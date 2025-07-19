#![allow(missing_docs)]
/// Exponentially Weighted Moving Average
#[derive(Clone, Copy, Debug)]
pub struct Ewma {
    average: Option<f64>,
    alpha: f64,
}

impl Ewma {
    pub const fn new(alpha: f64) -> Self {
        let average = None;
        Self { average, alpha }
    }

    pub const fn average(&self) -> Option<f64> {
        self.average
    }

    /// Update the current average and return it for convenience
    pub fn update(&mut self, point: f64) -> f64 {
        let average = match self.average {
            None => point,
            Some(avg) => point.mul_add(self.alpha, avg * (1.0 - self.alpha)),
        };
        self.average = Some(average);
        average
    }
}

/// Exponentially Weighted Moving Average that starts with a default average value
#[derive(Clone, Copy, Debug)]
pub struct EwmaDefault {
    average: f64,
    alpha: f64,
}

impl EwmaDefault {
    pub const fn new(alpha: f64, initial_value: f64) -> Self {
        Self {
            average: initial_value,
            alpha,
        }
    }

    pub const fn average(&self) -> f64 {
        self.average
    }

    /// Update the current average and return it for convenience
    pub fn update(&mut self, point: f64) -> f64 {
        self.average = point.mul_add(self.alpha, self.average * (1.0 - self.alpha));
        self.average
    }
}

/// Exponentially Weighted Moving Average with variance calculation
#[derive(Clone, Copy, Debug)]
pub struct EwmaVar {
    state: Option<MeanVariance>,
    alpha: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MeanVariance {
    pub mean: f64,
    pub variance: f64,
}

impl EwmaVar {
    pub const fn new(alpha: f64) -> Self {
        let state = None;
        Self { state, alpha }
    }

    pub const fn state(&self) -> Option<MeanVariance> {
        self.state
    }

    #[cfg(test)]
    pub fn average(&self) -> Option<f64> {
        self.state.map(|state| state.mean)
    }

    #[cfg(test)]
    pub fn variance(&self) -> Option<f64> {
        self.state.map(|state| state.variance)
    }

    /// Update the current average and variance, and return them for convenience
    pub fn update(&mut self, point: f64) -> MeanVariance {
        let (mean, variance) = match self.state {
            None => (point, 0.0),
            Some(state) => {
                let difference = point - state.mean;
                let increment = self.alpha * difference;
                (
                    state.mean + increment,
                    (1.0 - self.alpha) * difference.mul_add(increment, state.variance),
                )
            }
        };
        let state = MeanVariance { mean, variance };
        self.state = Some(state);
        state
    }
}

/// Simple unweighted arithmetic mean
#[derive(Clone, Copy, Debug, Default)]
pub struct Mean {
    mean: f64,
    count: usize,
}

impl Mean {
    /// Update the and return the current average
    pub fn update(&mut self, point: f64) {
        self.count += 1;
        self.mean += (point - self.mean) / self.count as f64;
    }

    pub const fn average(&self) -> Option<f64> {
        match self.count {
            0 => None,
            _ => Some(self.mean),
        }
    }
}

