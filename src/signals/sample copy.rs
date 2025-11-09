// Sample trading signal implementations
// This file contains a few example technical indicators to demonstrate the structure
// Full implementation is in the other signal files (which are gitignored)

use polars::prelude::*;
use std::cmp::*;

// Simple Moving Average
// Calculates the average of the last 'lookback' periods
pub fn sma(s: Series, lookback: usize) -> Vec<f64> {
    let len = s.len();
    let mut out = vec![std::f64::NAN; len];
    for i in lookback..len {
        out[i] = s
            .slice((i - lookback + 1).try_into().unwrap(), lookback)
            .mean()
            .unwrap();
    }
    out
}

// Exponential Moving Average
// Gives more weight to recent prices using an exponential decay
pub fn ema(s: Series, alpha: f64, lookback: usize) -> Vec<f64> {
    let len = s.len();
    let alpha = alpha / (lookback + 1) as f64;
    let beta = 1. - alpha;
    let mut out = sma(s.clone(), lookback);

    let arr = s.f64().unwrap();

    // find first non-NA value
    let mut lb: usize = 0;
    for i in 0..len {
        if out.get(i).unwrap().is_nan() {
            lb = i + 1;
        }
    }

    out[lb + 1] = (arr.get(lb + 1).unwrap() * alpha) + (out[lb] * beta);
    for i in lb + 2..len {
        out[i] = (arr.get(i).unwrap() * alpha) + (out[i - 1] * beta);
    }
    out
}

// Smoothed Moving Average (used by RSI)
pub fn smoothed_ma(s: Series, alpha: f64, lookback: usize) -> Vec<f64> {
    let len = s.len();
    let alpha = alpha / (lookback + 1) as f64;
    let beta = 1. - alpha;
    let mut out = sma(s.clone(), lookback);
    let arr = s.f64().unwrap();

    let mut lb: usize = 0;
    for i in 0..len {
        if out.get(i).unwrap().is_nan() {
            lb = i + 1;
        }
    }

    for i in lb + 1..len {
        out[i] = (arr.get(i).unwrap() * alpha) + (out[i - 1] * beta);
    }
    out
}

// Relative Strength Index (RSI)
// Momentum oscillator that measures speed and magnitude of price changes (0-100)
pub fn rsi(close: Series, lookback: usize) -> Vec<f64> {
    let len = close.len();
    let mut out = vec![std::f64::NAN; len];
    let mut pos = vec![0.; len];
    let mut neg = vec![0.; len];
    let mut rsi = vec![0.; len];
    let c = close.f64().unwrap();

    for i in 1..len {
        out[i] = c.get(i).unwrap() - c.get(i - 1).unwrap();
    }
    for i in 1..len {
        if out[i] > 0. {
            pos[i] = out[i];
        } else if out[i] < 0. {
            neg[i] = f64::abs(out[i]);
        }
    }
    let pos_smoothed = smoothed_ma(Series::new("".into(), &pos), 2., lookback);
    let neg_smoothed = smoothed_ma(Series::new("".into(), &neg), 2., lookback);
    for i in 1..len {
        rsi[i] = 100. - (100. / (1. + pos_smoothed[i] / neg_smoothed[i]));
    }
    rsi
}

// Moving Average Convergence Divergence (MACD)
// Trend-following momentum indicator showing relationship between two moving averages
// Returns (macd_line, signal_line)
pub fn macd(
    s: Series,
    long_ema: usize,
    short_ema: usize,
    signal_ema: usize,
) -> (Vec<f64>, Vec<f64>) {
    let len = s.len();
    let mut diff = vec![0.; len];

    let l_ema = ema(s.clone(), 2., long_ema);
    let s_ema = ema(s.clone(), 2., short_ema);

    for i in long_ema..len {
        diff[i] = s_ema[i] - l_ema[i];
    }

    let signal = ema(Series::new("".into(), &diff), 2., signal_ema);
    (diff, signal)
}
