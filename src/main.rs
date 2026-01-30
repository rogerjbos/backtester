#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use chrono::Utc;
use backtester::*;
use polars::prelude::*;
use std::{collections::HashSet, env, error::Error as StdError, fs, fs::File, process};
use tokio;
use clap::Parser;
use log::{info, debug, warn, error};

/// Backtester for analyzing trading strategies
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Universe: 'Crypto', 'SC', 'MC', 'LC', 'Micro', 'Stocks'
    #[arg(short, long, default_value = "Crypto")]
    universe: String,

    /// Mode: 'production', 'testing', or 'demo'
    #[arg(short, long, default_value = "testing")]
    mode: String,

    /// Filter by specific ticker(s) - comma separated (optional)
    #[arg(short = 't', long)]
    tickers: Option<String>,

    /// Filter by specific strategy (optional)
    #[arg(short, long)]
    strategy: Option<String>,

    /// Working directory path
    #[arg(short, long)]
    path: Option<String>,

    /// Enable verbose logging
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

mod signals {
    pub mod bots; // book of trading strategies
    pub mod mfpr; // mastering financial pattern recognition
    pub mod technical;
    pub mod trend_following;
}

#[cfg(test)]
mod tests;

pub async fn select_backtests(
    lf: LazyFrame,
    tag: &str,
    strategy_filter: Option<&str>,
) -> Result<Vec<(Backtest, Vec<Decision>)>, Box<dyn StdError>> {
    let mut signals: Vec<Signal> = Vec::new();
    let prod_signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        ("three_candles", signals::mfpr::three_candles, 0.0),
        (
            "trend_fol_3candle_ma",
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("pattern_marubozu", signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer", signals::bots::pattern_hammer, 0.0),
        ("hammer", signals::mfpr::hammer, 0.0),
        ("tweezers", signals::mfpr::tweezers, 0.0),
        ("hikkake", signals::mfpr::hikkake, 0.0),
        (
            "adx_indicator",
            signals::trend_following::adx_indicator,
            0.0,
        ),
        (
            "donchian_indicator",
            signals::trend_following::donchian_indicator,
            0.0,
        ),
        ("tower", signals::mfpr::tower, 0.0),
        ("slingshot", signals::mfpr::slingshot, 0.0),
        ("quintuplets_0005", signals::mfpr::quintuplets_0005, 0.0),
    ];

    let crypto_signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        (
            "squeeze_momentum",
            signals::trend_following::squeeze_momentum,
            0.0,
        ),
        (
            "vertical_horizontal_cross",
            signals::trend_following::vertical_horizontal_cross,
            0.0,
        ),
        ("hammer", signals::mfpr::hammer, 0.0),
        ("double_trouble", signals::mfpr::double_trouble_1, 0.0),
        (
            "donchian_indicator",
            signals::trend_following::donchian_indicator,
            0.0,
        ),
        ("key_reversal", signals::bots::key_reversal, 0.0),
        ("pattern_marubozu", signals::bots::pattern_marubozu, 0.0),
        ("h", signals::mfpr::h, 0.0),
        ("spinning_top", signals::mfpr::spinning_top, 0.0),
        (
            "candlestick_double_trouble",
            signals::mfpr::candlestick_double_trouble,
            2.0,
        ),
        (
            "trend_fol_2trouble_rsi",
            signals::mfpr::trend_fol_2trouble_rsi,
            0.0,
        ),
        ("heikin_ashi", signals::trend_following::heikin_ashi, 0.0),
        (
            "heikin_ashi_double_trouble",
            signals::mfpr::heikin_ashi_double_trouble,
            0.0,
        ),
        ("tf12_vama", signals::trend_following::tf12_vama, 0.0),
        ("tf9_tii", signals::trend_following::tf9_tii, 0.0),
        (
            "aroon_oscillator",
            signals::trend_following::aroon_oscillator,
            0.0,
        ),
        (
            "contrarian_rsi_extremes",
            signals::bots::contrarian_rsi_extremes,
            0.0,
        ),
        ("bottle", signals::mfpr::bottle, 0.0),
        ("macd_change", signals::trend_following::macd_change, 0.0),
        ("star", signals::mfpr::star, 0.0),
        ("mirror", signals::mfpr::mirror, 0.0),
        ("marubozu", signals::mfpr::marubozu, 0.0),
        (
            "contrarian_disparity_extremes",
            signals::bots::contrarian_disparity_extremes,
            0.0,
        ),
        ("pattern_piercing", signals::bots::pattern_piercing, 0.0),
        (
            "pattern_td_camouflauge",
            signals::bots::pattern_td_camouflage,
            0.0,
        ),
        ("pattern_td_clopwin", signals::bots::pattern_td_clopwin, 0.0),
        ("gri_index", signals::trend_following::gri_index, 0.0),
        ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2, 0.0),
        ("pattern_td_waldo_5", signals::bots::pattern_td_waldo_5, 0.0),
        ("pattern_td_waldo_6", signals::bots::pattern_td_waldo_6, 0.0),
        ("pattern_td_waldo_8", signals::bots::pattern_td_waldo_8, 0.0),
        (
            "trend_fol_h_trend_intensity",
            signals::mfpr::trend_fol_h_trend_intensity,
            0.0,
        ),
        (
            "heikin_ashi_euphoria",
            signals::mfpr::heikin_ashi_euphoria,
            0.0,
        ),
        ("euphoria", signals::mfpr::euphoria, 0.0),
        ("engulfing", signals::mfpr::engulfing, 0.0),
        ("doji", signals::mfpr::doji, 0.0),
        ("stick_sandwich", signals::mfpr::stick_sandwich, 0.0),
        (
            "contrarian_stochastic_extremes",
            signals::bots::contrarian_stochastic_extremes,
            0.0,
        ),
        (
            "contrarian_stochastic_divergences",
            signals::bots::contrarian_stochastic_divergences,
            0.0,
        ),
        (
            "contrarian_stochastic_duration",
            signals::bots::contrarian_stochastic_duration,
            0.0,
        ),
        (
            "contrarian_stochastic_cross",
            signals::bots::contrarian_stochastic_cross,
            0.0,
        ),
        (
            "contrarian_rsi_divergences",
            signals::bots::contrarian_rsi_divergences,
            0.0,
        ),
        (
            "trend_fol_3candle_ma",
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("tf1_ma", signals::trend_following::tf1_ma, 0.0),
        ("tf2_ma", signals::trend_following::tf2_ma, 0.0),
        ("tf3_rsi_ma", signals::trend_following::tf3_rsi_ma, 0.0),
        ("tf4_macd", signals::trend_following::tf4_macd, 0.0),
        ("tf5_ma_slope", signals::trend_following::tf5_ma_slope, 0.0),
        (
            "tf6_supertrend_flip",
            signals::trend_following::tf6_supertrend_flip,
            0.0,
        ),
        ("tf7_psar_ma", signals::trend_following::tf7_psar_ma, 0.0),
        (
            "trend_fol_marubozu_k_vol_bands",
            signals::mfpr::trend_fol_marubozu_k_vol_bands,
            0.0,
        ),
    ];

    let micro_signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        ("hammer", signals::mfpr::hammer, 0.0),
        (
            "trend_fol_3candle_ma",
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("three_candles", signals::mfpr::three_candles, 0.0),
        ("hikkake", signals::mfpr::hikkake, 0.0),
        (
            "contrarian_piercing_stoch",
            signals::mfpr::contrarian_piercing_stoch,
            0.0,
        ),
        ("piercing", signals::mfpr::piercing, 0.0),
        ("tasuki", signals::mfpr::tasuki, 0.0),
        (
            "heikin_ashi_double_trouble",
            signals::mfpr::heikin_ashi_double_trouble,
            0.0,
        ),
        ("heikin_ashi_tasuki", signals::mfpr::heikin_ashi_tasuki, 0.0),
        ("pattern_hammer", signals::bots::pattern_hammer, 0.0),
        (
            "contrarian_engulfing_bbands",
            signals::mfpr::contrarian_engulfing_bbands,
            0.0,
        ),
        ("h", signals::mfpr::h, 0.0),
        ("double_trouble", signals::mfpr::double_trouble_1, 0.0),
        (
            "candlestick_double_trouble",
            signals::mfpr::candlestick_double_trouble,
            0.0,
        ),
        ("candlestick_tasuki", signals::mfpr::candlestick_tasuki, 0.0),
        ("blockade", signals::mfpr::blockade, 0.0),
        (
            "trend_fol_h_trend_intensity",
            signals::mfpr::trend_fol_h_trend_intensity,
            0.0,
        ),
    ];

    let sc_signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        ("tweezers", signals::mfpr::tweezers, 0.0),
        (
            "trend_fol_3candle_ma",
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("three_candles", signals::mfpr::three_candles, 0.0),
        ("pattern_marubozu", signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer", signals::bots::pattern_hammer, 0.0),
        (
            "adx_indicator",
            signals::trend_following::adx_indicator,
            0.0,
        ),
        ("hammer", signals::mfpr::hammer, 0.0),
        ("tower", signals::mfpr::tower, 0.0),
        ("hikkake", signals::mfpr::hikkake, 0.0),
    ];

    let mc_signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        (
            "trend_fol_3candle_ma",
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("three_candles", signals::mfpr::three_candles, 0.0),
        ("pattern_marubozu", signals::bots::pattern_marubozu, 0.0),
        ("hikkake", signals::mfpr::hikkake, 0.0),
        (
            "adx_indicator",
            signals::trend_following::adx_indicator,
            0.0,
        ),
        ("tower", signals::mfpr::tower, 0.0),
        ("pattern_hammer", signals::bots::pattern_hammer, 0.0),
        ("hammer", signals::mfpr::hammer, 0.0),
    ];

    let lc_signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        ("tweezers", signals::mfpr::tweezers, 0.0),
        ("pattern_marubozu", signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer", signals::bots::pattern_hammer, 0.0),
        ("slingshot", signals::mfpr::slingshot, 0.0),
        (
            "trend_fol_3candle_ma",
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("tower", signals::mfpr::tower, 0.0),
        ("three_candles", signals::mfpr::three_candles, 0.0),
        (
            "adx_indicator",
            signals::trend_following::adx_indicator,
            0.0,
        ),
    ];

    let _signal_functions: Vec<(String, SignalFunctionWithParam, f64)> = (1..=10)
        .map(|i| {
            let param = 1.0 + (i as f64 * 0.2) - 0.2; // Generate param values from 1.0 to 3.0
            (
                format!("candlestick_double_trouble_{:.1}", param), // Use `String` instead of `&str`
                signals::mfpr::candlestick_double_trouble as SignalFunctionWithParam, // Cast to the correct type
                param,
            )
        })
        .collect();

    let param_functions: Vec<(String, SignalFunctionWithParam, f64)> =
        (1..=5).map(|i| {
            let param = (i * 10) as f64;
            (
                format!("donchian_indicator_{:.0}", param), // Use `String` instead of `&str`
                signals::trend_following::donchian_indicator as SignalFunctionWithParam, // Cast to the correct type
                param,
            )
        })
        .chain(std::iter::once((
            "donchian_indicator_inverse".to_string(), // Manually add the "three_candles" entry
            signals::trend_following::donchian_indicator_inverse as SignalFunctionWithParam,
            0.0,
        )))
        .chain(
            (1..=5).map(|i| {
                let param = (i * 10) as f64;
                (
                    format!("donchian_indicator_high_{:.0}", param), // Use `String` instead of `&str`
                    signals::trend_following::donchian_indicator_high as SignalFunctionWithParam, // Cast to the correct type
                    param,
                )
            })
        )
        .chain(
            (1..=5).map(|i| {
                let param = (i * 10) as f64;
                (
                    format!("donchian_indicator_low_{:.0}", param),
                    signals::trend_following::donchian_indicator_low as SignalFunctionWithParam, // Cast to the correct type
                    param,
                )
            })
        )
        .chain(
            (1..=5).map(|i| {
                let param = (i) as f64;
                (
                    format!("trend_fol_2trouble_rsi_atrparam_{:.0}", param),
                    signals::mfpr::trend_fol_2trouble_rsi_atrparam as SignalFunctionWithParam, // Cast to the correct type
                    param,
                )
            })
        )
        .chain(
            (2..=6).map(|i| {
                let param = (i * 10) as f64;
                (
                    format!("trend_fol_2trouble_rsi_rsiparam_{:.0}", param),
                    signals::mfpr::trend_fol_2trouble_rsi_rsiparam as SignalFunctionWithParam, // Cast to the correct type
                    param,
                )
            })
        )
        .collect();

    let testing_functions: Vec<(String, SignalFunctionWithParam, f64)> = (1..=2)
        .map(|i| {
            let param = 1.0 + (i as f64 * 0.5) - 0.5; // Generate param values from 1.0 to 3.0
            (
                format!("candlestick_double_trouble_{:.1}", param), // Use `String` instead of `&str`
                signals::mfpr::candlestick_double_trouble as SignalFunctionWithParam, // Cast to the correct type
                param,
            )
        })
        .chain(std::iter::once((
            "three_candles".to_string(), // Manually add the "three_candles" entry
            signals::mfpr::three_candles as SignalFunctionWithParam,
            0.0,
        )))
        .collect();

    let signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        (
            "candlestick_double_trouble_2.0",
            signals::mfpr::candlestick_double_trouble,
            2.0,
        ),
        ("three_candles", signals::mfpr::three_candles, 0.0),
        (
            "trend_fol_3candle_ma",
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("pattern_marubozu", signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer", signals::bots::pattern_hammer, 0.0),
        ("hammer", signals::mfpr::hammer, 0.0),
        ("tweezers", signals::mfpr::tweezers, 0.0),
        ("hikkake", signals::mfpr::hikkake, 0.0),
        ("slingshot", signals::mfpr::slingshot, 0.0),
        ("quintuplets_0005", signals::mfpr::quintuplets_0005, 0.0),
        ("quintuplets_2", signals::mfpr::quintuplets_2, 0.0),
        ("quintuplets_10", signals::mfpr::quintuplets_10, 0.0),
        ("quintuplets_50", signals::mfpr::quintuplets_50, 0.0),
        ("marubozu", signals::mfpr::marubozu, 0.0),
        ("tasuki", signals::mfpr::tasuki, 0.0),
        ("three_methods", signals::mfpr::three_methods, 0.0),
        (
            "fibonacci_range",
            signals::trend_following::fibonacci_range,
            0.0,
        ),
        (
            "adx_indicator",
            signals::trend_following::adx_indicator,
            0.0,
        ),
        (
            "donchian_indicator",
            signals::trend_following::donchian_indicator,
            0.0,
        ),
        (
            "donchian_indicator_inverse",
            signals::trend_following::donchian_indicator_inverse,
            0.0,
        ),
        ("tower", signals::mfpr::tower, 0.0),
        ("slingshot", signals::mfpr::slingshot, 0.0),
        ("quintuplets_0005", signals::mfpr::quintuplets_0005, 0.0),
        ("marubozu", signals::mfpr::marubozu, 0.0),
        ("tasuki", signals::mfpr::tasuki, 0.0),
        ("three_methods", signals::mfpr::three_methods, 0.0),
        ("bottle", signals::mfpr::bottle, 0.0),
        ("double_trouble", signals::mfpr::double_trouble_1, 0.0),
        ("h", signals::mfpr::h, 0.0),
        ("quintuplets_2", signals::mfpr::quintuplets_2, 0.0),
        ("quintuplets_10", signals::mfpr::quintuplets_10, 0.0),
        ("quintuplets_50", signals::mfpr::quintuplets_50, 0.0),
        ("abandoned_baby", signals::mfpr::abandoned_baby, 0.0),
        ("doji", signals::mfpr::doji, 0.0),
        ("engulfing", signals::mfpr::engulfing, 0.0),
        ("harami_flexible", signals::mfpr::harami_flexible, 0.0),
        ("harami_strict", signals::mfpr::harami_strict, 0.0),
        ("inside_up_down", signals::mfpr::inside_up_down, 0.0),
        ("on_neck", signals::mfpr::on_neck, 0.0),
        ("piercing", signals::mfpr::piercing, 0.0),
        ("spinning_top", signals::mfpr::spinning_top, 0.0),
        ("star", signals::mfpr::star, 0.0),
        ("stick_sandwich", signals::mfpr::stick_sandwich, 0.0),
        ("barrier", signals::mfpr::barrier, 0.0),
        ("blockade", signals::mfpr::blockade, 1.5), // Adjusted to a different value
        ("doppleganger", signals::mfpr::doppleganger, 0.0),
        ("euphoria", signals::mfpr::euphoria, 0.0),
        ("mirror", signals::mfpr::mirror, 0.0),
        ("shrinking", signals::mfpr::shrinking, 0.0),
        ("heikin_ashi_doji", signals::mfpr::heikin_ashi_doji, 0.0),
        (
            "heikin_ashi_double_trouble",
            signals::mfpr::heikin_ashi_double_trouble,
            0.0,
        ),
        (
            "heikin_ashi_euphoria",
            signals::mfpr::heikin_ashi_euphoria,
            0.0,
        ),
        ("heikin_ashi_tasuki", signals::mfpr::heikin_ashi_tasuki, 0.0),
        ("candlestick_doji", signals::mfpr::candlestick_doji, 0.0),
        (
            "candlestick_double_trouble",
            signals::mfpr::candlestick_double_trouble,
            2.0,
        ),
        ("candlestick_tasuki", signals::mfpr::candlestick_tasuki, 0.0),
        (
            "trend_fol_bottle_stoch",
            signals::mfpr::trend_fol_bottle_stoch,
            1.5,
        ),
        (
            "trend_fol_2trouble_rsi",
            signals::mfpr::trend_fol_2trouble_rsi,
            1.5,
        ),
        (
            "trend_fol_h_trend_intensity",
            signals::mfpr::trend_fol_h_trend_intensity,
            1.5,
        ),
        (
            "trend_fol_marubozu_k_vol_bands",
            signals::mfpr::trend_fol_marubozu_k_vol_bands,
            1.5,
        ),
        (
            "contrarian_barrier_rsi_atr",
            signals::mfpr::contrarian_barrier_rsi_atr,
            1.5,
        ),
        (
            "contrarian_doji_rsi",
            signals::mfpr::contrarian_doji_rsi,
            1.5,
        ),
        (
            "contrarian_engulfing_bbands",
            signals::mfpr::contrarian_engulfing_bbands,
            1.5,
        ),
        (
            "contrarian_euphoria_k_env",
            signals::mfpr::contrarian_euphoria_k_env,
            0.0,
        ),
        (
            "contrarian_piercing_stoch",
            signals::mfpr::contrarian_piercing_stoch,
            0.0,
        ),
        (
            "fibonacci_range",
            signals::trend_following::fibonacci_range,
            0.0,
        ),
        (
            "elder_impulse_1",
            signals::trend_following::elder_impulse_1,
            0.0,
        ),
        (
            "elder_impulse_2",
            signals::trend_following::elder_impulse_2,
            0.0,
        ),
        (
            "elder_impulse_3",
            signals::trend_following::elder_impulse_3,
            0.0,
        ),
        ("gri_index", signals::trend_following::gri_index, 0.0),
        (
            "slope_indicator",
            signals::trend_following::slope_indicator,
            0.0,
        ),
        ("heikin_ashi", signals::trend_following::heikin_ashi, 0.0),
        (
            "inside_candle",
            signals::trend_following::inside_candle,
            0.0,
        ),
        (
            "aroon_oscillator",
            signals::trend_following::aroon_oscillator,
            0.0,
        ),
        ("awesome", signals::trend_following::awesome_indicator, 0.0),
        ("macd_change", signals::trend_following::macd_change, 0.0),
        (
            "squeeze_momentum",
            signals::trend_following::squeeze_momentum,
            0.0,
        ),
        (
            "supertrend",
            signals::trend_following::supertrend_indicator,
            0.0,
        ),
        (
            "trend_intensity_ind",
            signals::trend_following::trend_intensity_ind,
            0.0,
        ),
        (
            "vertical_horizontal_cross",
            signals::trend_following::vertical_horizontal_cross,
            0.0,
        ),
        (
            "ichimoku_cloud",
            signals::trend_following::ichimoku_cloud,
            0.0,
        ),
        ("tf1_ma", signals::trend_following::tf1_ma, 0.0),
        ("tf2_ma", signals::trend_following::tf2_ma, 0.0),
        ("tf3_rsi_ma", signals::trend_following::tf3_rsi_ma, 0.0),
        ("tf4_macd", signals::trend_following::tf4_macd, 0.0),
        ("tf5_ma_slope", signals::trend_following::tf5_ma_slope, 0.0),
        (
            "tf6_supertrend_flip",
            signals::trend_following::tf6_supertrend_flip,
            0.0,
        ),
        ("tf7_psar_ma", signals::trend_following::tf7_psar_ma, 0.0),
        ("tf9_tii", signals::trend_following::tf9_tii, 0.0),
        ("tf10_ma", signals::trend_following::tf10_ma, 0.0),
        (
            "tf11_rsi_neutrality",
            signals::trend_following::tf11_rsi_neutrality,
            0.0,
        ),
        ("tf12_vama", signals::trend_following::tf12_vama, 0.0),
        (
            "tf13_rsi_supertrend",
            signals::trend_following::tf13_rsi_supertrend,
            0.0,
        ),
        (
            "tf14_catapult",
            signals::trend_following::tf14_catapult,
            0.0,
        ),
        (
            "contrarian_aug_bbands",
            signals::bots::contrarian_aug_bbands,
            0.0,
        ),
        ("contrarian_bbands", signals::bots::contrarian_bbands, 0.0),
        (
            "contrarian_dual_bbands",
            signals::bots::contrarian_dual_bbands,
            0.0,
        ),
        (
            "contrarian_countdown_cross",
            signals::bots::contrarian_countdown_cross,
            0.0,
        ),
        (
            "contrarian_countdown_duration",
            signals::bots::contrarian_countdown_duration,
            0.0,
        ),
        ("key_reversal", signals::bots::key_reversal, 0.0),
        ("k_extreme_duration", signals::bots::k_extreme_duration, 0.0),
        (
            "contrarian_countdown_extremes",
            signals::bots::contrarian_countdown_extremes,
            0.0,
        ),
        (
            "contrarian_demarker_cross",
            signals::bots::contrarian_demarker_cross,
            0.0,
        ),
        (
            "contrarian_demarker_extremes",
            signals::bots::contrarian_demarker_extremes,
            0.0,
        ),
        (
            "contrarian_disparity_extremes",
            signals::bots::contrarian_disparity_extremes,
            0.0,
        ),
        (
            "contrarian_fisher_duration",
            signals::bots::contrarian_fisher_duration,
            0.0,
        ),
        (
            "contrarian_fisher_extremes",
            signals::bots::contrarian_fisher_extremes,
            0.0,
        ),
        (
            "contrarian_real_range_extremes",
            signals::bots::contrarian_real_range_extremes,
            0.0,
        ),
        ("pattern_piercing", signals::bots::pattern_piercing, 0.0),
        (
            "pattern_td_camouflauge",
            signals::bots::pattern_td_camouflage,
            0.0,
        ),
        ("pattern_td_clopwin", signals::bots::pattern_td_clopwin, 0.0),
        ("gri_index", signals::trend_following::gri_index, 1.5),
        ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2, 0.0),
        ("contrarian_bbands", signals::bots::contrarian_bbands, 0.0),
        (
            "contrarian_dual_bbands",
            signals::bots::contrarian_dual_bbands,
            0.0,
        ),
        (
            "contrarian_countdown_cross",
            signals::bots::contrarian_countdown_cross,
            0.0,
        ),
        (
            "contrarian_countdown_duration",
            signals::bots::contrarian_countdown_duration,
            0.0,
        ),
        ("key_reversal", signals::bots::key_reversal, 0.0),
        ("k_extreme_duration", signals::bots::k_extreme_duration, 0.0),
        (
            "contrarian_countdown_extremes",
            signals::bots::contrarian_countdown_extremes,
            0.0,
        ),
        (
            "contrarian_demarker_cross",
            signals::bots::contrarian_demarker_cross,
            0.0,
        ),
        (
            "contrarian_demarker_extremes",
            signals::bots::contrarian_demarker_extremes,
            0.0,
        ),
        (
            "contrarian_disparity_extremes",
            signals::bots::contrarian_disparity_extremes,
            0.0,
        ),
        (
            "contrarian_fisher_duration",
            signals::bots::contrarian_fisher_duration,
            0.0,
        ),
        (
            "contrarian_fisher_extremes",
            signals::bots::contrarian_fisher_extremes,
            0.0,
        ),
        (
            "contrarian_real_range_extremes",
            signals::bots::contrarian_real_range_extremes,
            0.0,
        ),
        (
            "contrarian_rsi_cross",
            signals::bots::contrarian_rsi_cross,
            0.0,
        ),
        (
            "contrarian_rsi_divergences",
            signals::bots::contrarian_rsi_divergences,
            0.0,
        ),
        (
            "contrarian_rsi_duration",
            signals::bots::contrarian_rsi_duration,
            0.0,
        ),
        (
            "contrarian_rsi_extremes",
            signals::bots::contrarian_rsi_extremes,
            0.0,
        ),
        (
            "contrarian_stochastic_extremes",
            signals::bots::contrarian_stochastic_cross,
            0.0,
        ),
        (
            "contrarian_stochastic_divergences",
            signals::bots::contrarian_stochastic_divergences,
            0.0,
        ),
        (
            "contrarian_stochastic_duration",
            signals::bots::contrarian_stochastic_duration,
            0.0,
        ),
        (
            "contrarian_stochastic_extremes",
            signals::bots::contrarian_stochastic_extremes,
            0.0,
        ),
        (
            "contrarian_time_up_extremes",
            signals::bots::contrarian_time_up_extremes,
            0.0,
        ),
        ("contrarian_tsabm", signals::bots::contrarian_tsabm, 0.0),
        (
            "pattern_differentials",
            signals::bots::pattern_differentials,
            0.0,
        ),
        ("pattern_engulfing", signals::bots::pattern_engulfing, 0.0),
        (
            "pattern_fibonacci_timing",
            signals::bots::pattern_fibonacci_timing,
            0.0,
        ),
        ("pattern_piercing", signals::bots::pattern_piercing, 0.0),
        (
            "pattern_td_camouflauge",
            signals::bots::pattern_td_camouflage,
            0.0,
        ),
        ("pattern_td_clop", signals::bots::pattern_td_clop, 0.0),
        ("pattern_td_clopwin", signals::bots::pattern_td_clopwin, 0.0),
        ("pattern_td_open", signals::bots::pattern_td_open, 0.0),
        ("pattern_td_trap", signals::bots::pattern_td_trap, 0.0),
        ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2, 0.0),
        ("pattern_td_waldo_5", signals::bots::pattern_td_waldo_5, 0.0),
        ("pattern_td_waldo_6", signals::bots::pattern_td_waldo_6, 0.0),
        ("pattern_td_waldo_8", signals::bots::pattern_td_waldo_8, 0.0),
        (
            "pattern_three_line_strike",
            signals::bots::pattern_three_line_strike,
            0.0,
        ),
        (
            "pattern_three_methods",
            signals::bots::pattern_three_methods,
            0.0,
        ),
    ];

    let selected_signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = match tag {
        "lc" => lc_signal_functions
            .iter()
            .map(|(name, func, param)| (*name, *func, *param))
            .collect(),
        "mc" => mc_signal_functions
            .iter()
            .map(|(name, func, param)| (*name, *func, *param))
            .collect(),
        "sc" => sc_signal_functions
            .iter()
            .map(|(name, func, param)| (*name, *func, *param))
            .collect(),
        "micro" => micro_signal_functions
            .iter()
            .map(|(name, func, param)| (*name, *func, *param))
            .collect(),
        "crypto" => crypto_signal_functions
            .iter()
            .map(|(name, func, param)| (*name, *func, *param))
            .collect(),
        "prod" => prod_signal_functions
            .iter()
            .map(|(name, func, param)| (*name, *func, *param))
            .collect(),
        "param" => param_functions
            .iter()
            .map(|(name, func, param)| (name.as_str(), *func, *param)) // Dereference name
            .collect(),
        "signal" => signal_functions
            .iter()
            .map(|(name, func, param)| (*name, *func, *param)) // Dereference name
            .collect(),
        _ => testing_functions
            .iter()
            .map(|(name, func, param)| (name.as_str(), *func, *param)) // Use as_ref() instead
            .collect(),
    };

    for (name, function, param) in selected_signal_functions {
        signals.push(Signal {
            name: name.into(),
            func: Arc::new(function),
            param: param,
        });
    }

    // Filter signals by strategy if provided
    if let Some(filter) = strategy_filter {
        info!("Filtering strategies to: {}", filter);
        let original_count = signals.len();
        signals.retain(|s| s.name == filter);
        info!("Filtered from {} to {} strategies", original_count, signals.len());

        if signals.is_empty() {
            warn!("No strategy found matching '{}'", filter);
            return Ok(Vec::new());
        }
    }

    // needs to be awaited
    Ok(run_all_backtests(lf, signals).await?)
}

async fn backtest_helper(
    path: String,
    u: &str,
    batch_size: usize,
    production: bool,
    custom_tickers: Option<Vec<String>>, // Add an optional parameter for custom tickers
    demo_mode: bool,
    strategy_filter: Option<&str>,
) -> Result<(), Box<dyn StdError>> {
    let file_path = if demo_mode {
        // In demo mode, use the local CSV file from root directory
        format!("{}.csv", u)
    } else {
        // Normal mode: use downloaded files from data directory
        let folder = if production { "production" } else { "testing" };
        format!("{}/data/{}/{}.csv", path, folder, u)
    };

    let lf = read_price_file(file_path).await?;

    // Show latest date in the price data
    let latest_date_df = lf.clone().select([col("Date").max()]).collect()?;
    let latest_date = latest_date_df.column("Date")?.get(0)?.to_string();
    println!("Price file loaded for {} - latest date: {}", u, latest_date);

    // Collect the unique tickers into a DataFrame
    let unique_tickers_df = lf
        .clone()
        .select([col("Ticker").unique().alias("unique_tickers")])
        .collect()?;

    // Assuming the 'unique_tickers' column is of type Utf8
    let unique_tickers_series = unique_tickers_df.column("unique_tickers")?;

    let output = if u == "Crypto" {
        "output_crypto"
    } else {
        "output"
    };
    let folder = if production { "production" } else { "testing" };
    let dir_path = format!("{}/{}/{}", path, output, folder);

    let mut filenames: Vec<String> = Vec::new();
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        // Check if the entry is a file and has a `.parquet` extension
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                filenames.push(stem.to_owned());
            }
        }
    }

    // Convert filenames to a HashSet
    let filenames_set: HashSet<String> = filenames.into_iter().collect();

    // Determine the tickers to process
    let needed: Vec<String> = if let Some(custom_tickers) = custom_tickers {
        // Use the custom tickers, filtering out those already processed
        custom_tickers.into_iter().collect()
    } else {
        // Default logic for determining needed tickers
        unique_tickers_series
            .str()?
            .into_iter()
            .filter_map(|value| value.map(|v| v.to_string()))
            .filter(|ticker| !filenames_set.contains(ticker))
            // .take(5) // used for testing purposes
            .collect()
    };
    // println!("Needed tickers: {:?}", needed);

    let out_of = needed.len();
    let mut completed = 0; // Track completed backtests

    for i in (0..needed.len()).step_by(batch_size) {
        let last = if i + batch_size > needed.len() {
            needed.len() - i
        } else {
            batch_size
        };
        let unique_tickers = &needed[i..i + last];

        // collect futures for processing each ticker
        let futures: Vec<_> = unique_tickers
            .into_iter()
            .map(|ticker| {
                let lf_clone = lf.clone();
                let ticker_clone: String = ticker.clone();
                let path_clone: String = path.clone();
                let u_clone = u.to_string();

                async move {
                    let filtered_lf = lf_clone.filter(col("Ticker").eq(lit(ticker_clone.clone())));

                    // Debug: check if filtering returned any rows
                    if let Ok(filtered_df) = filtered_lf.clone().collect() {
                        debug!("Ticker '{}' filtered dataframe has {} rows", ticker_clone, filtered_df.height());
                        if filtered_df.height() == 0 {
                            warn!("No data found for ticker '{}' after filtering - skipping", ticker_clone);
                            return (ticker_clone, Ok(Vec::new()));
                        }
                    }

                    let tag: &str = match (production, u_clone.as_str()) {
                        ///////////////////////////////////
                        // Update testing functions here //
                        ///////////////////////////////////
                        // "signal" = ALL (signal_functions)
                        // "param" = param_functions
                        // "testing" = testing_functions
                        (false, _) => "signal", // signal // param // testing
                        (true, "Crypto") => "crypto",
                        (true, "Micro") => "micro",
                        (true, "SC") => "sc",
                        (true, "MC") => "mc",
                        (true, "LC") => "lc",
                        (_, _) => "prod",
                    };
                    // ./target/release/backtester -u LC2 -m testing -t IBM
                    // cargo run -- -u Crypto -m testing -t btc

                    match select_backtests(filtered_lf, tag, strategy_filter).await {
                        Ok(backtest_results) => {
                            if let Err(e) = save_backtest(
                                path_clone,
                                backtest_results.clone(),
                                &u_clone,
                                ticker_clone.clone(),
                                production,
                            )
                            .await
                            {
                                eprintln!("Error saving backtests (check output and decisions folders): {}", e);
                            }
                            (ticker_clone, Ok(backtest_results))
                        }
                        Err(e) => {
                            eprintln!("Error running '{}' backtests: {}", ticker_clone, e);
                            (ticker_clone, Err(e))
                        }
                    }
                }
            })
            .collect();

        // Process results sequentially as they complete
        let results = futures::future::join_all(futures).await;

        for (ticker, result) in results {
            match result {
                Ok(backtest_results) => {
                    if !backtest_results.is_empty() {
                        completed += 1;
                        println!(
                            "[{}] Finished {} '{}' backtests: {} of {}",
                            Utc::now().format("%H:%M:%S"),
                            u, ticker, completed, out_of
                        );
                    } else {
                        info!("Skipped '{}' - no data available", ticker);
                    }
                }
                Err(e) => {
                    error!("Failed to process '{}': {}", ticker, e);
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse();

    // Setup logging based on verbosity
    let log_level = match args.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
        .init();

    // Get path from args or environment
    let user_path = match env::var("CLICKHOUSE_USER_PATH") {
        Ok(path) => path,
        Err(_) => String::from("/srv"),
    };
    let default_path: String = format!("{}/rust_home/backtester", user_path);
    let path = args.path.as_ref().unwrap_or(&default_path);

    let univ_str = &args.universe;
    let mode_str = &args.mode;
    let custom_str = args.tickers.as_ref().map(|s| s.as_str()).unwrap_or("");
    let batch_size: usize = 10;

    info!("Starting backtester with universe: {}, mode: {}", univ_str, mode_str);
    if let Some(ref t) = args.tickers {
        info!("Filtering by tickers: {}", t);
    }
    if let Some(ref s) = args.strategy {
        info!("Filtering by strategy: {}", s);
    }

    let demo_mode = mode_str == "demo";
    let production = mode_str == "production" && !demo_mode;
    info!("Demo mode: {}, Production mode: {}", demo_mode, production);

    let univ: &[&str] = match univ_str.as_str() {
        "SC" => &["SC1", "SC2", "SC3", "SC4"],
        "MC" => &["MC1", "MC2"],
        "MC1" => &["MC1"],
        "MC2" => &["MC2"],
        "LC" => &["LC1", "LC2"],
        "LC1" => &["LC1"],
        "LC2" => &["LC2"],
        "Micro" => &["Micro1", "Micro2", "Micro3", "Micro4"],
        "Stocks" => &[
            "SC1", "SC2", "SC3", "SC4", "MC1", "MC2", "LC1", "LC2", "Micro1", "Micro2", "Micro3",
            "Micro4",
        ],
        _ => &["Crypto"],
    };
    let univ_vec: Vec<String> = univ.iter().map(|&s| s.into()).collect();

    // println!("Running backtests with params:");
    // println!("  Universe: {}", univ_str);
    // println!("  Production: {}", production_str);

    // delete prior production files before next run
    // also for some testing files
    if production {
        let paths = vec![
            format!("{}/output/production", path),
            format!("{}/output_crypto/production", path),
            format!("{}/data/production", path),
        ];
        for p in paths {
            info!("Deleting files in: {}", p);
            delete_all_files_in_folder(p).await?;
        }
    } else if !demo_mode {
        // Only delete testing files if not in demo mode
        // Delete only the output folder for the universe being run
        let output_path = if univ_str == "Crypto" {
            format!("{}/output_crypto/testing", path)
        } else {
            format!("{}/output/testing", path)
        };
        info!("Deleting files in: {}", output_path);
        delete_all_files_in_folder(output_path).await?;

        // Delete decision files based on universe
        if univ_str == "Crypto" {
            let p = format!("{}/decisions/crypto", path);
            info!("Deleting files in: {}", p);
            delete_all_files_in_folder(p).await?;
        } else {
            let p = format!("{}/decisions/stocks", path);
            info!("Deleting files in: {}", p);
            delete_all_files_in_folder(p).await?;
        }
    }

    // create price files if they don't already exist (from clickhouse tables)
    // Skip downloading in demo mode - use local files instead
    if !demo_mode {
        create_price_files(univ_vec.clone(), production.clone()).await?;
    }

    for u in univ {
        info!(
            "Backtest starting: {} (mode: {})",
            u,
            if demo_mode {
                "demo"
            } else if production {
                "production"
            } else {
                "testing"
            }
        );
        let custom_tickers = match custom_str {
            "" => None,
            _ => {
                let tickers: Vec<String> = custom_str
                    .split(',')
                    .map(|s| {
                        let trimmed = s.trim();
                        // Crypto tickers are lowercase, stocks are uppercase
                        if univ_str == "Crypto" {
                            trimmed.to_lowercase()
                        } else {
                            trimmed.to_uppercase()
                        }
                    })
                    .collect();
                debug!("Custom tickers (normalized for {}): {:?}", univ_str, tickers);
                Some(tickers)
            },
        };

        let _ = backtest_helper(
            path.to_string(),
            u,
            batch_size,
            production,
            custom_tickers,
            demo_mode,
            args.strategy.as_deref(),
        )
        .await;
    }

    info!("Backtest processing complete");

    if production {
        if univ_vec.contains(&"Crypto".to_string()) {
            let (datetag, _out) =
                summary_performance_file(path.to_string(), production, false, univ_vec.clone())
                    .await?;

            if let Err(e) = score(&datetag, "Crypto").await {
                eprintln!("Error inserting scores: {}", e);
            }
        }

        if univ_vec.contains(&"LC1".to_string())
            || univ_vec.contains(&"LC2".to_string())
            || univ_vec.contains(&"MC1".to_string())
            || univ_vec.contains(&"MC2".to_string())
            || univ_vec.contains(&"SC1".to_string())
            || univ_vec.contains(&"SC2".to_string())
            || univ_vec.contains(&"SC3".to_string())
            || univ_vec.contains(&"SC4".to_string())
            || univ_vec.contains(&"Micro1".to_string())
            || univ_vec.contains(&"Micro2".to_string())
            || univ_vec.contains(&"Micro3".to_string())
            || univ_vec.contains(&"Micro4".to_string())
        {
            let (datetag, _out) =
                summary_performance_file(path.clone(), production.clone(), true, univ_vec.clone())
                    .await?;

            if let Err(e) = score(&datetag, univ_str).await {
                eprintln!("Error inserting scores: {}", e);
            }
        }
    } else {
        //if !demo_mode
        // Only run summary for testing mode, skip for demo mode
        let stocks = if univ_vec.contains(&"Crypto".to_string()) {
            false
        } else {
            true
        };
        let path: String = "/Users/rogerbos/rust_home/backtester".to_string();

        match summary_performance_file(path.clone(), production, stocks, univ_vec).await {
            Ok((datetag, out)) => {
                // save out to a csv file
                let tag = if stocks { "output" } else { "output_crypto" };
                let output_path =
                    format!("{}/{}/testing/summary_performance.csv", path.clone(), tag);
                let mut file = File::create(output_path)?;
                let _ = CsvWriter::new(&mut file).finish(&mut out.clone());

                // Round numeric columns to 2 decimal places for display
                let out_rounded = out.clone().lazy()
                    .select([
                        col("strategy"),
                        col("universe"),
                        cols(["hit_ratio", "risk_reward", "avg_gain", "avg_loss", "max_gain", "max_loss",
                              "buys", "sells", "trades", "sharpe_ratio", "sortino_ratio", "max_drawdown",
                              "calmar_ratio", "win_loss_ratio", "recovery_factor", "profit_per_trade",
                              "expectancy", "profit_factor"]).round(1),
                        col("N")
                    ])
                    .collect()
                    .unwrap();

                // Split the DataFrame display to avoid truncation
                let col_names: Vec<String> = out_rounded.get_column_names().iter().map(|s| s.to_string()).collect();

                std::env::set_var("POLARS_FMT_MAX_COLS", "12");

                println!("{} Average Performance by Strategy:", datetag);
                let first_cols: Vec<&str> = col_names[0..12].iter().map(|s| s.as_str()).collect();
                let first_part = out_rounded.select(first_cols).unwrap();
                println!("{}", first_part);

                println!("\n{} Average Performance by Strategy:", datetag);
                let mut second_cols: Vec<&str> = col_names[0..2].iter().map(|s| s.as_str()).collect();
                second_cols.extend(col_names[12..].iter().map(|s| s.as_str()));
                let second_part = out_rounded.select(second_cols).unwrap();
                println!("{}", second_part);
            }
            Err(e) => eprintln!("Error in summary performance file: {}", e),
        }
    }

    Ok(())
}

pub async fn single_backtest(signal: Signal) -> Result<(), Box<dyn StdError>> {
    // Step 1: Load your data into a LazyFrame
    let file_path = "/Users/rogerbos/rust_home/backtester/data/testing/crypto.csv";
    let lf = read_price_file(file_path.to_string()).await?;
    let ticker = "btc";
    let filtered_lf = lf.filter(col("Ticker").eq(lit(ticker)));

    // Step 2: Run the backtest using the signal
    let signal_name = signal.name.clone();
    let backtest_result = sig(
        filtered_lf,
        *signal.func.clone(),
        signal.param,
        signal_name.clone(),
    )
    .await?;

    // Step 3: Print the backtest result
    println!("Backtest result for signal '{}':", signal_name);
    let _ = showbt(backtest_result.0);
    Ok(())
}

pub async fn single_backtest_sized(signal: Signal) -> Result<(), Box<dyn StdError>> {
    // Step 1: Load your data into a LazyFrame
    let file_path = "/Users/rogerbos/rust_home/backtester/data/testing/crypto.csv";
    let lf = read_price_file(file_path.to_string()).await?;
    let ticker = "btc";
    let filtered_lf = lf.filter(col("Ticker").eq(lit(ticker)));
    let entry_amount = 1000.0;
    let exit_amount = 1000.0;

    // Step 2: Run the backtest using the signal
    let signal_name = signal.name.clone();
    let backtest_result = sig_sized(
        filtered_lf,
        *signal.func.clone(),
        signal.param,
        signal_name.clone(),
        entry_amount,
        exit_amount,
    )
    .await?;

    // Step 3: Print the backtest result
    println!("Backtest (Sized) result for signal '{}':", signal_name);
    let _ = showbt(backtest_result.0);
    Ok(())
}

// cargo run crypto testing btc,eth,sol
// cargo test testme -- --nocapture
pub async fn testme() -> Result<(), Box<dyn StdError>> {
    let signal = Signal {
        name: "candlestick_double_trouble".to_string(),
        func: Arc::new(signals::mfpr::candlestick_double_trouble),
        param: 2.0,
    };
    let _ = single_backtest(signal).await?;
    // let _ = single_backtest_sized(signal).await?;
    Ok(())
}
