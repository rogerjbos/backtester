#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use backtester::*;
use polars::prelude::*;
use std::{collections::HashSet, env, error::Error as StdError, fs, fs::File, process};
use tokio;

mod signals {
    pub mod bots; // book of trading strategies
    pub mod mfpr; // mastering financial pattern recognition
    pub mod technical;
    pub mod trend_following;
}

pub async fn select_backtests(
    lf: LazyFrame,
    tag: &str,
) -> Result<Vec<Backtest>, Box<dyn StdError>> {
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

    let param_functions: Vec<(String, SignalFunctionWithParam, f64)> = (2..=5)
        .map(|i| {
            let param = i as f64;
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
        .collect();

    let testing_functions: Vec<(String, SignalFunctionWithParam, f64)> = (1..=2)
        .map(|i| {
            let param = 1.0 + (i as f64 * 0.2) - 0.2; // Generate param values from 1.0 to 3.0
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
        "testing" => signal_functions
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

    // needs to be awaited
    Ok(run_all_backtests(lf, signals).await?)
}

async fn backtest_helper(
    path: String,
    u: &str,
    batch_size: usize,
    production: bool,
    custom_tickers: Option<Vec<String>>, // Add an optional parameter for custom tickers
) -> Result<(), Box<dyn StdError>> {
    let folder = if production { "production" } else { "testing" };
    let file_path = format!("{}/data/{}/{}.csv", path, folder, u);

    let lf = read_price_file(file_path).await?;

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
                    let tag: &str = match (production, u_clone.as_str()) {
                        (false, _) => "param", // testing
                        (true, "Crypto") => "crypto",
                        (true, "Micro") => "micro",
                        (true, "SC") => "sc",
                        (true, "MC") => "mc",
                        (true, "LC") => "lc",
                        (_, _) => "prod",
                    };

                    match select_backtests(filtered_lf, tag).await {
                        Ok(backtest_results) => {
                            if let Err(e) = save_backtest(
                                path_clone,
                                backtest_results,
                                &u_clone,
                                ticker_clone.clone(),
                                production,
                            )
                            .await
                            {
                                eprintln!("Error saving backtest to parquet: {}", e);
                            }
                            Ok(ticker_clone)
                        }
                        Err(e) => {
                            eprintln!("Error running '{}' backtests: {}", ticker_clone, e);
                            Err(e)
                        }
                    }
                }
            })
            .collect();

        // Process results sequentially as they complete
        let results = futures::future::join_all(futures).await;

        for result in results {
            if let Ok(ticker) = result {
                completed += 1;
                println!(
                    "Finished {} '{}' backtests: {} of {}",
                    u, ticker, completed, out_of
                );
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // default params (overwritten by command line args)
    let user_path = match env::var("CLICKHOUSE_USER_PATH") {
        Ok(path) => path,
        Err(_) => String::from("/srv"),
    };
    let default_path: String = format!("{}/rust_home/backtester", user_path);

    let default_production: String = "production".to_string();
    let default_univ = "Crypto".to_string();
    let batch_size: usize = 10;
    let default_custom_str = "".to_string();

    // collect command line args
    let args: Vec<String> = env::args().collect();
    let univ_str: &str = args.get(1).unwrap_or(&default_univ);
    let production_str = args.get(2).unwrap_or(&default_production);
    let custom_str = args.get(3).unwrap_or(&default_custom_str); // Use the default value if arg 3 is missing
    let path = args.get(4).unwrap_or(&default_path);
    // println!("Custom_str: {}", custom_str);

    let production = production_str == "production";
    // println!("Production mode: {}\n", production);

    let univ: &[&str] = match univ_str {
        "SC" => &["SC1", "SC2", "SC3", "SC4"],
        "MC" => &["MC1", "MC2"],
        "MC1" => &["MC1"],
        "LC" => &["LC1", "LC2"],
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
            println!("Deleting files in: {}", p);
            delete_all_files_in_folder(p).await?;
        }
    } else {
        // delete testing files
        let paths = vec![
            format!("{}/output_crypto/testing", path),
            format!("{}/output/testing", path),
            // format!("{}/data/testing", path), DO NOT DELETE TESTING DATA
        ];
        for p in paths {
            println!("Deleting files in: {}", p);
            delete_all_files_in_folder(p).await?;
        }
    }

    // create price files if they don't already exist (from clickhouse tables)
    create_price_files(univ_vec.clone(), production.clone()).await?;

    for u in univ {
        println!("Backtest starting: {}", u);
        let custom_tickers = match custom_str.as_str() {
            "" => None, // If the custom_str is empty, set custom_tickers to None
            _ => Some(
                custom_str
                    .split(',') // Split the string by commas
                    .map(|s| s.trim().to_string()) // Trim whitespace and convert to String
                    .collect::<Vec<String>>(), // Collect into a Vec<String>
            ),
        };

        let _ = backtest_helper(path.to_string(), u, batch_size, production, custom_tickers).await;
    }
    // println!("Backtest finished");

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
                println!("{} Average Performance by Strategy:\n {:4.2}", datetag, out);
                // print_dataframe_vertically(&out);
            }
            Err(e) => eprintln!("Error in summary performance file: {}", e),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_perf() {
        // Params
        let path: String = "/Users/rogerbos/rust_home/backtester".to_string();
        let production: bool = false;
        let stocks: bool = false;
        let univ_vec: Vec<String> = vec!["Crypto".to_string()];

        match super::summary_performance_file(path, production, stocks, univ_vec).await {
            Ok(result) => println!("Result: {:?}", result),
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    #[tokio::test]
    async fn test_score() {
        let datetag = "20240409";
        let univ_str = "SC";
        if let Err(e) = super::score(datetag, univ_str).await {
            eprintln!("Error: {}", e);
        }
    }

    #[tokio::test]
    async fn test_testme() {
        if let Err(e) = super::testme().await {
            eprintln!("Error: {}", e);
        }
    }
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
    let _ = showbt(backtest_result);
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
    let _ = showbt(backtest_result);
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
