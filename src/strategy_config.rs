// Strategy configuration module
// Contains all strategy definitions organized by universe type

use crate::signals;
use backtester::SignalFunctionWithParam;

/// Get the appropriate strategy list for a given universe tag
pub fn get_strategies_for_tag(tag: &str) -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    match tag {
        "lc" => large_cap_strategies(),
        "mc" => mid_cap_strategies(),
        "sc" => small_cap_strategies(),
        "micro" => micro_cap_strategies(),
        "crypto" => crypto_strategies(),
        "prod" => production_strategies(),
        "param" => parameterized_strategies(),
        "signal" => all_strategies(),
        _ => testing_strategies(),
    }
}

/// Production strategies - optimized for production runs
pub fn production_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    vec![
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
    ]
}

/// Cryptocurrency strategies - optimized for crypto markets
pub fn crypto_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    vec![
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
    ]
}

/// Micro cap strategies - optimized for micro cap stocks
pub fn micro_cap_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    vec![
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
    ]
}

/// Small cap strategies - optimized for small cap stocks
pub fn small_cap_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    vec![
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
    ]
}

/// Mid cap strategies - optimized for mid cap stocks
pub fn mid_cap_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    vec![
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
    ]
}

/// Large cap strategies - optimized for large cap stocks
pub fn large_cap_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    vec![
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
    ]
}

/// Parameterized strategies - strategies with parameter sweeps
pub fn parameterized_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    let mut strategies = Vec::new();
    
    // Donchian indicator with various parameters
    for i in 1..=5 {
        let param = (i * 10) as f64;
        strategies.push((
            "donchian_indicator",
            signals::trend_following::donchian_indicator as SignalFunctionWithParam,
            param,
        ));
    }
    
    strategies.push((
        "donchian_indicator_inverse",
        signals::trend_following::donchian_indicator_inverse as SignalFunctionWithParam,
        0.0,
    ));
    
    // Donchian high
    for i in 1..=5 {
        let param = (i * 10) as f64;
        strategies.push((
            "donchian_indicator_high",
            signals::trend_following::donchian_indicator_high as SignalFunctionWithParam,
            param,
        ));
    }
    
    // Donchian low
    for i in 1..=5 {
        let param = (i * 10) as f64;
        strategies.push((
            "donchian_indicator_low",
            signals::trend_following::donchian_indicator_low as SignalFunctionWithParam,
            param,
        ));
    }
    
    // Trend following 2trouble RSI ATR parameter
    for i in 1..=5 {
        let param = i as f64;
        strategies.push((
            "trend_fol_2trouble_rsi_atrparam",
            signals::mfpr::trend_fol_2trouble_rsi_atrparam as SignalFunctionWithParam,
            param,
        ));
    }
    
    // Trend following 2trouble RSI parameter
    for i in 2..=6 {
        let param = (i * 10) as f64;
        strategies.push((
            "trend_fol_2trouble_rsi_rsiparam",
            signals::mfpr::trend_fol_2trouble_rsi_rsiparam as SignalFunctionWithParam,
            param,
        ));
    }
    
    strategies
}

/// Testing strategies - minimal set for quick testing
pub fn testing_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    let mut strategies = Vec::new();
    
    // Candlestick double trouble with two parameters
    for i in 1..=2 {
        let param = 1.0 + (i as f64 * 0.5) - 0.5;
        strategies.push((
            "candlestick_double_trouble",
            signals::mfpr::candlestick_double_trouble as SignalFunctionWithParam,
            param,
        ));
    }
    
    strategies.push((
        "three_candles",
        signals::mfpr::three_candles as SignalFunctionWithParam,
        0.0,
    ));
    
    strategies
}

/// All strategies - comprehensive list for testing/evaluation
pub fn all_strategies() -> Vec<(&'static str, SignalFunctionWithParam, f64)> {
    vec![
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
        ("bottle", signals::mfpr::bottle, 0.0),
        ("double_trouble", signals::mfpr::double_trouble_1, 0.0),
        ("h", signals::mfpr::h, 0.0),
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
        ("blockade", signals::mfpr::blockade, 1.5),
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
        ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2, 0.0),
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
            "contrarian_stochastic_cross",
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
        ("pattern_td_clop", signals::bots::pattern_td_clop, 0.0),
        ("pattern_td_open", signals::bots::pattern_td_open, 0.0),
        ("pattern_td_trap", signals::bots::pattern_td_trap, 0.0),
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
    ]
}
