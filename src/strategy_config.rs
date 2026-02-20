// Strategy configuration module
// Contains all strategy definitions organized by universe type

use crate::signals;
use backtester::SignalFunctionWithParam;

/// Get the appropriate strategy list for a given universe tag
pub fn get_strategies_for_tag(tag: &str) -> Vec<(String, SignalFunctionWithParam, f64)> {
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
pub fn production_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    vec![
        ("three_candles".to_string(), signals::mfpr::three_candles, 0.0),
        (
            "trend_fol_3candle_ma".to_string(),
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("pattern_marubozu".to_string(), signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer".to_string(), signals::bots::pattern_hammer, 0.0),
        ("hammer".to_string(), signals::mfpr::hammer, 0.0),
        ("tweezers".to_string(), signals::mfpr::tweezers, 0.0),
        ("hikkake".to_string(), signals::mfpr::hikkake, 0.0),
        (
            "adx_indicator".to_string(),
            signals::trend_following::adx_indicator,
            0.0,
        ),
        (
            "donchian_indicator".to_string(),
            signals::trend_following::donchian_indicator,
            0.0,
        ),
        ("tower".to_string(), signals::mfpr::tower, 0.0),
        ("slingshot".to_string(), signals::mfpr::slingshot, 0.0),
        ("quintuplets_0005".to_string(), signals::mfpr::quintuplets_0005, 0.0),
    ]
}

/// Cryptocurrency strategies - optimized for crypto markets
pub fn crypto_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    vec![
        (
            "squeeze_momentum".to_string(),
            signals::trend_following::squeeze_momentum,
            0.0,
        ),
        (
            "vertical_horizontal_cross".to_string(),
            signals::trend_following::vertical_horizontal_cross,
            0.0,
        ),
        ("hammer".to_string(), signals::mfpr::hammer, 0.0),
        ("double_trouble".to_string(), signals::mfpr::double_trouble_1, 0.0),
        (
            "donchian_indicator".to_string(),
            signals::trend_following::donchian_indicator,
            0.0,
        ),
        ("key_reversal".to_string(), signals::bots::key_reversal, 0.0),
        ("pattern_marubozu".to_string(), signals::bots::pattern_marubozu, 0.0),
        ("h".to_string(), signals::mfpr::h, 0.0),
        ("spinning_top".to_string(), signals::mfpr::spinning_top, 0.0),
        (
            "candlestick_double_trouble".to_string(),
            signals::mfpr::candlestick_double_trouble,
            2.0,
        ),
        (
            "trend_fol_2trouble_rsi".to_string(),
            signals::mfpr::trend_fol_2trouble_rsi,
            0.0,
        ),
        ("heikin_ashi".to_string(), signals::trend_following::heikin_ashi, 0.0),
        (
            "heikin_ashi_double_trouble".to_string(),
            signals::mfpr::heikin_ashi_double_trouble,
            0.0,
        ),
        ("tf12_vama".to_string(), signals::trend_following::tf12_vama, 0.0),
        ("tf9_tii".to_string(), signals::trend_following::tf9_tii, 0.0),
        (
            "aroon_oscillator".to_string(),
            signals::trend_following::aroon_oscillator,
            0.0,
        ),
        (
            "contrarian_rsi_extremes".to_string(),
            signals::bots::contrarian_rsi_extremes,
            0.0,
        ),
        ("bottle".to_string(), signals::mfpr::bottle, 0.0),
        ("macd_change".to_string(), signals::trend_following::macd_change, 0.0),
        ("star".to_string(), signals::mfpr::star, 0.0),
        ("mirror".to_string(), signals::mfpr::mirror, 0.0),
        ("marubozu".to_string(), signals::mfpr::marubozu, 0.0),
        (
            "contrarian_disparity_extremes".to_string(),
            signals::bots::contrarian_disparity_extremes,
            0.0,
        ),
        ("pattern_piercing".to_string(), signals::bots::pattern_piercing, 0.0),
        (
            "pattern_td_camouflauge".to_string(),
            signals::bots::pattern_td_camouflage,
            0.0,
        ),
        ("pattern_td_clopwin".to_string(), signals::bots::pattern_td_clopwin, 0.0),
        ("gri_index".to_string(), signals::trend_following::gri_index, 0.0),
        ("pattern_td_waldo_2".to_string(), signals::bots::pattern_td_waldo_2, 0.0),
        ("pattern_td_waldo_5".to_string(), signals::bots::pattern_td_waldo_5, 0.0),
        ("pattern_td_waldo_6".to_string(), signals::bots::pattern_td_waldo_6, 0.0),
        ("pattern_td_waldo_8".to_string(), signals::bots::pattern_td_waldo_8, 0.0),
        (
            "trend_fol_h_trend_intensity".to_string(),
            signals::mfpr::trend_fol_h_trend_intensity,
            0.0,
        ),
        (
            "heikin_ashi_euphoria".to_string(),
            signals::mfpr::heikin_ashi_euphoria,
            0.0,
        ),
        ("euphoria".to_string(), signals::mfpr::euphoria, 0.0),
        ("engulfing".to_string(), signals::mfpr::engulfing, 0.0),
        ("doji".to_string(), signals::mfpr::doji, 0.0),
        ("stick_sandwich".to_string(), signals::mfpr::stick_sandwich, 0.0),
        (
            "contrarian_stochastic_extremes".to_string(),
            signals::bots::contrarian_stochastic_extremes,
            0.0,
        ),
        (
            "contrarian_stochastic_divergences".to_string(),
            signals::bots::contrarian_stochastic_divergences,
            0.0,
        ),
        (
            "contrarian_stochastic_duration".to_string(),
            signals::bots::contrarian_stochastic_duration,
            0.0,
        ),
        (
            "contrarian_stochastic_cross".to_string(),
            signals::bots::contrarian_stochastic_cross,
            0.0,
        ),
        (
            "contrarian_rsi_divergences".to_string(),
            signals::bots::contrarian_rsi_divergences,
            0.0,
        ),
        (
            "trend_fol_3candle_ma".to_string(),
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("tf1_ma".to_string(), signals::trend_following::tf1_ma, 0.0),
        ("tf2_ma".to_string(), signals::trend_following::tf2_ma, 0.0),
        ("tf3_rsi_ma".to_string(), signals::trend_following::tf3_rsi_ma, 0.0),
        ("tf4_macd".to_string(), signals::trend_following::tf4_macd, 0.0),
        ("tf5_ma_slope".to_string(), signals::trend_following::tf5_ma_slope, 0.0),
        (
            "tf6_supertrend_flip".to_string(),
            signals::trend_following::tf6_supertrend_flip,
            0.0,
        ),
        ("tf7_psar_ma".to_string(), signals::trend_following::tf7_psar_ma, 0.0),
        (
            "trend_fol_marubozu_k_vol_bands".to_string(),
            signals::mfpr::trend_fol_marubozu_k_vol_bands,
            0.0,
        ),
    ]
}

/// Micro cap strategies - optimized for micro cap stocks
pub fn micro_cap_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    vec![
        ("hammer".to_string(), signals::mfpr::hammer, 0.0),
        (
            "trend_fol_3candle_ma".to_string(),
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("three_candles".to_string(), signals::mfpr::three_candles, 0.0),
        ("hikkake".to_string(), signals::mfpr::hikkake, 0.0),
        (
            "contrarian_piercing_stoch".to_string(),
            signals::mfpr::contrarian_piercing_stoch,
            0.0,
        ),
        ("piercing".to_string(), signals::mfpr::piercing, 0.0),
        ("tasuki".to_string(), signals::mfpr::tasuki, 0.0),
        (
            "heikin_ashi_double_trouble".to_string(),
            signals::mfpr::heikin_ashi_double_trouble,
            0.0,
        ),
        ("heikin_ashi_tasuki".to_string(), signals::mfpr::heikin_ashi_tasuki, 0.0),
        ("pattern_hammer".to_string(), signals::bots::pattern_hammer, 0.0),
        (
            "contrarian_engulfing_bbands".to_string(),
            signals::mfpr::contrarian_engulfing_bbands,
            0.0,
        ),
        ("h".to_string(), signals::mfpr::h, 0.0),
        ("double_trouble".to_string(), signals::mfpr::double_trouble_1, 0.0),
        (
            "candlestick_double_trouble".to_string(),
            signals::mfpr::candlestick_double_trouble,
            0.0,
        ),
        ("candlestick_tasuki".to_string(), signals::mfpr::candlestick_tasuki, 0.0),
        ("blockade".to_string(), signals::mfpr::blockade, 0.0),
        (
            "trend_fol_h_trend_intensity".to_string(),
            signals::mfpr::trend_fol_h_trend_intensity,
            0.0,
        ),
    ]
}

/// Small cap strategies - optimized for small cap stocks
pub fn small_cap_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    vec![
        ("tweezers".to_string(), signals::mfpr::tweezers, 0.0),
        (
            "trend_fol_3candle_ma".to_string(),
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("three_candles".to_string(), signals::mfpr::three_candles, 0.0),
        ("pattern_marubozu".to_string(), signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer".to_string(), signals::bots::pattern_hammer, 0.0),
        (
            "adx_indicator".to_string(),
            signals::trend_following::adx_indicator,
            0.0,
        ),
        ("hammer".to_string(), signals::mfpr::hammer, 0.0),
        ("tower".to_string(), signals::mfpr::tower, 0.0),
        ("hikkake".to_string(), signals::mfpr::hikkake, 0.0),
    ]
}

/// Mid cap strategies - optimized for mid cap stocks
pub fn mid_cap_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    vec![
        (
            "trend_fol_3candle_ma".to_string(),
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("three_candles".to_string(), signals::mfpr::three_candles, 0.0),
        ("pattern_marubozu".to_string(), signals::bots::pattern_marubozu, 0.0),
        ("hikkake".to_string(), signals::mfpr::hikkake, 0.0),
        (
            "adx_indicator".to_string(),
            signals::trend_following::adx_indicator,
            0.0,
        ),
        ("tower".to_string(), signals::mfpr::tower, 0.0),
        ("pattern_hammer".to_string(), signals::bots::pattern_hammer, 0.0),
        ("hammer".to_string(), signals::mfpr::hammer, 0.0),
    ]
}

/// Large cap strategies - optimized for large cap stocks
pub fn large_cap_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    vec![
        ("tweezers".to_string(), signals::mfpr::tweezers, 0.0),
        ("pattern_marubozu".to_string(), signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer".to_string(), signals::bots::pattern_hammer, 0.0),
        ("slingshot".to_string(), signals::mfpr::slingshot, 0.0),
        (
            "trend_fol_3candle_ma".to_string(),
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("tower".to_string(), signals::mfpr::tower, 0.0),
        ("three_candles".to_string(), signals::mfpr::three_candles, 0.0),
        (
            "adx_indicator".to_string(),
            signals::trend_following::adx_indicator,
            0.0,
        ),
    ]
}

/// Parameterized strategies - strategies with parameter sweeps
pub fn parameterized_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    let mut strategies = Vec::new();

    // rsi_level with various parameters
    for i in 1..=5 {
        let param = 10. + (i * 5) as f64;
        strategies.push((
            format!("rsi_level_{}", param as i32),
            signals::trend_following::rsi_level as SignalFunctionWithParam,
            param,
        ));
    }
    // // Donchian indicator with various parameters
    // for i in 1..=5 {
    //     let param = (i * 10) as f64;
    //     strategies.push((
    //         "donchian_indicator".to_string(),
    //         signals::trend_following::donchian_indicator as SignalFunctionWithParam,
    //         param,
    //     ));
    // }

    // strategies.push((
    //     "donchian_indicator_inverse".to_string(),
    //     signals::trend_following::donchian_indicator_inverse as SignalFunctionWithParam,
    //     0.0,
    // ));

    // // Donchian high
    // for i in 1..=5 {
    //     let param = (i * 10) as f64;
    //     strategies.push((
    //         "donchian_indicator_high".to_string(),
    //         signals::trend_following::donchian_indicator_high as SignalFunctionWithParam,
    //         param,
    //     ));
    // }

    // // Donchian low
    // for i in 1..=5 {
    //     let param = (i * 10) as f64;
    //     strategies.push((
    //         "donchian_indicator_low".to_string(),
    //         signals::trend_following::donchian_indicator_low as SignalFunctionWithParam,
    //         param,
    //     ));
    // }

    // // Trend following 2trouble RSI ATR parameter
    // for i in 1..=5 {
    //     let param = i as f64;
    //     strategies.push((
    //         "trend_fol_2trouble_rsi_atrparam".to_string(),
    //         signals::mfpr::trend_fol_2trouble_rsi_atrparam as SignalFunctionWithParam,
    //         param,
    //     ));
    // }

    // // Trend following 2trouble RSI parameter
    // for i in 2..=6 {
    //     let param = (i * 10) as f64;
    //     strategies.push((
    //         "trend_fol_2trouble_rsi_rsiparam".to_string(),
    //         signals::mfpr::trend_fol_2trouble_rsi_rsiparam as SignalFunctionWithParam,
    //         param,
    //     ));
    // }

    strategies
}

/// Testing strategies - minimal set for quick testing
pub fn testing_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    let mut strategies = Vec::new();

    // Candlestick double trouble with two parameters
    for i in 1..=2 {
        let param = 1.0 + (i as f64 * 0.5) - 0.5;
        strategies.push((
            "candlestick_double_trouble".to_string(),
            signals::mfpr::candlestick_double_trouble as SignalFunctionWithParam,
            param,
        ));
    }

    strategies.push((
        "three_candles".to_string(),
        signals::mfpr::three_candles as SignalFunctionWithParam,
        0.0,
    ));

    strategies
}

/// All strategies - comprehensive list for testing/evaluation
pub fn all_strategies() -> Vec<(String, SignalFunctionWithParam, f64)> {
    vec![
        (
            "candlestick_double_trouble_2.0".to_string(),
            signals::mfpr::candlestick_double_trouble,
            2.0,
        ),
        ("three_candles".to_string(), signals::mfpr::three_candles, 0.0),
        (
            "trend_fol_3candle_ma".to_string(),
            signals::mfpr::trend_following_3candle_ma,
            0.0,
        ),
        ("pattern_marubozu".to_string(), signals::bots::pattern_marubozu, 0.0),
        ("pattern_hammer".to_string(), signals::bots::pattern_hammer, 0.0),
        ("hammer".to_string(), signals::mfpr::hammer, 0.0),
        ("tweezers".to_string(), signals::mfpr::tweezers, 0.0),
        ("hikkake".to_string(), signals::mfpr::hikkake, 0.0),
        ("slingshot".to_string(), signals::mfpr::slingshot, 0.0),
        ("quintuplets_0005".to_string(), signals::mfpr::quintuplets_0005, 0.0),
        ("quintuplets_2".to_string(), signals::mfpr::quintuplets_2, 0.0),
        ("quintuplets_10".to_string(), signals::mfpr::quintuplets_10, 0.0),
        ("quintuplets_50".to_string(), signals::mfpr::quintuplets_50, 0.0),
        ("marubozu".to_string(), signals::mfpr::marubozu, 0.0),
        ("tasuki".to_string(), signals::mfpr::tasuki, 0.0),
        ("three_methods".to_string(), signals::mfpr::three_methods, 0.0),
        (
            "fibonacci_range".to_string(),
            signals::trend_following::fibonacci_range,
            0.0,
        ),
        (
            "adx_indicator".to_string(),
            signals::trend_following::adx_indicator,
            0.0,
        ),
        (
            "donchian_indicator".to_string(),
            signals::trend_following::donchian_indicator,
            0.0,
        ),
        (
            "donchian_indicator_inverse".to_string(),
            signals::trend_following::donchian_indicator_inverse,
            0.0,
        ),
        ("tower".to_string(), signals::mfpr::tower, 0.0),
        ("bottle".to_string(), signals::mfpr::bottle, 0.0),
        ("double_trouble".to_string(), signals::mfpr::double_trouble_1, 0.0),
        ("h".to_string(), signals::mfpr::h, 0.0),
        ("abandoned_baby".to_string(), signals::mfpr::abandoned_baby, 0.0),
        ("doji".to_string(), signals::mfpr::doji, 0.0),
        ("engulfing".to_string(), signals::mfpr::engulfing, 0.0),
        ("harami_flexible".to_string(), signals::mfpr::harami_flexible, 0.0),
        ("harami_strict".to_string(), signals::mfpr::harami_strict, 0.0),
        ("inside_up_down".to_string(), signals::mfpr::inside_up_down, 0.0),
        ("on_neck".to_string(), signals::mfpr::on_neck, 0.0),
        ("piercing".to_string(), signals::mfpr::piercing, 0.0),
        ("spinning_top".to_string(), signals::mfpr::spinning_top, 0.0),
        ("star".to_string(), signals::mfpr::star, 0.0),
        ("stick_sandwich".to_string(), signals::mfpr::stick_sandwich, 0.0),
        ("barrier".to_string(), signals::mfpr::barrier, 0.0),
        ("blockade".to_string(), signals::mfpr::blockade, 1.5),
        ("doppleganger".to_string(), signals::mfpr::doppleganger, 0.0),
        ("euphoria".to_string(), signals::mfpr::euphoria, 0.0),
        ("mirror".to_string(), signals::mfpr::mirror, 0.0),
        ("shrinking".to_string(), signals::mfpr::shrinking, 0.0),
        ("heikin_ashi_doji".to_string(), signals::mfpr::heikin_ashi_doji, 0.0),
        (
            "heikin_ashi_double_trouble".to_string(),
            signals::mfpr::heikin_ashi_double_trouble,
            0.0,
        ),
        (
            "heikin_ashi_euphoria".to_string(),
            signals::mfpr::heikin_ashi_euphoria,
            0.0,
        ),
        ("heikin_ashi_tasuki".to_string(), signals::mfpr::heikin_ashi_tasuki, 0.0),
        ("candlestick_doji".to_string(), signals::mfpr::candlestick_doji, 0.0),
        (
            "candlestick_double_trouble".to_string(),
            signals::mfpr::candlestick_double_trouble,
            2.0,
        ),
        ("candlestick_tasuki".to_string(), signals::mfpr::candlestick_tasuki, 0.0),
        (
            "trend_fol_bottle_stoch".to_string(),
            signals::mfpr::trend_fol_bottle_stoch,
            1.5,
        ),
        (
            "trend_fol_2trouble_rsi".to_string(),
            signals::mfpr::trend_fol_2trouble_rsi,
            1.5,
        ),
        (
            "trend_fol_h_trend_intensity".to_string(),
            signals::mfpr::trend_fol_h_trend_intensity,
            1.5,
        ),
        (
            "trend_fol_marubozu_k_vol_bands".to_string(),
            signals::mfpr::trend_fol_marubozu_k_vol_bands,
            1.5,
        ),
        (
            "contrarian_barrier_rsi_atr".to_string(),
            signals::mfpr::contrarian_barrier_rsi_atr,
            1.5,
        ),
        (
            "contrarian_doji_rsi".to_string(),
            signals::mfpr::contrarian_doji_rsi,
            1.5,
        ),
        (
            "contrarian_engulfing_bbands".to_string(),
            signals::mfpr::contrarian_engulfing_bbands,
            1.5,
        ),
        (
            "contrarian_euphoria_k_env".to_string(),
            signals::mfpr::contrarian_euphoria_k_env,
            0.0,
        ),
        (
            "contrarian_piercing_stoch".to_string(),
            signals::mfpr::contrarian_piercing_stoch,
            0.0,
        ),
        (
            "elder_impulse_1".to_string(),
            signals::trend_following::elder_impulse_1,
            0.0,
        ),
        (
            "elder_impulse_2".to_string(),
            signals::trend_following::elder_impulse_2,
            0.0,
        ),
        (
            "elder_impulse_3".to_string(),
            signals::trend_following::elder_impulse_3,
            0.0,
        ),
        ("gri_index".to_string(), signals::trend_following::gri_index, 0.0),
        (
            "slope_indicator".to_string(),
            signals::trend_following::slope_indicator,
            0.0,
        ),
        ("heikin_ashi".to_string(), signals::trend_following::heikin_ashi, 0.0),
        (
            "inside_candle".to_string(),
            signals::trend_following::inside_candle,
            0.0,
        ),
        (
            "aroon_oscillator".to_string(),
            signals::trend_following::aroon_oscillator,
            0.0,
        ),
        ("awesome".to_string(), signals::trend_following::awesome_indicator, 0.0),
        ("macd_change".to_string(), signals::trend_following::macd_change, 0.0),
        (
            "squeeze_momentum".to_string(),
            signals::trend_following::squeeze_momentum,
            0.0,
        ),
        (
            "supertrend".to_string(),
            signals::trend_following::supertrend_indicator,
            0.0,
        ),
        (
            "trend_intensity_ind".to_string(),
            signals::trend_following::trend_intensity_ind,
            0.0,
        ),
        (
            "vertical_horizontal_cross".to_string(),
            signals::trend_following::vertical_horizontal_cross,
            0.0,
        ),
        (
            "ichimoku_cloud".to_string(),
            signals::trend_following::ichimoku_cloud,
            0.0,
        ),
        ("tf1_ma".to_string(), signals::trend_following::tf1_ma, 0.0),
        ("tf2_ma".to_string(), signals::trend_following::tf2_ma, 0.0),
        ("tf3_rsi_ma".to_string(), signals::trend_following::tf3_rsi_ma, 0.0),
        ("tf4_macd".to_string(), signals::trend_following::tf4_macd, 0.0),
        ("tf5_ma_slope".to_string(), signals::trend_following::tf5_ma_slope, 0.0),
        (
            "tf6_supertrend_flip".to_string(),
            signals::trend_following::tf6_supertrend_flip,
            0.0,
        ),
        ("tf7_psar_ma".to_string(), signals::trend_following::tf7_psar_ma, 0.0),
        ("tf9_tii".to_string(), signals::trend_following::tf9_tii, 0.0),
        ("tf10_ma".to_string(), signals::trend_following::tf10_ma, 0.0),
        (
            "tf11_rsi_neutrality".to_string(),
            signals::trend_following::tf11_rsi_neutrality,
            0.0,
        ),
        ("tf12_vama".to_string(), signals::trend_following::tf12_vama, 0.0),
        (
            "tf13_rsi_supertrend".to_string(),
            signals::trend_following::tf13_rsi_supertrend,
            0.0,
        ),
        (
            "tf14_catapult".to_string(),
            signals::trend_following::tf14_catapult,
            0.0,
        ),
        (
            "contrarian_aug_bbands".to_string(),
            signals::bots::contrarian_aug_bbands,
            0.0,
        ),
        ("contrarian_bbands".to_string(), signals::bots::contrarian_bbands, 0.0),
        (
            "contrarian_dual_bbands".to_string(),
            signals::bots::contrarian_dual_bbands,
            0.0,
        ),
        (
            "contrarian_countdown_cross".to_string(),
            signals::bots::contrarian_countdown_cross,
            0.0,
        ),
        (
            "contrarian_countdown_duration".to_string(),
            signals::bots::contrarian_countdown_duration,
            0.0,
        ),
        ("key_reversal".to_string(), signals::bots::key_reversal, 0.0),
        ("k_extreme_duration".to_string(), signals::bots::k_extreme_duration, 0.0),
        (
            "contrarian_countdown_extremes".to_string(),
            signals::bots::contrarian_countdown_extremes,
            0.0,
        ),
        (
            "contrarian_demarker_cross".to_string(),
            signals::bots::contrarian_demarker_cross,
            0.0,
        ),
        (
            "contrarian_demarker_extremes".to_string(),
            signals::bots::contrarian_demarker_extremes,
            0.0,
        ),
        (
            "contrarian_disparity_extremes".to_string(),
            signals::bots::contrarian_disparity_extremes,
            0.0,
        ),
        (
            "contrarian_fisher_duration".to_string(),
            signals::bots::contrarian_fisher_duration,
            0.0,
        ),
        (
            "contrarian_fisher_extremes".to_string(),
            signals::bots::contrarian_fisher_extremes,
            0.0,
        ),
        (
            "contrarian_real_range_extremes".to_string(),
            signals::bots::contrarian_real_range_extremes,
            0.0,
        ),
        ("pattern_piercing".to_string(), signals::bots::pattern_piercing, 0.0),
        (
            "pattern_td_camouflauge".to_string(),
            signals::bots::pattern_td_camouflage,
            0.0,
        ),
        ("pattern_td_clopwin".to_string(), signals::bots::pattern_td_clopwin, 0.0),
        ("pattern_td_waldo_2".to_string(), signals::bots::pattern_td_waldo_2, 0.0),
        (
            "contrarian_rsi_cross".to_string(),
            signals::bots::contrarian_rsi_cross,
            0.0,
        ),
        (
            "contrarian_rsi_divergences".to_string(),
            signals::bots::contrarian_rsi_divergences,
            0.0,
        ),
        (
            "contrarian_rsi_duration".to_string(),
            signals::bots::contrarian_rsi_duration,
            0.0,
        ),
        (
            "contrarian_rsi_extremes".to_string(),
            signals::bots::contrarian_rsi_extremes,
            0.0,
        ),
        (
            "contrarian_stochastic_cross".to_string(),
            signals::bots::contrarian_stochastic_cross,
            0.0,
        ),
        (
            "contrarian_stochastic_divergences".to_string(),
            signals::bots::contrarian_stochastic_divergences,
            0.0,
        ),
        (
            "contrarian_stochastic_duration".to_string(),
            signals::bots::contrarian_stochastic_duration,
            0.0,
        ),
        (
            "contrarian_stochastic_extremes".to_string(),
            signals::bots::contrarian_stochastic_extremes,
            0.0,
        ),
        (
            "contrarian_time_up_extremes".to_string(),
            signals::bots::contrarian_time_up_extremes,
            0.0,
        ),
        ("contrarian_tsabm".to_string(), signals::bots::contrarian_tsabm, 0.0),
        (
            "pattern_differentials".to_string(),
            signals::bots::pattern_differentials,
            0.0,
        ),
        ("pattern_engulfing".to_string(), signals::bots::pattern_engulfing, 0.0),
        (
            "pattern_fibonacci_timing".to_string(),
            signals::bots::pattern_fibonacci_timing,
            0.0,
        ),
        ("pattern_td_clop".to_string(), signals::bots::pattern_td_clop, 0.0),
        ("pattern_td_open".to_string(), signals::bots::pattern_td_open, 0.0),
        ("pattern_td_trap".to_string(), signals::bots::pattern_td_trap, 0.0),
        ("pattern_td_waldo_5".to_string(), signals::bots::pattern_td_waldo_5, 0.0),
        ("pattern_td_waldo_6".to_string(), signals::bots::pattern_td_waldo_6, 0.0),
        ("pattern_td_waldo_8".to_string(), signals::bots::pattern_td_waldo_8, 0.0),
        (
            "pattern_three_line_strike".to_string(),
            signals::bots::pattern_three_line_strike,
            0.0,
        ),
        (
            "pattern_three_methods".to_string(),
            signals::bots::pattern_three_methods,
            0.0,
        ),
    ]
}
