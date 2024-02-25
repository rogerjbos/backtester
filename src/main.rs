#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use backtester::*;
use polars::prelude::*;

mod signals {
    pub mod mfpr;
    pub mod technical; 
    pub mod trend_following;
    pub mod bots;
}

fn apply_signal(df: LazyFrame, name: &str, f: fn(DataFrame) -> BuySell) -> Backtest {
    let s = f(df.clone().collect().unwrap());
    backtest_performance(df.clone().collect().unwrap(), s, &name)
}

pub fn run_backtests(data: LazyFrame) -> Vec<Backtest> {
    let mut a = vec![];

    // a.push(apply_signal(data.clone(), "alpha", signals::mfpr::alpha));
    // for i in 1..2 {
    //     let name = "test_signal".to_owned() + &*i.to_string();
    //     a.push(apply_signal(data.clone(), &name, signals::mfpr::test_signal2));
    // }
    a.push(apply_signal(data.clone(), "hikkake", signals::mfpr::hikkake));
    a.push(apply_signal(data.clone(), "marubozu", signals::mfpr::marubozu));
    a.push(apply_signal(data.clone(), "tasuki", signals::mfpr::tasuki));
    a.push(apply_signal(data.clone(), "three_candles", signals::mfpr::three_candles));
    a.push(apply_signal(data.clone(), "three_methods", signals::mfpr::three_methods));
    a.push(apply_signal(data.clone(), "bottle", signals::mfpr::bottle));
    a.push(apply_signal(data.clone(), "double_trouble", signals::mfpr::double_trouble_1));
    a.push(apply_signal(data.clone(), "h", signals::mfpr::h));
    a.push(apply_signal(data.clone(), "quintuplets_0005", signals::mfpr::quintuplets_0005));
    a.push(apply_signal(data.clone(), "quintuplets_2", signals::mfpr::quintuplets_2));
    a.push(apply_signal(data.clone(), "quintuplets_10", signals::mfpr::quintuplets_10));
    a.push(apply_signal(data.clone(), "quintuplets_50", signals::mfpr::quintuplets_50));
    a.push(apply_signal(data.clone(), "slingshot", signals::mfpr::slingshot));
    a.push(apply_signal(data.clone(), "abandoned_baby", signals::mfpr::abandoned_baby));
    a.push(apply_signal(data.clone(), "doji", signals::mfpr::doji));
    a.push(apply_signal(data.clone(), "engulfing", signals::mfpr::engulfing));
    a.push(apply_signal(data.clone(), "hammer", signals::mfpr::hammer));
    a.push(apply_signal(data.clone(), "harami_flexible", signals::mfpr::harami_flexible));
    a.push(apply_signal(data.clone(), "harami_strict", signals::mfpr::harami_strict));
    a.push(apply_signal(data.clone(), "inside_up_down", signals::mfpr::inside_up_down));
    a.push(apply_signal(data.clone(), "on_neck", signals::mfpr::on_neck));
    a.push(apply_signal(data.clone(), "piercing", signals::mfpr::piercing));
    a.push(apply_signal(data.clone(), "spinning_top", signals::mfpr::spinning_top));
    a.push(apply_signal(data.clone(), "star", signals::mfpr::star));
    a.push(apply_signal(data.clone(), "stick_sandwich", signals::mfpr::stick_sandwich));
    a.push(apply_signal(data.clone(), "tower", signals::mfpr::tower));
    a.push(apply_signal(data.clone(), "tweezers", signals::mfpr::tweezers));
    a.push(apply_signal(data.clone(), "barrier", signals::mfpr::barrier));
    a.push(apply_signal(data.clone(), "blockade", signals::mfpr::blockade));
    a.push(apply_signal(data.clone(), "doppleganger", signals::mfpr::doppleganger));
    a.push(apply_signal(data.clone(), "euphoria", signals::mfpr::euphoria));
    a.push(apply_signal(data.clone(), "mirror", signals::mfpr::mirror));
    a.push(apply_signal(data.clone(), "shrinking", signals::mfpr::shrinking));
    a.push(apply_signal(data.clone(), "heikin_ashi_doji", signals::mfpr::heikin_ashi_doji));
    a.push(apply_signal(data.clone(), "heikin_ashi_double_trouble", signals::mfpr::heikin_ashi_double_trouble));
    a.push(apply_signal(data.clone(), "heikin_ashi_euphoria", signals::mfpr::heikin_ashi_euphoria));
    a.push(apply_signal(data.clone(), "heikin_ashi_tasuki", signals::mfpr::heikin_ashi_tasuki));
    a.push(apply_signal(data.clone(), "candlestick_doji", signals::mfpr::candlestick_doji));
    a.push(apply_signal(data.clone(), "candlestick_double_trouble", signals::mfpr::candlestick_double_trouble));
    a.push(apply_signal(data.clone(), "candlestick_euphoria", signals::mfpr::candlestick_euphoria));
    a.push(apply_signal(data.clone(), "candlestick_tasuki", signals::mfpr::candlestick_tasuki));
    a.push(apply_signal(data.clone(), "trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma));
    a.push(apply_signal(data.clone(), "trend_fol_bottle_stoch", signals::mfpr::trend_fol_bottle_stoch));
    a.push(apply_signal(data.clone(), "trend_fol_2trouble_rsi", signals::mfpr::trend_fol_2trouble_rsi));
    a.push(apply_signal(data.clone(), "trend_fol_h_trend_intensity", signals::mfpr::trend_fol_h_trend_intensity));
    a.push(apply_signal(data.clone(), "trend_fol_marubozu_k_vol_bands", signals::mfpr::trend_fol_marubozu_k_vol_bands));
    a.push(apply_signal(data.clone(), "contrarian_barrier_rsi_atr", signals::mfpr::contrarian_barrier_rsi_atr));
    a.push(apply_signal(data.clone(), "contrarian_doji_rsi", signals::mfpr::contrarian_doji_rsi));
    a.push(apply_signal(data.clone(), "contrarian_engulfing_bbands", signals::mfpr::contrarian_engulfing_bbands));
    a.push(apply_signal(data.clone(), "contrarian_euphoria_k_env", signals::mfpr::contrarian_euphoria_k_env));
    a.push(apply_signal(data.clone(), "contrarian_piercing_stoch", signals::mfpr::contrarian_piercing_stoch));
    a.push(apply_signal(data.clone(), "fibonacci_range", signals::trend_following::fibonacci_range));
    a.push(apply_signal(data.clone(), "elder_impulse_1", signals::trend_following::elder_impulse_1));
    a.push(apply_signal(data.clone(), "elder_impulse_2", signals::trend_following::elder_impulse_2));
    a.push(apply_signal(data.clone(), "elder_impulse_3", signals::trend_following::elder_impulse_3));
    a.push(apply_signal(data.clone(), "gri_index", signals::trend_following::gri_index));
    a.push(apply_signal(data.clone(), "slope_indicator", signals::trend_following::slope_indicator));
    a.push(apply_signal(data.clone(), "heikin_ashi", signals::trend_following::heikin_ashi));
    a.push(apply_signal(data.clone(), "inside_candle", signals::trend_following::inside_candle));
    a.push(apply_signal(data.clone(), "aroon_oscillator", signals::trend_following::aroon_oscillator));
    a.push(apply_signal(data.clone(), "adx_indicator", signals::trend_following::adx_indicator));
    a.push(apply_signal(data.clone(), "awesome", signals::trend_following::awesome_indicator));
    a.push(apply_signal(data.clone(), "donchian_indicator", signals::trend_following::donchian_indicator));
    a.push(apply_signal(data.clone(), "macd_change", signals::trend_following::macd_change));
    a.push(apply_signal(data.clone(), "squeeze_momentum", signals::trend_following::squeeze_momentum));
    a.push(apply_signal(data.clone(), "supertrend", signals::trend_following::supertrend_indicator));
    a.push(apply_signal(data.clone(), "trend_intensity_ind", signals::trend_following::trend_intensity_ind));
    a.push(apply_signal(data.clone(), "vertical_horizontal_cross", signals::trend_following::vertical_horizontal_cross));
    a.push(apply_signal(data.clone(), "ichimoku_cloud", signals::trend_following::ichimoku_cloud));
    a.push(apply_signal(data.clone(), "tf1_ma", signals::trend_following::tf1_ma));
    a.push(apply_signal(data.clone(), "tf2_ma", signals::trend_following::tf2_ma));
    a.push(apply_signal(data.clone(), "tf3_rsi_ma", signals::trend_following::tf3_rsi_ma));
    a.push(apply_signal(data.clone(), "tf4_macd", signals::trend_following::tf4_macd));
    a.push(apply_signal(data.clone(), "tf5_ma_slope", signals::trend_following::tf5_ma_slope));
    a.push(apply_signal(data.clone(), "tf6_supertrend_flip", signals::trend_following::tf6_supertrend_flip));
    a.push(apply_signal(data.clone(), "tf7_psar_ma", signals::trend_following::tf7_psar_ma));
    a.push(apply_signal(data.clone(), "tf9_tii", signals::trend_following::tf9_tii));
    a.push(apply_signal(data.clone(), "tf11_rsi_neutrality", signals::trend_following::tf11_rsi_neutrality));
    a.push(apply_signal(data.clone(), "tf12_vama", signals::trend_following::tf12_vama));
    a.push(apply_signal(data.clone(), "tf13_rsi_supertrend", signals::trend_following::tf13_rsi_supertrend));
    a.push(apply_signal(data.clone(), "tf14_catapult", signals::trend_following::tf14_catapult));
    a.push(apply_signal(data.clone(), "contrarian_aug_bbands", signals::bots::contrarian_aug_bbands));
    a.push(apply_signal(data.clone(), "contrarian_bbands", signals::bots::contrarian_bbands));
    a.push(apply_signal(data.clone(), "contrarian_dual_bbands", signals::bots::contrarian_dual_bbands));
    a.push(apply_signal(data.clone(), "contrarian_countdown_cross", signals::bots::contrarian_countdown_cross));
    a.push(apply_signal(data.clone(), "contrarian_countdown_duration", signals::bots::contrarian_countdown_duration));
    a.push(apply_signal(data.clone(), "key_reversal", signals::bots::key_reversal));
    a.push(apply_signal(data.clone(), "k_extreme_duration", signals::bots::k_extreme_duration));
    a.push(apply_signal(data.clone(), "contrarian_countdown_extremes", signals::bots::contrarian_countdown_extremes));
    a.push(apply_signal(data.clone(), "contrarian_demarker_cross", signals::bots::contrarian_demarker_cross));
    a.push(apply_signal(data.clone(), "contrarian_demarker_extremes", signals::bots::contrarian_demarker_extremes));
    a.push(apply_signal(data.clone(), "contrarian_disparity_extremes", signals::bots::contrarian_disparity_extremes));
    a.push(apply_signal(data.clone(), "contrarian_fisher_duration", signals::bots::contrarian_fisher_duration));
    a.push(apply_signal(data.clone(), "contrarian_fisher_extremes", signals::bots::contrarian_fisher_extremes));
    a.push(apply_signal(data.clone(), "contrarian_real_range_extremes", signals::bots::contrarian_real_range_extremes));
    a.push(apply_signal(data.clone(), "contrarian_rsi_cross", signals::bots::contrarian_rsi_cross));
    a.push(apply_signal(data.clone(), "contrarian_rsi_divergences", signals::bots::contrarian_rsi_divergences));
    a.push(apply_signal(data.clone(), "contrarian_rsi_duration", signals::bots::contrarian_rsi_duration));
    a.push(apply_signal(data.clone(), "contrarian_rsi_extremes", signals::bots::contrarian_rsi_extremes));
    a.push(apply_signal(data.clone(), "contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_cross));
    a.push(apply_signal(data.clone(), "contrarian_stochastic_divergences", signals::bots::contrarian_stochastic_divergences));
    a.push(apply_signal(data.clone(), "contrarian_stochastic_duration", signals::bots::contrarian_stochastic_duration));
    a.push(apply_signal(data.clone(), "contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_extremes));
    a.push(apply_signal(data.clone(), "contrarian_time_up_extremes", signals::bots::contrarian_time_up_extremes));
    a.push(apply_signal(data.clone(), "contrarian_tsabm", signals::bots::contrarian_tsabm));
    a.push(apply_signal(data.clone(), "pattern_differentials", signals::bots::pattern_differentials));
    a.push(apply_signal(data.clone(), "pattern_engulfing", signals::bots::pattern_engulfing));
    a.push(apply_signal(data.clone(), "pattern_fibonacci_timing", signals::bots::pattern_fibonacci_timing));
    a.push(apply_signal(data.clone(), "pattern_hammer", signals::bots::pattern_hammer));
    a.push(apply_signal(data.clone(), "pattern_marubozu", signals::bots::pattern_marubozu));
    a.push(apply_signal(data.clone(), "pattern_piercing", signals::bots::pattern_piercing));
    a.push(apply_signal(data.clone(), "pattern_TD_camouflauge", signals::bots::pattern_TD_camouflage));
    a.push(apply_signal(data.clone(), "pattern_TD_clop", signals::bots::pattern_TD_clop));
    a.push(apply_signal(data.clone(), "pattern_TD_clopwin", signals::bots::pattern_TD_clopwin));
    a.push(apply_signal(data.clone(), "pattern_TD_open", signals::bots::pattern_TD_open));
    a.push(apply_signal(data.clone(), "pattern_TD_trap", signals::bots::pattern_TD_trap));
    a.push(apply_signal(data.clone(), "pattern_TD_waldo_2", signals::bots::pattern_TD_waldo_2));
    a.push(apply_signal(data.clone(), "pattern_TD_waldo_5", signals::bots::pattern_TD_waldo_5));
    a.push(apply_signal(data.clone(), "pattern_TD_waldo_6", signals::bots::pattern_TD_waldo_6));
    a.push(apply_signal(data.clone(), "pattern_TD_waldo_8", signals::bots::pattern_TD_waldo_8));
    a.push(apply_signal(data.clone(), "pattern_three_line_strike", signals::bots::pattern_three_line_strike));
    a.push(apply_signal(data.clone(), "pattern_three_methods", signals::bots::pattern_three_methods));
    
    a 
}

fn main() {    

    let demo = true;
    let pre_process: bool = false;
    let post_process: bool = false;
    let dat = match demo {
        false => production_data(),
        _ => demo_data(),
    };

    // let dout = preprocess(dat.clone());
    // let dout = dat.clone().collect().unwrap();
    let dout = match pre_process {
        false => dat.clone().collect().unwrap(),
        _ => preprocess(dat.clone()),
    };

    // Convert each element to String and collect into a Vec<String>
    let unique_tickers = dat.collect().unwrap().column("Ticker").unwrap().unique().unwrap();
    let tickers: Vec<String> = unique_tickers
        .utf8().unwrap()
        .into_iter()
        .filter_map(|option| option.map(|s| s.to_string()))
        .collect();
    // let tickers: [&str; 1] = ["US000000013186"];

    // Run all the backtests and store them in a vec
    let mut output: Vec<Backtest> = Vec::new();
    for ticker in &tickers {
        println!("Running backtests for {}", ticker);
        let df = dout
            .filter(&dout.column("Ticker")
            .unwrap()
            .utf8()
            .unwrap()
            .contains(ticker, true).unwrap())
            .unwrap();

        let df = match post_process {
            false => df.clone(),
            _ => {
                let p = postprocess(df.clone());
                println!("df: {:?}", p);
                p
            },
        };
    
        let b = run_backtests(df.lazy());
        // Collect all the backtest results into a list
        output.extend(b);
    }
    // Convert the list of backtest structs to a DataFrame
    let df = records_to_dataframe(output);

    let avg_by_strategy = summary_performance(df);
    println!("{}", avg_by_strategy);

}   
