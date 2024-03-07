#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use backtester::*;
use polars::prelude::*;
use std::process;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};

use std::path::Path;

// use tokio::spawn;
// use tokio_postgres::{NoTls, Error};
use std::error::Error as StdError;
use rayon::prelude::*;
use std::sync::Mutex;
// use polars::export::rayon::iter::IntoParallelIterator;

mod signals {
    pub mod mfpr;
    pub mod technical; 
    pub mod trend_following;
    pub mod bots;
}

fn sig(df: LazyFrame, name: &str, f: fn(DataFrame) -> BuySell) -> Backtest {
    let s = f(df.clone().collect().unwrap());
    backtest_performance(df.clone().collect().unwrap(), s, &name)
}

pub fn run_backtests(d: LazyFrame) -> Vec<Backtest> {
    let mut a = vec![];

    // a.push(sig(d.clone(), "alpha", signals::mfpr::alpha));
    // for i in 1..2 {
    //     let name = "test_signal".to_owned() + &*i.to_string();
    //     a.push(sig(d.clone(), &name, signals::mfpr::test_signal2));
    // }
              
    a.push(sig(d.clone(), "hikkake", signals::mfpr::hikkake));
    // a.push(sig(d.clone(), "marubozu", signals::mfpr::marubozu));
    // a.push(sig(d.clone(), "tasuki", signals::mfpr::tasuki));
    // a.push(sig(d.clone(), "three_candles", signals::mfpr::three_candles));
    // a.push(sig(d.clone(), "three_methods", signals::mfpr::three_methods));
    // a.push(sig(d.clone(), "bottle", signals::mfpr::bottle));
    // a.push(sig(d.clone(), "double_trouble", signals::mfpr::double_trouble_1));
    // a.push(sig(d.clone(), "h", signals::mfpr::h));
    // a.push(sig(d.clone(), "quintuplets_0005", signals::mfpr::quintuplets_0005));
    // a.push(sig(d.clone(), "quintuplets_2", signals::mfpr::quintuplets_2));
    // a.push(sig(d.clone(), "quintuplets_10", signals::mfpr::quintuplets_10));
    // a.push(sig(d.clone(), "quintuplets_50", signals::mfpr::quintuplets_50));
    // a.push(sig(d.clone(), "slingshot", signals::mfpr::slingshot));
    // a.push(sig(d.clone(), "abandoned_baby", signals::mfpr::abandoned_baby));
    // a.push(sig(d.clone(), "doji", signals::mfpr::doji));
    // a.push(sig(d.clone(), "engulfing", signals::mfpr::engulfing));
    // a.push(sig(d.clone(), "hammer", signals::mfpr::hammer));
    // a.push(sig(d.clone(), "harami_flexible", signals::mfpr::harami_flexible));
    // a.push(sig(d.clone(), "harami_strict", signals::mfpr::harami_strict));
    // a.push(sig(d.clone(), "inside_up_down", signals::mfpr::inside_up_down));
    // a.push(sig(d.clone(), "on_neck", signals::mfpr::on_neck));
    // a.push(sig(d.clone(), "piercing", signals::mfpr::piercing));
    // a.push(sig(d.clone(), "spinning_top", signals::mfpr::spinning_top));
    // a.push(sig(d.clone(), "star", signals::mfpr::star));
    // a.push(sig(d.clone(), "stick_sandwich", signals::mfpr::stick_sandwich));
    // a.push(sig(d.clone(), "tower", signals::mfpr::tower));
    // a.push(sig(d.clone(), "tweezers", signals::mfpr::tweezers));
    // a.push(sig(d.clone(), "barrier", signals::mfpr::barrier));
    // a.push(sig(d.clone(), "blockade", signals::mfpr::blockade));
    // a.push(sig(d.clone(), "doppleganger", signals::mfpr::doppleganger));
    // a.push(sig(d.clone(), "euphoria", signals::mfpr::euphoria));
    // a.push(sig(d.clone(), "mirror", signals::mfpr::mirror));
    // a.push(sig(d.clone(), "shrinking", signals::mfpr::shrinking));
    // a.push(sig(d.clone(), "heikin_ashi_doji", signals::mfpr::heikin_ashi_doji));
    // a.push(sig(d.clone(), "heikin_ashi_double_trouble", signals::mfpr::heikin_ashi_double_trouble));
    // a.push(sig(d.clone(), "heikin_ashi_euphoria", signals::mfpr::heikin_ashi_euphoria));
    // a.push(sig(d.clone(), "heikin_ashi_tasuki", signals::mfpr::heikin_ashi_tasuki));
    // a.push(sig(d.clone(), "candlestick_doji", signals::mfpr::candlestick_doji));
    // a.push(sig(d.clone(), "candlestick_double_trouble", signals::mfpr::candlestick_double_trouble));
    // a.push(sig(d.clone(), "candlestick_euphoria", signals::mfpr::candlestick_euphoria));
    // a.push(sig(d.clone(), "candlestick_tasuki", signals::mfpr::candlestick_tasuki));
    // a.push(sig(d.clone(), "trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma));
    // a.push(sig(d.clone(), "trend_fol_bottle_stoch", signals::mfpr::trend_fol_bottle_stoch));
    // a.push(sig(d.clone(), "trend_fol_2trouble_rsi", signals::mfpr::trend_fol_2trouble_rsi));
    // a.push(sig(d.clone(), "trend_fol_h_trend_intensity", signals::mfpr::trend_fol_h_trend_intensity));
    // a.push(sig(d.clone(), "trend_fol_marubozu_k_vol_bands", signals::mfpr::trend_fol_marubozu_k_vol_bands));
    // a.push(sig(d.clone(), "contrarian_barrier_rsi_atr", signals::mfpr::contrarian_barrier_rsi_atr));
    // a.push(sig(d.clone(), "contrarian_doji_rsi", signals::mfpr::contrarian_doji_rsi));
    // a.push(sig(d.clone(), "contrarian_engulfing_bbands", signals::mfpr::contrarian_engulfing_bbands));
    // a.push(sig(d.clone(), "contrarian_euphoria_k_env", signals::mfpr::contrarian_euphoria_k_env));
    // a.push(sig(d.clone(), "contrarian_piercing_stoch", signals::mfpr::contrarian_piercing_stoch));
    // a.push(sig(d.clone(), "fibonacci_range", signals::trend_following::fibonacci_range));
    // a.push(sig(d.clone(), "elder_impulse_1", signals::trend_following::elder_impulse_1));
    // a.push(sig(d.clone(), "elder_impulse_2", signals::trend_following::elder_impulse_2));
    // a.push(sig(d.clone(), "elder_impulse_3", signals::trend_following::elder_impulse_3));
    // a.push(sig(d.clone(), "gri_index", signals::trend_following::gri_index));
    // a.push(sig(d.clone(), "slope_indicator", signals::trend_following::slope_indicator));
    // a.push(sig(d.clone(), "heikin_ashi", signals::trend_following::heikin_ashi));
    // a.push(sig(d.clone(), "inside_candle", signals::trend_following::inside_candle));
    // a.push(sig(d.clone(), "aroon_oscillator", signals::trend_following::aroon_oscillator));
    // a.push(sig(d.clone(), "adx_indicator", signals::trend_following::adx_indicator));
    // a.push(sig(d.clone(), "awesome", signals::trend_following::awesome_indicator));
    // a.push(sig(d.clone(), "donchian_indicator", signals::trend_following::donchian_indicator));
    // a.push(sig(d.clone(), "macd_change", signals::trend_following::macd_change));
    // a.push(sig(d.clone(), "squeeze_momentum", signals::trend_following::squeeze_momentum));
    // a.push(sig(d.clone(), "supertrend", signals::trend_following::supertrend_indicator));
    // a.push(sig(d.clone(), "trend_intensity_ind", signals::trend_following::trend_intensity_ind));
    // a.push(sig(d.clone(), "vertical_horizontal_cross", signals::trend_following::vertical_horizontal_cross));
    // a.push(sig(d.clone(), "ichimoku_cloud", signals::trend_following::ichimoku_cloud));
    // a.push(sig(d.clone(), "tf1_ma", signals::trend_following::tf1_ma));
    // a.push(sig(d.clone(), "tf2_ma", signals::trend_following::tf2_ma));
    // a.push(sig(d.clone(), "tf3_rsi_ma", signals::trend_following::tf3_rsi_ma));
    // a.push(sig(d.clone(), "tf4_macd", signals::trend_following::tf4_macd));
    // a.push(sig(d.clone(), "tf5_ma_slope", signals::trend_following::tf5_ma_slope));
    // a.push(sig(d.clone(), "tf6_supertrend_flip", signals::trend_following::tf6_supertrend_flip));
    // a.push(sig(d.clone(), "tf7_psar_ma", signals::trend_following::tf7_psar_ma));
    // a.push(sig(d.clone(), "tf9_tii", signals::trend_following::tf9_tii));
    // a.push(sig(d.clone(), "tf11_rsi_neutrality", signals::trend_following::tf11_rsi_neutrality));
    // a.push(sig(d.clone(), "tf12_vama", signals::trend_following::tf12_vama));
    // a.push(sig(d.clone(), "tf13_rsi_supertrend", signals::trend_following::tf13_rsi_supertrend));
    // a.push(sig(d.clone(), "tf14_catapult", signals::trend_following::tf14_catapult));
    // a.push(sig(d.clone(), "contrarian_aug_bbands", signals::bots::contrarian_aug_bbands));
    // a.push(sig(d.clone(), "contrarian_bbands", signals::bots::contrarian_bbands));
    // a.push(sig(d.clone(), "contrarian_dual_bbands", signals::bots::contrarian_dual_bbands));
    // a.push(sig(d.clone(), "contrarian_countdown_cross", signals::bots::contrarian_countdown_cross));
    // a.push(sig(d.clone(), "contrarian_countdown_duration", signals::bots::contrarian_countdown_duration));
    // a.push(sig(d.clone(), "key_reversal", signals::bots::key_reversal));
    // a.push(sig(d.clone(), "k_extreme_duration", signals::bots::k_extreme_duration));
    // a.push(sig(d.clone(), "contrarian_countdown_extremes", signals::bots::contrarian_countdown_extremes));
    // a.push(sig(d.clone(), "contrarian_demarker_cross", signals::bots::contrarian_demarker_cross));
    // a.push(sig(d.clone(), "contrarian_demarker_extremes", signals::bots::contrarian_demarker_extremes));
    // a.push(sig(d.clone(), "contrarian_disparity_extremes", signals::bots::contrarian_disparity_extremes));
    // a.push(sig(d.clone(), "contrarian_fisher_duration", signals::bots::contrarian_fisher_duration));
    // a.push(sig(d.clone(), "contrarian_fisher_extremes", signals::bots::contrarian_fisher_extremes));
    // a.push(sig(d.clone(), "contrarian_real_range_extremes", signals::bots::contrarian_real_range_extremes));
    // a.push(sig(d.clone(), "contrarian_rsi_cross", signals::bots::contrarian_rsi_cross));
    // a.push(sig(d.clone(), "contrarian_rsi_divergences", signals::bots::contrarian_rsi_divergences));
    // a.push(sig(d.clone(), "contrarian_rsi_duration", signals::bots::contrarian_rsi_duration));
    // a.push(sig(d.clone(), "contrarian_rsi_extremes", signals::bots::contrarian_rsi_extremes));
    // a.push(sig(d.clone(), "contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_cross));
    // a.push(sig(d.clone(), "contrarian_stochastic_divergences", signals::bots::contrarian_stochastic_divergences));
    // a.push(sig(d.clone(), "contrarian_stochastic_duration", signals::bots::contrarian_stochastic_duration));
    // a.push(sig(d.clone(), "contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_extremes));
    // a.push(sig(d.clone(), "contrarian_time_up_extremes", signals::bots::contrarian_time_up_extremes));
    // a.push(sig(d.clone(), "contrarian_tsabm", signals::bots::contrarian_tsabm));
    // a.push(sig(d.clone(), "pattern_differentials", signals::bots::pattern_differentials));
    // a.push(sig(d.clone(), "pattern_engulfing", signals::bots::pattern_engulfing));
    // a.push(sig(d.clone(), "pattern_fibonacci_timing", signals::bots::pattern_fibonacci_timing));
    // a.push(sig(d.clone(), "pattern_hammer", signals::bots::pattern_hammer));
    // a.push(sig(d.clone(), "pattern_marubozu", signals::bots::pattern_marubozu));
    // a.push(sig(d.clone(), "pattern_piercing", signals::bots::pattern_piercing));
    // a.push(sig(d.clone(), "pattern_td_camouflauge", signals::bots::pattern_td_camouflage));
    // a.push(sig(d.clone(), "pattern_td_clop", signals::bots::pattern_td_clop));
    // a.push(sig(d.clone(), "pattern_td_clopwin", signals::bots::pattern_td_clopwin));
    // a.push(sig(d.clone(), "pattern_td_open", signals::bots::pattern_td_open));
    // a.push(sig(d.clone(), "pattern_td_trap", signals::bots::pattern_td_trap));
    // a.push(sig(d.clone(), "pattern_td_waldo_2", signals::bots::pattern_td_waldo_2));
    // a.push(sig(d.clone(), "pattern_td_waldo_5", signals::bots::pattern_td_waldo_5));
    // a.push(sig(d.clone(), "pattern_td_waldo_6", signals::bots::pattern_td_waldo_6));
    // a.push(sig(d.clone(), "pattern_td_waldo_8", signals::bots::pattern_td_waldo_8));
    a.push(sig(d.clone(), "pattern_three_line_strike", signals::bots::pattern_three_line_strike));
    a.push(sig(d.clone(), "pattern_three_methods", signals::bots::pattern_three_methods));
    
    a 
}



#[cfg(test)]
mod tests {
    #[test]
    fn test_summarize_performance_file() {
        // Use `super` to refer to the parent module where `summarize_performance` is defined.
        super::summarize_performance("backtest_output_{u}.parquet".to_string());
    }
}

fn main() -> Result<(), Box<dyn StdError>> {
    
    let _ = create_price_files();
 
    // let univ = ["LC1","LC2","MC1","MC2","SC1","SC2","SC3","SC4","Micro1","Micro2"];
    let univ = ["LC1","MC1","SC1","Micro1"];
    
    for u in univ {
        // Run all the backtests and store them in a vec
        let file_path = format!("./data/{}.parquet", u);
        let lf = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())?;

        // Collect the unique tickers into a DataFrame
        let unique_tickers_df = lf
            .clone()
            .select([col("Ticker").unique().alias("unique_tickers")])
            .collect()?;
    
        // Assuming the 'unique_tickers' column is of type Utf8
        let unique_tickers_series = unique_tickers_df.column("unique_tickers")?;
    
        // Convert the Series into a Vec<String>
        let unique_tickers: Vec<String> = unique_tickers_series.utf8()?
            .into_iter()
            .filter_map(|value| value.map(|v| v.to_string()))
            .collect();

        let num_tickers = unique_tickers.clone().len();

        let output = Mutex::new(Vec::new()); // Wrap output in a Mutex for thread-safe access

        unique_tickers.into_par_iter().enumerate().for_each(|(index, ticker)| {
            println!("Running backtests for {}: {} of {}", ticker, index, num_tickers);
            let filtered_lf = lf.clone().filter(col("Ticker").eq(lit(ticker)));
            
            // if index < 4 {
            let backtest_results = run_backtests(filtered_lf);
            // Acquire the lock before accessing output
            let mut output_lock = output.lock().unwrap(); 
            // Mutex guard is automatically released here when `output_lock` goes out of scope
            output_lock.extend(backtest_results);
            // }
        });
    
        // Remember, accessing output outside the parallel iteration also requires locking
        let final_output_guard = output.lock().unwrap();

        // Dereference the MutexGuard to get a reference to the Vec<Backtest>
        let final_output = &*final_output_guard;

        // Convert the list of backtest structs to a DataFrame
        let df = records_to_dataframe(final_output);
        // Write the output
        let output_path: String = format!("backtest_output_{}.parquet", u);
        let mut output_file = File::create(output_path).expect("could not create file");
        ParquetWriter::new(&mut output_file).finish(&mut df.clone()).unwrap();

    }

    Ok(())
}
