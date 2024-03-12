#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use backtester::*;
use polars::{functions, prelude::*};
use std::process;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use std::env;
use sqlx::postgres::PgPoolOptions;
use tokio;
use futures::future::join_all;
use std::collections::HashSet;

use std::error::Error as StdError;
use std::fs;
// use std::sync::Mutex;

mod signals {
    pub mod mfpr;
    pub mod technical; 
    pub mod trend_following;
    pub mod bots;
}

pub async fn run_backtests(lf: LazyFrame) -> Result<Vec<Backtest>, Box<dyn std::error::Error>> {

    let mut signals: Vec<Signal> = Vec::new();
    let signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("marubozu", signals::mfpr::marubozu),
        ("tasuki", signals::mfpr::tasuki),
        ("three_candles", signals::mfpr::three_candles),
        ("three_methods", signals::mfpr::three_methods),
        ("bottle", signals::mfpr::bottle),
        ("double_trouble", signals::mfpr::double_trouble_1),
        ("h", signals::mfpr::h),
        ("quintuplets_0005", signals::mfpr::quintuplets_0005),
        ("quintuplets_2", signals::mfpr::quintuplets_2),
        ("quintuplets_10", signals::mfpr::quintuplets_10),
        ("quintuplets_50", signals::mfpr::quintuplets_50),
        ("slingshot", signals::mfpr::slingshot),
        ("abandoned_baby", signals::mfpr::abandoned_baby),
        ("doji", signals::mfpr::doji),
        ("engulfing", signals::mfpr::engulfing),
        ("hammer", signals::mfpr::hammer),
        ("harami_flexible", signals::mfpr::harami_flexible),
        ("harami_strict", signals::mfpr::harami_strict),
        ("inside_up_down", signals::mfpr::inside_up_down),
        ("on_neck", signals::mfpr::on_neck),
        ("piercing", signals::mfpr::piercing),
        ("spinning_top", signals::mfpr::spinning_top),
        ("star", signals::mfpr::star),
        ("stick_sandwich", signals::mfpr::stick_sandwich),
        ("tower", signals::mfpr::tower),
        ("tweezers", signals::mfpr::tweezers),
        ("barrier", signals::mfpr::barrier),
        ("blockade", signals::mfpr::blockade),
        ("doppleganger", signals::mfpr::doppleganger),
        ("euphoria", signals::mfpr::euphoria),
        ("mirror", signals::mfpr::mirror),
        ("shrinking", signals::mfpr::shrinking),
        ("heikin_ashi_doji", signals::mfpr::heikin_ashi_doji),
        ("heikin_ashi_double_trouble", signals::mfpr::heikin_ashi_double_trouble),
        ("heikin_ashi_euphoria", signals::mfpr::heikin_ashi_euphoria),
        ("heikin_ashi_tasuki", signals::mfpr::heikin_ashi_tasuki),
        ("candlestick_doji", signals::mfpr::candlestick_doji),
        ("candlestick_double_trouble", signals::mfpr::candlestick_double_trouble),
        ("candlestick_euphoria", signals::mfpr::candlestick_euphoria),
        ("candlestick_tasuki", signals::mfpr::candlestick_tasuki),
        ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        ("trend_fol_bottle_stoch", signals::mfpr::trend_fol_bottle_stoch),
        ("trend_fol_2trouble_rsi", signals::mfpr::trend_fol_2trouble_rsi),
        ("trend_fol_h_trend_intensity", signals::mfpr::trend_fol_h_trend_intensity),
        ("trend_fol_marubozu_k_vol_bands", signals::mfpr::trend_fol_marubozu_k_vol_bands),
        ("contrarian_barrier_rsi_atr", signals::mfpr::contrarian_barrier_rsi_atr),
        ("contrarian_doji_rsi", signals::mfpr::contrarian_doji_rsi),
        ("contrarian_engulfing_bbands", signals::mfpr::contrarian_engulfing_bbands),
        ("contrarian_euphoria_k_env", signals::mfpr::contrarian_euphoria_k_env),
        ("contrarian_piercing_stoch", signals::mfpr::contrarian_piercing_stoch),
        ("fibonacci_range", signals::trend_following::fibonacci_range),
        ("elder_impulse_1", signals::trend_following::elder_impulse_1),
        ("elder_impulse_2", signals::trend_following::elder_impulse_2),
        ("elder_impulse_3", signals::trend_following::elder_impulse_3),
        ("gri_index", signals::trend_following::gri_index),
        ("slope_indicator", signals::trend_following::slope_indicator),
        ("heikin_ashi", signals::trend_following::heikin_ashi),
        ("inside_candle", signals::trend_following::inside_candle),
        ("aroon_oscillator", signals::trend_following::aroon_oscillator),
        ("adx_indicator", signals::trend_following::adx_indicator),
        ("awesome", signals::trend_following::awesome_indicator),
        ("donchian_indicator", signals::trend_following::donchian_indicator),
        ("macd_change", signals::trend_following::macd_change),
        ("squeeze_momentum", signals::trend_following::squeeze_momentum),
        ("supertrend", signals::trend_following::supertrend_indicator),
        ("trend_intensity_ind", signals::trend_following::trend_intensity_ind),
        ("vertical_horizontal_cross", signals::trend_following::vertical_horizontal_cross),
        ("ichimoku_cloud", signals::trend_following::ichimoku_cloud),
        ("tf1_ma", signals::trend_following::tf1_ma),
        ("tf2_ma", signals::trend_following::tf2_ma),
        ("tf3_rsi_ma", signals::trend_following::tf3_rsi_ma),
        ("tf4_macd", signals::trend_following::tf4_macd),
        ("tf5_ma_slope", signals::trend_following::tf5_ma_slope),
        ("tf6_supertrend_flip", signals::trend_following::tf6_supertrend_flip),
        ("tf7_psar_ma", signals::trend_following::tf7_psar_ma),
        ("tf9_tii", signals::trend_following::tf9_tii),
        ("tf11_rsi_neutrality", signals::trend_following::tf11_rsi_neutrality),
        ("tf12_vama", signals::trend_following::tf12_vama),
        ("tf13_rsi_supertrend", signals::trend_following::tf13_rsi_supertrend),
        ("tf14_catapult", signals::trend_following::tf14_catapult),
        ("contrarian_aug_bbands", signals::bots::contrarian_aug_bbands),
        ("contrarian_bbands", signals::bots::contrarian_bbands),
        ("contrarian_dual_bbands", signals::bots::contrarian_dual_bbands),
        ("contrarian_countdown_cross", signals::bots::contrarian_countdown_cross),
        ("contrarian_countdown_duration", signals::bots::contrarian_countdown_duration),
        ("key_reversal", signals::bots::key_reversal),
        ("k_extreme_duration", signals::bots::k_extreme_duration),
        ("contrarian_countdown_extremes", signals::bots::contrarian_countdown_extremes),
        ("contrarian_demarker_cross", signals::bots::contrarian_demarker_cross),
        ("contrarian_demarker_extremes", signals::bots::contrarian_demarker_extremes),
        ("contrarian_disparity_extremes", signals::bots::contrarian_disparity_extremes),
        ("contrarian_fisher_duration", signals::bots::contrarian_fisher_duration),
        ("contrarian_fisher_extremes", signals::bots::contrarian_fisher_extremes),
        ("contrarian_real_range_extremes", signals::bots::contrarian_real_range_extremes),
        ("contrarian_rsi_cross", signals::bots::contrarian_rsi_cross),
        ("contrarian_rsi_divergences", signals::bots::contrarian_rsi_divergences),
        ("contrarian_rsi_duration", signals::bots::contrarian_rsi_duration),
        ("contrarian_rsi_extremes", signals::bots::contrarian_rsi_extremes),
        ("contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_cross),
        ("contrarian_stochastic_divergences", signals::bots::contrarian_stochastic_divergences),
        ("contrarian_stochastic_duration", signals::bots::contrarian_stochastic_duration),
        ("contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_extremes),
        ("contrarian_time_up_extremes", signals::bots::contrarian_time_up_extremes),
        ("contrarian_tsabm", signals::bots::contrarian_tsabm),
        ("pattern_differentials", signals::bots::pattern_differentials),
        ("pattern_engulfing", signals::bots::pattern_engulfing),
        ("pattern_fibonacci_timing", signals::bots::pattern_fibonacci_timing),
        ("pattern_hammer", signals::bots::pattern_hammer),
        ("pattern_marubozu", signals::bots::pattern_marubozu),
        ("pattern_piercing", signals::bots::pattern_piercing),
        ("pattern_td_camouflauge", signals::bots::pattern_td_camouflage),
        ("pattern_td_clop", signals::bots::pattern_td_clop),
        ("pattern_td_clopwin", signals::bots::pattern_td_clopwin),
        ("pattern_td_open", signals::bots::pattern_td_open),
        ("pattern_td_trap", signals::bots::pattern_td_trap),
        ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2),
        ("pattern_td_waldo_5", signals::bots::pattern_td_waldo_5),
        ("pattern_td_waldo_6", signals::bots::pattern_td_waldo_6),
        ("pattern_td_waldo_8", signals::bots::pattern_td_waldo_8),
        ("pattern_three_line_strike", signals::bots::pattern_three_line_strike),
        ("pattern_three_methods", signals::bots::pattern_three_methods),
    ];

    for (name, function) in signal_functions {
        signals.push(Signal {
            name: name.to_string(),
            f: Arc::new(function),
        });
    }

    Ok(run_all_backtests(lf, signals).await?) // This needs to be awaited

}

async fn backtest_helper(u: &str) -> Result<(), Box<dyn std::error::Error>> {

    // Run all the backtests and store them in a vec
    let file_path = format!("./data/{}.parquet", u);
    let lf = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())?;
    // println!("lf {:?}", lf.clone().collect().unwrap());

    // Collect the unique tickers into a DataFrame
    let unique_tickers_df = lf
        .clone()
        .select([col("Ticker").unique().alias("unique_tickers")])
        .collect()?;

    // Assuming the 'unique_tickers' column is of type Utf8
    let unique_tickers_series = unique_tickers_df.column("unique_tickers")?;

    let dir_path = "./output";
    let mut filenames: Vec<String> = Vec::new();
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        // Check if the entry is a file and has a `.parquet` extension
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            // Convert the file stem to a String and push it into the filenames vector
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                filenames.push(stem.to_owned());
            }
        }
    }

    // Convert filenames to a HashSet
    let filenames_set: HashSet<String> = filenames.into_iter().collect(); 

    // Filter out tickers that are already done
    let unique_tickers: Vec<String> = unique_tickers_series.utf8()?
        .into_iter()
        .filter_map(|value| value.map(|v| v.to_string()))
        .filter(|ticker| !filenames_set.contains(ticker))
        .take(10) 
        .collect();

    let num_tickers = unique_tickers.clone().len();

    // Collect futures for processing each ticker
    let futures: Vec<_> = unique_tickers.into_iter().enumerate().map(|(index, ticker)| {
        let lf_clone = lf.clone(); // Clone outside the async block
        let ticker_clone: String = ticker.clone();

        async move {
            let filtered_lf = lf_clone.clone().filter(col("Ticker").eq(lit(ticker)));
            println!("Running {} '{}' backtests: {} of {}", u, ticker_clone, index, num_tickers);

            match run_backtests(filtered_lf).await {
                Ok(backtest_results) => {
                    if let Err(e) = parquet_save_backtest(backtest_results, ticker_clone.clone()).await {
                        eprintln!("Error saving '{}' backtest to parquet: {}", ticker_clone, e);
                    }
                },
                Err(e) => eprintln!("Error running '{}' backtests: {}", ticker_clone, e),
            }
        }
    }).collect();

    // Await all futures to complete
    futures::future::join_all(futures).await;

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // if let Err(e) = pg_create_backtest_table().await {
    //     eprintln!("Error creating table: {}", e);
    // }

    // Params
    let price_download = false;
    // let univ = ["LC1","LC2","MC1","MC2","SC1","SC2","SC3","SC4","Micro1","Micro2"];
    let univ = ["LC1", "MC1", "SC1", "Micro1"];
    let univ_vec: Vec<String> = univ.iter().map(|&s| s.into()).collect();

    let _ = match price_download {
        false => println!("skipping download..."),
        _ => create_price_files(univ_vec)?,
    };

    for _ in 0..30 {

        for u in univ {
            let _ = backtest_helper(u).await;
        }

    }

    Ok(())
}


#[cfg(test)]
mod tests {
    #[test]
    fn test_summarize_performance_file() {

        let u = "LC1";
        // let output_path: String = format!("backtest_output_{}.parquet", u);
        // Use `super` to refer to the parent module where `summarize_performance` is defined.
        super::summarize_performance(u.to_string()).expect("error reading output files");
    }
}

