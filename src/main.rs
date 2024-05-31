#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use backtester::*;
use polars::prelude::*;
use std::{collections::HashSet, env, fs, error::Error as StdError};
use tokio;

mod signals {
    pub mod bots; // book of trading strategies
    pub mod mfpr; // mastering financial pattern recognition
    pub mod technical;
    pub mod trend_following;
}

pub async fn run_backtests(lf: LazyFrame, tag: &str) -> Result<Vec<Backtest>, Box<dyn StdError>> {

    let mut signals: Vec<Signal> = Vec::new();
    let prod_signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("three_candles", signals::mfpr::three_candles),
        ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        ("pattern_marubozu", signals::bots::pattern_marubozu),
        ("pattern_hammer", signals::bots::pattern_hammer),
        ("hammer", signals::mfpr::hammer),
        ("tweezers", signals::mfpr::tweezers),
        ("hikkake", signals::mfpr::hikkake),
        ("adx_indicator", signals::trend_following::adx_indicator),
        ("donchian_indicator", signals::trend_following::donchian_indicator),
        ("tower", signals::mfpr::tower),
        ("slingshot", signals::mfpr::slingshot),
        ("quintuplets_0005", signals::mfpr::quintuplets_0005),
    ];
        
    let crypto_signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("squeeze_momentum", signals::trend_following::squeeze_momentum),
        ("vertical_horizontal_cross", signals::trend_following::vertical_horizontal_cross),
        ("hammer", signals::mfpr::hammer),
        ("double_trouble", signals::mfpr::double_trouble_1),
        ("donchian_indicator", signals::trend_following::donchian_indicator),
        ("key_reversal", signals::bots::key_reversal),
        ("pattern_marubozu", signals::bots::pattern_marubozu),
        ("h", signals::mfpr::h),
        ("spinning_top", signals::mfpr::spinning_top),
        ("candlestick_double_trouble", signals::mfpr::candlestick_double_trouble),
        ("trend_fol_2trouble_rsi", signals::mfpr::trend_fol_2trouble_rsi),
        ("heikin_ashi", signals::trend_following::heikin_ashi),
        ("heikin_ashi_double_trouble", signals::mfpr::heikin_ashi_double_trouble),
        ("tf12_vama", signals::trend_following::tf12_vama),
        ("tf9_tii", signals::trend_following::tf9_tii),
        ("aroon_oscillator", signals::trend_following::aroon_oscillator),
        ("contrarian_rsi_extremes", signals::bots::contrarian_rsi_extremes),
        ("bottle", signals::mfpr::bottle),
        ("macd_change", signals::trend_following::macd_change),
        ("star", signals::mfpr::star),
        ("mirror", signals::mfpr::mirror),
        ("marubozu", signals::mfpr::marubozu),
        ("contrarian_disparity_extremes", signals::bots::contrarian_disparity_extremes),
        ("pattern_piercing", signals::bots::pattern_piercing),
        ("pattern_td_camouflauge", signals::bots::pattern_td_camouflage),
        ("pattern_td_clopwin", signals::bots::pattern_td_clopwin),
        ("gri_index", signals::trend_following::gri_index),
        ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2),
    ];
        
    let micro_signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("hammer", signals::mfpr::hammer),
        ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        ("three_candles", signals::mfpr::three_candles),
        ("hikkake", signals::mfpr::hikkake),
        ("contrarian_piercing_stoch", signals::mfpr::contrarian_piercing_stoch),
        ("piercing", signals::mfpr::piercing),
        ("tasuki", signals::mfpr::tasuki),
        ("heikin_ashi_double_trouble", signals::mfpr::heikin_ashi_double_trouble),
        ("heikin_ashi_tasuki", signals::mfpr::heikin_ashi_tasuki),
        ("pattern_hammer", signals::bots::pattern_hammer),
        ("contrarian_engulfing_bbands", signals::mfpr::contrarian_engulfing_bbands),
        ("h", signals::mfpr::h),
        ("double_trouble", signals::mfpr::double_trouble_1),
        ("candlestick_double_trouble", signals::mfpr::candlestick_double_trouble),
        ("candlestick_tasuki", signals::mfpr::candlestick_tasuki),
        ("blockade", signals::mfpr::blockade),
        ("trend_fol_h_trend_intensity", signals::mfpr::trend_fol_h_trend_intensity)
    ];
    
    let sc_signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("tweezers", signals::mfpr::tweezers),
        ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        ("three_candles", signals::mfpr::three_candles),
        ("pattern_marubozu", signals::bots::pattern_marubozu),
        ("pattern_hammer", signals::bots::pattern_hammer),
        ("adx_indicator", signals::trend_following::adx_indicator),
        ("hammer", signals::mfpr::hammer),
        ("tower", signals::mfpr::tower),
        ("hikkake", signals::mfpr::hikkake),
    ];
        
    let mc_signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        ("three_candles", signals::mfpr::three_candles),
        ("pattern_marubozu", signals::bots::pattern_marubozu),
        ("hikkake", signals::mfpr::hikkake),
        ("adx_indicator", signals::trend_following::adx_indicator),
        ("tower", signals::mfpr::tower),
        ("pattern_hammer", signals::bots::pattern_hammer),
        ("hammer", signals::mfpr::hammer),
    ];
    
    let lc_signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("tweezers", signals::mfpr::tweezers),
        ("pattern_marubozu", signals::bots::pattern_marubozu),
        ("pattern_hammer", signals::bots::pattern_hammer),
        ("slingshot", signals::mfpr::slingshot),
        ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        ("tower", signals::mfpr::tower),
        ("three_candles", signals::mfpr::three_candles),
        ("adx_indicator", signals::trend_following::adx_indicator)
    ];
        
    let signal_functions: Vec<(&str, SignalFunction)> = vec![
        ("three_candles", signals::mfpr::three_candles),
        ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        ("pattern_marubozu", signals::bots::pattern_marubozu),
        ("pattern_hammer", signals::bots::pattern_hammer),
        ("hammer", signals::mfpr::hammer),
        ("tweezers", signals::mfpr::tweezers),
        ("hikkake", signals::mfpr::hikkake),
        ("adx_indicator", signals::trend_following::adx_indicator),
        ("donchian_indicator", signals::trend_following::donchian_indicator),
        ("tower", signals::mfpr::tower),
        ("slingshot", signals::mfpr::slingshot),
        ("quintuplets_0005", signals::mfpr::quintuplets_0005),
        ("marubozu", signals::mfpr::marubozu),
        ("tasuki", signals::mfpr::tasuki),
        ("three_methods", signals::mfpr::three_methods),
        ("bottle", signals::mfpr::bottle),
        ("double_trouble", signals::mfpr::double_trouble_1),
        ("h", signals::mfpr::h),
        ("quintuplets_2", signals::mfpr::quintuplets_2),
        ("quintuplets_10", signals::mfpr::quintuplets_10),
        ("quintuplets_50", signals::mfpr::quintuplets_50),
        ("abandoned_baby", signals::mfpr::abandoned_baby),
        ("doji", signals::mfpr::doji),
        ("engulfing", signals::mfpr::engulfing),
        ("harami_flexible", signals::mfpr::harami_flexible),
        ("harami_strict", signals::mfpr::harami_strict),
        ("inside_up_down", signals::mfpr::inside_up_down),
        ("on_neck", signals::mfpr::on_neck),
        ("piercing", signals::mfpr::piercing),
        ("spinning_top", signals::mfpr::spinning_top),
        ("star", signals::mfpr::star),
        ("stick_sandwich", signals::mfpr::stick_sandwich),
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
        ("awesome", signals::trend_following::awesome_indicator),
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
        ("tf10_ma", signals::trend_following::tf10_ma),
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

    let selected_signal_functions = match tag {
        "lc" => lc_signal_functions,
        "mc" => mc_signal_functions,
        "sc" => sc_signal_functions,
        "micro" => micro_signal_functions,
        "crypto" => crypto_signal_functions,
        "prod" => prod_signal_functions,
        _ => signal_functions,
    };

    for (name, function) in selected_signal_functions {
        signals.push(Signal {
            name: name.to_string(),
            f: Arc::new(function),
        });
    }
    
    // needs to be awaited
    Ok(run_all_backtests(lf, signals).await?)
}

async fn backtest_helper(path: String, u: &str, batch_size: usize, production: bool) -> Result<(), Box<dyn StdError>> {

    let folder = if production { "production".to_string() } else { "testing".to_string() };
    let file_path = format!("{}/data/{}/{}.parquet", path, folder, u);
    let lf = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())?;

    // Collect the unique tickers into a DataFrame
    let unique_tickers_df = lf
        .clone()
        .select([col("Ticker").unique().alias("unique_tickers")])
        .collect()?;

    // Assuming the 'unique_tickers' column is of type Utf8
    let unique_tickers_series = unique_tickers_df.column("unique_tickers")?;

    let dir_path = match u {
        "Crypto" => format!("{}/output_crypto/{}", path, folder),
        _ => format!("{}/output/{}", path, folder),
    };
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
    let needed: Vec<String> = unique_tickers_series.utf8()?
        .into_iter()
        .filter_map(|value| value.map(|v| v.to_string()))
        .filter(|ticker| !filenames_set.contains(ticker))
        .collect();
    
    let out_of = needed.len();
    let mut remaining = out_of;
    for i in (0..needed.len()).step_by(batch_size) {
        let last = if remaining < batch_size { remaining } else { batch_size };
        let unique_tickers = &needed[i..i+last];
        // collect futures for processing each ticker
        let futures: Vec<_> = unique_tickers.into_iter().map(|ticker| {
            // clone outside the async block
            let lf_clone = lf.clone(); 
            let ticker_clone: String = ticker.clone();
            let path_clone: String = path.clone();

            async move {
                let filtered_lf = lf_clone.filter(col("Ticker").eq(lit(ticker.to_string())));
                let tag: &str = match (production, u) {
                    (false, _) => "testing",
                    (true, "Crypto") => "crypto",
                    (true, "Micro") => "micro",
                    (true, "SC") => "sc",
                    (true, "MC") => "mc",
                    (true, "LC") => "lc",
                    (_, _) => "prod",
                };
                println!("Running {} '{}' backtests: {} of {} {}", u, ticker_clone, out_of - remaining, out_of, tag);
                match run_backtests(filtered_lf, tag).await {
                    Ok(backtest_results) => {
                        if let Err(e) = parquet_save_backtest(path_clone, backtest_results, u, ticker_clone, production).await {
                            eprintln!("Error saving backtest to parquet: {}", e);
                        }
                    },
                    Err(e) => eprintln!("Error running '{}' backtests: {}", ticker_clone, e),
                }
            }
        }).collect();
        // await all futures to complete
        futures::future::join_all(futures).await;
        remaining = remaining - last;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {

    // default params (overwritten by command line args)
    let default_path: String = "/Users/rogerbos/rust_home/backtester".to_string();
    let default_production: String = "production".to_string();
    let default_univ = "Crypto".to_string();
    let batch_size: usize = 10;

    // collect command line args
    let args: Vec<String> = env::args().collect();
    let univ_str: &str = args.get(1).unwrap_or(&default_univ);
    let production_str = args.get(2).unwrap_or(&default_production);
    let path = args.get(3).unwrap_or(&default_path);

    let production = production_str == "production";
    let univ: &[&str] = match univ_str {
        "SC" => &["SC1", "SC2", "SC3", "SC4"],
        "MC" => &["MC1", "MC2"],
        "LC" => &["LC1", "LC2"],
        "Micro" => &["Micro1","Micro2"],
        "stocks" => &["LC1", "LC2", "MC1", "MC2", "SC1", "SC2", "SC3", "SC4", "Micro1", "Micro2"],
        _ => &["Crypto"],
    };
    let univ_vec: Vec<String> = univ.iter().map(|&s| s.into()).collect();

    // delete prior production files before next run
    if production {
        let paths = vec!(
            format!("{}/output/production", path),
            format!("{}/output_crypto/production", path),
            format!("{}/data/production", path)
        );
        for path in paths {
            delete_all_files_in_folder(path).await?;
        }
    }
    // create price files if they don't already exist (from clickhouse tables)
    create_price_files(univ_vec.clone(), production.clone()).await?;

    for u in univ {
        println!("Test starting {}", u);
        let _ = backtest_helper(path.clone(), u, batch_size, production.clone()).await;
    }

    if production.clone() {

        if univ_vec.contains(&"Crypto".to_string()) {
            let datetag = summary_performance_file(path.clone(), production.clone(), false, univ_vec.clone()).await?;
            let _ = score(&datetag, false).await?;
        }        

        if univ_vec.contains(&"LC1".to_string()) ||
            univ_vec.contains(&"LC2".to_string()) ||
            univ_vec.contains(&"MC1".to_string()) ||
            univ_vec.contains(&"MC2".to_string()) ||
            univ_vec.contains(&"SC1".to_string()) ||
            univ_vec.contains(&"SC2".to_string()) ||
            univ_vec.contains(&"SC3".to_string()) ||
            univ_vec.contains(&"SC4".to_string()) ||
            univ_vec.contains(&"Micro1".to_string()) ||
            univ_vec.contains(&"Micro2".to_string()) {
                let datetag = summary_performance_file(path.clone(), production.clone(), true, univ_vec.clone()).await?;
                let _ = score(&datetag, true).await?;
        }        

    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_summarize_performance_file() {

        // Params
        let path: String = "/Users/rogerbos/rust_home/backtester".to_string();
        let production: bool = true;
        let stocks: bool = false;
       
        if let Err(e) = super::summary_performance_file(path, production, stocks).await {
            eprintln!("Error: {}", e);
        }
    }

    #[tokio::test]
    async fn test_score() {
        let datetag = "20240409";
        let stocks: bool = true;
        if let Err(e) = super::score(datetag, stocks).await {
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

pub async fn testme() -> Result<(), Box<dyn StdError>> {

    let ticker="US000000060777";

    let path: String = "/Users/rogerbos/rust_home/backtester".to_string();
    let file_path = format!("{}/data/production/Micro2.parquet", path);
    let lf = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())?;
    let df = lf.filter(col("Ticker").eq(lit(ticker.to_string())));

    let df1 = df.clone().collect()?;
    let df2 = df.clone().collect()?;
    let df3 = df.clone().collect()?;

    println!("filtered df: {:?}", df1);

    let s = signals::mfpr::candlestick_euphoria(df2); 
    println!("buysell: {:?}", s);
    
    let bt = backtest_performance(df3, s, "candlestick_euphoria");
    println!("backtest: {:?}", bt);

    Ok(())

}
