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

pub async fn select_backtests(lf: LazyFrame, tag: &str) -> Result<Vec<Backtest>, Box<dyn StdError>> {
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
        ("adx_indicator", signals::trend_following::adx_indicator, 0.0),
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
        ("candlestick_double_trouble", signals::mfpr::candlestick_double_trouble, 2.0),
    ];


    // let crypto_signal_functions: Vec<(&str, SignalFunction, param)> = vec![
    //     (
    //         "squeeze_momentum",
    //         signals::trend_following::squeeze_momentum,
    //         0.0,
    //     ),
    //     (
    //         "vertical_horizontal_cross",
    //         signals::trend_following::vertical_horizontal_cross,
    //         0.0,
    //     ),
    //     ("hammer", signals::mfpr::hammer, 0.0),
    //     ("double_trouble", signals::mfpr::double_trouble_1, 0.0),
    //     (
    //         "donchian_indicator",
    //         signals::trend_following::donchian_indicator,
    //         0.0,
    //     ),
    //     ("key_reversal", signals::bots::key_reversal, 0.0),
    //     ("pattern_marubozu", signals::bots::pattern_marubozu, 0.0),
    //     ("h", signals::mfpr::h),
    //     ("spinning_top", signals::mfpr::spinning_top, 0.0),
    //     (
    //         "candlestick_double_trouble",
    //         signals::mfpr::candlestick_double_trouble,
    //         2.0,
    //     ),
    //     (
    //         "trend_fol_2trouble_rsi",
    //         signals::mfpr::trend_fol_2trouble_rsi,
    //         0.0,
    //     ),
    //     ("heikin_ashi", signals::trend_following::heikin_ashi, 0.0),
    //     (
    //         "heikin_ashi_double_trouble",
    //         signals::mfpr::heikin_ashi_double_trouble,
    //         0.0,
    //     ),
    //     ("tf12_vama", signals::trend_following::tf12_vama, 0.0),
    //     ("tf9_tii", signals::trend_following::tf9_tii, 0.0),
    //     (
    //         "aroon_oscillator",
    //         signals::trend_following::aroon_oscillator,
    //         0.0,
    //     ),
    //     (
    //         "contrarian_rsi_extremes",
    //         signals::bots::contrarian_rsi_extremes,
    //         0.0,
    //     ),
    //     ("bottle", signals::mfpr::bottle, 0.0),
    //     ("macd_change", signals::trend_following::macd_change, 0.0),
    //     ("star", signals::mfpr::star, 0.0),
    //     ("mirror", signals::mfpr::mirror, 0.0),
    //     ("marubozu", signals::mfpr::marubozu, 0.0),
    //     (
    //         "contrarian_disparity_extremes",
    //         signals::bots::contrarian_disparity_extremes,
    //         0.0,
    //     ),
    //     ("pattern_piercing", signals::bots::pattern_piercing, 0.0),
    //     (
    //         "pattern_td_camouflauge",
    //         signals::bots::pattern_td_camouflage,
    //         0.0,
    //     ),
    //     ("pattern_td_clopwin", signals::bots::pattern_td_clopwin, 0.0),
    //     ("gri_index", signals::trend_following::gri_index),
    //     ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2, 0.0),
    //     ("pattern_td_waldo_5", signals::bots::pattern_td_waldo_5, 0.0),
    //     ("pattern_td_waldo_6", signals::bots::pattern_td_waldo_6, 0.0),
    //     ("pattern_td_waldo_8", signals::bots::pattern_td_waldo_8, 0.0),
    //     (
    //         "trend_fol_h_trend_intensity",
    //         signals::mfpr::trend_fol_h_trend_intensity,
    //         0.0,
    //     ),
    //     ("heikin_ashi_euphoria", signals::mfpr::heikin_ashi_euphoria),
    //     ("euphoria", signals::mfpr::euphoria, 0.0),
    //     ("engulfing", signals::mfpr::engulfing, 0.0),
    //     ("doji", signals::mfpr::doji),
    //     ("stick_sandwich", signals::mfpr::stick_sandwich, 0.0),  
    //     ("contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_extremes, 0.0),
    //     ("contrarian_stochastic_divergences", signals::bots::contrarian_stochastic_divergences, 0.0),
    //     ("contrarian_stochastic_duration", signals::bots::contrarian_stochastic_duration, 0.0),
    //     ("contrarian_stochastic_cross", signals::bots::contrarian_stochastic_cross, 0.0),
    //     ("contrarian_rsi_divergences", signals::bots::contrarian_rsi_divergences, 0.0),
    //     ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma, 0.0),
    //     ("tf1_ma", signals::trend_following::tf1_ma, 0.0),
    //     ("tf2_ma", signals::trend_following::tf2_ma, 0.0),
    //     ("tf3_rsi_ma", signals::trend_following::tf3_rsi_ma, 0.0),
    //     ("tf4_macd", signals::trend_following::tf4_macd, 0.0),
    //     ("tf5_ma_slope", signals::trend_following::tf5_ma_slope, 0.0),
    //     ("tf6_supertrend_flip", signals::trend_following::tf6_supertrend_flip, 0.0),
    //     ("tf7_psar_ma", signals::trend_following::tf7_psar_ma, 0.0),
    //     (
    //         "trend_fol_marubozu_k_vol_bands",
    //         signals::mfpr::trend_fol_marubozu_k_vol_bands,
    //         0.0,
    //     ),
    // ];

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
        ("adx_indicator", signals::trend_following::adx_indicator, 0.0),
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
        ("adx_indicator", signals::trend_following::adx_indicator, 0.0),
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
        ("adx_indicator", signals::trend_following::adx_indicator, 0.0),
    ];

    let signal_functions: Vec<(&str, SignalFunctionWithParam, f64)> = vec![
        ("three_candles", signals::mfpr::three_candles, 0.0),
        // ("trend_fol_3candle_ma", signals::mfpr::trend_following_3candle_ma),
        // ("pattern_marubozu", signals::bots::pattern_marubozu),
        // ("pattern_hammer", signals::bots::pattern_hammer),
        // ("hammer", signals::mfpr::hammer),
        // ("tweezers", signals::mfpr::tweezers),
        // ("hikkake", signals::mfpr::hikkake),
        // ("adx_indicator", signals::trend_following::adx_indicator),
        // ("donchian_indicator", signals::trend_following::donchian_indicator),
        // ("tower", signals::mfpr::tower),
        // ("slingshot", signals::mfpr::slingshot),
        // ("quintuplets_0005", signals::mfpr::quintuplets_0005),
        // ("marubozu", signals::mfpr::marubozu),
        // ("tasuki", signals::mfpr::tasuki),
        // ("three_methods", signals::mfpr::three_methods),
        // ("bottle", signals::mfpr::bottle),
        // ("double_trouble", signals::mfpr::double_trouble_1),
        // ("h", signals::mfpr::h),
        // ("quintuplets_2", signals::mfpr::quintuplets_2),
        // ("quintuplets_10", signals::mfpr::quintuplets_10),
        // ("quintuplets_50", signals::mfpr::quintuplets_50),
        // ("abandoned_baby", signals::mfpr::abandoned_baby),
        // ("doji", signals::mfpr::doji),
        // ("engulfing", signals::mfpr::engulfing),
        // ("harami_flexible", signals::mfpr::harami_flexible),
        // ("harami_strict", signals::mfpr::harami_strict),
        // ("inside_up_down", signals::mfpr::inside_up_down),
        // ("on_neck", signals::mfpr::on_neck),
        // ("piercing", signals::mfpr::piercing),
        // ("spinning_top", signals::mfpr::spinning_top),
        // ("star", signals::mfpr::star),
        // ("stick_sandwich", signals::mfpr::stick_sandwich),
        // ("barrier", signals::mfpr::barrier),
        // ("blockade", signals::mfpr::blockade),
        // ("doppleganger", signals::mfpr::doppleganger),
        // ("euphoria", signals::mfpr::euphoria),
        // ("mirror", signals::mfpr::mirror),
        // ("shrinking", signals::mfpr::shrinking),
        // ("heikin_ashi_doji", signals::mfpr::heikin_ashi_doji),
        // ("heikin_ashi_double_trouble", signals::mfpr::heikin_ashi_double_trouble),
        // ("heikin_ashi_euphoria", signals::mfpr::heikin_ashi_euphoria),
        // ("heikin_ashi_tasuki", signals::mfpr::heikin_ashi_tasuki),
        // ("candlestick_doji", signals::mfpr::candlestick_doji),
        // ("candlestick_double_trouble", signals::mfpr::candlestick_double_trouble),
        // ("candlestick_euphoria", signals::mfpr::candlestick_euphoria),
        // ("candlestick_tasuki", signals::mfpr::candlestick_tasuki),
        // ("trend_fol_bottle_stoch", signals::mfpr::trend_fol_bottle_stoch),
        // ("trend_fol_2trouble_rsi", signals::mfpr::trend_fol_2trouble_rsi),
        // ("trend_fol_h_trend_intensity", signals::mfpr::trend_fol_h_trend_intensity),
        // ("trend_fol_marubozu_k_vol_bands", signals::mfpr::trend_fol_marubozu_k_vol_bands),
        // ("contrarian_barrier_rsi_atr", signals::mfpr::contrarian_barrier_rsi_atr),
        // ("contrarian_doji_rsi", signals::mfpr::contrarian_doji_rsi),
        // ("contrarian_engulfing_bbands", signals::mfpr::contrarian_engulfing_bbands),
        // ("contrarian_euphoria_k_env", signals::mfpr::contrarian_euphoria_k_env),
        // ("contrarian_piercing_stoch", signals::mfpr::contrarian_piercing_stoch),
        // ("fibonacci_range", signals::trend_following::fibonacci_range),
        // ("elder_impulse_1", signals::trend_following::elder_impulse_1),
        // ("elder_impulse_2", signals::trend_following::elder_impulse_2),
        // ("elder_impulse_3", signals::trend_following::elder_impulse_3),
        // ("gri_index", signals::trend_following::gri_index),
        // ("slope_indicator", signals::trend_following::slope_indicator),
        // ("heikin_ashi", signals::trend_following::heikin_ashi),
        // ("inside_candle", signals::trend_following::inside_candle),
        // ("aroon_oscillator", signals::trend_following::aroon_oscillator),
        // ("awesome", signals::trend_following::awesome_indicator),
        // ("macd_change", signals::trend_following::macd_change),
        // ("squeeze_momentum", signals::trend_following::squeeze_momentum),
        // ("supertrend", signals::trend_following::supertrend_indicator),
        // ("trend_intensity_ind", signals::trend_following::trend_intensity_ind),
        // ("vertical_horizontal_cross", signals::trend_following::vertical_horizontal_cross),
        // ("ichimoku_cloud", signals::trend_following::ichimoku_cloud),
        // ("tf1_ma", signals::trend_following::tf1_ma),
        // ("tf2_ma", signals::trend_following::tf2_ma),
        // ("tf3_rsi_ma", signals::trend_following::tf3_rsi_ma),
        // ("tf4_macd", signals::trend_following::tf4_macd),
        // ("tf5_ma_slope", signals::trend_following::tf5_ma_slope),
        // ("tf6_supertrend_flip", signals::trend_following::tf6_supertrend_flip),
        // ("tf7_psar_ma", signals::trend_following::tf7_psar_ma),
        // ("tf9_tii", signals::trend_following::tf9_tii),
        // ("tf10_ma", signals::trend_following::tf10_ma),
        // ("tf11_rsi_neutrality", signals::trend_following::tf11_rsi_neutrality),
        // ("tf12_vama", signals::trend_following::tf12_vama),
        // ("tf13_rsi_supertrend", signals::trend_following::tf13_rsi_supertrend),
        // ("tf14_catapult", signals::trend_following::tf14_catapult),
        // ("contrarian_aug_bbands", signals::bots::contrarian_aug_bbands),
        // ("contrarian_bbands", signals::bots::contrarian_bbands),
        // ("contrarian_dual_bbands", signals::bots::contrarian_dual_bbands),
        // ("contrarian_countdown_cross", signals::bots::contrarian_countdown_cross),
        // ("contrarian_countdown_duration", signals::bots::contrarian_countdown_duration),
        // ("key_reversal", signals::bots::key_reversal),
        // ("k_extreme_duration", signals::bots::k_extreme_duration),
        // ("contrarian_countdown_extremes", signals::bots::contrarian_countdown_extremes),
        // ("contrarian_demarker_cross", signals::bots::contrarian_demarker_cross),
        // ("contrarian_demarker_extremes", signals::bots::contrarian_demarker_extremes),
        // ("contrarian_disparity_extremes", signals::bots::contrarian_disparity_extremes),
        // ("contrarian_fisher_duration", signals::bots::contrarian_fisher_duration),
        // ("contrarian_fisher_extremes", signals::bots::contrarian_fisher_extremes),
        // ("contrarian_real_range_extremes", signals::bots::contrarian_real_range_extremes),
        // ("contrarian_rsi_cross", signals::bots::contrarian_rsi_cross),
        // ("contrarian_rsi_divergences", signals::bots::contrarian_rsi_divergences),
        // ("contrarian_rsi_duration", signals::bots::contrarian_rsi_duration),
        // ("contrarian_rsi_extremes", signals::bots::contrarian_rsi_extremes),
        // ("contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_cross),
        // ("contrarian_stochastic_divergences", signals::bots::contrarian_stochastic_divergences),
        // ("contrarian_stochastic_duration", signals::bots::contrarian_stochastic_duration),
        // ("contrarian_stochastic_extremes", signals::bots::contrarian_stochastic_extremes),
        // ("contrarian_time_up_extremes", signals::bots::contrarian_time_up_extremes),
        // ("contrarian_tsabm", signals::bots::contrarian_tsabm),
        // ("pattern_differentials", signals::bots::pattern_differentials),
        // ("pattern_engulfing", signals::bots::pattern_engulfing),
        // ("pattern_fibonacci_timing", signals::bots::pattern_fibonacci_timing),
        // ("pattern_piercing", signals::bots::pattern_piercing),
        // ("pattern_td_camouflauge", signals::bots::pattern_td_camouflage),
        // ("pattern_td_clop", signals::bots::pattern_td_clop),
        // ("pattern_td_clopwin", signals::bots::pattern_td_clopwin),
        // ("pattern_td_open", signals::bots::pattern_td_open),
        // ("pattern_td_trap", signals::bots::pattern_td_trap),
        // ("pattern_td_waldo_2", signals::bots::pattern_td_waldo_2),
        // ("pattern_td_waldo_5", signals::bots::pattern_td_waldo_5),
        // ("pattern_td_waldo_6", signals::bots::pattern_td_waldo_6),
        // ("pattern_td_waldo_8", signals::bots::pattern_td_waldo_8),
        // ("pattern_three_line_strike", signals::bots::pattern_three_line_strike),
        // ("pattern_three_methods", signals::bots::pattern_three_methods),
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

    for (name, function, param) in selected_signal_functions {
        signals.push(Signal {
            name: name.to_string(),
            func: Arc::new(function), 
            param: param,
        });
        // signals.push(Signal {
        //     name: name.to_string(),
        //     f: Arc::new(function),
        // });
    }

    // needs to be awaited
    Ok(run_all_backtests(lf, signals).await?)
}

async fn backtest_helper(
    path: String,
    u: &str,
    batch_size: usize,
    production: bool,
) -> Result<(), Box<dyn StdError>> {
    // println!("here 0");

    let folder = if production { "production" } else { "testing" };
    let file_path = format!("{}/data/{}/{}.csv", path, folder, u);
    println!("file_path: {}", file_path);
    
    let lf = read_price_file(file_path).await?;

    // println!("{:?}", lf.clone().collect());
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
            // Convert the file stem to a String and push it into the filenames vector
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                filenames.push(stem.to_owned());
            }
        }
    }

    // Convert filenames to a HashSet
    let filenames_set: HashSet<String> = filenames.into_iter().collect();

    // Filter out tickers that are already done
    let needed: Vec<String> = unique_tickers_series
        .str()?
        .into_iter()
        .filter_map(|value| value.map(|v| v.to_string()))
        .filter(|ticker| !filenames_set.contains(ticker))
        .take(5) // used for testing purposes
        .collect();

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
                        (false, _) => "testing",
                        (true, "Crypto") => "crypto",
                        (true, "Micro") => "micro",
                        (true, "SC") => "sc",
                        (true, "MC") => "mc",
                        (true, "LC") => "lc",
                        (_, _) => "prod",
                    };

                    match select_backtests(filtered_lf, tag).await {
                        Ok(backtest_results) => {
                            if let Err(e) = parquet_save_backtest(
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
                    "Running {} '{}' backtests: {} of {}",
                    u, ticker, completed, out_of
                );
            }
        }
    }

    Ok(())
}

pub fn print_dataframe_vertically(df: &DataFrame) {
    for idx in 0..df.height() {
        println!("Row {}:", idx);
        match df.get_row(idx) {
            Ok(row) => {
                for (col_name, value) in df.get_column_names().iter().zip(row.0.iter()) {
                    println!("  {}: {:?}", col_name, value);
                }
            }
            Err(e) => eprintln!("Error retrieving row {}: {}", idx, e),
        }
        println!(); // Add a blank line between rows
    }
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
    let batch_size: usize = 3;

    // collect command line args
    let args: Vec<String> = env::args().collect();
    let univ_str: &str = args.get(1).unwrap_or(&default_univ);
    let production_str = args.get(2).unwrap_or(&default_production);
    let path = args.get(3).unwrap_or(&default_path);

    let production = production_str == "production";
    // println!("Production mode: {}\n", production);

    let univ: &[&str] = match univ_str {
        "SC" => &["SC1", "SC2", "SC3", "SC4"],
        "MC" => &["MC1", "MC2"],
        "LC" => &["LC1", "LC2"],
        "Micro" => &["Micro1", "Micro2", "Micro3", "Micro4"],
        "Stocks" => &["LC1"],
        _ => &["Crypto"],
    };
    let univ_vec: Vec<String> = univ.iter().map(|&s| s.into()).collect();

    // delete prior production files before next run
    if production {
        let paths = vec![
            format!("{}/output/production", path),
            format!("{}/output_crypto/production", path),
            format!("{}/data/production", path),
        ];
        for p in paths {
            delete_all_files_in_folder(p).await?;
        }
    }

    // create price files if they don't already exist (from clickhouse tables)
    create_price_files(univ_vec.clone(), production.clone()).await?;

    for u in univ {
        println!("Backtest starting: {}", u);
        let _ = backtest_helper(path.to_string(), u, batch_size, production).await;
    }
    // println!("Backtest finished");

    if production {
        if univ_vec.contains(&"Crypto".to_string()) {
            let (datetag, _out) =
                summary_performance_file(path.to_string(), production, false, univ_vec.clone())
                    .await?;

            if let Err(e) = score(&datetag, false).await {
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

            if let Err(e) = score(&datetag, true).await {
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

        match summary_performance_file(path, production, stocks, univ_vec).await {
            Ok((_datetag, out)) => {
                // println!("{} Average Performance by Strategy:\n {:?}", datetag, out);
                print_dataframe_vertically(&out);
            },
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

// pub async fn single_backtest() -> Result<(), Box<dyn StdError>> {

//     // Step 1: Define an ad-hoc signal function
//     let custom_signal_function: SignalFunction = |df: DataFrame| {
//         let len = df.height();
//         let mut buy = vec![0; len];
//         let mut sell = vec![0; len];

//         // Example logic: Buy if "Close" > "Open", Sell if "Close" < "Open"
//         if let (Ok(close), Ok(open)) = (df.column("Close"), df.column("Open")) {
//             for i in 0..len {
//                 if close.get(i).unwrap() > open.get(i).unwrap() {
//                     buy[i] = 1;
//                 } else if close.get(i).unwrap() < open.get(i).unwrap() {
//                     sell[i] = -1;
//                 }
//             }
//         }

//         BuySell { buy, sell }
//     };

//     // Step 2: Wrap the signal function in a Signal struct
//     let custom_signal = Signal {
//         name: "custom_signal".to_string(),
//         f: Arc::new(custom_signal_function),
//     };

//     // Step 3: Load your data into a LazyFrame
//     let file_path = "/Users/rogerbos/rust_home/backtester/data/testing/crypto.csv";
//     let lf = read_price_file(file_path.to_string()).await?;
//     let ticker = "btc";
//     let filtered_lf = lf.filter(col("Ticker").eq(lit(ticker)));

//     // Step 4: Run the backtest using the custom signal
//     let backtest_result = sig(filtered_lf, &custom_signal).await?;

//     // Step 5: Print the backtest result
//     let _ = showbt(backtest_result);
//     Ok(())
// }


pub async fn single_backtest(signal: Signal) -> Result<(), Box<dyn StdError>> {
    // Step 4: Load your data into a LazyFrame
    let file_path = "/Users/rogerbos/rust_home/backtester/data/testing/crypto.csv";
    let lf = read_price_file(file_path.to_string()).await?;
    let ticker = "btc";
    let filtered_lf = lf.filter(col("Ticker").eq(lit(ticker)));

    // Step 5: Run the backtest using the signal
    // let p = 2.0; //param.unwrap_or(0.0); // Use default value if no parameter is provided
    let signal_name = signal.name.clone();
    let backtest_result = sig(filtered_lf, *signal.func.clone(), signal.param).await?;
    // let backtest_result = sig(filtered_lf, &signal).await?;
    // let backtest_result = match signal.signal_type {
    //     SignalType::WithoutParam(ref func) => sig(filtered_lf, &signal).await?,
    //     SignalType::WithParam(ref func) => {
    //         sig_with_param(filtered_lf, &signal, p).await?
    //     }
    // };

    // Step 6: Print the backtest result
    println!("Backtest result for signal '{}':", signal_name);
    let _ = showbt(backtest_result);
    Ok(())
}

// cargo test testme -- --nocapture
pub async fn testme() -> Result<(), Box<dyn StdError>> {
    let signal = Signal {
        name: "candlestick_double_trouble".to_string(),
        func: Arc::new(signals::mfpr::candlestick_double_trouble), 
        param: 2.0,
    };
    let _ = single_backtest(signal).await?;
    Ok(())
}
