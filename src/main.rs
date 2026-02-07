#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]
use backtester::*;
use polars::prelude::*;
use std::{collections::HashSet, env, error::Error as StdError, fs, fs::File, process};
use tokio;
use clap::Parser;
use log::{info, debug, warn, error};

mod display;

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

    /// Output date suffix for testing mode (e.g., '20260204' uses folder 'testing_20260204')
    #[arg(short, long)]
    output: Option<String>,

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

mod strategy_config;

#[cfg(test)]
mod tests;

use backtester::config::{BacktestConfig, ExecutionMode, PathConfig};

pub async fn select_backtests(
    lf: LazyFrame,
    tag: &str,
    strategy_filter: Option<&str>,
) -> Result<Vec<(Backtest, Vec<Decision>)>, Box<dyn StdError>> {
    // Get strategies for the specified tag
    let strategy_functions = strategy_config::get_strategies_for_tag(tag);

    // Convert to Signal objects
    let mut signals: Vec<Signal> = strategy_functions
        .iter()
        .map(|(name, func, param)| Signal {
            name: (*name).into(),
            func: Arc::new(*func),
            param: *param,
        })
        .collect();

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

    // Run all backtests
    Ok(run_all_backtests(lf, signals).await?)
}

/// Load price data and return LazyFrame with latest date
async fn load_price_data(
    paths: &PathConfig,
    universe: &str,
    mode: ExecutionMode,
) -> Result<(LazyFrame, String), Box<dyn StdError>> {
    let file_path = paths.data_file(universe, mode);
    let lf = read_price_file(file_path).await?;

    // Show latest date in the price data
    let latest_date_df = lf.clone().select([col("Date").max()]).collect()?;
    let latest_date = latest_date_df.column("Date")?.get(0)?.to_string();
    println!("{}", display::format_price_loaded(universe, &latest_date));

    Ok((lf, latest_date))
}

/// Extract unique tickers from LazyFrame
fn extract_unique_tickers(lf: &LazyFrame) -> Result<Vec<String>, Box<dyn StdError>> {
    let unique_tickers_df = lf
        .clone()
        .select([col("Ticker").unique().alias("unique_tickers")])
        .collect()?;

    let unique_tickers_series = unique_tickers_df.column("unique_tickers")?;

    Ok(unique_tickers_series
        .str()?
        .into_iter()
        .filter_map(|value| value.map(|v| v.to_string()))
        .collect())
}

/// Scan output directory for already-processed tickers
fn load_processed_tickers(
    paths: &PathConfig,
    universe: &str,
    mode: ExecutionMode,
) -> Result<HashSet<String>, Box<dyn StdError>> {
    let dir_path = paths.output_dir(universe, mode);
    let mut processed = HashSet::new();

    // If directory doesn't exist yet, return empty set
    match fs::read_dir(&dir_path) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
                    // Skip decision files (they have "_decisions" suffix)
                    if let Some(filename) = path.file_stem() {
                        let filename_str = filename.to_string_lossy();
                        if filename_str.contains("_decisions") {
                            continue;
                        }
                        processed.insert(filename_str.to_string());
                    }
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Directory doesn't exist yet, which is fine - no processed tickers
            debug!("Output directory doesn't exist yet: {}", dir_path);
        }
        Err(e) => return Err(Box::new(e)),
    }

    Ok(processed)
}

/// Determine which tickers need to be processed
fn determine_tickers_to_process(
    lf: &LazyFrame,
    paths: &PathConfig,
    universe: &str,
    mode: ExecutionMode,
    custom_tickers: Option<Vec<String>>,
) -> Result<Vec<String>, Box<dyn StdError>> {
    if let Some(custom_tickers) = custom_tickers {
        return Ok(custom_tickers);
    }

    let all_tickers = extract_unique_tickers(lf)?;
    let processed = load_processed_tickers(paths, universe, mode)?;

    let remaining: Vec<String> = all_tickers
        .into_iter()
        .filter(|ticker| !processed.contains(ticker))
        .collect();

    if !processed.is_empty() {
        info!("Skipping {} already-processed tickers, {} remaining", processed.len(), remaining.len());
    }

    Ok(remaining)
}

async fn backtest_helper(
    paths: &PathConfig,
    u: &str,
    batch_size: usize,
    mode: ExecutionMode,
    custom_tickers: Option<Vec<String>>,
    strategy_filter: Option<&str>,
) -> Result<(), Box<dyn StdError>> {
    let (lf, _latest_date) = load_price_data(paths, u, mode).await?;

    let needed = determine_tickers_to_process(&lf, paths, u, mode, custom_tickers)?;

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
                let paths_clone = paths.clone();
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

                    let tag: &str = match (mode, u_clone.as_str()) {
                        ///////////////////////////////////
                        // Update testing functions here //
                        ///////////////////////////////////
                        // "signal" = ALL (signal_functions)
                        // "param" = param_functions
                        // "testing" = testing_functions
                        (ExecutionMode::Testing | ExecutionMode::Demo, _) => "signal",
                        (ExecutionMode::Production, "Crypto") => "crypto",
                        (ExecutionMode::Production, u) if u.starts_with("Micro") => "micro",
                        (ExecutionMode::Production, u) if u.starts_with("SC") => "sc",
                        (ExecutionMode::Production, u) if u.starts_with("MC") => "mc",
                        (ExecutionMode::Production, u) if u.starts_with("LC") => "lc",
                        (ExecutionMode::Production, _) => "prod",
                    };
                    // ./target/release/backtester -u LC -m testing -t IBM
                    // cargo run -- -u Crypto -m testing -t btc

                    match select_backtests(filtered_lf, tag, strategy_filter).await {
                        Ok(backtest_results) => {
                            if let Err(e) = save_backtest(
                                &paths_clone,
                                backtest_results.clone(),
                                &u_clone,
                                ticker_clone.clone(),
                                mode.is_production(),
                            )
                            .await
                            {
                                eprintln!("{}", display::format_save_error(e.as_ref()));
                            }
                            (ticker_clone, Ok(backtest_results))
                        }
                        Err(e) => {
                            eprintln!("{}", display::format_execution_error(&ticker_clone, e.as_ref()));
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
                            "{}",
                            display::format_backtest_progress(u, &ticker, completed, out_of)
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

/// Setup logging based on verbosity level
fn setup_logging(verbose: u8) {
    let log_level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
        .init();
}

/// Clean up output directories and decision files based on mode
async fn cleanup_files(config: &BacktestConfig) -> Result<(), Box<dyn StdError>> {
    // Skip cleanup when using --output (resume mode for existing folders)
    if config.paths.output_suffix.is_some() {
        info!("Skipping cleanup: using existing output folder (resume mode)");
        return Ok(());
    }

    if config.mode.is_production() {
        let paths = vec![
            config.paths.output_dir("Stock", config.mode),
            config.paths.output_dir("Crypto", config.mode),
            config.paths.data_dir(config.mode),
        ];
        for p in paths {
            info!("Deleting files in: {}", p);
            delete_all_files_in_folder(p).await?;
        }
    } else if config.mode.is_testing() {
        // Only delete testing files in testing mode (not demo)
        let is_crypto = config::any_crypto_universe(&config.universes);
        let universe_type = if is_crypto { "Crypto" } else { "Stock" };
        let output_path = config.paths.output_dir(universe_type, config.mode);
        info!("Deleting files in: {}", output_path);
        delete_all_files_in_folder(output_path).await?;

        // Delete decision files based on universe
        let decision_path = config.paths.decision_dir(is_crypto);
        info!("Deleting files in: {}", decision_path);
        delete_all_files_in_folder(decision_path).await?;
    }
    Ok(())
}

/// Run backtests for all configured universes
async fn run_backtests(config: &BacktestConfig) -> Result<(), Box<dyn StdError>> {
    for u in &config.universes {
        info!("Backtest starting: {} (mode: {:?})", u, config.mode);

        backtest_helper(
            &config.paths,
            u,
            config.batch_size,
            config.mode,
            config.custom_tickers.clone(),
            config.strategy_filter.as_deref(),
        )
        .await?;
    }
    info!("Backtest processing complete");
    Ok(())
}

/// Generate performance summaries and insert scores for production mode
async fn generate_summaries(config: &BacktestConfig) -> Result<(), Box<dyn StdError>> {
    if config.mode.is_production() {
        if config::any_crypto_universe(&config.universes) {
            let (datetag, _out) = summary_performance_file(
                &config.paths,
                true,
                false,
                config.universes.clone(),
                &config.universe_label,
            )
            .await?;

            if let Err(e) = score(&datetag, "Crypto", &config.universe_label).await {
                eprintln!("{}", display::format_score_error(e.as_ref()));
            }
        }

        if config::any_stock_universe(&config.universes) {
            let (datetag, _out) = summary_performance_file(
                &config.paths,
                true,
                true,
                config.universes.clone(),
                &config.universe_label,
            )
            .await?;

            if let Err(e) = score(&datetag, "Stocks", &config.universe_label).await {
                eprintln!("{}", display::format_score_error(e.as_ref()));
            }
        }
    } else {
        // In testing mode, generate a summary but don't insert scores
        let is_crypto = config::any_crypto_universe(&config.universes);
        if !config.mode.is_demo() {
            let _ = summary_performance_file(&config.paths, false, !is_crypto, config.universes.clone(), &config.universe_label)
                .await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse();

    // Setup logging
    setup_logging(args.verbose);

    // Create configuration from args
    let config = BacktestConfig::new(
        args.path,
        args.universe,
        args.mode,
        args.tickers,
        args.strategy,
        args.output,
    )?;

    info!("Starting backtester with universe: {:?}, mode: {:?}", config.universes, config.mode);
    if let Some(ref t) = config.custom_tickers {
        info!("Filtering by tickers: {:?}", t);
    }
    if let Some(ref s) = config.strategy_filter {
        info!("Filtering by strategy: {}", s);
    }

    // Clean up prior files
    cleanup_files(&config).await?;

    // Create price files if needed (skip in demo mode)
    if !config.mode.is_demo() {
        create_price_files(config.universes.clone(), config.mode.is_production()).await?;
    }

    // Run backtests
    run_backtests(&config).await?;

    // Generate summaries and scores
    generate_summaries(&config).await?;

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
    println!("{}", display::format_single_backtest_result(&signal_name));
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
    println!("{}", display::format_sized_backtest_result(&signal_name));
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
