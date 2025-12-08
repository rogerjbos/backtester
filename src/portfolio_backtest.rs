#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]

use backtester::*;
use polars::prelude::*;
use chrono::NaiveDate;
use std::{
    collections::{HashMap, HashSet},
    error::Error as StdError,
    fs,
    path::Path,
};
use clap::Parser;
use log::{info, debug, warn};

/// Portfolio backtester - combines signals from multiple strategies
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input folder containing decision files
    #[arg(short, long, default_value = "output/testing")]
    input: String,

    /// Minimum number of strategies that must agree (default: majority)
    #[arg(short = 'n', long)]
    min_agree: Option<usize>,

    /// Strategy combination method: 'average', 'majority', 'unanimous'
    #[arg(short = 'c', long, default_value = "majority")]
    combine: String,

    /// Output file for portfolio results
    #[arg(short, long, default_value = "portfolio_results.csv")]
    output: String,

    /// Filter by specific ticker(s) - comma separated (optional)
    #[arg(short, long)]
    tickers: Option<String>,

    /// Enable verbose logging
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Debug, Clone)]
struct PortfolioDecision {
    ticker: String,
    date: NaiveDate,
    buy_signals: usize,
    sell_signals: usize,
    total_strategies: usize,
    position: f64, // -1.0 to 1.0, where 0 is neutral
}

#[derive(Debug, Clone)]
struct PortfolioPosition {
    ticker: String,
    entry_date: NaiveDate,
    entry_price: f64,
    exit_date: Option<NaiveDate>,
    exit_price: Option<f64>,
    size: f64,
    return_pct: Option<f64>,
}

fn read_decision_files(input_folder: &str, ticker_filter: Option<Vec<String>>) 
    -> Result<HashMap<String, Vec<DataFrame>>, Box<dyn StdError>> {
    
    info!("Reading decision files from: {}", input_folder);
    
    let mut ticker_decisions: HashMap<String, Vec<DataFrame>> = HashMap::new();
    
    let paths = fs::read_dir(input_folder)?;
    
    for path in paths {
        let path = path?.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            let filename = path.file_name().unwrap().to_string_lossy();
            
            // Only process decision files
            if !filename.contains("_decisions.csv") {
                continue;
            }
            
            // Extract ticker from filename (format: TICKER_strategy_decisions.csv)
            let parts: Vec<&str> = filename.split('_').collect();
            if parts.len() < 3 {
                warn!("Unexpected filename format: {}", filename);
                continue;
            }
            
            let ticker = parts[0].to_string();
            
            // Apply ticker filter if specified
            if let Some(ref filter) = ticker_filter {
                if !filter.iter().any(|t| t.eq_ignore_ascii_case(&ticker)) {
                    continue;
                }
            }
            
            debug!("Reading decision file: {}", filename);
            
            let df = CsvReadOptions::default()
                .with_has_header(true)
                .with_parse_options(
                    CsvParseOptions::default()
                        .with_try_parse_dates(true)
                )
                .try_into_reader_with_file_path(Some(path.clone()))?
                .finish()?;
            
            ticker_decisions.entry(ticker).or_insert_with(Vec::new).push(df);
        }
    }
    
    info!("Loaded decision files for {} tickers", ticker_decisions.len());
    Ok(ticker_decisions)
}

fn combine_signals(
    ticker_decisions: HashMap<String, Vec<DataFrame>>,
    combine_method: &str,
    min_agree: Option<usize>,
) -> Result<Vec<PortfolioDecision>, Box<dyn StdError>> {
    
    info!("Combining signals using method: {}", combine_method);
    
    let mut portfolio_decisions = Vec::new();
    
    for (ticker, dfs) in ticker_decisions {
        let num_strategies = dfs.len();
        info!("Processing ticker {} with {} strategies", ticker, num_strategies);
        
        // Concatenate all strategy decisions for this ticker
        let combined_dfs: Vec<LazyFrame> = dfs.iter()
            .map(|df| df.clone().lazy())
            .collect();
        
        let combined = concat(
            combined_dfs.as_slice(),
            UnionArgs::default()
        )?;
        
        // Group by date and count buy/sell signals
        let aggregated = combined
            .group_by([col("date")])
            .agg([
                when(col("action").eq(lit("buy")))
                    .then(lit(1))
                    .otherwise(lit(0))
                    .sum()
                    .alias("buy_signals"),
                when(col("action").eq(lit("sell")))
                    .then(lit(1))
                    .otherwise(lit(0))
                    .sum()
                    .alias("sell_signals"),
            ])
            .sort(["date"], SortMultipleOptions::default())
            .collect()?;
        
        // Convert to PortfolioDecisions
        let dates = aggregated.column("date")?.date()?;
        let buy_signals = aggregated.column("buy_signals")?.i32()?;
        let sell_signals = aggregated.column("sell_signals")?.i32()?;
        
        let threshold = min_agree.unwrap_or((num_strategies + 1) / 2); // Default to majority
        
        for idx in 0..aggregated.height() {
            let date_days = dates.get(idx).unwrap();
            // Convert days since epoch to NaiveDate
            let date = NaiveDate::from_num_days_from_ce_opt(date_days + 719163).unwrap();
            let buys = buy_signals.get(idx).unwrap_or(0) as usize;
            let sells = sell_signals.get(idx).unwrap_or(0) as usize;
            
            // Calculate position based on combine method
            let position = match combine_method {
                "average" => {
                    // Average of all signals: buy=+1, sell=-1
                    (buys as f64 - sells as f64) / num_strategies as f64
                }
                "majority" => {
                    // Position based on majority vote
                    if buys >= threshold {
                        1.0
                    } else if sells >= threshold {
                        -1.0
                    } else {
                        0.0
                    }
                }
                "unanimous" => {
                    // Only act if all strategies agree
                    if buys == num_strategies {
                        1.0
                    } else if sells == num_strategies {
                        -1.0
                    } else {
                        0.0
                    }
                }
                _ => {
                    warn!("Unknown combine method: {}, using majority", combine_method);
                    if buys >= threshold { 1.0 } else if sells >= threshold { -1.0 } else { 0.0 }
                }
            };
            
            if position != 0.0 {
                portfolio_decisions.push(PortfolioDecision {
                    ticker: ticker.clone(),
                    date,
                    buy_signals: buys,
                    sell_signals: sells,
                    total_strategies: num_strategies,
                    position,
                });
            }
        }
    }
    
    info!("Generated {} portfolio decisions", portfolio_decisions.len());
    Ok(portfolio_decisions)
}

async fn backtest_portfolio(
    decisions: Vec<PortfolioDecision>,
    price_file: &str,
) -> Result<Vec<PortfolioPosition>, Box<dyn StdError>> {
    
    info!("Running portfolio backtest with {} decisions", decisions.len());
    info!("Reading price data from: {}", price_file);
    
    // Get unique tickers
    let tickers: HashSet<String> = decisions.iter().map(|d| d.ticker.clone()).collect();
    
    // Read price data from CSV file with proper schema
    let all_prices = LazyCsvReader::new(price_file)
        .with_has_header(true)
        .with_infer_schema_length(Some(0)) // Don't infer, we'll select what we need
        .finish()?
        .select([
            col("Date"),
            col("Ticker"),
            col("Close").cast(DataType::Float64),
        ])
        .collect()?;
    
    // Filter and organize price data by ticker
    let mut price_data: HashMap<String, DataFrame> = HashMap::new();
    
    for ticker in &tickers {
        debug!("Extracting price data for {}", ticker);
        
        let ticker_prices = all_prices
            .clone()
            .lazy()
            .filter(col("Ticker").eq(lit(ticker.clone())))
            .select([col("Date"), col("Close")])
            .collect()?;
        
        if ticker_prices.height() == 0 {
            warn!("No price data found for {}", ticker);
            continue;
        }
        
        price_data.insert(ticker.clone(), ticker_prices);
    }
    
    info!("Loaded price data for {} tickers", price_data.len());
    
    // Track positions for each ticker
    let mut positions = Vec::new();
    let mut open_positions: HashMap<String, PortfolioPosition> = HashMap::new();
    
    // Sort decisions by date
    let mut sorted_decisions = decisions.clone();
    sorted_decisions.sort_by_key(|d| d.date);
    
    for decision in sorted_decisions {
        if let Some(prices) = price_data.get(&decision.ticker) {
            // Find price on decision date  
            let price_row = prices
                .clone()
                .lazy()
                .filter(col("Date").eq(lit(decision.date.format("%Y-%m-%d").to_string())))
                .collect();
            
            if let Ok(price_df) = price_row {
                if price_df.height() > 0 {
                    if let Some(row) = price_df.get(0) {
                        let close = row[1].try_extract::<f64>().unwrap_or(0.0);
                        
                        if close <= 0.0 {
                            continue;
                        }
                        
                        // Check if we have an open position
                        if let Some(mut open_pos) = open_positions.remove(&decision.ticker) {
                            // Close existing position
                            open_pos.exit_date = Some(decision.date);
                            open_pos.exit_price = Some(close);
                            open_pos.return_pct = Some(
                                ((close / open_pos.entry_price) - 1.0) * 100.0 * open_pos.size.signum()
                            );
                            positions.push(open_pos);
                        }
                        
                        // Open new position if signal is strong enough
                        if decision.position.abs() > 0.0 {
                            let new_pos = PortfolioPosition {
                                ticker: decision.ticker.clone(),
                                entry_date: decision.date,
                                entry_price: close,
                                exit_date: None,
                                exit_price: None,
                                size: decision.position,
                                return_pct: None,
                            };
                            open_positions.insert(decision.ticker.clone(), new_pos);
                        }
                    }
                }
            }
        }
    }
    
    // Close any remaining open positions at last available price
    for (ticker, mut pos) in open_positions {
        if let Some(prices) = price_data.get(&ticker) {
            if let Ok(last_row) = prices.tail(Some(1)).get_row(0) {
                if let Ok(close) = last_row.0[1].try_extract::<f64>() {
                    pos.exit_price = Some(close);
                    pos.return_pct = Some(
                        ((close / pos.entry_price) - 1.0) * 100.0 * pos.size.signum()
                    );
                }
            }
        }
        positions.push(pos);
    }
    
    info!("Completed {} trades", positions.len());
    Ok(positions)
}

fn calculate_portfolio_metrics(positions: &[PortfolioPosition]) -> Result<(), Box<dyn StdError>> {
    if positions.is_empty() {
        println!("No completed trades to analyze");
        return Ok(());
    }
    
    let completed: Vec<_> = positions.iter()
        .filter(|p| p.return_pct.is_some())
        .collect();
    
    if completed.is_empty() {
        println!("No completed trades with returns");
        return Ok(());
    }
    
    let returns: Vec<f64> = completed.iter()
        .map(|p| p.return_pct.unwrap())
        .collect();
    
    let total_return: f64 = returns.iter().sum();
    let avg_return: f64 = total_return / returns.len() as f64;
    let winning_trades = returns.iter().filter(|&&r| r > 0.0).count();
    let win_rate = (winning_trades as f64 / returns.len() as f64) * 100.0;
    
    let winning_returns: Vec<f64> = returns.iter().filter(|&&r| r > 0.0).cloned().collect();
    let losing_returns: Vec<f64> = returns.iter().filter(|&&r| r < 0.0).cloned().collect();
    
    let avg_win = if !winning_returns.is_empty() {
        winning_returns.iter().sum::<f64>() / winning_returns.len() as f64
    } else {
        0.0
    };
    
    let avg_loss = if !losing_returns.is_empty() {
        losing_returns.iter().sum::<f64>() / losing_returns.len() as f64
    } else {
        0.0
    };
    
    let profit_factor = if avg_loss != 0.0 {
        (avg_win * winning_returns.len() as f64).abs() / (avg_loss.abs() * losing_returns.len() as f64)
    } else {
        0.0
    };
    
    println!("\n=== Portfolio Performance Metrics ===");
    println!("Total Trades: {}", completed.len());
    println!("Winning Trades: {}", winning_trades);
    println!("Win Rate: {:.2}%", win_rate);
    println!("Total Return: {:.2}%", total_return);
    println!("Average Return per Trade: {:.2}%", avg_return);
    println!("Average Win: {:.2}%", avg_win);
    println!("Average Loss: {:.2}%", avg_loss);
    println!("Profit Factor: {:.3}", profit_factor);
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse();
    
    // Setup logging
    let log_level = match args.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();
    
    info!("Portfolio Backtester starting");
    info!("Input folder: {}", args.input);
    info!("Combine method: {}", args.combine);
    
    // Parse ticker filter if provided
    let ticker_filter = args.tickers.as_ref().map(|t| {
        t.split(',')
            .map(|s| s.trim().to_uppercase())
            .collect::<Vec<String>>()
    });
    
    // Read decision files
    let ticker_decisions = read_decision_files(&args.input, ticker_filter)?;
    
    if ticker_decisions.is_empty() {
        eprintln!("No decision files found in {}", args.input);
        return Ok(());
    }
    
    // Combine signals
    let portfolio_decisions = combine_signals(
        ticker_decisions,
        &args.combine,
        args.min_agree,
    )?;
    
    if portfolio_decisions.is_empty() {
        eprintln!("No portfolio decisions generated");
        return Ok(());
    }
    
    // Determine price file path from input path
    let price_file = if args.input.contains("crypto") {
        "data/testing/Crypto.csv"
    } else if args.input.contains("MC") {
        "data/testing/MC1.csv" // Use MC1 as default, could be made configurable
    } else if args.input.contains("SC") {
        "data/testing/SC1.csv"
    } else if args.input.contains("LC") {
        "data/testing/LC1.csv"
    } else {
        "data/testing/LC1.csv" // Default fallback
    };
    
    info!("Using price file: {}", price_file);
    
    // Run backtest
    let positions = backtest_portfolio(portfolio_decisions, price_file).await?;
    
    // Calculate metrics
    calculate_portfolio_metrics(&positions)?;
    
    // Save results to CSV
    if !positions.is_empty() {
        let tickers: Vec<String> = positions.iter().map(|p| p.ticker.clone()).collect();
        let entry_dates: Vec<String> = positions.iter()
            .map(|p| p.entry_date.to_string())
            .collect();
        let entry_prices: Vec<f64> = positions.iter().map(|p| p.entry_price).collect();
        let exit_dates: Vec<Option<String>> = positions.iter()
            .map(|p| p.exit_date.map(|d| d.to_string()))
            .collect();
        let exit_prices: Vec<Option<f64>> = positions.iter().map(|p| p.exit_price).collect();
        let sizes: Vec<f64> = positions.iter().map(|p| p.size).collect();
        let returns: Vec<Option<f64>> = positions.iter().map(|p| p.return_pct).collect();
        
        let df = df! {
            "ticker" => tickers,
            "entry_date" => entry_dates,
            "entry_price" => entry_prices,
            "exit_date" => exit_dates,
            "exit_price" => exit_prices,
            "size" => sizes,
            "return_pct" => returns,
        }?;
        
        let mut file = std::fs::File::create(&args.output)?;
        CsvWriter::new(&mut file).finish(&mut df.clone())?;
        
        info!("Results saved to: {}", args.output);
    }
    
    Ok(())
}
