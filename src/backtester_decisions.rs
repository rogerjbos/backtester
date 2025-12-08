use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use polars::df;
use std::error::Error as StdError;
use std::env;
use polars::frame::DataFrame;
use std::path::Path;
use std::fs;
use std::sync::Arc;
use polars::prelude::LazyCsvReader;
use polars::series::Series;
use polars::error::PolarsError;
use polars::prelude::{col, lit};
use std::collections::HashMap;
use polars::chunked_array::ops::SortMultipleOptions;
use polars::prelude::{JoinArgs, JoinType};
use chrono::NaiveDate;
use polars::datatypes::AnyValue;
use polars::prelude::{DataType, Field, Schema};
use polars::prelude::{LazyFileListReader, NamedFrom, IntoLazy};
use polars::prelude::{CsvWriter, SerWriter};
use clap::Parser;
use log::{info, debug, warn};

/// Backtester for analyzing trading decisions
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Asset type: 'stocks' or 'crypto'
    #[arg(short, long)]
    asset_type: String,

    /// Filter by specific ticker (optional)
    #[arg(short, long)]
    ticker: Option<String>,

    /// Filter by specific strategy (optional)
    #[arg(short, long)]
    strategy: Option<String>,

    /// Filter by specific universe (optional, not applicable for decisions analysis)
    #[arg(short, long)]
    universe: Option<String>,

    /// Enable verbose logging
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

// Add this enum above the client functions
pub enum ChConnectionType {
    Local,
    Remote,
}

pub async fn test_connection(connection_type: ChConnectionType) -> Result<(), Box<dyn StdError>> {
    let (host, port) = match connection_type {
        ChConnectionType::Local => ("192.168.86.46", 8123),
        ChConnectionType::Remote => ("192.168.86.56", 8123),
    };

    println!("Testing connection to {}:{}...", host, port);

    match timeout(Duration::from_secs(5), TcpStream::connect((host, port))).await {
        Ok(Ok(_)) => {
            println!("✓ TCP connection to {}:{} successful", host, port);
            Ok(())
        }
        Ok(Err(e)) => {
            println!("✗ TCP connection to {}:{} failed: {}", host, port, e);
            Err(Box::new(e))
        }
        Err(_) => {
            println!("✗ TCP connection to {}:{} timed out after 5 seconds", host, port);
            Err("Connection timeout".into())
        }
    }
}

fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
}

pub async fn get_ch_client(connection_type: ChConnectionType) -> Result<Client, Box<dyn StdError>> {
    let (url, user, password, database, conn_type_str) = match connection_type {
        ChConnectionType::Local => {
            let host = "192.168.86.46";
            (
                format!("http://{}:8123", host),
                "roger".to_string(),
                read_env_var("PG"),
                "tiingo".to_string(),
                "Local",
            )
        }
        ChConnectionType::Remote => {
            let host = "192.168.86.56";
            (
                format!("http://{}:8123", host),
                "roger".to_string(),
                read_env_var("PG"),
                "tiingo".to_string(),
                "Remote",
            )
        }
    };

    let client = Client::default()
        .with_url(url)
        .with_user(user)
        .with_password(password)
        .with_database(database);

    let query_result = client.query("SELECT version()").fetch_one::<String>().await;

    match query_result {
        Ok(version) => {
            println!(
                "Successfully connected to ClickHouse {}. Server version: {}",
                conn_type_str, version
            );
            Ok(client)
        }
        Err(e) => {
            println!("Failed to connect to ClickHouse {}: {:?}", conn_type_str, e);
            Err(Box::new(e))
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct OHLCV {
    date: String,
    ticker: String,
    universe: String,
    close: Option<f64>,
}

pub async fn get_price_dataframe(univ: &str, tickers: &[String], production: bool, connection_type: ChConnectionType) -> Result<DataFrame, Box<dyn StdError>> {
    // Process in chunks of 10 tickers
    let chunk_size = 10;
    let ticker_chunks: Vec<Vec<String>> = tickers
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    // Get a client connection
    let client = get_ch_client(connection_type).await?;

    // Collect all data
    let mut all_data: Vec<OHLCV> = Vec::new();

    // Process each chunk
    for (i, chunk) in ticker_chunks.iter().enumerate() {
        let ticker_list = chunk
            .iter()
            .map(|t| format!("'{}'", t))
            .collect::<Vec<_>>()
            .join(",");

        let query = if production && univ == "crypto" {
            format!(
                "WITH univ AS (
                SELECT baseCurrency ticker, max(date) maxdate
                FROM tiingo.crypto
                WHERE baseCurrency IN ({})
                group by ticker
                having count(date) > 120 and COUNT(*) * 2 - COUNT(high) - COUNT(low) = 0
                )
                SELECT toString(date(p.date)) Date, u.ticker Ticker, 'Crypto' as Universe, close AS Close
                FROM tiingo.crypto p
                INNER JOIN univ u
                ON u.ticker = p.baseCurrency
                WHERE p.date >= subtractDays(now(), 252)
                and maxdate IN (select max(date) from tiingo.crypto)
                order by ticker, date",
                ticker_list
            )
        } else if production && univ != "crypto" {
            format!(
                "WITH mdate AS (
                SELECT symbol, max(date(date)) AS maxdate
                FROM tiingo.usd p
                WHERE symbol IN ({})
                group by symbol
                having count(date) >= 250 and COUNT(*) * 2 - COUNT(adjHigh) - COUNT(adjLow) = 0
                )
                SELECT toString(date(p.date)) Date
                , symbol AS Ticker
                , '{univ}' AS Universe
                , round(adjClose, 2) AS Close
                FROM tiingo.usd p
                INNER JOIN mdate m
                ON m.symbol = p.symbol
                WHERE p.date >= subtractDays(now(), 365)
                and m.maxdate IN (select max(date(date)) from tiingo.usd)
                order by Ticker, date",
                ticker_list
            )
        } else if !production && univ == "crypto" {
            format!(
                "WITH univ AS (
                SELECT baseCurrency ticker, max(date) maxdate
                FROM tiingo.crypto
                WHERE baseCurrency IN ({})
                group by ticker
                having count(date) > 360 and COUNT(*) * 2 - COUNT(high) - COUNT(low) = 0
                )
                SELECT toString(date(p.date)) Date, u.ticker Ticker, 'Crypto' as Universe, close AS Close
                FROM tiingo.crypto p
                INNER JOIN univ u
                ON u.ticker = p.baseCurrency
                order by ticker, date",
                ticker_list
            )
        } else if !production && univ != "crypto" {
            format!(
                "WITH mdate AS (
                SELECT symbol, max(date(date)) AS maxdate
                FROM tiingo.usd p
                WHERE symbol IN ({})
                group by symbol
                having count(date) >= 1000 and COUNT(*) * 2 - COUNT(adjHigh) - COUNT(adjLow) = 0
                )
                SELECT toString(date(p.date)) Date
                , symbol AS Ticker
                , '{univ}' AS Universe
                , round(adjClose, 2) AS Close
                FROM tiingo.usd p
                INNER JOIN mdate m
                ON m.symbol = p.symbol
                WHERE m.maxdate IN (select max(date(date)) from tiingo.usd)
                order by Ticker, date",
                ticker_list
            )
        } else {
            return Err("Invalid universe or production flag".into());
        };

        println!(
            "Executing query for chunk {}/{}",
            i + 1,
            ticker_chunks.len()
        );

        let mut cursor = client.query(&query).fetch::<OHLCV>()?;
        while let Some(row) = cursor.next().await? {
            all_data.push(row);
        }
    }

    // Convert to Polars DataFrame
    let dates: Vec<&str> = all_data.iter().map(|r| r.date.as_str()).collect();
    let tickers: Vec<&str> = all_data.iter().map(|r| r.ticker.as_str()).collect();
    let universes: Vec<&str> = all_data.iter().map(|r| r.universe.as_str()).collect();
    let closes: Vec<Option<f64>> = all_data.iter().map(|r| r.close).collect();

    let df = df! {
        "date" => dates,
        "ticker" => tickers,
        "universe" => universes,
        "close" => closes,
    }?;

    Ok(df)
}

#[derive(Debug, Serialize, Deserialize)]
struct Decision {
    date: String,
    action: String,
    strategy: String,
}

#[derive(Debug, Serialize)]
struct PerformanceResult {
    ticker: String,
    strategy: String,
    // Core performance
    total_return_pct: f64,
    buy_hold_return_pct: f64,
    excess_return_pct: f64,
    // Trade statistics
    num_trades: i32,
    win_rate_pct: f64,
    avg_win_pct: f64,
    avg_loss_pct: f64,
    profit_factor: f64,
    // Risk metrics
    sharpe_ratio: f64,
    max_drawdown_pct: f64,
    // Market exposure
    avg_position_days: f64,
    pct_time_in_market: f64,
}

pub fn load_decisions(asset_type: &str) -> Result<DataFrame, Box<dyn StdError>> {
    let decisions_path = format!("website/baseline_20251109/decisions/{}", asset_type);
    let path = Path::new(&decisions_path);

    let mut dfs: Vec<DataFrame> = Vec::new();

    // Define schema to ensure consistent types
    let schema = Schema::from_iter(vec![
        Field::new("ticker".into(), DataType::String),
        Field::new("strategy".into(), DataType::String),
        Field::new("date".into(), DataType::String),
        Field::new("action".into(), DataType::String),
    ]);

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();
            if file_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                let ticker = file_path.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
                let mut df = LazyCsvReader::new(&file_path)
                    .with_has_header(true)
                    .with_schema(Some(Arc::new(schema.clone())))
                    .finish()?
                    .collect()?;
                if !df.schema().contains("ticker") {
                    let ticker_series = Series::new("ticker".into(), vec![ticker.as_str(); df.height()]);
                    df = df.hstack(&[ticker_series.into()])?;
                }
                dfs.push(df);
            }
        }
    }

    let mut decisions_df = dfs[0].clone();
    for df in dfs.iter().skip(1) {
        decisions_df = decisions_df.vstack(&df)?;
    }
    Ok(decisions_df)
}

pub fn calculate_daily_return(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let lf = df.clone().lazy();
    let daily_return_expr = ((col("close") / col("close").shift(lit(1)) - lit(1.0)) * lit(100.0)).alias("daily_return");
    lf.with_column(daily_return_expr).collect()
}

pub fn calculate_strategy_data(decisions_df: &DataFrame, returns_df: &DataFrame, ticker: &str, strategy_name: &str) -> Result<HashMap<String, f64>, Box<dyn StdError>> {
    let strategy_decisions = decisions_df
        .clone()
        .lazy()
        .filter(col("ticker").eq(lit(ticker)))
        .filter(col("strategy").eq(lit(strategy_name)))
        .collect()?;

    if strategy_decisions.height() == 0 {
        return Ok(HashMap::new());
    }

    // Filter returns to this ticker
    let df = returns_df
        .clone()
        .lazy()
        .filter(col("ticker").eq(lit(ticker)))
        .sort(["date"], SortMultipleOptions::default())
        .collect()?;

    // Merge decisions
    let merged = df
        .lazy()
        .join(strategy_decisions.lazy(), [col("date")], [col("date")], JoinArgs::new(JoinType::Left))
        .collect()?;

    let action_col = merged.column("action")?;
    let daily_return_col = merged.column("daily_return")?;
    let mut bh: Vec<f64> = Vec::new();
    let mut st: Vec<f64> = Vec::new();
    let mut mt: Vec<f64> = Vec::new();
    let mut lt: Vec<f64> = Vec::new();
    let mut daily_returns: Vec<f64> = Vec::new();

    let st_days = 20;
    let mt_days = 100;
    let lt_days = 250;

    let mut last_buy_date: Option<NaiveDate> = None;
    let mut position_held = false;

    for i in 0..merged.height() {
        let action = match action_col.get(i)? {
            AnyValue::String(s) => s.as_ref(),
            _ => "",
        };

        let date_str = merged.column("date")?.str()?.get(i).unwrap_or("");
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;

        let daily_return = daily_return_col.f64()?.get(i).unwrap_or(0.0);

        let mut current_bh = if position_held { 1.0 } else { 0.0 };
        let mut current_st = if let Some(lbd) = last_buy_date {
            if (date - lbd).num_days() <= st_days { 1.0 } else { 0.0 }
        } else { 0.0 };
        let mut current_mt = if let Some(lbd) = last_buy_date {
            if (date - lbd).num_days() <= mt_days { 1.0 } else { 0.0 }
        } else { 0.0 };
        let mut current_lt = if let Some(lbd) = last_buy_date {
            if (date - lbd).num_days() <= lt_days { 1.0 } else { 0.0 }
        } else { 0.0 };

        if action == "buy" {
            position_held = true;
            last_buy_date = Some(date);
            current_bh = 1.0;
            current_st = 1.0;
            current_mt = 1.0;
            current_lt = 1.0;
        } else if action == "sell" {
            position_held = false;
            last_buy_date = None;
            current_bh = 0.0;
            current_st = 0.0;
            current_mt = 0.0;
            current_lt = 0.0;
        }

        bh.push(current_bh);
        st.push(current_st);
        mt.push(current_mt);
        lt.push(current_lt);
        daily_returns.push(daily_return);
    }

    // Calculate comprehensive trading metrics
    let mut position_stats = HashMap::new();

    // 1. Calculate total returns
    let strategy_return: f64 = bh.iter()
        .zip(daily_returns.iter())
        .map(|(pos, ret)| if *pos == 1.0 { ret } else { &0.0 })
        .sum();
    
    let buy_hold_return: f64 = daily_returns.iter().sum();
    let excess_return = strategy_return - buy_hold_return;
    
    position_stats.insert("total_return_pct".to_string(), strategy_return);
    position_stats.insert("buy_hold_return_pct".to_string(), buy_hold_return);
    position_stats.insert("excess_return_pct".to_string(), excess_return);

    // 2. Calculate trade statistics
    let mut trades: Vec<f64> = Vec::new();
    let mut current_trade_return = 0.0;
    let mut in_position = false;
    
    for i in 0..bh.len() {
        if bh[i] == 1.0 && !in_position {
            // Starting new position
            in_position = true;
            current_trade_return = 0.0;
        }
        
        if bh[i] == 1.0 {
            current_trade_return += daily_returns[i];
        }
        
        if bh[i] == 0.0 && in_position {
            // Closed position
            trades.push(current_trade_return);
            in_position = false;
        }
    }
    // Handle open position at end
    if in_position {
        trades.push(current_trade_return);
    }
    
    let num_trades = trades.len() as i32;
    let winning_trades: Vec<f64> = trades.iter().filter(|&&t| t > 0.0).copied().collect();
    let losing_trades: Vec<f64> = trades.iter().filter(|&&t| t < 0.0).copied().collect();
    
    let win_rate = if num_trades > 0 { 
        (winning_trades.len() as f64 / num_trades as f64) * 100.0 
    } else { 0.0 };
    
    let avg_win = if !winning_trades.is_empty() { 
        winning_trades.iter().sum::<f64>() / winning_trades.len() as f64 
    } else { 0.0 };
    
    let avg_loss = if !losing_trades.is_empty() { 
        losing_trades.iter().sum::<f64>() / losing_trades.len() as f64 
    } else { 0.0 };
    
    let profit_factor = if !losing_trades.is_empty() && avg_loss != 0.0 {
        winning_trades.iter().sum::<f64>() / losing_trades.iter().sum::<f64>().abs()
    } else if winning_trades.is_empty() { 0.0 } else { 999.0 };
    
    position_stats.insert("num_trades".to_string(), num_trades as f64);
    position_stats.insert("win_rate_pct".to_string(), win_rate);
    position_stats.insert("avg_win_pct".to_string(), avg_win);
    position_stats.insert("avg_loss_pct".to_string(), avg_loss);
    position_stats.insert("profit_factor".to_string(), profit_factor);

    // 3. Calculate risk metrics
    let position_returns: Vec<f64> = bh.iter()
        .zip(daily_returns.iter())
        .filter(|(pos, _)| **pos == 1.0)
        .map(|(_, ret)| *ret)
        .collect();
    
    let sharpe_ratio = if !position_returns.is_empty() {
        let mean_return = position_returns.iter().sum::<f64>() / position_returns.len() as f64;
        let variance = position_returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / position_returns.len() as f64;
        let std_dev = variance.sqrt();
        if std_dev > 0.0 { mean_return / std_dev * (252.0_f64).sqrt() } else { 0.0 }
    } else { 0.0 };
    
    // Calculate drawdown
    let mut cumulative_returns: Vec<f64> = Vec::new();
    let mut cum_sum = 0.0;
    for i in 0..bh.len() {
        if bh[i] == 1.0 {
            cum_sum += daily_returns[i];
        }
        cumulative_returns.push(cum_sum);
    }
    
    let mut max_drawdown = 0.0;
    let mut peak = cumulative_returns[0];
    for &value in &cumulative_returns {
        if value > peak {
            peak = value;
        }
        let drawdown = peak - value;
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }
    
    position_stats.insert("sharpe_ratio".to_string(), sharpe_ratio);
    position_stats.insert("max_drawdown_pct".to_string(), max_drawdown);

    // 4. Calculate market exposure
    let days_in_market = bh.iter().filter(|&&p| p == 1.0).count() as f64;
    let total_days = bh.len() as f64;
    let pct_time_in_market = if total_days > 0.0 { (days_in_market / total_days) * 100.0 } else { 0.0 };
    let avg_position_days = if num_trades > 0 { days_in_market / num_trades as f64 } else { 0.0 };
    
    position_stats.insert("avg_position_days".to_string(), avg_position_days);
    position_stats.insert("pct_time_in_market".to_string(), pct_time_in_market);

    let mut result_df = merged.clone();
    result_df.with_column(Series::new("bh".into(), bh))?;
    result_df.with_column(Series::new("st".into(), st))?;
    result_df.with_column(Series::new("mt".into(), mt))?;
    result_df.with_column(Series::new("lt".into(), lt))?;

    Ok(position_stats)
}

pub async fn run_analysis(
    asset_type: &str,
    connection_type: ChConnectionType,
    ticker_filter: Option<&str>,
    strategy_filter: Option<&str>,
    universe_filter: Option<&str>,
) -> Result<(), Box<dyn StdError>> {
    // Load decisions
    let decisions_df = load_decisions(asset_type)?;
    info!("Loaded {} decisions", decisions_df.height());

    // Note: universe filter is not applicable here since decisions CSV doesn't have universe column
    // The asset_type already determines the universe (crypto vs stocks)
    if universe_filter.is_some() {
        warn!("Universe filter is not applicable for this analysis. Use --asset-type to select crypto or stocks.");
    }

    // Get unique tickers
    let mut tickers: Vec<String> = decisions_df
        .column("ticker")?
        .unique()?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    // Apply ticker filter if provided
    if let Some(ticker) = ticker_filter {
        // Crypto tickers are lowercase, stocks are uppercase
        let ticker_normalized = if asset_type == "crypto" {
            ticker.to_lowercase()
        } else {
            ticker.to_uppercase()
        };
        info!("Filtering by ticker: {} (normalized for {})", ticker_normalized, asset_type);
        tickers.retain(|t| {
            if asset_type == "crypto" {
                t.to_lowercase() == ticker_normalized
            } else {
                t.to_uppercase() == ticker_normalized
            }
        });
        if tickers.is_empty() {
            warn!("No matching tickers found for: {}", ticker_normalized);
            return Err(format!("No matching tickers found: {}", ticker_normalized).into());
        }
    }

    info!("Found {} unique tickers", tickers.len());
    debug!("Tickers: {:?}", tickers);

    // Get price data
    info!("Fetching price data for {} tickers...", tickers.len());
    let returns_df = get_price_dataframe(asset_type, &tickers, false, connection_type).await?;
    let returns_df = calculate_daily_return(&returns_df)?;

    info!("Loaded {} price records", returns_df.height());
    debug!("Price data shape: {:?}", returns_df.shape());

    // Get unique strategies
    let mut strategies: Vec<String> = decisions_df
        .column("strategy")?
        .unique()?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    // Apply strategy filter if provided
    if let Some(strategy) = strategy_filter {
        info!("Filtering by strategy: {}", strategy);
        strategies.retain(|s| s == strategy);
        if strategies.is_empty() {
            warn!("No matching strategies found for: {}", strategy);
            return Err(format!("No matching strategies found: {}", strategy).into());
        }
    }

    info!("Found {} unique strategies", strategies.len());
    debug!("Strategies: {:?}", strategies);

    // Collect results
    let mut results: Vec<PerformanceResult> = Vec::new();

    // For each ticker and strategy, calculate
    info!("Calculating performance for {} ticker-strategy combinations...", tickers.len() * strategies.len());
    let mut processed = 0;
    for ticker in &tickers {
        for strategy in &strategies {
            debug!("Processing: ticker={}, strategy={}", ticker, strategy);
            let stats = calculate_strategy_data(&decisions_df, &returns_df, ticker, strategy)?;
            if !stats.is_empty() {
                debug!("Stats for {}/{}: {:?}", ticker, strategy, stats);
                let result = PerformanceResult {
                    ticker: ticker.clone(),
                    strategy: strategy.clone(),
                    total_return_pct: (stats.get("total_return_pct").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    buy_hold_return_pct: (stats.get("buy_hold_return_pct").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    excess_return_pct: (stats.get("excess_return_pct").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    num_trades: *stats.get("num_trades").unwrap_or(&0.0) as i32,
                    win_rate_pct: (stats.get("win_rate_pct").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    avg_win_pct: (stats.get("avg_win_pct").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    avg_loss_pct: (stats.get("avg_loss_pct").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    profit_factor: (stats.get("profit_factor").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    sharpe_ratio: (stats.get("sharpe_ratio").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    max_drawdown_pct: (stats.get("max_drawdown_pct").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    avg_position_days: (stats.get("avg_position_days").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                    pct_time_in_market: (stats.get("pct_time_in_market").unwrap_or(&0.0) * 1000.0).round() / 1000.0,
                };
                results.push(result);
                processed += 1;
                if processed % 100 == 0 {
                    info!("Processed {} combinations...", processed);
                }
            } else {
                debug!("No stats for ticker={}, strategy={}", ticker, strategy);
            }
        }
    }

    info!("Completed processing {} results", results.len());

    // Write to CSV
    let csv_path = format!("performance_results_{}.csv", asset_type);
    info!("Writing {} results to {}...", results.len(), csv_path);
    let mut df = df! {
        "ticker" => results.iter().map(|r| r.ticker.as_str()).collect::<Vec<_>>(),
        "strategy" => results.iter().map(|r| r.strategy.as_str()).collect::<Vec<_>>(),
        "total_return_pct" => results.iter().map(|r| r.total_return_pct).collect::<Vec<_>>(),
        "buy_hold_return_pct" => results.iter().map(|r| r.buy_hold_return_pct).collect::<Vec<_>>(),
        "excess_return_pct" => results.iter().map(|r| r.excess_return_pct).collect::<Vec<_>>(),
        "num_trades" => results.iter().map(|r| r.num_trades).collect::<Vec<_>>(),
        "win_rate_pct" => results.iter().map(|r| r.win_rate_pct).collect::<Vec<_>>(),
        "avg_win_pct" => results.iter().map(|r| r.avg_win_pct).collect::<Vec<_>>(),
        "avg_loss_pct" => results.iter().map(|r| r.avg_loss_pct).collect::<Vec<_>>(),
        "profit_factor" => results.iter().map(|r| r.profit_factor).collect::<Vec<_>>(),
        "sharpe_ratio" => results.iter().map(|r| r.sharpe_ratio).collect::<Vec<_>>(),
        "max_drawdown_pct" => results.iter().map(|r| r.max_drawdown_pct).collect::<Vec<_>>(),
        "avg_position_days" => results.iter().map(|r| r.avg_position_days).collect::<Vec<_>>(),
        "pct_time_in_market" => results.iter().map(|r| r.pct_time_in_market).collect::<Vec<_>>(),
    }?;
    let mut file = std::fs::File::create(&csv_path)?;
    CsvWriter::new(&mut file).finish(&mut df)?;
    info!("Successfully wrote {} rows to {}", results.len(), csv_path);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args = Args::parse();

    // Setup logging based on verbosity
    let log_level = match args.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
        .init();

    let asset_type = &args.asset_type;
    if asset_type != "stocks" && asset_type != "crypto" {
        eprintln!("asset_type must be 'stocks' or 'crypto'");
        std::process::exit(1);
    }

    info!("Starting backtester for asset type: {}", asset_type);
    if let Some(ref t) = args.ticker {
        info!("Filtering by ticker: {}", t);
    }
    if let Some(ref s) = args.strategy {
        info!("Filtering by strategy: {}", s);
    }
    if let Some(ref u) = args.universe {
        info!("Filtering by universe: {}", u);
    }

    // Test connection first
    info!("Testing ClickHouse connection...");
    let connection_type = if test_connection(ChConnectionType::Local).await.is_ok() {
        ChConnectionType::Local
    } else if test_connection(ChConnectionType::Remote).await.is_ok() {
        ChConnectionType::Remote
    } else {
        warn!("Both local and remote connections failed.");
        std::process::exit(1);
    };

    run_analysis(
        asset_type,
        connection_type,
        args.ticker.as_deref(),
        args.strategy.as_deref(),
        args.universe.as_deref(),
    )
    .await?;

    info!("Backtesting complete!");
    Ok(())
}