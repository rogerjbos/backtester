#![cfg_attr(debug_assertions, allow(dead_code, unused_imports))]

use polars::prelude::*;
use chrono::{NaiveDate, Datelike};
use std::{
    collections::{HashMap, BTreeMap},
    error::Error as StdError,
    fs::File,
    fs,
    path::Path,
    env,
    sync::Arc,
};
use clap::Parser;
use log::{info, debug, warn};
use backtester::portfolio_accounting::PortfolioAccounting;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

/// ClickHouse connection type
#[derive(Clone, Copy)]
pub enum ChConnectionType {
    Local,
    Remote,
}

/// Portfolio backtester with position limits and priority-based ranking
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Signal date folder (e.g., output/testing_20260204 or just 20260204)
    #[arg(long, default_value = "20260204")]
    signal_date: String,

    /// Start date for backtest (format: YYYY-MM-DD), filters signals to this date or later
    #[arg(long, default_value = "2020-01-02")]
    start_date: Option<String>,

    /// Maximum number of positions in portfolio
    #[arg(short = 's', long, default_value = "20")]
    portfolio_size: usize,

    /// List of specific signals to process (comma-separated)
    #[arg(long)]
    signals: Option<String>,

    /// Priority strategy for ranking stocks (e.g., "candlestick_double_trouble")
    #[arg(short, long)]
    priority_strategy: String,

    /// Stop-loss percentage (e.g., 0.10 for 10% loss), 0 to disable
    #[arg(short = 'l', long, default_value = "0.10")]
    stop_loss_pct: f64,

    /// Universe type: MC1, MC2, SC1, LC1, or Crypto
    #[arg(short, long)]
    universe: String,

    /// Sector: Technology,Consumer Cyclical,Industrials,Healthcare,Energy,Communication Services,
    // Financial Services,Real Estate,Consumer Defensive,Utilities,Basic Materials
    #[arg(long)]
    sector: Option<String>,

    /// Commission (default commission-free)
    #[arg(long, default_value = "0.50")]
    commission: f64,

    /// prefix for filenames
    #[arg(short, long, default_value = "portfolio")]
    prefix: String,

    /// Filter by specific ticker(s) - comma separated (optional)
    #[arg(short, long)]
    tickers: Option<String>,

    /// Enable verbose logging
    #[arg(short, long, default_value = "0", value_parser = clap::value_parser!(u8).range(0..=3))]
    verbose: u8,

    /// Enable accounting reports (CSV, JSON, console output)
    #[arg(short = 'a', long)]
    accounting_reports: bool,

    /// Enable rebalancing (monthly)
    #[arg(short = 'r', long)]
    rebalance: bool,

    /// Rebalance threshold percentage (e.g., 0.10 for 10% over target)
    #[arg(long, default_value = "0.10")]
    rebalance_threshold: f64,

    /// Number of days to look back for buy signals (default 1 = current day only)
    #[arg(long, default_value = "1")]
    lookback_days: i64,

    /// Minimum number of buy signals required within lookback window to trigger a buy
    #[arg(long, default_value = "1")]
    min_buy_signals: usize,

    /// Output single-line CSV summary (for batch runs)
    #[arg(long)]
    oneline: bool,

}

#[derive(Debug, Row, Serialize, Deserialize)]
struct UnivRow {
    ticker: String,
}

/// Represents a single trading signal from decision files
#[derive(Debug, Clone)]
struct Signal {
    ticker: String,
    strategy: String,
    date: NaiveDate,
    action: String, // "buy" or "sell"
}

/// ClickHouse price data row
#[derive(Debug, Row, Serialize, Deserialize)]
struct PriceRow {
    date: u32,  // YYYYMMDD format as integer
    ticker: String,
    close: f64,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct DTC {
    date: String,
    ticker: String,
    close: Option<f64>,
}

// ============================================================================
// ClickHouse Connection Functions
// ============================================================================

/// Read environment variable for ClickHouse password
fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{} env variable should be set", key))
}

/// Test connection to ClickHouse
pub async fn test_connection(connection_type: ChConnectionType) -> bool {
    let (host, port) = match connection_type {
        ChConnectionType::Local => ("192.168.86.46", 8123),
        ChConnectionType::Remote => ("192.168.86.56", 8123),
    };

    info!("Testing connection to {}:{}...", host, port);

    match timeout(Duration::from_secs(5), TcpStream::connect((host, port))).await {
        Ok(Ok(_)) => {
            info!("✓ TCP connection to {}:{} successful", host, port);
            true
        }
        Ok(Err(e)) => {
            warn!("✗ TCP connection to {}:{} failed: {}", host, port, e);
            false
        }
        Err(_) => {
            warn!("✗ TCP connection to {}:{} timed out after 5 seconds", host, port);
            false
        }
    }
}

/// Get ClickHouse client
pub async fn get_ch_client(connection_type: ChConnectionType) -> Result<Client, Box<dyn StdError>> {
    let (host, conn_type_str) = match connection_type {
        ChConnectionType::Local => ("192.168.86.46", "Local"),
        ChConnectionType::Remote => ("192.168.86.56", "Remote"),
    };

    let client = Client::default()
        .with_url(format!("http://{}:8123", host))
        .with_user("roger")
        .with_password(read_env_var("PG"))
        .with_database("tiingo");

    let query_result = client.query("SELECT version()").fetch_one::<String>().await;

    match query_result {
        Ok(version) => {
            info!("Connected to ClickHouse {}. Server version: {}", conn_type_str, version);
            Ok(client)
        }
        Err(e) => {
            warn!("Failed to connect to ClickHouse {}: {:?}", conn_type_str, e);
            Err(Box::new(e))
        }
    }
}

pub async fn load_price_data_native(
    universe: &str,
    tickers: &[String],
    connection_type: ChConnectionType,
) -> Result<DataFrame, Box<dyn StdError>> {

    let ticker_list = tickers
        .iter()
        .map(|t| format!("'{}'", t.replace("'", "''")))
        .collect::<Vec<_>>()
        .join(",");

    let query = if universe.to_uppercase() == "CRYPTO" {
        format!(
            "SELECT formatDateTime(date, '%Y-%m-%d') AS date, baseCurrency AS ticker, max(close) AS close
             FROM tiingo.crypto FINAL
             WHERE baseCurrency IN ({})
             GROUP BY date, baseCurrency
             ORDER BY ticker, date",
            ticker_list
        )
    } else {
        format!(
            "SELECT formatDateTime(date, '%Y-%m-%d') AS date, symbol AS ticker, max(adjClose) AS close
             FROM tiingo.usd FINAL
             WHERE symbol IN ({})
             GROUP BY date, symbol
             ORDER BY ticker, date",
            ticker_list
        )
    };

    info!("Fetching prices for {} tickers from ClickHouse", tickers.len());
    debug!("ClickHouse query: {}", query);

    // Get a client connection
    let client = get_ch_client(connection_type).await?;

    // Collect all data
    let mut all_data: Vec<DTC> = Vec::new();
    let mut cursor = client.query(&query).fetch::<DTC>()?;

    while let Some(row) = cursor.next().await? {
        all_data.push(row);
    }

    // Convert to Polars DataFrame
    let dates: Vec<&str> = all_data.iter().map(|r| r.date.as_str()).collect();
    let tickers: Vec<&str> = all_data.iter().map(|r| r.ticker.as_str()).collect();
    let closes: Vec<Option<f64>> = all_data.iter().map(|r| r.close).collect();

    let df = df! {
        "Date" => dates,
        "Ticker" => tickers,
        "Close" => closes,
    }?;

    // Print a determinism check for price data
    let price_hash: u64 = all_data.iter().enumerate().fold(0u64, |acc, (i, r)| {
        acc.wrapping_add((i as u64).wrapping_mul(31))
            .wrapping_add(r.ticker.bytes().map(|b| b as u64).sum::<u64>())
            .wrapping_add(r.close.map(|c| (c * 100.0) as u64).unwrap_or(0))
    });
    info!("Price data determinism check: hash={}, rows={}", price_hash, all_data.len());

    Ok(df)

}

async fn load_universe_tickers(
    universe: &str,
    sector: Option<&str>,
    connection_type: ChConnectionType,
) -> Result<Option<Vec<String>>, Box<dyn StdError>> {
    // Skip filtering for crypto or if no specific universe/sector requested
    if universe.to_uppercase() == "CRYPTO" {
        return Ok(None);
    }

    let univ_code = match universe.to_uppercase().as_str() {
        "LC1" | "LC" => Some("LC"),
        "MC1" | "MC" => Some("MC"),
        "SC1" | "SC" => Some("SC"),
        _ => None,
    };

    // If no universe code matched and no sector, nothing to filter
    if univ_code.is_none() && sector.is_none() {
        return Ok(None);
    }

    let mut conditions = Vec::new();

    if let Some(code) = univ_code {
        conditions.push(format!("univ = '{}'", code));
    }

    if let Some(sec) = sector {
        if sec.to_uppercase() != "ALL" {
            conditions.push(format!("sector = '{}'", sec.replace("'", "''")));
        }
    }

    let where_clause = conditions.join(" AND ");

    let query = format!(
        "SELECT ticker FROM (
            SELECT f.Ticker AS ticker, u.univ AS univ, f.sector AS sector,
                ROW_NUMBER() OVER (
                    PARTITION BY f.Ticker
                    ORDER BY CASE u.univ
                        WHEN 'LC' THEN 1 WHEN 'MC' THEN 2 WHEN 'SC' THEN 3 WHEN 'Micro' THEN 4 ELSE 5
                    END
                ) AS rn
            FROM tiingo.univ u
            JOIN tiingo.fundamentals_list f ON f.Ticker = u.Ticker
        ) WHERE rn = 1 AND {}",
        where_clause
    );

    info!("Loading universe tickers: {}", query);
    let client = get_ch_client(connection_type).await?;

    let mut tickers = Vec::new();
    let mut cursor = client.query(&query).fetch::<UnivRow>()?;
    while let Some(row) = cursor.next().await? {
        tickers.push(row.ticker.to_uppercase());
    }

    info!("Universe filter matched {} tickers", tickers.len());
    Ok(Some(tickers))
}

// ============================================================================
// Signal and Price Loading Functions
// ============================================================================

/// Read all decision files from signal folder (output/testing_DATE or output_crypto/testing_DATE)
// Update the read_decision_files function to extract just the signal name
fn read_decision_files(
    signal_folder: &str,
    ticker_filter: Option<Vec<String>>,
    signal_list: Option<Vec<String>>,
) -> Result<(Vec<Signal>, Vec<String>), Box<dyn StdError>> {
    let path = Path::new(signal_folder);
    if !path.exists() {
        return Err(format!("Signal folder does not exist: {}", signal_folder).into());
    }

    let mut signals = Vec::new();
    let mut available_signals = Vec::new(); // Track available signals

    // Collect and sort directory entries for deterministic ordering
    let mut entries: Vec<_> = fs::read_dir(path)?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let path = entry.path();
        if path.is_file() {
            let filename = path.file_name().unwrap().to_string_lossy();

            // Only process decision CSV files
            if !filename.ends_with("_decisions.csv") {
                continue;
            }

            // Extract signal name from filename (remove _decisions.csv suffix)
            let full_signal_name = filename.trim_end_matches("_decisions.csv").to_string();

            // Extract just the signal part (after the ticker)
            let signal_name = if let Some(underscore_pos) = full_signal_name.find('_') {
                full_signal_name[underscore_pos + 1..].to_string()
            } else {
                full_signal_name.clone()
            };

            available_signals.push(signal_name.clone()); // Add to available signals

            // Skip if signal_list is provided and this signal is not in the list
            if let Some(ref signals_to_process) = signal_list {
                if !signals_to_process.contains(&signal_name) {
                    continue;
                }
            }

            // Read the CSV file - it has columns: ticker,strategy,date,action
            let schema = Schema::from_iter(vec![
                Field::new("ticker".into(), DataType::String),
                Field::new("strategy".into(), DataType::String),
                Field::new("date".into(), DataType::String),
                Field::new("action".into(), DataType::String),
            ]);
            let df = CsvReadOptions::default()
                .with_has_header(true)
                .with_schema(Some(Arc::new(schema)))
                .into_reader_with_file_handle(File::open(&path)?)
                .finish()?;
            if df.height() == 0 {
                continue;
            }

            // Extract signals from CSV
            let tickers_col = df.column("ticker")?.str()?;
            let strategies_col = df.column("strategy")?.str()?;
            let dates_col = df.column("date")?.str()?;
            let actions_col = df.column("action")?.str()?;

            for i in 0..df.height() {
                if let (Some(ticker_str), Some(strategy_str), Some(date_str), Some(action_str)) = (
                    tickers_col.get(i),
                    strategies_col.get(i),
                    dates_col.get(i),
                    actions_col.get(i),
                ) {
                    let ticker = ticker_str.to_uppercase();

                    // Apply ticker filter if provided
                    if let Some(ref filter) = ticker_filter {
                        if !filter.contains(&ticker) {
                            continue;
                        }
                    }

                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        signals.push(Signal {
                            ticker,
                            strategy: strategy_str.to_string(),
                            date,
                            action: action_str.to_lowercase(),
                        });
                    }
                }
            }
        }
    }

    // Sort signals for deterministic ordering (by date, ticker, strategy, action)
    signals.sort_by(|a, b| {
        a.date.cmp(&b.date)
            .then_with(|| a.ticker.cmp(&b.ticker))
            .then_with(|| a.strategy.cmp(&b.strategy))
            .then_with(|| a.action.cmp(&b.action))
    });

    // Sort available_signals for deterministic ordering
    available_signals.sort();
    available_signals.dedup();

    info!("Loaded {} signals from {} files", signals.len(), signals.iter().map(|s| s.ticker.as_str()).collect::<std::collections::HashSet<_>>().len());

    // Print a determinism check - this hash should be identical across runs
    let signal_hash: u64 = signals.iter().enumerate().fold(0u64, |acc, (i, s)| {
        acc.wrapping_add((i as u64).wrapping_mul(31))
            .wrapping_add(s.ticker.bytes().map(|b| b as u64).sum::<u64>())
            .wrapping_add(s.date.num_days_from_ce() as u64)
    });
    info!("Signal determinism check: hash={}, count={}, first={:?}",
          signal_hash, signals.len(), signals.first().map(|s| (&s.ticker, &s.date, &s.action)));

    Ok((signals, available_signals))
}

/// Get price for a specific ticker on a specific date
/// If multiple prices exist for the same ticker+date, takes the MAX (deterministic)
fn get_price(
    prices_df: &DataFrame,
    ticker: &str,
    date: NaiveDate,
) -> Result<Option<f64>, Box<dyn StdError>> {
    let date_str = date.format("%Y-%m-%d").to_string();

    let filtered = prices_df
        .clone()
        .lazy()
        .filter(
            col("Ticker").eq(lit(ticker))
            .and(col("Date").eq(lit(date_str.clone())))
        )
        .collect()?;

    if filtered.height() == 0 {
        return Ok(None);
    }

    // Warn if there are duplicate prices (data quality issue)
    if filtered.height() > 1 {
        warn!("Multiple prices ({}) found for {} on {}", filtered.height(), ticker, date_str);
    }

    // Use max() for deterministic result when duplicates exist
    let close = filtered.column("Close")?
        .f64()?
        .max();

    Ok(close)
}

/// Get prices for all tickers on a specific date
/// Uses max() aggregation to handle duplicate ticker+date entries deterministically
fn get_prices_for_date(
    prices_df: &DataFrame,
    date: NaiveDate,
) -> Result<HashMap<String, f64>, Box<dyn StdError>> {
    use polars::prelude::*;

    let date_str = date.format("%Y-%m-%d").to_string();

    // Filter by date and group by ticker, taking max price for determinism
    let aggregated = prices_df
        .clone()
        .lazy()
        .filter(col("Date").eq(lit(date_str)))
        .group_by([col("Ticker")])
        .agg([col("Close").max().alias("Close")])
        .collect()?;

    let mut prices = HashMap::new();

    if aggregated.height() == 0 {
        return Ok(prices);
    }

    let tickers = aggregated.column("Ticker")?.str()?;
    let closes = aggregated.column("Close")?.f64()?;

    for i in 0..aggregated.height() {
        if let (Some(ticker), Some(price)) = (tickers.get(i), closes.get(i)) {
            prices.insert(ticker.to_uppercase(), price);
        }
    }

    Ok(prices)
}

/// Rank buy candidates by priority strategy signal count
fn rank_buy_candidates(
    buy_signals: &[Signal],
    priority_strategy: &str,
) -> Vec<(String, usize)> {
    let mut ticker_scores: HashMap<String, usize> = HashMap::new();

    // Count priority strategy signals
    for signal in buy_signals {
        if signal.strategy == priority_strategy {
            *ticker_scores.entry(signal.ticker.clone()).or_insert(0) += 1;
        }
    }

    // Convert to sorted vector (descending by score, then alphabetically by ticker for determinism)
    let mut ranked: Vec<(String, usize)> = ticker_scores.into_iter().collect();
    ranked.sort_by(|a, b| {
        match b.1.cmp(&a.1) {
            std::cmp::Ordering::Equal => a.0.cmp(&b.0),  // Alphabetical for ties
            other => other,
        }
    });

    debug!("Ranked {} buy candidates", ranked.len());
    ranked
}

/// Run the portfolio backtest
async fn backtest_portfolio(
    args: &Args,
    signals: Vec<Signal>,
    prices_df: DataFrame,
) -> Result<PortfolioAccounting, Box<dyn StdError>> {

    // Initialize portfolio accounting
    let initial_cash = 10_000.0;
    let mut portfolio = PortfolioAccounting::new(initial_cash);
    let commission = args.commission;

    // Track last rebalance date if rebalancing is enabled
    let mut last_rebalance_date: Option<NaiveDate> = None;

    // Parse start_date if provided
    let start_date_filter: Option<NaiveDate> = if let Some(ref start_date_str) = args.start_date {
        match NaiveDate::parse_from_str(start_date_str, "%Y-%m-%d") {
            Ok(date) => {
                info!("Filtering signals to start from {}", date);
                Some(date)
            },
            Err(e) => {
                warn!("Invalid start_date format '{}': {}. Expected YYYY-MM-DD format.", start_date_str, e);
                None
            }
        }
    } else {
        None
    };

    // Group signals by date
    let mut signals_by_date: BTreeMap<NaiveDate, Vec<Signal>> = BTreeMap::new();
    for signal in signals {
        // Filter by start_date if provided
        if let Some(start_date) = start_date_filter {
            if signal.date < start_date {
                continue;
            }
        }
        signals_by_date.entry(signal.date).or_insert_with(Vec::new).push(signal);
    }

    // Sort signals within each day for deterministic ordering (by ticker, then strategy, then action)
    for signals in signals_by_date.values_mut() {
        signals.sort_by(|a, b| {
            a.ticker.cmp(&b.ticker)
                .then_with(|| a.strategy.cmp(&b.strategy))
                .then_with(|| a.action.cmp(&b.action))
        });
    }

    info!("Processing {} trading dates", signals_by_date.len());

    // Process each trading date
    for (date, day_signals) in &signals_by_date {
        let date = *date; // Copy the date since we're borrowing
        debug!("Processing date: {}", date);

        // Get prices for today
        let prices = get_prices_for_date(&prices_df, date)?;

        if prices.is_empty() {
            debug!("No price data for {}, skipping", date);
            continue;
        }

        // 1. Check stop-losses on existing positions
        let mut positions_to_close = Vec::new();
        let position_tickers: Vec<String> = portfolio.get_all_positions()
            .iter()
            .map(|p| p.ticker.clone())
            .collect();

        for ticker in position_tickers {
            if let Some(position) = portfolio.get_position(&ticker) {
                // Calculate stop-loss price based on entry price
                if args.stop_loss_pct > 0.0 {
                    let stop_price = position.avg_cost_basis * (1.0 - args.stop_loss_pct);
                    if let Some(&current_price) = prices.get(&ticker) {
                        if current_price <= stop_price {
                            positions_to_close.push(ticker.clone());
                            debug!("Stop-loss triggered for {}: price {} <= stop {}",
                                   ticker, current_price, stop_price);
                        }
                    }
                }
            }
        }

        // Close stop-loss positions
        for ticker in positions_to_close {
            if let Some(&exit_price) = prices.get(&ticker) {
                if let Ok(_) = portfolio.execute_sell(date, &ticker, exit_price, commission) {
                    info!("STOP-LOSS {} on {}: @ ${:.2}", ticker, date, exit_price);
                }
            }
        }

        // 2. Process sell signals (day after signal, so check if we have positions)
        let sell_signals: Vec<&Signal> = day_signals.iter()
            .filter(|s| s.action == "sell")
            .collect();

        for sell_signal in sell_signals {
            if portfolio.has_position(&sell_signal.ticker) {
                // Close position on day after sell signal
                let next_date = date + chrono::Duration::days(1);
                if let Ok(Some(exit_price)) = get_price(&prices_df, &sell_signal.ticker, next_date) {
                    if let Ok(_) = portfolio.execute_sell(next_date, &sell_signal.ticker, exit_price, commission) {
                        info!("SELL-SIGNAL {} on {}: @ ${:.2}", sell_signal.ticker, next_date, exit_price);
                    }
                }
            }
        }

        // 3. Process buy signals (only if we have available slots)
        let current_position_count = portfolio.get_position_count();
        let available_slots = if current_position_count < args.portfolio_size {
            args.portfolio_size - current_position_count
        } else {
            0
        };

        if available_slots > 0 {
            // Collect buy signals from lookback window (last N days including today)
            let mut multi_day_buy_signals: Vec<Signal> = Vec::new();
            let mut tickers_with_sell_signals: std::collections::HashSet<String> = std::collections::HashSet::new();

            for lookback in 0..args.lookback_days {
                let check_date = date - chrono::Duration::days(lookback);
                if let Some(past_signals) = signals_by_date.get(&check_date) {
                    multi_day_buy_signals.extend(
                        past_signals.iter()
                            .filter(|s| s.action == "buy" && !portfolio.has_position(&s.ticker))
                            .cloned()
                    );
                    // Track tickers that also have sell signals in the window
                    for s in past_signals.iter().filter(|s| s.action == "sell") {
                        tickers_with_sell_signals.insert(s.ticker.clone());
                    }
                }
            }

            // Remove buy candidates that also had sell signals in the lookback window
            if !tickers_with_sell_signals.is_empty() {
                let before = multi_day_buy_signals.len();
                multi_day_buy_signals.retain(|s| !tickers_with_sell_signals.contains(&s.ticker));
                let filtered = before - multi_day_buy_signals.len();
                if filtered > 0 {
                    warn!("Date {}: filtered {} buy signals due to conflicting sell signals in lookback window",
                        date, filtered);
                }
            }

            // Count buy signals per ticker across the lookback window
            let mut ticker_signal_counts: HashMap<String, usize> = HashMap::new();
            for signal in &multi_day_buy_signals {
                *ticker_signal_counts.entry(signal.ticker.clone()).or_insert(0) += 1;
            }

            // Filter to only tickers with minimum required buy signals
            let qualified_tickers: std::collections::HashSet<String> = ticker_signal_counts.iter()
                .filter(|(_, &count)| count >= args.min_buy_signals)
                .map(|(ticker, _)| ticker.clone())
                .collect();

            // Log lookback results if using multi-day lookback
            if args.lookback_days > 1 {
                debug!("Date {}: {} tickers with signals in {}-day window, {} qualified with {}+ signals",
                       date, ticker_signal_counts.len(), args.lookback_days,
                       qualified_tickers.len(), args.min_buy_signals);
            }

            // Filter buy signals to only include qualified tickers
            let buy_signals: Vec<Signal> = multi_day_buy_signals.into_iter()
                .filter(|s| qualified_tickers.contains(&s.ticker))
                .collect();

            if !buy_signals.is_empty() {
                // Rank candidates by priority strategy
                let ranked = rank_buy_candidates(&buy_signals, &args.priority_strategy);

                // Log ranking for determinism debugging
                if !ranked.is_empty() {
                    debug!("Date {}: ranked {} candidates, top 3: {:?}",
                           date, ranked.len(), ranked.iter().take(3).collect::<Vec<_>>());
                }

                // Fill available slots with top-ranked candidates
                let slots_to_fill = available_slots.min(ranked.len());

                for i in 0..slots_to_fill {
                    let (ticker, _score) = &ranked[i];

                    // Entry on day after buy signal
                    let next_date = date + chrono::Duration::days(1);

                    if let Ok(Some(entry_price)) = get_price(&prices_df, ticker, next_date) {
                        // Calculate position size: equal weight allocation
                        let target_positions = args.portfolio_size as f64;
                        let position_value = portfolio.get_total_value() / target_positions;
                        let shares = (position_value / entry_price).floor();

                        if shares > 0.0 {
                            // Execute buy through accounting system with explicit shares and commission
                            if let Ok(txn) = portfolio.execute_buy(next_date, ticker, shares, entry_price, commission) {
                                info!("BUY {} on {}: {} shares @ ${:.2} (total value: ${:.2})",
                                      ticker, next_date, txn.shares, entry_price, portfolio.get_total_value());
                            }
                        }
                    }
                }
            }
        }

        // 4. Mark-to-market all positions and take daily snapshot
        portfolio.mark_to_market(date, &prices);
        portfolio.take_daily_snapshot(date);

        // 5. Check and perform rebalancing if needed
        if args.rebalance {
            let should_rebalance = match last_rebalance_date {
                None => true, // First rebalance
                Some(last_date) => {
                    // Rebalance monthly (check if we're in a new month)
                    date.year() > last_date.year() ||
                    (date.year() == last_date.year() && date.month() > last_date.month())
                }
            };

            if should_rebalance {
                debug!("Rebalancing portfolio on {}", date);

                // Calculate target weight per position (equal weight)
                let position_count = portfolio.get_position_count();
                if position_count > 0 {
                    let target_weight = 1.0 / position_count as f64;
                    let total_value = portfolio.get_total_value();

                    // Check each position and rebalance if needed
                    let position_tickers: Vec<String> = portfolio.get_all_positions()
                        .iter()
                        .map(|p| p.ticker.clone())
                        .collect();

                    for ticker in position_tickers {
                        if let Some(current_weight) = portfolio.get_position_weight(&ticker) {
                            let weight_diff = (current_weight - target_weight).abs();

                            // Only rebalance if deviation exceeds threshold
                            if weight_diff > args.rebalance_threshold {
                                let target_value = target_weight * total_value;

                                if let Some(position) = portfolio.get_position(&ticker) {
                                    if let Some(&current_price) = prices.get(&ticker) {
                                        let current_value = position.current_value;

                                        if current_value > target_value {
                                            // Sell excess shares
                                            let excess_value = current_value - target_value;
                                            let shares_to_sell = (excess_value / current_price).floor();

                                            if shares_to_sell > 0.0 {
                                                let _ = portfolio.execute_sell_shares(
                                                    date,
                                                    &ticker,
                                                    shares_to_sell,
                                                    current_price,
                                                    commission
                                                );
                                                debug!("Rebalanced {}: sold {:.0} shares", ticker, shares_to_sell);
                                            }
                                        }
                                        // Note: We don't buy more shares during rebalancing in this implementation
                                        // to keep it simple. Could be added if needed.
                                    }
                                }
                            }
                        }
                    }
                }

                last_rebalance_date = Some(date);
            }
        }
    }

    // Close any remaining positions at final prices
    let position_tickers: Vec<String> = portfolio.get_all_positions()
        .iter()
        .map(|p| p.ticker.clone())
        .collect();

    if !position_tickers.is_empty() {
        let final_date = portfolio.daily_snapshots.last()
            .map(|s| s.date)
            .unwrap_or_else(|| chrono::Utc::now().date_naive());

        let final_prices = get_prices_for_date(&prices_df, final_date)?;
        let commission = 0.0;

        for ticker in position_tickers {
            if let Some(&exit_price) = final_prices.get(&ticker) {
                if let Ok(_) = portfolio.execute_sell(final_date, &ticker, exit_price, commission) {
                    debug!("Closed final position {} at {}", ticker, exit_price);
                }
            }
        }
    }

    // Create a hash of all transactions for determinism verification
    let txn_hash: u64 = portfolio.transactions.iter().enumerate().fold(0u64, |acc, (i, t)| {
        acc.wrapping_add((i as u64).wrapping_mul(31))
            .wrapping_add(t.ticker.bytes().map(|b| b as u64).sum::<u64>())
            .wrapping_add((t.shares * 100.0) as u64)
            .wrapping_add((t.price * 100.0) as u64)
    });
    info!("Completed backtest: {} transactions, txn_hash={}", portfolio.transactions.len(), txn_hash);
    Ok(portfolio)
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
    info!("Signal date: {}", args.signal_date);
    info!("Portfolio size: {}", args.portfolio_size);
    info!("Priority strategy: {}", args.priority_strategy);
    info!("Universe: {}", args.universe);
    if args.stop_loss_pct > 0.0 {
        info!("Stop-loss: {:.1}%", args.stop_loss_pct * 100.0);
    }
    if args.lookback_days > 1 || args.min_buy_signals > 1 {
        info!("Buy signal lookback: {} days, minimum signals: {}", args.lookback_days, args.min_buy_signals);
    }

    // Determine signal folder path based on universe and date
    let signal_folder = if args.signal_date.starts_with("output") {
        // Full path provided
        args.signal_date.clone()
    } else {
        // Just date provided - construct path
        let folder_prefix = if args.universe.to_uppercase() == "CRYPTO" {
            "output_crypto"
        } else {
            "output"
        };
        format!("{}/testing_{}", folder_prefix, args.signal_date)
    };

    info!("Signal folder: {}", signal_folder);

    // Parse ticker filter if provided
    let ticker_filter = args.tickers.as_ref().map(|t| {
        t.split(',')
        .map(|s| s.trim().to_uppercase())
        .collect::<Vec<String>>()
    });

    // Read decision files from signal folder
    info!("Reading signal files from {}...", signal_folder);

    // Parse the signal list from command line argument
    let signal_list = args.signals.clone().map(|s| s.split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>()
    );

    // Determine ClickHouse connection type
    let connection_type = {
        info!("Testing ClickHouse connection...");
        if test_connection(ChConnectionType::Local).await {
            info!("Using Local ClickHouse connection (192.168.86.46)");
            ChConnectionType::Local
        } else if test_connection(ChConnectionType::Remote).await {
            info!("Using Remote ClickHouse connection (192.168.86.56)");
            ChConnectionType::Remote
        } else {
            return Err("Failed to connect to ClickHouse. ".into());
        }
    };


    // Load universe/sector filter from ClickHouse
    let universe_tickers = load_universe_tickers(
        &args.universe,
        args.sector.as_deref(),
        connection_type,
    ).await?;

    // Merge universe filter with explicit --tickers filter
    let ticker_filter = match (ticker_filter, universe_tickers) {
        (Some(explicit), Some(univ)) => {
            // Intersect: only tickers in both lists
            let univ_set: std::collections::HashSet<String> = univ.into_iter().collect();
            let intersected: Vec<String> = explicit.into_iter()
                .filter(|t| univ_set.contains(t))
                .collect();
            info!("Ticker filter intersected: {} tickers", intersected.len());
            Some(intersected)
        }
        (Some(explicit), None) => Some(explicit),
        (None, Some(univ)) => Some(univ),
        (None, None) => None,
    };

    // Call the function with the signal list
    let (signals, available_signals) = read_decision_files(&signal_folder, ticker_filter, signal_list.clone())?;


    if signals.is_empty() {
        eprintln!("No signals found in {}", signal_folder);
        eprintln!("Available signals in folder:");
        for signal in &available_signals {
            eprintln!("  - {}", signal);
        }

        if let Some(requested_signals) = signal_list {
            eprintln!("Requested signals:");
            for signal in &requested_signals {
                eprintln!("  - {}", signal);
            }

            // Find which requested signals weren't found
            let missing_signals: Vec<_> = requested_signals.iter()
                .filter(|s| !available_signals.contains(s))
                .collect();

            if !missing_signals.is_empty() {
                eprintln!("Missing signals (not found in folder):");
                for signal in missing_signals {
                    eprintln!("  - {}", signal);
                }
            }
        }

        return Ok(());
    }

    info!("Loaded {} signals", signals.len());

    // println!("signals {:?}", signals.clone());

    // Get unique tickers from signals
    let unique_tickers: Vec<String> = signals
        .iter()
        .map(|s| s.ticker.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    info!("Found {} unique tickers in signals", unique_tickers.len());

    // Load price data
    info!("Loading price data...");
    let prices_df = load_price_data_native(
        &args.universe,
        &unique_tickers,
        connection_type,
    )
    .await?;

    info!("Loaded price data: {} rows, {} columns", prices_df.height(), prices_df.width());

    // Run backtest
    info!("Running backtest...");
    let portfolio = backtest_portfolio(&args, signals, prices_df).await?;

    // Calculate and display metrics from accounting system
    let perf_summary = portfolio.calculate_performance_summary();
    if !args.oneline {
        portfolio.print_performance_summary(&perf_summary);
    }

    if args.oneline {
        // Print header if stdout is a terminal (first run hint) - or always print to stderr
        let header = "universe,sector,priority_strategy,signals,portfolio_size,stop_loss_pct,lookback_days,initial_value,final_value,total_return_pct,realized_pnl,unrealized_pnl,commissions,total_trades,winning_trades,win_rate_pct,avg_win_pct,avg_loss_pct,profit_factor,max_drawdown_pct,sharpe_ratio,avg_holding_days";

        // Print header to stderr so it doesn't mix with CSV data
        static HEADER_PRINTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !HEADER_PRINTED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            eprintln!("{}", header);
        }

        println!("{},{},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{:.1},{:.2},{:.2},{:.3},{:.2},{:.3},{:.1}",
            args.universe,
            args.sector.as_deref().unwrap_or("ALL"),
            args.priority_strategy,
            args.signals.as_deref().unwrap_or("ALL"),
            args.portfolio_size,
            args.stop_loss_pct,
            args.lookback_days,
            perf_summary.initial_value,
            perf_summary.final_value,
            perf_summary.total_return_pct,
            perf_summary.total_realized_pnl,
            perf_summary.total_unrealized_pnl,
            perf_summary.total_commissions,
            perf_summary.total_trades,
            perf_summary.winning_trades,
            perf_summary.win_rate_pct,
            perf_summary.avg_win_pct,
            perf_summary.avg_loss_pct,
            perf_summary.profit_factor,
            perf_summary.max_drawdown_pct,
            perf_summary.sharpe_ratio,
            perf_summary.avg_holding_days,
        );
    }

    // Save accounting reports if requested
    if args.accounting_reports {
        info!("Generating accounting reports...");
        portfolio.save_all_reports(&args.prefix, true)?;  // json_only=true since we already printed summary above
        println!("\nAccounting reports saved:");
        println!("  Transactions: {}_transactions.csv", &args.prefix);
        println!("  Positions: {}_positions.csv", &args.prefix);
        println!("  Daily Snapshots: {}_daily.csv", &args.prefix);
        println!("  Realized P&L: {}_realized_pnl.csv", &args.prefix);
        println!("  Unrealized P&L: {}_unrealized_pnl.csv", &args.prefix);
        println!("  Cash Flows: {}_cashflows.csv", &args.prefix);
        println!("  Performance: {}_performance.json", &args.prefix);
    }
    Ok(())
}
