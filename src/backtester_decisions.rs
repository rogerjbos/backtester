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

// Add this enum above the client functions
pub enum ChConnectionType {
    Local,
    Remote,
}

pub async fn test_connection(connection_type: ChConnectionType) -> Result<(), Box<dyn StdError>> {
    let (host, port) = match connection_type {
        ChConnectionType::Local => ("192.168.86.246", 8123),
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
            let host = "192.168.86.246";
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
    st_cum_return: f64,
    st_accuracy: f64,
    mt_cum_return: f64,
    mt_accuracy: f64,
    lt_cum_return: f64,
    lt_accuracy: f64,
    bh_cum_return: f64,
    bh_accuracy: f64,
    buy_and_hold_return: f64,
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

    // Calculate cumulative returns and accuracy
    let mut position_stats = HashMap::new();

    for (col_name, pos_vec) in &[("bh", &bh), ("st", &st), ("mt", &mt), ("lt", &lt)] {
        let cum_return = pos_vec.iter().zip(daily_returns.iter()).map(|(p, r)| p * r).sum::<f64>();
        position_stats.insert(format!("{}_cum_return", col_name), cum_return);

        let num_positive = pos_vec.iter().zip(daily_returns.iter()).filter(|(p, r)| **p == 1.0 && **r > 0.0).count();
        let total_days = pos_vec.iter().filter(|p| **p == 1.0).count();
        let accuracy = if total_days > 0 { num_positive as f64 / total_days as f64 } else { 0.0 };
        position_stats.insert(format!("{}_accuracy", col_name), accuracy);
    }

    // Buy and hold return
    let buy_and_hold_return = daily_returns.iter().map(|r| 1.0 + r / 100.0).product::<f64>() - 1.0 * 100.0;
    position_stats.insert("buy_and_hold_return".to_string(), buy_and_hold_return);

    let mut result_df = merged.clone();
    result_df.with_column(Series::new("bh".into(), bh))?;
    result_df.with_column(Series::new("st".into(), st))?;
    result_df.with_column(Series::new("mt".into(), mt))?;
    result_df.with_column(Series::new("lt".into(), lt))?;

    Ok(position_stats)
}

pub async fn run_analysis(asset_type: &str, connection_type: ChConnectionType) -> Result<(), Box<dyn StdError>> {
    // Load decisions
    let decisions_df = load_decisions(asset_type)?;
    println!("Loaded {} decisions", decisions_df.height());

    // Get unique tickers
    let tickers: Vec<String> = decisions_df
        .column("ticker")?
        .unique()?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    println!("Found {} unique tickers", tickers.len());

    // Get price data
    let returns_df = get_price_dataframe(asset_type, &tickers, false, connection_type).await?;
    let returns_df = calculate_daily_return(&returns_df)?;

    println!("Loaded {} price records", returns_df.height());

    // Get unique strategies
    let strategies: Vec<String> = decisions_df
        .column("strategy")?
        .unique()?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    println!("Found {} unique strategies", strategies.len());

    // Collect results
    let mut results: Vec<PerformanceResult> = Vec::new();

    // For each ticker and strategy, calculate
    for ticker in &tickers {
        for strategy in &strategies {
            let stats = calculate_strategy_data(&decisions_df, &returns_df, ticker, strategy)?;
            if !stats.is_empty() {
                let result = PerformanceResult {
                    ticker: ticker.clone(),
                    strategy: strategy.clone(),
                    st_cum_return: *stats.get("st_cum_return").unwrap_or(&0.0),
                    st_accuracy: *stats.get("st_accuracy").unwrap_or(&0.0),
                    mt_cum_return: *stats.get("mt_cum_return").unwrap_or(&0.0),
                    mt_accuracy: *stats.get("mt_accuracy").unwrap_or(&0.0),
                    lt_cum_return: *stats.get("lt_cum_return").unwrap_or(&0.0),
                    lt_accuracy: *stats.get("lt_accuracy").unwrap_or(&0.0),
                    bh_cum_return: *stats.get("bh_cum_return").unwrap_or(&0.0),
                    bh_accuracy: *stats.get("bh_accuracy").unwrap_or(&0.0),
                    buy_and_hold_return: *stats.get("buy_and_hold_return").unwrap_or(&0.0),
                };
                results.push(result);
            }
        }
    }

    // Write to CSV
    let csv_path = format!("performance_results_{}.csv", asset_type);
    let mut df = df! {
        "ticker" => results.iter().map(|r| r.ticker.as_str()).collect::<Vec<_>>(),
        "strategy" => results.iter().map(|r| r.strategy.as_str()).collect::<Vec<_>>(),
        "st_cum_return" => results.iter().map(|r| r.st_cum_return).collect::<Vec<_>>(),
        "st_accuracy" => results.iter().map(|r| r.st_accuracy).collect::<Vec<_>>(),
        "mt_cum_return" => results.iter().map(|r| r.mt_cum_return).collect::<Vec<_>>(),
        "mt_accuracy" => results.iter().map(|r| r.mt_accuracy).collect::<Vec<_>>(),
        "lt_cum_return" => results.iter().map(|r| r.lt_cum_return).collect::<Vec<_>>(),
        "lt_accuracy" => results.iter().map(|r| r.lt_accuracy).collect::<Vec<_>>(),
        "bh_cum_return" => results.iter().map(|r| r.bh_cum_return).collect::<Vec<_>>(),
        "bh_accuracy" => results.iter().map(|r| r.bh_accuracy).collect::<Vec<_>>(),
        "buy_and_hold_return" => results.iter().map(|r| r.buy_and_hold_return).collect::<Vec<_>>(),
    }?;
    let mut file = std::fs::File::create(&csv_path)?;
    CsvWriter::new(&mut file).finish(&mut df)?;
    println!("Results written to {}", csv_path);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <asset_type>", args[0]);
        eprintln!("asset_type: 'stocks' or 'crypto'");
        std::process::exit(1);
    }
    let asset_type = &args[1];
    if asset_type != "stocks" && asset_type != "crypto" {
        eprintln!("asset_type must be 'stocks' or 'crypto'");
        std::process::exit(1);
    }

    // Test connection first
    println!("Testing ClickHouse connection...");
    let connection_type = if test_connection(ChConnectionType::Local).await.is_ok() {
        ChConnectionType::Local
    } else if test_connection(ChConnectionType::Remote).await.is_ok() {
        ChConnectionType::Remote
    } else {
        println!("Both local and remote connections failed.");
        std::process::exit(1);
    };

    run_analysis(asset_type, connection_type).await
}