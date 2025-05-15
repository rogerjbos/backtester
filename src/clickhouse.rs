use chrono::{Duration, NaiveDate};
use clickhouse::{Client, Row};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::{env, error::Error as StdError, fmt::Debug, process::Command};
use std::fs::File;
use std::io::{self, Write, BufReader}; // Add these imports
use std::path::Path;

#[derive(Debug, Row, Serialize, Deserialize)]
struct OHLCV {
    date: String,
    ticker: String,
    universe: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

pub async fn write_price_file(univ: String, production: bool) -> Result<(), Box<dyn StdError>> {
    let user_path = match env::var("CLICKHOUSE_USER_PATH") {
        Ok(path) => path,
        Err(_) => String::from("/srv"),
    };
    let folder = if production { "production" } else { "testing" };
    let filename = format!(
        "{}/rust_home/backtester/data/{}/{}.csv",
        user_path.to_string(),
        folder.to_string(),
        univ
    );

    // Get the list of tickers in the universe
    let tickers = get_universe_tickers(&univ).await?;
    
    // Process in chunks of 50 tickers (adjust based on your memory constraints)
    let chunk_size = 100;
    let ticker_chunks: Vec<Vec<String>> = tickers
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();
    
    // Create a temp file for each chunk
    let mut temp_files = Vec::new();
    
    // Process each chunk
    for (i, chunk) in ticker_chunks.iter().enumerate() {
        let temp_filename = format!("{}.part{}", &filename, i);
        temp_files.push(temp_filename.clone());
        
        // Join the ticker list into a quoted, comma-separated string for SQL IN clause
        let ticker_list = chunk
            .iter()
            .map(|t| format!("'{}'", t))
            .collect::<Vec<_>>()
            .join(",");
        
        // Modify your queries to filter by the specific chunk of tickers
        let query = if production && univ == "Crypto" {
            format!(
                "WITH univ AS (
                SELECT baseCurrency ticker, max(date) maxdate
                FROM tiingo.crypto
                WHERE baseCurrency IN ({})
                group by ticker
                having count(date) > 120 and COUNT(*) * 2 - COUNT(high) - COUNT(low) = 0
                )
                SELECT date(p.date) Date, u.ticker Ticker, 'Crypto' as Universe,
                open AS Open, high AS High, low AS Low, close AS Close, volume AS Volume
                FROM tiingo.crypto p
                INNER JOIN univ u
                ON u.ticker = p.baseCurrency
                WHERE p.date >= subtractDays(now(), 252)
                and maxdate IN (select max(date) from tiingo.crypto)
                order by ticker, date",
                ticker_list
            )
        } else if production && univ != "Crypto" {
            format!(
                "WITH mdate AS (
                SELECT symbol, max(date(date)) AS maxdate
                FROM tiingo.usd p
                WHERE symbol IN ({})
                group by symbol
                having count(date) >= 250 and COUNT(*) * 2 - COUNT(adjHigh) - COUNT(adjLow) = 0
                )
                SELECT date(p.date) Date
                , symbol AS Ticker
                , '{univ}' AS Universe
                , round(adjOpen, 2) AS Open
                , round(adjHigh, 2) AS High
                , round(adjLow, 2) AS Low
                , round(adjClose, 2) AS Close
                , round(adjVolume, 2) AS Volume
                FROM tiingo.usd p
                INNER JOIN mdate m
                ON m.symbol = p.symbol
                WHERE p.date >= subtractDays(now(), 365)
                and m.maxdate IN (select max(date(date)) from tiingo.usd)
                order by Ticker, date",
                ticker_list
            )
        } else if !production && univ == "Crypto" {
            format!(
                "WITH univ AS (
                SELECT baseCurrency ticker, max(date) maxdate
                FROM tiingo.crypto
                WHERE baseCurrency IN ({})
                group by ticker
                having count(date) > 360 and COUNT(*) * 2 - COUNT(high) - COUNT(low) = 0
                )
                SELECT date(p.date) Date, u.ticker Ticker, 'Crypto' as Universe,
                open AS Open, high AS High, low AS Low, close AS Close, volume AS Volume
                FROM tiingo.crypto p
                INNER JOIN univ u
                ON u.ticker = p.baseCurrency
                order by ticker, date",
                ticker_list
            )
        } else if !production && univ != "Crypto" {
            format!(
                "WITH mdate AS (
                SELECT symbol, max(date(date)) AS maxdate
                FROM tiingo.usd p
                WHERE symbol IN ({})
                group by symbol
                having count(date) >= 1000 and COUNT(*) * 2 - COUNT(adjHigh) - COUNT(adjLow) = 0
                )
                SELECT date(p.date) Date
                , symbol AS Ticker
                , '{univ}' AS Universe
                , round(adjOpen, 2) AS Open
                , round(adjHigh, 2) AS High
                , round(adjLow, 2) AS Low
                , round(adjClose, 2) AS Close
                , round(adjVolume, 2) AS Volume
                FROM tiingo.usd p
                INNER JOIN mdate m
                ON m.symbol = p.symbol
                WHERE m.maxdate IN (select max(date(date)) from tiingo.usd)
                order by Ticker, date",
                ticker_list
            )
        } else {
            panic!("Error: no query match")
        };

        // Execute the query and write to temp file
        let user = env::var("CLICKHOUSE_USER")?;
        let pw = env::var("CLICKHOUSE_PASSWORD")?;

        let clickhouse_client_path = if cfg!(target_os = "macos") {
            "clickhouse-client"
        } else {
            "/usr/local/bin/clickhouse-client"
        };

        let cmd = format!(
            r#"{} --host='vdib5n7pan.europe-west4.gcp.clickhouse.cloud' --user='{}' --password='{}' --secure --database=tiingo -q "{}" --format=CSVWithNames > {}"#,
            clickhouse_client_path, 
            user,
            pw,
            query,
            temp_filename
        );

        let output = Command::new("/bin/sh").arg("-c").arg(&cmd).output()?;

        if !output.status.success() {
            eprintln!("Query failed with status: {:?}", output.status);
            eprintln!("stderr: {:?}", String::from_utf8_lossy(&output.stderr));
            return Err("Failed to execute query".into());
        }
    }

    // Combine the temp files into the final file
    combine_csv_files(&temp_files, &filename)?;
    
    // Clean up temp files
    for temp_file in temp_files {
        std::fs::remove_file(temp_file)?;
    }

    Ok(())
}

// Helper function to get the list of tickers in a universe
async fn get_universe_tickers(univ: &str) -> Result<Vec<String>, Box<dyn StdError>> {
    let user = env::var("CLICKHOUSE_USER")?;
    let pw = env::var("CLICKHOUSE_PASSWORD")?;
    
    let query = if univ == "Crypto" {
        "SELECT DISTINCT baseCurrency FROM tiingo.crypto".to_string()
    } else {
        format!("SELECT DISTINCT Ticker FROM univ WHERE batch = '{}'", univ)
    };
    
    // Create a temporary file to store the ticker list
    let temp_file = format!("/tmp/tickers_{}.csv", univ);
    
    let clickhouse_client_path = if cfg!(target_os = "macos") {
        "/Users/rogerbos/ClickHouse/build/programs/clickhouse-client"
    } else {
        "/usr/local/bin/clickhouse-client"
    };

    let cmd = format!(
        r#"{} --host='vdib5n7pan.europe-west4.gcp.clickhouse.cloud' --user='{}' --password='{}' --secure --database=tiingo -q "{}" --format=CSVWithNames > {}"#,
        clickhouse_client_path, 
        user,
        pw,
        query,
        temp_file
    );

    let output = Command::new("/bin/sh").arg("-c").arg(&cmd).output()?;

    if !output.status.success() {
        eprintln!("Query failed with status: {:?}", output.status);
        eprintln!("stderr: {:?}", String::from_utf8_lossy(&output.stderr));
        return Err("Failed to execute query".into());
    }
    
    // Read the ticker list from the temp file using minimal configuration
    let file = File::open(&temp_file)?;
    let df = CsvReader::new(file)
        .finish()?;
    
    let column_name = if univ == "Crypto" { "baseCurrency" } else { "Ticker" };
    let tickers: Vec<String> = df.column(column_name)?
        .str()?
        .into_iter()
        .filter_map(|s| s.map(|t| t.to_string()))
        .collect();
    
    // Clean up temp file
    std::fs::remove_file(temp_file)?;
    
    Ok(tickers)
}

// Helper function to combine CSV files, preserving the header from the first file
fn combine_csv_files(input_files: &[String], output_file: &str) -> Result<(), Box<dyn StdError>> {
    if input_files.is_empty() {
        return Ok(());
    }
    
    let mut output = File::create(output_file)?;
    
    // Process the first file (including header)
    let mut first_file = true;
    
    for file_path in input_files {
        let content = std::fs::read_to_string(file_path)?;
        let mut lines = content.lines();
        
        if first_file {
            // Write the header for the first file
            if let Some(header) = lines.next() {
                writeln!(output, "{}", header)?;
            }
            first_file = false;
        } else {
            // Skip header for subsequent files
            lines.next();
        }
        
        // Write the data rows
        for line in lines {
            writeln!(output, "{}", line)?;
        }
    }
    
    Ok(())
}

fn read_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} env variable should be set"))
}

pub async fn get_ch_cloud_client() -> Result<Client, Box<dyn StdError>> {
    println!("Connecting to ClickHouse at https://vdib5n7pan.europe-west4.gcp.clickhouse.cloud");
    let client = Client::default()
        .with_url("https://vdib5n7pan.europe-west4.gcp.clickhouse.cloud")
        .with_user(read_env_var("CLICKHOUSE_USER"))
        .with_password(read_env_var("CLICKHOUSE_CLOUD_PASSWORD"))
        .with_database("tiingo");
    let query_result = client.query("SELECT version()").fetch_one::<String>().await;

    match query_result {
        Ok(version) => {
            println!(
                "Successfully connected to ClickHouse. Server version: {}",
                version
            );
            Ok(client) // Connection is successful
        }
        Err(e) => {
            println!("Failed to connect to ClickHouse: {:?}", e);
            Err(Box::new(e)) // Propagate the error
        }
    }
}

pub async fn get_ch_client(remote: bool) -> Result<Client, Box<dyn StdError>> {
    
    let host = if remote {
        read_env_var("CLICKHOUSE_HOSTR")
    } else {
        read_env_var("CLICKHOUSE_HOSTL")
    };
    let url = format!("http://{}:8123", host);
    println!("Connecting to ClickHouse at {}", url);

    let client = Client::default()
        .with_url(&url)
        .with_user(read_env_var("CLICKHOUSE_USER"))
        .with_password(read_env_var("PG"))
        .with_database("tiingo");
    let query_result = client.query("SELECT version()").fetch_one::<String>().await;

    match query_result {
        Ok(version) => {
            println!(
                "Successfully connected to ClickHouse. Server version: {}",
                version
            );
            Ok(client) // Connection is successful
        }
        Err(e) => {
            println!("Failed to connect to ClickHouse: {:?}", e);
            Err(Box::new(e)) // Propagate the error
        }
    }
}

pub async fn insert_score_dataframe(df: DataFrame) -> Result<(), Box<dyn StdError>> {
    let client = get_ch_client(false).await?;
    let client_remote = get_ch_client(true).await?;

    let date_column = df.column("date")?.date()?;
    let universe_column = df.column("universe")?.str()?;
    let ticker_column = df.column("ticker")?.str()?;
    let side_column = df.column("side")?.i64()?;
    let risk_reward_column = df.column("risk_reward")?.f64()?;
    let sharpe_ratio_column = df.column("sharpe_ratio")?.f64()?;
    let sortino_ratio_column = df.column("sortino_ratio")?.f64()?;
    let max_drawdown_column = df.column("max_drawdown")?.f64()?;
    let calmar_ratio_column = df.column("calmar_ratio")?.f64()?;
    let win_loss_ratio_column = df.column("win_loss_ratio")?.f64()?;
    let recovery_factor_column = df.column("recovery_factor")?.f64()?;
    let profit_per_trade_column = df.column("profit_per_trade")?.f64()?;
    let expectancy_column = df.column("expectancy")?.f64()?;
    let profit_factor_column = df.column("profit_factor")?.f64()?;

    let mut insert = client.insert("strategy")?;
    for i in 0..df.height() {
        let date_days = date_column.get(i).unwrap(); // Number of days since 1970-01-01
        let naive_date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(date_days as i64);
        let naive_datetime = naive_date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
            * 1000;

        let row = Score {
            date: naive_datetime,
            universe: universe_column.get(i).unwrap().to_string(),
            ticker: ticker_column.get(i).unwrap().to_string(),
            side: side_column.get(i).unwrap(),
            risk_reward: risk_reward_column.get(i).unwrap(),
            sharpe_ratio: sharpe_ratio_column.get(i).unwrap(),
            sortino_ratio: sortino_ratio_column.get(i).unwrap(),
            max_drawdown: max_drawdown_column.get(i).unwrap(),
            calmar_ratio: calmar_ratio_column.get(i).unwrap(),
            win_loss_ratio: win_loss_ratio_column.get(i).unwrap(),
            recovery_factor: recovery_factor_column.get(i).unwrap(),
            profit_per_trade: profit_per_trade_column.get(i).unwrap(),
            expectancy: expectancy_column.get(i).unwrap(),
            profit_factor: profit_factor_column.get(i).unwrap(),
        };
        insert.write(&row).await?;
    }
    insert.end().await?;

    let mut insert = client_remote.insert("strategy")?;
    for i in 0..df.height() {
        let date_days = date_column.get(i).unwrap(); // Number of days since 1970-01-01
        let naive_date =
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(date_days as i64);
        let naive_datetime = naive_date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
            * 1000;

        let row = Score {
            date: naive_datetime,
            universe: universe_column.get(i).unwrap().to_string(),
            ticker: ticker_column.get(i).unwrap().to_string(),
            side: side_column.get(i).unwrap(),
            risk_reward: risk_reward_column.get(i).unwrap(),
            sharpe_ratio: sharpe_ratio_column.get(i).unwrap(),
            sortino_ratio: sortino_ratio_column.get(i).unwrap(),
            max_drawdown: max_drawdown_column.get(i).unwrap(),
            calmar_ratio: calmar_ratio_column.get(i).unwrap(),
            win_loss_ratio: win_loss_ratio_column.get(i).unwrap(),
            recovery_factor: recovery_factor_column.get(i).unwrap(),
            profit_per_trade: profit_per_trade_column.get(i).unwrap(),
            expectancy: expectancy_column.get(i).unwrap(),
            profit_factor: profit_factor_column.get(i).unwrap(),
        };
        insert.write(&row).await?;
    }
    insert.end().await?;

    Ok(())
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct Score {
    date: i64,
    universe: String,
    ticker: String,
    side: i64,
    risk_reward: f64,
    sharpe_ratio: f64,
    sortino_ratio: f64,
    max_drawdown: f64,
    calmar_ratio: f64,
    win_loss_ratio: f64,
    recovery_factor: f64,
    profit_per_trade: f64,
    expectancy: f64,
    profit_factor: f64,
}

// async fn _create_score_table() -> Result<(), Box<dyn StdError>> {
//     let client = get_ch_client().await?;
//     let txt: &str = "CREATE OR REPLACE TABLE strategy_score (
//         date String,
//         universe LowCardinality(String),
//         ticker LowCardinality(String),
//         side Int64,
//         risk_reward Float64,
//         expectancy Float64,
//         profit_factor Float64 )
//     ENGINE = ReplacingMergeTree
//     ORDER BY ticker";
//     let _ = client.query(&txt).execute().await;

//     Ok(())
// }
