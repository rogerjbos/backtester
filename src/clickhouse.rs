use chrono::{Duration, NaiveDate, TimeZone};
use chrono_tz::America::New_York;
use clickhouse::{Client, Row};
use csv::WriterBuilder;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::{env, error::Error as StdError, fmt::Debug};

// Add this enum above the client functions
pub enum ChConnectionType {
    Local,
    Remote,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct OHLCV {
    date: String,
    ticker: String,
    universe: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
}

// Helper struct for get_universe_tickers
#[derive(Row, Deserialize, Debug)]
struct TickerRow {
    ticker: String,
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

    // Get the list of tickers in the universe that are already pre-filtered for validity
    let tickers = get_universe_tickers(&univ).await?;

    // Process in chunks of 50 tickers (adjust based on your memory constraints)
    let chunk_size = 50; // Reduced chunk size to avoid server memory limit
    let ticker_chunks: Vec<Vec<String>> = tickers
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    // Get a client connection once
    let client = get_ch_client(ChConnectionType::Local).await?;

    // Create the final CSV file and writer once
    let file = File::create(&filename)?;
    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(file);

    // Write the header record once
    wtr.write_record(&[
        "Date", "Ticker", "Universe", "Open", "High", "Low", "Close", "Volume",
    ])?;

    // Process each chunk
    for (i, chunk) in ticker_chunks.iter().enumerate() {
        // Join the ticker list into a quoted, comma-separated string for SQL IN clause
        let ticker_list = chunk
            .iter()
            .map(|t| format!("'{}'", t))
            .collect::<Vec<_>>()
            .join(",");

        // This query is now much simpler because get_universe_tickers has already filtered for validity.
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
                SELECT toString(date(p.date)) Date, u.ticker Ticker, 'Crypto' as Universe,
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
                SELECT toString(date(p.date)) Date
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
                SELECT toString(date(p.date)) Date, u.ticker Ticker, 'Crypto' as Universe,
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
                SELECT toString(date(p.date)) Date
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

        println!(
            "Executing query for chunk {}/{}",
            i + 1,
            ticker_chunks.len()
        );
        // println!("Query: {}", query);

        // Execute the query and write to the single CSV file
        let mut cursor = client.query(&query).fetch::<OHLCV>()?;

        // Write all rows from the cursor to the CSV file
        while let Some(row) = cursor.next().await? {
            wtr.serialize(row)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

// Helper function to get the list of tickers in a universe
async fn get_universe_tickers(univ: &str) -> Result<Vec<String>, Box<dyn StdError>> {
    let client = get_ch_client(ChConnectionType::Local).await?;

    let query = if univ == "Crypto" {
        "SELECT DISTINCT baseCurrency FROM tiingo.crypto".to_string()
    } else {
        format!("SELECT DISTINCT Ticker FROM univ WHERE batch = '{}'", univ)
    };

    // println!("Fetching tickers for universe: {}", univ);
    // println!("Ticker query: {}", query);

    let tickers: Vec<String> = client
        .query(&query)
        .fetch_all::<TickerRow>()
        .await?
        .into_iter()
        .map(|row| row.ticker)
        .collect();

    // println!("********** Found {} tickers for universe: {}", tickers.len(), univ);
    // println!("Tickers: {:?}", tickers.clone());

    Ok(tickers)
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

pub async fn insert_score_dataframe(df: DataFrame) -> Result<(), Box<dyn StdError>> {
    // Create both clients
    let client_local = get_ch_client(ChConnectionType::Local).await?;
    // let client_remote = get_ch_client(ChConnectionType::Remote).await?;

    // Extract all columns once
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

    // Create a vector of (client, name) tuples to process
    let clients = vec![
        (client_local, "local"),
        // (client_remote, "remote"),
    ];

    for (client, location) in clients {
        let result = async {
            let mut insert = client.insert("strategy")?;
            for i in 0..df.height() {
                let date_days = date_column.get(i).unwrap(); // Number of days since 1970-01-01
                let naive_date =
                    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(date_days as i64);
                // Create a naive datetime at midnight
                let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
                // Convert to New York timezone
                let ny_datetime = New_York
                    .from_local_datetime(&naive_datetime)
                    .single()
                    .unwrap()
                    .timestamp()
                    * 1000; // Convert to milliseconds
                let row = Score {
                    date: ny_datetime,
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
            Ok::<(), Box<dyn StdError>>(())
        }
        .await;
        match result {
            Ok(_) => println!(
                "Successfully inserted data into {} ClickHouse database",
                location
            ),
            Err(e) => eprintln!(
                "Failed to insert data into {} ClickHouse database: {:?}",
                location, e
            ),
        }
    }

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
