use chrono::{Duration, NaiveDate, TimeZone};
use chrono_tz::America::New_York;
use clickhouse::{Client, Row};
use csv::WriterBuilder;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::{env, error::Error as StdError, fmt::Debug};
use tokio::time;

// Add this enum above the client functions
pub enum ChConnectionType {
    Ace,
    Mini,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct OHLCV {
    #[serde(rename = "Date")]
    date: String,
    #[serde(rename = "Ticker")]
    ticker: String,
    #[serde(rename = "Universe")]
    universe: String,
    #[serde(rename = "Open")]
    open: Option<f64>,
    #[serde(rename = "High")]
    high: Option<f64>,
    #[serde(rename = "Low")]
    low: Option<f64>,
    #[serde(rename = "Close")]
    close: Option<f64>,
    #[serde(rename = "Volume")]
    volume: Option<f64>,
}

// Helper struct for get_universe_tickers
#[derive(Row, Deserialize, Debug)]
struct TickerRow {
    #[serde(rename = "Ticker")]
    ticker: String,
}

pub async fn write_price_file(univ: String, is_production: bool) -> Result<(), Box<dyn StdError>> {
    let user_path = match env::var("CLICKHOUSE_USER_PATH") {
        Ok(path) => path,
        Err(_) => String::from("/srv"),
    };
    let folder = if is_production { "production" } else { "testing" };
    let filename = format!(
        "{}/rust_home/backtester/data/{}/{}.csv",
        user_path.to_string(),
        folder.to_string(),
        univ
    );

    // Get the list of tickers in the universe that are already pre-filtered for validity
    let tickers = get_universe_tickers(&univ).await?;

    // Process in chunks of 500 tickers (adjust based on your memory constraints)
    let chunk_size = 500; // Reduced chunk size to avoid server memory limit
    let ticker_chunks: Vec<Vec<String>> = tickers
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    // Get a client connection once
    let client = get_ch_client(ChConnectionType::Ace).await?;

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

        let query = build_price_query(&univ, &ticker_list, is_production);

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

// Helper function to build price queries
fn build_price_query(univ: &str, ticker_list: &str, is_production: bool) -> String {
    let is_crypto = univ == "Crypto";

    if is_crypto {
        let min_days = if is_production { 120 } else { 360 };
        let date_filter = if is_production {
            // Relax date filter: allow tickers with data within 7 days of the latest date
            "WHERE p.date >= subtractDays(now(), 252)
                and maxdate >= subtractDays((select toDate(max(date)) from tiingo.crypto), 7)"
        } else {
            ""
        };

        format!(
            "WITH univ AS (
            SELECT baseCurrency ticker, toDate(max(date)) maxdate
            FROM tiingo.crypto
            WHERE baseCurrency IN ({})
            group by ticker
            having count(date) > {} and COUNT(*) * 2 - COUNT(high) - COUNT(low) = 0
            )
            SELECT toString(date(formatDateTime(p.date, '%Y-%m-%d %H:%i:%s'))) Date, u.ticker Ticker, 'Crypto' as Universe,
            open AS Open, high AS High, low AS Low, close AS Close, volume AS Volume
            FROM tiingo.crypto p
            INNER JOIN univ u
            ON u.ticker = p.baseCurrency
            {}
            order by ticker, date",
            ticker_list, min_days, date_filter
        )
    } else {
        let min_days = if is_production { 250 } else { 1000 };
        let date_filter = if is_production {
            // Relax date filter: allow tickers with data within 7 days of the latest date
            // This handles holidays, weekends, and slight data delays
            "WHERE p.date >= subtractDays(now(), 365)
                and m.maxdate >= subtractDays((select max(date(formatDateTime(date, '%Y-%m-%d %H:%i:%s'))) from tiingo.usd), 7)"
        } else {
            ""
        };

        format!(
            "WITH mdate AS (
            SELECT symbol, max(date(formatDateTime(date, '%Y-%m-%d %H:%i:%s'))) AS maxdate
            FROM tiingo.usd p
            WHERE symbol IN ({})
            group by symbol
            having count(date) >= {} and COUNT(*) * 2 - COUNT(adjHigh) - COUNT(adjLow) = 0
            )
            SELECT toString(date(formatDateTime(p.date, '%Y-%m-%d %H:%i:%s'))) Date
            , symbol AS Ticker
            , '{}' AS Universe
            , round(toFloat64(adjOpen), 2) AS Open
            , round(toFloat64(adjHigh), 2) AS High
            , round(toFloat64(adjLow), 2) AS Low
            , round(toFloat64(adjClose), 2) AS Close
            , toFloat64(adjVolume) AS Volume
            FROM tiingo.usd p
            INNER JOIN mdate m
            ON m.symbol = p.symbol
            {}
            order by Ticker, date",
            ticker_list, min_days, univ, date_filter
        )
    }
}

// Helper function to get the list of tickers in a universe
async fn get_universe_tickers(univ: &str) -> Result<Vec<String>, Box<dyn StdError>> {
    let client = get_ch_client(ChConnectionType::Ace).await?;

    let query = if univ == "Crypto" {
        "SELECT DISTINCT baseCurrency AS Ticker FROM tiingo.crypto".to_string()
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
    let (host, conn_type_str) = match connection_type {
        ChConnectionType::Ace => ("192.168.86.46", "Ace"),
        ChConnectionType::Mini => ("192.168.86.56", "Mini"),
    };

    let client = Client::default()
        .with_url(format!("http://{}:8123", host))
        .with_user("roger")
        .with_password(read_env_var("PG"))
        .with_database("tiingo");

    match client.query("SELECT version()").fetch_one::<String>().await {
        Ok(version) => {
            println!("Successfully connected to ClickHouse {}. Server version: {}", conn_type_str, version);
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
    let client_ace = get_ch_client(ChConnectionType::Ace).await?;
    let client_mini = get_ch_client(ChConnectionType::Mini).await?;

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

    // Create a vector of (client, name, use_binary, batch_size) tuples to process
    // Both servers now support binary mode with clickhouse-rs 0.14.2
    let clients = vec![
        (client_ace, "ace", true, 1000),
        (client_mini, "mimi", true, 1000),
    ];

    for (client, location, use_binary, batch_size) in clients {
        let result = async {
            if use_binary {
                // Use binary format
                for batch_start in (0..df.height()).step_by(batch_size) {
                    let batch_end = (batch_start + batch_size).min(df.height());
                    let mut insert = client.insert::<Score>("strategy").await?;

                    for i in batch_start..batch_end {
                        let date_days = date_column.get(i).unwrap();
                        let naive_date =
                            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(date_days as i64);
                        let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
                        let ny_datetime = New_York
                            .from_local_datetime(&naive_datetime)
                            .single()
                            .unwrap()
                            .timestamp()
                            * 1000;

                        let universe_str = universe_column.get(i).unwrap().to_string();
                        let ticker_str = ticker_column.get(i).unwrap().to_string();

                        let row = Score {
                            date: ny_datetime,
                            universe: universe_str,
                            ticker: ticker_str,
                            side: Some(side_column.get(i).unwrap()),
                            risk_reward: Some(risk_reward_column.get(i).unwrap()),
                            sharpe_ratio: Some(sharpe_ratio_column.get(i).unwrap()),
                            sortino_ratio: Some(sortino_ratio_column.get(i).unwrap()),
                            max_drawdown: Some(max_drawdown_column.get(i).unwrap()),
                            calmar_ratio: Some(calmar_ratio_column.get(i).unwrap()),
                            win_loss_ratio: Some(win_loss_ratio_column.get(i).unwrap()),
                            recovery_factor: Some(recovery_factor_column.get(i).unwrap()),
                            profit_per_trade: Some(profit_per_trade_column.get(i).unwrap()),
                            expectancy: Some(expectancy_column.get(i).unwrap()),
                            profit_factor: Some(profit_factor_column.get(i).unwrap()),
                        };
                        insert.write(&row).await?;
                    }
                    insert.end().await?;
                }
            } else {
                // Use SQL VALUES format (more compatible across versions)
                for batch_start in (0..df.height()).step_by(batch_size) {
                    let batch_end = (batch_start + batch_size).min(df.height());

                    let mut values = Vec::new();
                    for i in batch_start..batch_end {
                        let date_days = date_column.get(i).unwrap();
                        let naive_date =
                            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(date_days as i64);
                        let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
                        let ny_datetime = New_York
                            .from_local_datetime(&naive_datetime)
                            .single()
                            .unwrap()
                            .timestamp()
                            * 1000;

                        let universe = universe_column.get(i).unwrap();
                        let ticker = ticker_column.get(i).unwrap().replace("'", "''"); // Escape single quotes
                        let side = side_column.get(i).unwrap();
                        let risk_reward = risk_reward_column.get(i).unwrap();
                        let sharpe_ratio = sharpe_ratio_column.get(i).unwrap();
                        let sortino_ratio = sortino_ratio_column.get(i).unwrap();
                        let max_drawdown = max_drawdown_column.get(i).unwrap();
                        let calmar_ratio = calmar_ratio_column.get(i).unwrap();
                        let win_loss_ratio = win_loss_ratio_column.get(i).unwrap();
                        let recovery_factor = recovery_factor_column.get(i).unwrap();
                        let profit_per_trade = profit_per_trade_column.get(i).unwrap();
                        let expectancy = expectancy_column.get(i).unwrap();
                        let profit_factor = profit_factor_column.get(i).unwrap();

                        values.push(format!(
                            "({}, '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                            ny_datetime, universe, ticker, side, risk_reward, sharpe_ratio,
                            sortino_ratio, max_drawdown, calmar_ratio, win_loss_ratio,
                            recovery_factor, profit_per_trade, expectancy, profit_factor
                        ));
                    }

                    let query = format!(
                        "INSERT INTO strategy (date, universe, ticker, side, risk_reward, sharpe_ratio, \
                         sortino_ratio, max_drawdown, calmar_ratio, win_loss_ratio, recovery_factor, \
                         profit_per_trade, expectancy, profit_factor) VALUES {}",
                        values.join(", ")
                    );

                    client.query(&query).execute().await?;

                    // Progress indicator
                    if batch_end % 100 == 0 || batch_end == df.height() {
                        println!("Progress {}: {}/{} rows", location, batch_end, df.height());
                    }

                    // Small delay between batches
                    time::sleep(time::Duration::from_millis(100)).await;
                }
            }
            Ok::<(), Box<dyn StdError>>(())
        }
        .await;

        match result {
            Ok(_) => println!(
                "Successfully inserted {} rows into ClickHouse {}",
                df.height(), location
            ),
            Err(e) => eprintln!(
                "Failed to insert rows into ClickHouse {}: {:?}",
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
    side: Option<i64>,
    risk_reward: Option<f64>,
    sharpe_ratio: Option<f64>,
    sortino_ratio: Option<f64>,
    max_drawdown: Option<f64>,
    calmar_ratio: Option<f64>,
    win_loss_ratio: Option<f64>,
    recovery_factor: Option<f64>,
    profit_per_trade: Option<f64>,
    expectancy: Option<f64>,
    profit_factor: Option<f64>,
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
