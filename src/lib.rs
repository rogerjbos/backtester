use clickhouse::{Client, Row};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::{cmp, collections::HashSet, env, error::Error as StdError, fmt::Debug, fs::File, io::Cursor, path::Path, sync::Arc};
use tokio::{fs, task::JoinError};
mod signals {
    pub mod technical;    
}

#[derive(Debug, Serialize)]
pub struct Backtest {
    pub ticker: String,
    pub universe: String,
    pub strategy: String,
    pub expectancy: f64,
    pub profit_factor: f64,
    pub hit_ratio: f64,
    pub realized_risk_reward: f64,
    pub avg_gain: f64,
    pub avg_loss: f64,
    pub max_gain: f64,
    pub max_loss: f64,
    pub buys: i32,
    pub sells: i32,
    pub trades: i32,
    pub date: String,
    pub buy: i32,
    pub sell: i32,
}

#[derive(Debug, Serialize)]
pub struct BuySell {
    pub buy: Vec<i32>,
    pub sell: Vec<i32>
}

// Define the function type for your signals. Assuming BuySell and Backtest are defined somewhere
pub type SignalFunction = fn(DataFrame) -> BuySell;

pub async fn delete_all_files_in_folder<P: AsRef<Path>>(folder_path: P) -> Result<(), Box<dyn StdError>> {
    let mut entries = fs::read_dir(folder_path).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let path = entry.path();
        if path.is_file() {
            fs::remove_file(path).await.unwrap();
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct Signal {
    pub name: String,
    pub f: Arc<SignalFunction>, // Using Rc to allow the struct to be cloned if needed
}

pub async fn score(datetag: &str, stocks: bool) -> Result<(), Box<dyn StdError>> {

    // read in the testing file to get the historical performance for scoring
    let path: String = "/Users/rogerbos/rust_home/backtester".to_string();
    let tag = if stocks { "stocks" } else { "crypto" };
    let file_path = format!("{}/performance/{}_testing.csv", path, tag);
    let testing = CsvReader::from_path(file_path).unwrap().finish().unwrap();
    // println!("testing: {:?}", testing.clone());
        
    // read in the buys
    let buy_path = format!("{}/performance/{}_buys_{}.csv", path, tag, datetag);
    let buys = CsvReader::from_path(buy_path).unwrap().finish().unwrap()
        .left_join(&testing, ["strategy","universe"], ["strategy","universe"])?
        .lazy()
        .groupby_stable([col("universe"), col("ticker")])
        .agg([
            col("buy").sum().alias("side"),
            col("risk_reward").sum().alias("risk_reward"),
            col("expectancy").sum().alias("expectancy"),
            col("profit_factor").sum().alias("profit_factor"),
        ])
        .sort("profit_factor", SortOptions {descending: true, nulls_last: true, ..Default::default()});
    // println!("buys: {:?}", buys.clone().collect().unwrap());

    // read in the sells
    let sell_path = format!("{}/performance/{}_sells_{}.csv", path, tag, datetag);
    let sells = CsvReader::from_path(sell_path).unwrap().finish().unwrap()
        .left_join(&testing, ["strategy","universe"], ["strategy","universe"])?
        .lazy()
        .groupby_stable([col("universe"), col("ticker")])
        .agg([
            col("sell").sum().alias("side"),
            (col("risk_reward").sum() * lit(-1.)).alias("risk_reward"),
            (col("expectancy").sum() * lit(-1.)).alias("expectancy"),
            (col("profit_factor").sum() * lit(-1.)).alias("profit_factor"),
        ])
        .sort("profit_factor", SortOptions {descending: true, nulls_last: true, ..Default::default()});
    // println!("sells: {:?}", sells.clone().collect().unwrap());

    let both = concat(&[buys, sells], Default::default())?
        .groupby_stable([col("universe"),col("ticker")])
        .agg([
            col("side").sum().alias("side"),
            col("risk_reward").sum().alias("risk_reward"),
            col("expectancy").sum().alias("expectancy"),
            col("profit_factor").sum().alias("profit_factor"),
        ])
        .sort("side", SortOptions {descending: true, nulls_last: true, ..Default::default()})
        .collect()?;
    println!("both: {:?}", both);

    let both_path = format!("{}/score/{}_{}.csv", path, tag, datetag);
    let mut file = File::create(both_path)?;
    let _ = CsvWriter::new(&mut file).finish(&mut both.clone());

    // Get DB client and connection
    let url: String = "http://32.219.187.60:8123".to_string();
    let client = Client::default()
        .with_url(url)
        .with_user("roger")
        .with_password(env::var("PG")?)
        .with_database("default");
        // let _ = create_score_table(&client).await;
        let _ = insert_score_dataframe(&client, both).await;
    Ok(())

}

async fn _create_score_table(client: &Client) -> Result<(), clickhouse::error::Error> {
    let txt = "CREATE OR REPLACE TABLE strategy_score(
        universe LowCardinality(String),
        ticker LowCardinality(String),
        side Int8,
        risk_reward Float64,
        expectancy Float64,
        profit_factor Float64)
        ENGINE = ReplacingMergeTree
        ORDER BY ticker, universe".to_string();
    client.query(&txt).execute().await
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct Score {
    universe:  String,
    ticker: String,
    side: i8,
    risk_reward: f64,
    expectancy: f64,
    profit_factor: f64
}

pub async fn insert_score_dataframe(client: &Client, df: DataFrame) -> Result<(), Box<dyn StdError>> {
    let mut insert = client
        .insert("strategy_score")?;

    let universe_column = df.column("universe")?.utf8()?;
    let ticker_column = df.column("ticker")?.utf8()?;
    let side_column = df.column("side")?.i8()?;
    let risk_reward_column = df.column("risk_reward")?.f64()?;
    let expectancy_column = df.column("expectancy")?.f64()?;
    let profit_factor_column = df.column("profit_factor")?.f64()?;

    for i in 0..df.height() {
        let row = Score {
            universe: universe_column.get(i).unwrap().to_string(),
            ticker: ticker_column.get(i).unwrap().to_string(),
            side: side_column.get(i).unwrap(),
            risk_reward: risk_reward_column.get(i).unwrap(),
            expectancy: expectancy_column.get(i).unwrap(),
            profit_factor: profit_factor_column.get(i).unwrap(),
        };
        insert.write(&row).await?;
    }
    insert.end().await?;

    Ok(())
}

// pub async fn insert_score_dataframe(client: &Client, df: DataFrame) -> Result<(), clickhouse::error::Error> {
//     // let mut insert = client.insert("strategy_score").unwrap();
//     let mut insert = client
//         .insert("strategy_score")?;

//     let universe_column = df.column("universe").unwrap().utf8().unwrap();
//     let ticker_column = df.column("ticker").unwrap().utf8().unwrap();
//     let side_column = df.column("side").unwrap().i64().unwrap();
//     let risk_reward_column = df.column("risk_reward").unwrap().f64().unwrap();
//     let expectancy_column = df.column("expectancy").unwrap().f64().unwrap();
//     let profit_factor_column = df.column("profit_factor").unwrap().f64().unwrap();

//     for i in 0..df.height() {
//         let row = Score {
//             universe: universe_column.get(i).unwrap().to_string(),
//             ticker: ticker_column.get(i).unwrap().to_string(),
//             side: side_column.get(i).unwrap(),
//             risk_reward: risk_reward_column.get(i).unwrap(),
//             expectancy: expectancy_column.get(i).unwrap(),
//             profit_factor: profit_factor_column.get(i).unwrap(),
//         };
//         insert.write(&row).await?;
//     }
//     insert.end().await?;
//     Ok(())
// }

async fn concat_dataframes(dfs: Vec<DataFrame>) -> Result<DataFrame, PolarsError> {
    let lazy_frames: Vec<LazyFrame> = dfs.into_iter().map(|df| df.lazy()).collect();
    
    // Use the concat function for LazyFrames
    let concatenated_lazy_frame = concat(
        &lazy_frames,
        UnionArgs::default(),
    )?;

    // Collect the concatenated LazyFrame back into a DataFrame
    let result_df = concatenated_lazy_frame.collect()?;

    Ok(result_df)
}
    
pub async fn summary_performance_file(path: String, production: bool, stocks: bool) -> Result<(), Box<dyn StdError>> {
    let bt_names = vec![
        "ticker", "universe", "strategy", "expectancy", "profit_factor", "hit_ratio",
        "realized_risk_reward", "avg_gain", "avg_loss", "max_gain", "max_loss", "buys", "sells", 
        "trades", "date", "buy", "sell"
    ];
    let set_bt: HashSet<_> = bt_names.iter().cloned().collect();

    let b_names = vec!["ticker", "universe", "strategy", "date", "buy", "sell"];

    let folder = match (stocks, production) {
        (true, true)   => "output/production",
        (true, false)  => "output/testing",
        (false, true)  => "output_crypto/production",
        (false, false) => "output_crypto/testing",
    };

    let dir_path = format!("{}/{}", path, folder);
    let mut a: Vec<DataFrame> = Vec::new();
    let mut b: Vec<DataFrame> = Vec::new();
    let mut entries = fs::read_dir(dir_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let lf = LazyFrame::scan_parquet(path.to_str().expect("path error"), ScanArgsParquet::default())?.collect();
            match lf {
                Ok(df) => {
                    // Ensure all required columns are present
                    let df_names = df.get_column_names();
                    let set_df: HashSet<_> = df_names.into_iter().collect();
                    if set_bt.is_subset(&set_df) {
                        a.push(df.select(bt_names.clone())?);
                        b.push(df.select(b_names.clone())?);
                    }
                },
                Err(e) => println!("Error processing file {}: {}", path.display(), e),
            }
        }
    }

    // ALL
    let df = concat_dataframes(a).await?;
    let out = summary_performance(df.clone())?;
    println!("Average Performance by Strategy:\n {:?}", out);
    
    let datetag = df.column("date")?
        .get(0)?
        .to_string()
        .trim_matches('"')
        .replace("-", "");

    let tag: &str = if stocks { "stocks" } else { "crypto" };

    let perf_filename = if production { 
        format!("{}/performance/{}_all_{}.csv", path, tag, datetag) 
    } else {
        format!("{}/performance/{}_testing.csv", path, tag) 
    };
    let mut file = File::create(perf_filename)?;
    let _ = CsvWriter::new(&mut file).finish(&mut out.clone());

    // write buys and sells only for production
    if production {

        let datetag = df.column("date")?
            .get(0)?
            .to_string()
            .trim_matches('"')
            .replace("-", "");
        let _ = score(&datetag, stocks);

        // coverage
        // concat all the price dfs 
        let mut p: Vec<DataFrame> = Vec::new();
        let univ = ["Crypto","LC1","LC2","MC1","MC2","SC1","SC2","SC3","SC4","Micro1","Micro2"];
        for u in univ {
            let file_path = format!("{}/data/production/{}.parquet", path, u);
            let tmp = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())?;
            let grouped = tmp.groupby_stable([col("Ticker")])
                .agg([ 
                    col("Date").count().alias("observations"),
                    col("Date").last().alias("last date") 
                ])
                .sort("Ticker", SortOptions {descending: false, nulls_last: true, ..Default::default()});
                p.push(grouped.collect().unwrap());
        }
        let all_p = concat_dataframes(p).await?;
        // println!("all_p: {:?}", all_p.clone());

        let df_grouped = df.clone().lazy().groupby_stable([col("ticker")])
            .agg([ col("strategy").count().alias("strategies") ])
            .sort("ticker", SortOptions {descending: false, nulls_last: true, ..Default::default()});
        // println!("df_grouped: {:?}", df_grouped.clone().collect().unwrap());

        let both = all_p.lazy()
            .inner_join(df_grouped, col("Ticker"), col("ticker"))
            .filter(
                col("strategies").lt(lit(121))
            )
            .sort("strategies", SortOptions {descending: false, nulls_last: true, ..Default::default()})
            .collect();
        println!("Strategy Coverage: {:?}", both);

        // buys and sells for the current date
        let df_b = concat_dataframes(b).await?;
        let mut buys = df_b.clone().lazy()
            .filter(col("buy").eq(lit(1)))
            .sort("ticker", SortOptions {descending: false, nulls_last: true, ..Default::default()})
            .collect()?;

        let mut sells = df_b.clone().lazy()
            .filter(col("sell").eq(lit(-1)))
            .sort("ticker", SortOptions {descending: false, nulls_last: true, ..Default::default()})
            .collect()?;

        let buy_filename = format!("{}/performance/{}_buys_{}.csv", path, tag, datetag);
        let mut buy_file = File::create(buy_filename)?;
        let _ = CsvWriter::new(&mut buy_file).finish(&mut buys);
    
        let sell_filename = format!("{}/performance/{}_sells_{}.csv", path, tag, datetag);
        let mut sell_file = File::create(sell_filename)?;
        let _ = CsvWriter::new(&mut sell_file).finish(&mut sells);
    
    };

    // only show for testing
    if !production {

        // LC
        let lc = out.clone()
        .lazy()
        .filter(
            col("universe").eq(lit("LC1"))
            .or(col("universe").eq(lit("LC2")))
        )
        .collect();

        match lc {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "LC");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut lc?);
            
            },
            Err(ref e) => println!("Error filtering DataFrame for LC: \n{:?}", e),
        }

        // MC
        let mc = out.clone()
        .lazy()
        .filter(
            col("universe").eq(lit("MC1"))
            .or(col("universe").eq(lit("MC2")))
        )
        .collect();

        match mc {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "MC");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut mc?);
            
            },
            Err(ref e) => println!("Error filtering DataFrame for MC: \n{:?}", e),
        }

        // SC
        let sc = out.clone()
        .lazy()
        .filter(
            col("universe").eq(lit("SC1"))
            .or(col("universe").eq(lit("SC2")))
            .or(col("universe").eq(lit("SC3")))
            .or(col("universe").eq(lit("SC4")))
        )
        .collect();

        match sc {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "SC");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut sc?);
            },
            Err(ref e) => println!("Error filtering DataFrame for SC: \n{:?}", e),
        }

        // Microcap
        let micro = out.clone()
        .lazy()
        .filter(
            col("universe").eq(lit("Micro1"))
            .or(col("universe").eq(lit("Micro2")))
        )
        .collect();

        match micro {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "Micro");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut micro?);
            
            },
            Err(ref e) => println!("Error filtering DataFrame for Micro: \n{:?}", e),
        }
    }
    Ok(())

}

pub fn summary_performance(df: DataFrame)-> Result<DataFrame, Box<dyn StdError>> {       
    let out = df.lazy()
        .groupby_stable([col("strategy"), col("universe")])
        .agg([
            col("hit_ratio").mean().alias("hit_ratio"),
            col("realized_risk_reward").mean().alias("risk_reward"),
            col("avg_gain").mean().alias("avg_gain"),
            col("avg_loss").mean().alias("avg_loss"),
            col("max_gain").mean().alias("max_gain"),
            col("max_loss").mean().alias("max_loss"),
            col("buys").mean().alias("buys"),
            col("sells").mean().alias("sells"),
            col("trades").mean().alias("trades"),
            col("profit_factor").count().alias("N"),
            col("expectancy").mean().alias("expectancy"),
            col("profit_factor").mean().alias("profit_factor"),      
        ])
        .filter(col("trades").gt(lit(3)))
        .sort("profit_factor", SortOptions {descending: true, nulls_last: true, ..Default::default()})
        .collect()?;

    Ok(out)
}

// Apply a signal function to data and calculate strategy performance
pub async fn sig(df: LazyFrame, signal: &Signal) -> Result<Backtest, Box<dyn StdError>> {
    let func = &signal.f;
    let s = func(df.clone().collect()?);
    let bt = backtest_performance(df.collect()?, s, &signal.name)?;

    Ok(bt)
}

pub async fn run_all_backtests(df: LazyFrame, signals: Vec<Signal>) -> Result<Vec<Backtest>, JoinError> {
    // wrap df in an Arc for shared ownership across tasks
    let df = Arc::new(df);

    let futures: Vec<_> = signals.into_iter()
        .map(|signal| {
            // clone Arc for each task
            let df_clone = Arc::clone(&df);
            tokio::spawn(async move { 
                sig(df_clone.as_ref().clone(), &signal).await.unwrap()
            })
        })
        .collect();
    
    let results = futures::future::join_all(futures).await;

    // Handle the results, assuming `sig` returns `Result<Backtest, _>`
    let backtests: Vec<Backtest> = results.into_iter()
        .filter_map(Result::ok).collect();

    Ok(backtests)
}

pub async fn create_price_files(univ_vec: Vec<String>, production: bool) -> Result<(), Box<dyn StdError>> {
    
    let folder = if production { "production" } else { "testing" };

    for u in univ_vec {
        let file_path = format!("/Users/rogerbos/rust_home/backtester/data/{}/{}.parquet", folder.to_string(), u.to_string());
        if production==false && Path::new(&file_path).exists() {
            println!("Price file skippig for {}", file_path);
        } else {
            println!("Price file generating for {}", file_path);
            match u.as_str() {
                "Crypto" => get_crypto_universe(u, production).await?,
                _ => get_stock_universe(u, production).await?,
            };
        }
    }
    Ok(())
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct OHLCV {
    date: String,
    ticker: String,
    universe: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64
}

pub async fn get_crypto_universe(univ: String, production: bool) -> Result<(), Box<dyn StdError>> {

    let url: String = "http://32.219.187.60:8123".to_string();
    let txt = if production { "WITH univ AS (
        SELECT baseCurrency ticker, max(date) maxdate
        FROM crypto_price 
        group by ticker
        having count(date) > 120 and COUNT(*) * 2 - COUNT(high) - COUNT(low) = 0
        )
        SELECT toString(p.date) date
        , u.ticker ticker
        , 'Crypto' as universe
        , open, high, low, close, toFloat64(p.volume) volume
        FROM crypto_price p
        INNER JOIN univ u
        ON u.ticker = p.baseCurrency
        WHERE p.date >= subtractDays(now(), 252)
        and maxdate IN (select max(date) from crypto_price)
        order by ticker, date".to_string() 
    } else { "WITH univ AS (
        SELECT baseCurrency ticker
        FROM crypto_price 
        group by baseCurrency
        having count(date) > 360 and COUNT(*) * 2 - COUNT(high) - COUNT(low) = 0
        )
        SELECT toString(p.date) date
        , u.ticker ticker
        , 'Crypto' as universe
        , open, high, low, close, toFloat64(p.volume) volume
        FROM crypto_price p
        INNER JOIN univ u
        ON u.ticker = p.baseCurrency
        order by ticker, date".to_string()
    };
            
    // Get DB client and connection
    let client = Client::default()
        .with_url(url)
        .with_user("roger")
        .with_password(env::var("PG")?)
        .with_database("default");

    // 1. Step to convert vec to DataFrame
    let vec = client
        .query(&txt)
        .fetch_all::<OHLCV>()
        .await?;
    
    // 2. Jsonify your struct Vec
    let json = serde_json::to_string(&vec)?;
    // 3. Create cursor from json 
    let cursor = Cursor::new(json);
    // 4. Create polars DataFrame from reading cursor as json
    let mut data = JsonReader::new(cursor).finish()?;

    let df = data
        .rename("ticker", "Ticker")?.clone()
        .rename("universe", "Universe")?.clone()
        .rename("date", "Date")?.clone()
        .rename("open", "Open")?.clone()
        .rename("high", "High")?.clone()
        .rename("low", "Low")?.clone()
        .rename("close", "Close")?.clone()
        .rename("volume", "Volume")?.clone()
        .lazy()
        .with_column(col("Date")
            .str()
            .strptime(DataType::Date, StrptimeOptions {
                format: Some("%Y-%m-%d".into()), // %H:%M:%S
                use_earliest: Some(false),
                strict: false,
                exact: true,
                cache: true,
            })
            .alias("Date"))
        .collect();

    let folder = if production { "production" } else { "testing" };
    let filename = format!("/Users/rogerbos/rust_home/backtester/data/{}/{}.parquet", folder.to_string(), univ); 
    let mut file = File::create(filename)?;
    let _ = ParquetWriter::new(&mut file).finish(&mut df?.clone())?;

    Ok(())
}

pub async fn get_stock_universe(univ: String, production: bool) -> Result<(), Box<dyn StdError>> {

    let url: String = "http://32.219.187.60:8123".to_string();
    let txt = if production { format!("WITH univ AS (
        SELECT r.permaTicker, r.ticker, max(date) maxdate
        FROM price_history p
        INNER JOIN ranks r
        ON r.permaTicker = p.ticker
        where r.tag='{univ}'
        group by r.permaTicker, r.ticker
        having count(p.date) >= 250 and COUNT(*) * 2 - COUNT(p.adjHigh) - COUNT(p.adjLow) = 0
        )
        SELECT toString(p.date) date
            , p.ticker, '{univ}' as universe
            , round(adjOpen, 2) open
            , round(adjHigh, 2) high
            , round(adjLow, 2) low
            , round(adjClose, 2) close
            , round(adjVolume, 2) volume
        FROM price_history p
        INNER JOIN univ u
        ON u.permaTicker = p.ticker
        WHERE p.date >= subtractDays(now(), 365)
        and maxdate IN (select max(date) from price_history)
        order by ticker, date") 
    } else { format!("WITH univ AS (
        SELECT r.permaTicker, r.ticker
        FROM price_history p
        INNER JOIN ranks r
        ON r.permaTicker = p.ticker
        where r.tag = '{univ}' and r.date in (select max(date) from ranks where tag = '{univ}')
        group by r.permaTicker, r.ticker
        having count(p.date) > 1000 and COUNT(*) * 2 - COUNT(p.adjHigh) - COUNT(p.adjLow) = 0)
        
        SELECT toString(p.date) date
            , p.ticker, '{univ}' as universe
            , round(adjOpen, 2) open
            , round(adjHigh, 2) high
            , round(adjLow, 2) low
            , round(adjClose, 2) close
            , round(adjVolume, 2) volume
        FROM price_history p
        INNER JOIN univ u
        ON u.permaTicker = p.ticker
        order by ticker, date") 
    };
        
    // Get DB client and connection
    let client = Client::default()
        .with_url(url)
        .with_user("roger")
        .with_password(env::var("PG")?)
        .with_database("default");

    // 1. Step to convert vec to DataFrame
    let vec = client
        .query(&txt)
        .fetch_all::<OHLCV>()
        .await?;
    // 2. Jsonify your struct Vec
    let json = serde_json::to_string(&vec)?;
    // 3. Create cursor from json 
    let cursor = Cursor::new(json);
    // 4. Create polars DataFrame from reading cursor as json
    let mut data = JsonReader::new(cursor).finish()?;

    let df = data
        .rename("ticker", "Ticker")?.clone()
        .rename("universe", "Universe")?.clone()
        .rename("date", "Date")?.clone()
        .rename("open", "Open")?.clone()
        .rename("high", "High")?.clone()
        .rename("low", "Low")?.clone()
        .rename("close", "Close")?.clone()
        .rename("volume", "Volume")?.clone()
        .lazy()
        .with_column(col("Date")
            .str()
            .strptime(DataType::Date, StrptimeOptions {
                format: Some("%Y-%m-%d".into()), // %H:%M:%S
                use_earliest: Some(false),
                strict: false,
                exact: true,
                cache: true,
            })
            .alias("Date"))
        .collect();

    let folder = if production { "production" } else { "testing" };
    let filename = format!("/Users/rogerbos/rust_home/backtester/data/{}/{}.parquet", folder.to_string(), univ); 
    let mut file = File::create(filename)?;
    let _ = ParquetWriter::new(&mut file).finish(&mut df?.clone())?;

    Ok(())
}

pub fn backtest_performance(df: DataFrame, side: BuySell, strategy: &str) -> Result<Backtest, Box<dyn StdError>> {

    let df = df.clone();
    let len = df.height();
    
    let mut long_result = vec![0.0; len];
    let mut short_result = vec![0.0; len];
    
    let open = df.column("Open").unwrap().f64().unwrap(); 

    // Variable holding period
    for i in 0..len {
        if side.buy[i] == 1 {
            for a in i+1..cmp::min(i + 1000, len) {
                if side.buy[a] == 1 || side.sell[a] == -1 {
                    long_result[a] = open.get(a).unwrap() - open.get(i).unwrap();
                    break
                }
            }
        }
    }            
    for i in 0..len {
        if side.sell[i] == -1 {
            for a in i+1..cmp::min(i + 1000, len) {
                if side.buy[a] == 1 || side.sell[a] == -1 {
                    short_result[a] = open.get(i).unwrap() - open.get(a).unwrap();
                    break
                }
            }
        }
    }   

    // Aggregating the long & short results into one column
    let total_result: Vec<f64> = long_result.iter().zip(short_result.iter()).map(|(&l, &s)| l + s).collect();
    // println!("total_result: {:?}", total_result);

    // Profit factor   
    let total_net_profits: Vec<f64> = total_result.clone().into_iter().filter(|&x| x > 0.0).collect();
    let total_net_losses: Vec<f64> = total_result.clone().into_iter().filter(|&x| x < 0.0).collect();
    let sum_total_net_profits = total_net_profits.iter().sum::<f64>();
    let sum_total_net_losses = total_net_losses.iter().sum::<f64>().abs();
    let profit_factor = f64::min(999., sum_total_net_profits / sum_total_net_losses);

    // Hit ratio    
    let hit_ratio: f64 = (total_net_profits.len() as f64 / (total_net_losses.len() + total_net_profits.len()) as f64) * 100.0;

    // Risk reward ratio
    let average_gain = sum_total_net_profits / total_net_profits.len() as f64;
    let average_loss = sum_total_net_losses / total_net_losses.len() as f64;
    let realized_risk_reward = average_gain / average_loss;

    let trades: i32 = total_result.clone().into_iter().filter(|&x| x != 0.0).collect::<Vec<_>>().len() as i32;
        
    // Expectancy
    let expectancy  = (average_gain * hit_ratio) - ((1. - hit_ratio) * average_loss);

    let max_gain = total_net_profits.into_iter().max_by(|a, b| a.partial_cmp(b).unwrap());
    let max_loss = total_net_losses.into_iter().min_by(|a, b| a.partial_cmp(b).unwrap());
    
    let buys = side.buy.iter().sum::<i32>();
    let sells = side.sell.iter().sum::<i32>().abs();

    let buy = side.buy[len-1];
    let sell = side.sell[len-1];
    let ticker1 = df.column("Ticker").unwrap().get(0).unwrap().to_string();
    let ticker = ticker1.trim_matches('"').to_string();
    let universe1 = df.column("Universe").unwrap().get(0).unwrap().to_string();
    let universe = universe1.trim_matches('"').to_string();
    let date1 = df.column("Date").unwrap().get(len-1).unwrap().to_string();
    let date = date1.trim_matches('"').to_string();
    // println!("finished {} signal {:?}", ticker, strategy);

    Ok(Backtest {
        ticker: ticker,
        universe: universe,
        strategy: strategy.to_string(), 
        expectancy,
        profit_factor: profit_factor,
        hit_ratio: hit_ratio, 
        realized_risk_reward: realized_risk_reward,
        avg_gain: average_gain,
        avg_loss: average_loss,
        max_gain: match max_gain {
            Some(x) => x,
            None => 0.0,
        }, 
        max_loss: match max_loss {
            Some(x) => x,
            None => 0.0,
        },  
        buys: buys,  
        sells: sells,  
        trades: trades,
        date: date,
        buy: buy,  
        sell: sell
    })

}

pub fn showbt(bt: Backtest) -> Result<(), Box<dyn StdError>> {
    println!("");
    println!("Ticker:           {}", bt.ticker);
    println!("Universe:         {}", bt.universe);
    println!("Strategy:         {}", bt.strategy);
    println!("Profit Factor:    {:.1}", bt.profit_factor);
    println!("Hit Ratio:        {:.1}", bt.hit_ratio);
    println!("Expectancy:       {:.1}", bt.expectancy);
    println!("Risk-Reward:      {:.1}", bt.realized_risk_reward);
    println!("Avg Gain:         {:.1}", bt.avg_gain);
    println!("Avg Loss:         {:.1}", bt.avg_loss);
    println!("Max Gain:         {:.1}", bt.max_gain);
    println!("Max Loss:         {:.1}", bt.max_loss);
    println!("Buys:             {:.1}", bt.buys);
    println!("Sells:            {:.1}", bt.sells);
    println!("Trades:           {:.1}", bt.trades);
    Ok(())
}

pub fn preprocess(df: LazyFrame) -> Result<DataFrame, Box<dyn StdError>> {

    let window_size_5 = RollingOptions {
        window_size: polars::prelude::Duration::new(5),
        min_periods: 5,        // Minimum number of observations in window required to have a value
        center: false,         // Set to true to set the labels at the center of the window
        weights: None,         // Optional weights for the window
        by: None,              // Optional Series to perform operation by,
        ..Default::default()
    };

    let window_size_20 = RollingOptions {
        window_size: polars::prelude::Duration::new(20),
        min_periods: 20,      // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_50 = RollingOptions {
        window_size: polars::prelude::Duration::new(50),
        min_periods: 50,      // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_100 = RollingOptions {
        window_size: polars::prelude::Duration::new(100),
        min_periods: 100,      // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_200 = RollingOptions {
        window_size: polars::prelude::Duration::new(200),
        min_periods: 200,      // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_250 = RollingOptions {
        window_size: polars::prelude::Duration::new(250),
        min_periods: 250,      // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let out = df.clone()
        .select([cols(["Ticker","Date","Open","High","Low","Close","Volume"])])
        .sort("Date", SortOptions {descending: false, ..Default::default()})
        .sort("Ticker", SortOptions {descending: false, ..Default::default()})
        .with_columns([
            // (col("Close") / col("Close").shift(polars::prelude::Expr::Nth(1)).over([col("Ticker")]) - lit(1)).alias("Ret"),
            (col("Close") / col("Close").shift(1).over([col("Ticker")]) - lit(1)).alias("Ret"),
            col("Low").rolling_min(window_size_20.clone().into()).over([col("Ticker")]).alias("min_low_20"),
            col("High").rolling_max(window_size_20.clone().into()).over([col("Ticker")]).alias("max_high_20"),

            col("Low").rolling_min(window_size_250.clone().into()).over([col("Ticker")]).alias("min_low_250"),
            col("High").rolling_max(window_size_250.clone().into()).over([col("Ticker")]).alias("max_high_250"),
            
            col("Close").rolling_mean(window_size_5.clone().into()).over([col("Ticker")]).alias("MA_5"),
            col("Close").rolling_mean(window_size_20.clone().into()).over([col("Ticker")]).alias("MA_20"),
            col("Close").rolling_mean(window_size_50.clone().into()).over([col("Ticker")]).alias("MA_50"),
            col("Close").rolling_mean(window_size_100.clone().into()).over([col("Ticker")]).alias("MA_100"),
            col("Close").rolling_mean(window_size_200.clone().into()).over([col("Ticker")]).alias("MA_200"),
        ])
        .with_columns([
            (((col("Close") - col("min_low_20")) / (col("max_high_20") - col("min_low_20")))*lit(100.0)).alias("stoch_oscillator_20"),
        ])
        .collect()
        .unwrap();

    Ok(out)
}

pub fn postprocess(df: DataFrame) -> Result<DataFrame, Box<dyn StdError>> {

    let _sma = signals::technical::sma(df.column("Close").unwrap().clone(), 20);
    let _ema = signals::technical::ema(df.column("Close").unwrap().clone(), 0.5, 20);
    let _smoothed_ma = signals::technical::smoothed_ma(df.column("Close").unwrap().clone(), 0.5, 20);
    let _vol = signals::technical::volatility(df.column("Close").unwrap().clone(), 20);
    let _atr = signals::technical::atr(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 10);
    let _rsi = signals::technical::rsi(df.column("Close").unwrap().clone(), 20);
    let (_out, _stoch, _signal) = signals::technical::stochastic_oscillator(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 250, true, true, 3, 3);
    let _normalized_index = signals::technical::normalized_index(df.column("Close").unwrap().clone(), 20);
    let (upper_aug_bbands, lower_aug_bbands) = signals::technical::augmented_bollinger_bands(df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 20, 2.);
    let (_upper_bbands, _lower_bbands) = signals::technical::bollinger_bands(df.column("Close").unwrap().clone(), 20, 2.);
    let (_upper_kband, _lower_kband, _middle_kband) = signals::technical::k_volatility_band(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 20, 2.);
    let _rsi_atr = signals::technical::rsi_atr(df.column("Close").unwrap().clone(),df.column("High").unwrap().clone(),df.column("Low").unwrap().clone(), 3, 5, 7);
    let _trend_intensity = signals::technical::trend_intensity_indicator(df.column("Close").unwrap().clone(), 20);
    let _kama_10 = signals::technical::kama(df.column("Close").unwrap().clone(), 10);
    let (_fma_high, _fma_low) = signals::technical::fma(df.column("High").unwrap().clone(), df.column("Low").unwrap().clone());
    let _frama = signals::technical::fractal_adaptive_ma(df.column("Close").unwrap().clone(),df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 10);
    let _lwma = signals::technical::lwma(df.column("Close").unwrap().clone(), 10);
    let _hull_ma = signals::technical::hull_ma(df.column("Close").unwrap().clone(), 10);
    let _vama = signals::technical::volatility_adjusted_moving_average(df.column("Close").unwrap().clone(), 3, 30);
    let _ema = signals::technical::ema(df.column("Close").unwrap().clone(), 2., 13);
    let (_macd_diff, _macd_signal) = signals::technical::macd(df.column("Close").unwrap().clone(), 26, 12, 9);
    let _elder = signals::technical::elder_impulse(df.column("Close").unwrap().clone(), 250);
    let (_aroon_up, _aroon_down) = signals::technical::aroon(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 25);
    let (_di_plus, _di_minus, _adx, _smoothed_adx) = signals::technical::adx(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 14);
    let _awesome = signals::technical::awesome_oscillator( df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 34, 5);
    let (_donchian_low, _donchian_high, _donchian_med) = signals::technical::donchian( df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 20);
    let (_keltner_upper, _keltner_lower) = signals::technical::keltner_channel(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 60, 60, 20);
    // let atr2 = signals::technical::atr_ema(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 60);
    let _squeeze = signals::technical::squeeze(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 60, 10., 60, 20);
    let _supertrend = signals::technical::supertrend(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 10, 2.);
    let _trend_intensity = signals::technical::trend_intensity_indicator(df.column("Close").unwrap().clone(), 20);
    let _trix = signals::technical::trix(df.column("Close").unwrap().clone(), 20);
    let _vertical_horizontal_ind = signals::technical::vertical_horizontal_indicator(df.column("Close").unwrap().clone(), 60);
    let (_kijun, _tenkan, _senkou_span_a, _senkou_span_b) = signals::technical::ichimoku(df.column("Close").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 26, 9, 26, 26, 52);
    let countdown_indicator = signals::technical::countdown_indicator(df.column("Open").unwrap().clone(), df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), df.column("Close").unwrap().clone(), 8, 3);
    let countdown_indicator_series = Series::new("", &countdown_indicator);
    let (_downward, _upward, _net) = signals::technical::extreme_duration(countdown_indicator_series, 5., -5.);
    let _demarker = signals::technical::demarker( df.column("High").unwrap().clone(), df.column("Low").unwrap().clone(), 14);
    let _disparity = signals::technical::disparity_index( df.column("Close").unwrap().clone(), 14);
    let _fisher = signals::technical::fisher_transform( df.column("High").unwrap().clone(),  df.column("Low").unwrap().clone(),  df.column("Close").unwrap().clone(), 14);
    let _time_up = signals::technical::time_up(df.column("Close").unwrap().clone(), 1);
    let _tsabm = signals::technical::time_spent_above_below_mean(df.column("Close").unwrap().clone(), 34);
    let (ftp_buy, ftp_sell) = signals::technical::fibonacci_timing_pattern(df.column("Close").unwrap().clone(), 8., 5, 3, 2);

    let out = df.lazy().with_columns([
        // lit(Series::new("", &sma)).alias("sma_20"),
        // lit(Series::new("", &ema)).alias("ema_20"),
        // lit(Series::new("", &smoothed_ma)).alias("smoothed_ma_20"),
        // lit(Series::new("", &vol)).alias("vol_20"),
        // lit(Series::new("", &atr)).alias("atr_10"),
        // lit(Series::new("", &rsi)).alias("rsi_20"),
        // lit(Series::new("", &out)).alias("stoch_osc_out"),
        // lit(Series::new("", &stoch)).alias("stoch_osc_stoch"),
        // lit(Series::new("", &signal)).alias("stoch_osc_signal"),
        // lit(Series::new("", &normalized_index)).alias("normalized_index"),
        lit(Series::new("", &upper_aug_bbands)).alias("upper_aug_bbands"),
        lit(Series::new("", &lower_aug_bbands)).alias("lower_aug_bbands"),
        // lit(Series::new("", &upper_kband)).alias("upper_kband"),
        // lit(Series::new("", &lower_kband)).alias("lower_kband"),
        // lit(Series::new("", &middle_kband)).alias("middle_kband"),
        // lit(Series::new("", &rsi_atr)).alias("rsi_atr"),
        // lit(Series::new("", &kama_10)).alias("kama_10"),
        // lit(Series::new("", &fma_high)).alias("fma_high"),
        // lit(Series::new("", &fma_low)).alias("fma_low"),
        // lit(Series::new("", &frama)).alias("frama"),
        // lit(Series::new("", &lwma)).alias("lwma"),
        // lit(Series::new("", &hull_ma)).alias("hull_ma"),
        // lit(Series::new("", &vama)).alias("vama"),
        // lit(Series::new("", &macd_diff)).alias("macd_diff"),
        // lit(Series::new("", &macd_signal)).alias("macd_signal"),
        // lit(Series::new("", &elder)).alias("elder"),
        // lit(Series::new("", &aroon_up)).alias("aroon_up"),
        // lit(Series::new("", &aroon_down)).alias("aroon_down"),
        // lit(Series::new("", &di_plus)).alias("di_plus"),
        // lit(Series::new("", &di_minus)).alias("di_minus"),
        // lit(Series::new("", &adx)).alias("adx"),
        // lit(Series::new("", &smoothed_adx)).alias("smoothed_adx"),
        // lit(Series::new("", &awesome)).alias("awesome_oscillator"),
        // lit(Series::new("", &donchian_low)).alias("donchian_low"),
        // lit(Series::new("", &donchian_high)).alias("donchian_high"),
        // lit(Series::new("", &donchian_med)).alias("donchian_med"),z
        // lit(Series::new("", &squeeze)).alias("squeeze"),
        // lit(Series::new("", &keltner_upper)).alias("keltner_upper"),
        // lit(Series::new("", &keltner_lower)).alias("keltner_lower"),
        // lit(Series::new("", &atr2)).alias("atr2"),
        // lit(Series::new("", &squeeze)).alias("squeeze"),
        // lit(Series::new("", &supertrend)).alias("supertrend"),
        // lit(Series::new("", &trend_intensity)).alias("trend_intensity"),
        // lit(Series::new("", &trix)).alias("trix"),
        // lit(Series::new("", &vertical_horizontal_ind)).alias("vh_indicator"),
        // lit(Series::new("", &kijun)).alias("kijun"),
        // lit(Series::new("", &tenkan)).alias("tenkan"),
        // lit(Series::new("", &senkou_span_a)).alias("senkou_span_a"),
        // lit(Series::new("", &senkou_span_b)).alias("senkou_span_b"),
        // lit(Series::new("", &countdown_indicator)).alias("countdown_indicator"),
        // lit(Series::new("", &downward)).alias("downward"),
        // lit(Series::new("", &upward)).alias("upward"),
        // lit(Series::new("", &net)).alias("net"),
        // lit(Series::new("", &demarker)).alias("demarker"),
        // lit(Series::new("", &disparity)).alias("disparity"),
        // lit(Series::new("", &fisher)).alias("fisher"),
        // lit(Series::new("", &time_up)).alias("time_up"),
        // lit(Series::new("", &tsabm)).alias("tsabm"),
        lit(Series::new("", &ftp_buy)).alias("ftp_buy"),
        lit(Series::new("", &ftp_sell)).alias("ftp_sell"),
    ])
    .collect()
    .unwrap();

    Ok(out)
}

pub async fn parquet_save_backtest(path: String, bt: Vec<Backtest>, univ: &str, ticker: String, production: bool) -> Result<(), Box<dyn StdError>> { 
    // 2. Jsonify your struct Vec
    let json = serde_json::to_string(&bt)?;
    // 3. Create cursor from json 
    let cursor = Cursor::new(json);
    // 4. Create polars DataFrame from reading cursor as json
    let mut df = JsonReader::new(cursor).finish()?;

    let folder = if production { "production".to_string() } else { "testing".to_string() };
    let file_path = match univ {
        "Crypto" => format!("{}/output_crypto/{}/{}.parquet", &path, folder, &ticker),
        _ => format!("{}/output/{}/{}.parquet", &path, folder, &ticker),
    };
    let mut file = File::create(file_path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}
