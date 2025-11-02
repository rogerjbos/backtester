use polars::datatypes::DataType;
use polars::prelude::*;
use serde::Serialize;
use std::{
    cmp, collections::HashSet, env, error::Error as StdError, fmt::Debug, fs::File, io::Cursor,
    path::Path, sync::Arc,
};
use tokio::{fs, task::JoinError};
mod signals {
    pub mod technical;
}

pub mod clickhouse;
use crate::clickhouse::{insert_score_dataframe, write_price_file};

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
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub calmar_ratio: f64,
    pub win_loss_ratio: f64,
    pub recovery_factor: f64,
    pub profit_per_trade: f64,
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
    pub sell: Vec<i32>,
}

// Define the function type for your signals.
pub type SignalFunctionWithParam = fn(DataFrame, f64) -> BuySell;

pub async fn delete_all_files_in_folder<P: AsRef<Path> + std::fmt::Debug>(
    folder_path: P,
) -> Result<(), Box<dyn StdError>> {
    match fs::read_dir(&folder_path).await {
        Ok(mut entries) => {
            // Loop through the directory entries
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.is_file() {
                    // Delete the file
                    if let Err(e) = fs::remove_file(path).await {
                        eprintln!("Error deleting file: {:?}", e);
                    }
                }
            }
        }
        Err(e) => {
            // Handle the error if the directory cannot be read (e.g., folder does not exist)
            eprintln!("Failed to read directory {:?}: {:?}", folder_path, e);
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct Signal {
    pub name: String,
    pub func: Arc<SignalFunctionWithParam>,
    pub param: f64,
}

pub async fn score(datetag: &str, univ_str: &str) -> Result<(), Box<dyn StdError>> {
    // read in the testing file to get the historical performance for scoring
    let user_path = match env::var("CLICKHOUSE_USER_PATH") {
        Ok(path) => path,
        Err(_) => String::from("/srv"),
    };
    let path = format!("{}/rust_home/backtester", user_path);
    let path: &str = &path;

    // let path: &str = "/Users/rogerbos/rust_home/backtester";
    // Determine tag based on universe string
    let tag = if univ_str == "Crypto" {
        "crypto"
    } else {
        "stocks"
    };
    let file_path = format!("{}/final/{}_testing.csv", path, tag);

    // Manually create the schema and add fields
    let mut buysell_schema = Schema::with_capacity(6);
    buysell_schema.with_column("ticker".into(), DataType::String);
    buysell_schema.with_column("universe".into(), DataType::String);
    buysell_schema.with_column("strategy".into(), DataType::String);
    buysell_schema.with_column("date".into(), DataType::Date);
    buysell_schema.with_column("buy".into(), DataType::Int64);
    buysell_schema.with_column("sell".into(), DataType::Int64);
    let buysell_schema = Arc::new(buysell_schema);

    let file = File::open(file_path)?; // Open the file
    let testing = CsvReader::new(file).finish()?; // Pass the file handle to CsvReader
                                                  // println!("testing columns: {:?}", testing.clone().get_columns());
                                                  // println!("testing column_names: {:?}", testing.clone().get_column_names());
                                                  // println!("testing: {:?}", testing.clone());

    // read in the buys
    let buy_path = format!("{}/performance/{}_buys_{}.csv", path, tag, datetag);
    let buys = LazyCsvReader::new(buy_path)
        .with_schema(Some(buysell_schema.clone()))
        .with_has_header(true)
        .finish()?
        .lazy()
        .join(
            testing.clone().lazy(),
            [col("universe"), col("strategy")],
            [col("universe"), col("strategy")],
            JoinArgs::new(JoinType::Left),
        )
        .group_by_stable([col("date"), col("universe"), col("ticker")])
        .agg([
            col("buy").sum().alias("side"),
            col("risk_reward").sum().alias("risk_reward"),
            col("sharpe_ratio").sum().alias("sharpe_ratio"),
            col("sortino_ratio").sum().alias("sortino_ratio"),
            col("max_drawdown").sum().alias("max_drawdown"),
            col("calmar_ratio").sum().alias("calmar_ratio"),
            col("win_loss_ratio").sum().alias("win_loss_ratio"),
            col("recovery_factor").sum().alias("recovery_factor"),
            col("profit_per_trade").sum().alias("profit_per_trade"),
            col("expectancy").sum().alias("expectancy"),
            col("profit_factor").sum().alias("profit_factor"),
        ])
        .sort(
            vec!["profit_factor"],
            SortMultipleOptions {
                descending: vec![true],
                nulls_last: vec![true],
                ..Default::default()
            },
        );

    // println!("buys columns: {:?}", buys.clone().collect()?);

    // read in the sells
    let sell_path = format!("{}/performance/{}_sells_{}.csv", path, tag, datetag);
    let sells = LazyCsvReader::new(sell_path)
        .with_schema(Some(buysell_schema))
        .with_has_header(true)
        .finish()?
        .join(
            testing.clone().lazy(),
            [col("universe"), col("strategy")],
            [col("universe"), col("strategy")],
            JoinArgs::new(JoinType::Left),
        )
        .group_by_stable([col("date"), col("universe"), col("ticker")])
        .agg([
            col("sell").sum().alias("side"),
            (col("risk_reward").sum() * lit(-1.))
                .round(2)
                .alias("risk_reward"),
            (col("sharpe_ratio").sum() * lit(-1.))
                .round(2)
                .alias("sharpe_ratio"),
            (col("sortino_ratio").sum() * lit(-1.))
                .round(2)
                .alias("sortino_ratio"),
            (col("max_drawdown").sum() * lit(-1.))
                .round(2)
                .alias("max_drawdown"),
            (col("calmar_ratio").sum() * lit(-1.))
                .round(2)
                .alias("calmar_ratio"),
            (col("win_loss_ratio").sum() * lit(-1.))
                .round(2)
                .alias("win_loss_ratio"),
            (col("recovery_factor").sum() * lit(-1.))
                .round(2)
                .alias("recovery_factor"),
            (col("profit_per_trade").sum() * lit(-1.))
                .round(2)
                .alias("profit_per_trade"),
            (col("expectancy").sum() * lit(-1.))
                .round(2)
                .alias("expectancy"),
            (col("profit_factor").sum() * lit(-1.))
                .round(2)
                .alias("profit_factor"),
        ])
        .sort(
            vec!["profit_factor"],
            SortMultipleOptions {
                descending: vec![true],
                nulls_last: vec![true],
                ..Default::default()
            },
        );

    let both = concat(&[buys, sells], Default::default())?
        .group_by_stable([col("date"), col("universe"), col("ticker")])
        .agg([
            col("side").sum().alias("side"),
            col("risk_reward").sum().round(2).alias("risk_reward"),
            col("sharpe_ratio").sum().round(2).alias("sharpe_ratio"),
            col("sortino_ratio").sum().round(2).alias("sortino_ratio"),
            col("max_drawdown").sum().round(2).alias("max_drawdown"),
            col("calmar_ratio").sum().round(2).alias("calmar_ratio"),
            col("win_loss_ratio").sum().round(2).alias("win_loss_ratio"),
            col("recovery_factor")
                .sum()
                .round(2)
                .alias("recovery_factor"),
            col("profit_per_trade")
                .sum()
                .round(2)
                .alias("profit_per_trade"),
            col("expectancy").sum().round(2).alias("expectancy"),
            col("profit_factor").sum().round(2).alias("profit_factor"),
        ])
        .sort(
            vec!["side"],
            SortMultipleOptions {
                descending: vec![true],
                nulls_last: vec![true],
                ..Default::default()
            },
        )
        .collect()?;

    println!("Scoring...4");
    println!("both columns: {:?}", both.clone());

    // Use universe-specific filename instead of just stocks/crypto
    let file_tag = if univ_str == "Crypto" {
        "crypto"
    } else {
        univ_str
    };
    let both_path = format!("{}/score/{}_{}.csv", path, file_tag, datetag);
    let mut file = File::create(both_path)?;
    let _ = CsvWriter::new(&mut file).finish(&mut both.clone());

    if both.height() > 0 {
        if let Err(e) = insert_score_dataframe(both).await {
            eprintln!("Error in insert_score_dataframe: {}", e);
        }
    } else {
        println!("No observations: skipping insert.");
    }
    Ok(())
}

async fn concat_dataframes(dfs: Vec<DataFrame>) -> Result<DataFrame, PolarsError> {
    let lazy_frames: Vec<LazyFrame> = dfs.into_iter().map(|df| df.lazy()).collect();

    // Use the concat function for LazyFrames
    let concatenated_lazy_frame = concat(&lazy_frames, UnionArgs::default())?;

    // Collect the concatenated LazyFrame back into a DataFrame
    let result_df = concatenated_lazy_frame.collect()?;

    Ok(result_df)
}

pub async fn summary_performance_file(
    path: String,
    production: bool,
    stocks: bool,
    univ: Vec<String>,
) -> Result<(String, DataFrame), Box<dyn StdError>> {
    let bt_names = vec![
        "ticker",
        "universe",
        "strategy",
        "expectancy",
        "profit_factor",
        "hit_ratio",
        "realized_risk_reward",
        "avg_gain",
        "avg_loss",
        "max_gain",
        "max_loss",
        "sharpe_ratio",
        "sortino_ratio",
        "max_drawdown",
        "calmar_ratio",
        "win_loss_ratio",
        "recovery_factor",
        "profit_per_trade",
        "buys",
        "sells",
        "trades",
        "date",
        "buy",
        "sell",
    ];
    let set_bt: HashSet<_> = bt_names.iter().cloned().collect();

    let b_names = vec!["ticker", "universe", "strategy", "date", "buy", "sell"];

    let folder = match (stocks, production) {
        (true, true) => "output/production",
        (true, false) => "output/testing",
        (false, true) => "output_crypto/production",
        (false, false) => "output_crypto/testing",
    };

    let dir_path = format!("{}/{}", path, folder);

    let mut a: Vec<DataFrame> = Vec::new();
    let mut b: Vec<DataFrame> = Vec::new();
    let mut entries = fs::read_dir(dir_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
            let mut schema = Schema::with_capacity(24);
            schema.with_column("ticker".into(), DataType::String);
            schema.with_column("universe".into(), DataType::String);
            schema.with_column("strategy".into(), DataType::String);
            schema.with_column("expectancy".into(), DataType::Float64);
            schema.with_column("profit_factor".into(), DataType::Float64);
            schema.with_column("hit_ratio".into(), DataType::Float64);
            schema.with_column("realized_risk_reward".into(), DataType::Float64);
            schema.with_column("avg_gain".into(), DataType::Float64);
            schema.with_column("avg_loss".into(), DataType::Float64);
            schema.with_column("max_gain".into(), DataType::Float64);
            schema.with_column("max_loss".into(), DataType::Float64);
            schema.with_column("sharpe_ratio".into(), DataType::Float64);
            schema.with_column("sortino_ratio".into(), DataType::Float64);
            schema.with_column("max_drawdown".into(), DataType::Float64);
            schema.with_column("calmar_ratio".into(), DataType::Float64);
            schema.with_column("win_loss_ratio".into(), DataType::Float64);
            schema.with_column("recovery_factor".into(), DataType::Float64);
            schema.with_column("profit_per_trade".into(), DataType::Float64);
            schema.with_column("buys".into(), DataType::Float64);
            schema.with_column("sells".into(), DataType::Float64);
            schema.with_column("trades".into(), DataType::Float64);
            schema.with_column("date".into(), DataType::Date);
            schema.with_column("buy".into(), DataType::Int64);
            schema.with_column("sell".into(), DataType::Int64);
            let schema = Arc::new(schema);

            let lf = LazyCsvReader::new(&path)
                .with_schema(Some(schema))
                .with_has_header(true)
                .finish()?
                .collect();

            match lf {
                Ok(df) => {
                    // Ensure all required columns are present
                    let df_names = df.get_column_names();
                    let set_df: HashSet<&str> = df_names.iter().map(|s| s.as_str()).collect();
                    if set_bt.is_subset(&set_df) {
                        a.push(df.select(bt_names.clone())?);
                        b.push(df.select(b_names.clone())?);
                    }
                }
                Err(e) => println!("Error processing file {}: {}", path.display(), e),
            }
        }
    }

    // ALL
    let df = concat_dataframes(a).await?;
    // println!("ALL: {}", df.to_string());

    let out = summary_performance(df.clone())?;
    // println!("Average Performance by Strategy:\n {:?}", out);

    let datetag = df
        .column("date")?
        .get(0)?
        .to_string()
        .trim_matches('"')
        .replace("-", "");

    let tag: &str = if stocks { "stocks" } else { "crypto" };

    let perf_filename = if production {
        format!("{}/performance/{}_all_{}.csv", path, tag, &datetag)
    } else {
        format!("{}/performance/{}_testing.csv", path, tag)
    };
    let mut file = File::create(perf_filename)?;
    let _ = CsvWriter::new(&mut file).finish(&mut out.clone());

    // coverage
    if production {
        // concat all the price dfs
        let mut p: Vec<DataFrame> = Vec::new();
        // let univ = ["Crypto","LC1","LC2","MC1","MC2","SC1","SC2","SC3","SC4","Micro1","Micro2"];

        for u in univ {
            let file_path = format!("{}/data/production/{}.csv", path, u);
            let mut schema = Schema::with_capacity(8);
            schema.with_column("Date".into(), DataType::Date);
            schema.with_column("Ticker".into(), DataType::String);
            schema.with_column("Universe".into(), DataType::String);
            schema.with_column("Open".into(), DataType::Float64);
            schema.with_column("High".into(), DataType::Float64);
            schema.with_column("Low".into(), DataType::Float64);
            schema.with_column("Close".into(), DataType::Float64);
            schema.with_column("Volume".into(), DataType::Float64);
            let schema = Arc::new(schema);

            let tmp = LazyCsvReader::new(file_path)
                .with_schema(Some(schema))
                .with_has_header(true)
                .finish()?;

            // let df2 = tmp.clone().collect()?
            // println!("tmp columns: {:?}", df2.get_columns());
            // println!("tmp: {:?}", df2);

            let grouped = tmp
                .group_by_stable([col("Ticker")])
                .agg([
                    col("Date").count().alias("observations"),
                    col("Date").last().alias("last date"),
                ])
                .sort(
                    vec!["Ticker"],
                    SortMultipleOptions {
                        descending: vec![false],
                        nulls_last: vec![true],
                        ..Default::default()
                    },
                );
            p.push(grouped.collect().unwrap());
        }

        let all_p = concat_dataframes(p).await?;

        let df_grouped = df
            .clone()
            .lazy()
            .group_by_stable([col("ticker")])
            .agg([col("strategy").count().alias("strategies")])
            .sort(
                vec!["ticker"],
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![true],
                    ..Default::default()
                },
            );

        let both = all_p
            .lazy()
            .inner_join(df_grouped, col("Ticker"), col("ticker"))
            .filter(col("strategies").lt(lit(121)))
            .sort(
                vec!["strategies"],
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![true],
                    ..Default::default()
                },
            )
            .collect();
        println!("Strategy Coverage: {:?}", both);

        // buys and sells for the current date
        let df_b = concat_dataframes(b).await?;
        let mut buys = df_b
            .clone()
            .lazy()
            .filter(col("buy").eq(lit(1)))
            .sort(
                vec!["ticker"],
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![true],
                    ..Default::default()
                },
            )
            .collect()?;

        let mut sells = df_b
            .clone()
            .lazy()
            .filter(col("sell").eq(lit(-1)))
            .sort(
                vec!["ticker"],
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![true],
                    ..Default::default()
                },
            )
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
        let lc = out
            .clone()
            .lazy()
            .filter(
                col("universe")
                    .eq(lit("LC1"))
                    .or(col("universe").eq(lit("LC2"))),
            )
            .collect();

        match lc {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "LC");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut lc?);
            }
            Err(ref e) => println!("Error filtering DataFrame for LC: \n{:?}", e),
        }

        // MC
        let mc = out
            .clone()
            .lazy()
            .filter(
                col("universe")
                    .eq(lit("MC1"))
                    .or(col("universe").eq(lit("MC2"))),
            )
            .collect();

        match mc {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "MC");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut mc?);
            }
            Err(ref e) => println!("Error filtering DataFrame for MC: \n{:?}", e),
        }

        // SC
        let sc = out
            .clone()
            .lazy()
            .filter(
                col("universe")
                    .eq(lit("SC1"))
                    .or(col("universe").eq(lit("SC2")))
                    .or(col("universe").eq(lit("SC3")))
                    .or(col("universe").eq(lit("SC4"))),
            )
            .collect();

        match sc {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "SC");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut sc?);
            }
            Err(ref e) => println!("Error filtering DataFrame for SC: \n{:?}", e),
        }

        // Microcap
        let micro = out
            .clone()
            .lazy()
            .filter(
                col("universe")
                    .eq(lit("Micro1"))
                    .or(col("universe").eq(lit("Micro2"))),
            )
            .collect();

        match micro {
            Ok(ref _df) => {
                let perf_filename = format!("{}/performance/{}.csv", path, "Micro");
                let mut file = File::create(perf_filename)?;
                let _ = CsvWriter::new(&mut file).finish(&mut micro?);
            }
            Err(ref e) => println!("Error filtering DataFrame for Micro: \n{:?}", e),
        }
    }

    Ok((datetag, out))
}

pub fn summary_performance(df: DataFrame) -> Result<DataFrame, Box<dyn StdError>> {
    let out = df
        .lazy()
        .group_by_stable([col("strategy"), col("universe")])
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
            col("sharpe_ratio").mean().alias("sharpe_ratio"),
            col("sortino_ratio").mean().alias("sortino_ratio"),
            col("max_drawdown").mean().alias("max_drawdown"),
            col("calmar_ratio").mean().alias("calmar_ratio"),
            col("win_loss_ratio").mean().alias("win_loss_ratio"),
            col("recovery_factor").mean().alias("recovery_factor"),
            col("profit_per_trade").mean().alias("profit_per_trade"),
            col("expectancy").mean().alias("expectancy"),
            col("profit_factor").mean().alias("profit_factor"),
        ])
        .filter(col("trades").gt(lit(3)))
        .sort(
            vec!["profit_factor"],
            SortMultipleOptions {
                descending: vec![true],
                nulls_last: vec![true],
                ..Default::default()
            },
        )
        .collect()?;

    Ok(out)
}

pub async fn sig(
    df: LazyFrame,
    func: SignalFunctionWithParam, // Use the correct type
    param: f64,
    signal_name: String,
) -> Result<Backtest, Box<dyn StdError>> {
    let s = (func)(df.clone().collect()?, param); // Call the signal function
    let bt = backtest_performance(df.collect()?, s, &signal_name)?;
    Ok(bt)
}

pub async fn sig_sized(
    df: LazyFrame,
    func: SignalFunctionWithParam, // Use the correct type
    param: f64,
    signal_name: String,
    entry_amount: f64,
    exit_amount: f64,
) -> Result<Backtest, Box<dyn StdError>> {
    let s = (func)(df.clone().collect()?, param); // Call the signal function
    let bt = backtest_performance_sized(df.collect()?, s, &signal_name, entry_amount, exit_amount)?;
    Ok(bt)
}

pub async fn run_all_backtests(
    df: LazyFrame,
    signals: Vec<Signal>,
) -> Result<Vec<Backtest>, JoinError> {
    // Wrap df in an Arc for shared ownership across tasks
    let df = Arc::new(df);

    let futures: Vec<_> = signals
        .into_iter()
        .map(|signal| {
            // Clone Arc for each task
            let df_clone = Arc::clone(&df);
            let func = signal.func.clone(); // Extract the function from the Signal struct
            let _p = signal.param; // Use default value if no parameter is provided

            tokio::spawn(async move {
                // sig(df_clone.as_ref().clone(), *func, p).await.unwrap()
                sig(df_clone.as_ref().clone(), *func, signal.param, signal.name)
                    .await
                    .unwrap()
            })
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    // Handle the results, assuming `sig` returns `Result<Backtest, _>`
    let backtests: Vec<Backtest> = results.into_iter().filter_map(Result::ok).collect();

    Ok(backtests)
}

pub async fn create_price_files(
    univ_vec: Vec<String>,
    production: bool,
) -> Result<(), Box<dyn StdError>> {
    let folder = if production { "production" } else { "testing" };

    for u in univ_vec {
        let user_path = match env::var("CLICKHOUSE_USER_PATH") {
            Ok(path) => path,
            Err(_) => String::from("/srv"),
        };
        let file_path = format!(
            "{}/rust_home/backtester/data/{}/{}.csv",
            user_path,
            folder.to_string(),
            u.to_string()
        );
        let file_path: &str = &file_path;
        if production == false && Path::new(&file_path).exists() {
            println!("Price file exists for {}", file_path);
        } else {
            println!("Price file generating for {}", file_path);
            write_price_file(u, production).await?;
        }
    }
    Ok(())
}

pub fn backtest_performance(
    df: DataFrame,
    side: BuySell,
    strategy: &str,
) -> Result<Backtest, Box<dyn StdError>> {
    let df = df.clone();
    let len = df.height();

    let mut long_result = vec![0.0; len];
    let mut short_result = vec![0.0; len];

    let open = df.column("Open").unwrap().f64().unwrap();

    // Variable holding period
    for i in 0..len {
        if side.buy[i] == 1 {
            for a in i + 1..cmp::min(i + 1000, len) {
                if side.buy[a] == 1 || side.sell[a] == -1 {
                    long_result[a] = open.get(a).unwrap_or(0.0) - open.get(i).unwrap_or(0.0);
                    break;
                }
            }
        }
    }
    for i in 0..len {
        if side.sell[i] == -1 {
            for a in i + 1..cmp::min(i + 1000, len) {
                if side.buy[a] == 1 || side.sell[a] == -1 {
                    short_result[a] = open.get(i).unwrap_or(0.0) - open.get(a).unwrap_or(0.0);
                    break;
                }
            }
        }
    }

    // Aggregating the long & short results into one column
    let total_result: Vec<f64> = long_result
        .iter()
        .zip(short_result.iter())
        .map(|(&l, &s)| l + s)
        .collect();

    // Profit factor
    let total_net_profits: Vec<f64> = total_result
        .clone()
        .into_iter()
        .filter(|&x| x > 0.0)
        .collect();
    let total_net_losses: Vec<f64> = total_result
        .clone()
        .into_iter()
        .filter(|&x| x < 0.0)
        .collect();
    let sum_total_net_profits = total_net_profits.iter().sum::<f64>();
    let sum_total_net_losses = total_net_losses.iter().sum::<f64>().abs();
    let profit_factor = if sum_total_net_losses > 0.0 {
        f64::min(999.0, sum_total_net_profits / sum_total_net_losses)
    } else {
        0.0
    };

    // Hit ratio
    let hit_ratio: f64 = if total_net_losses.len() + total_net_profits.len() > 0 {
        (total_net_profits.len() as f64 / (total_net_losses.len() + total_net_profits.len()) as f64)
            * 100.0
    } else {
        0.0
    };

    // Risk reward ratio
    let average_gain = if total_net_profits.len() > 0 {
        sum_total_net_profits / total_net_profits.len() as f64
    } else {
        0.0
    };
    let average_loss = if total_net_losses.len() > 0 {
        sum_total_net_losses / total_net_losses.len() as f64
    } else {
        0.0
    };
    let realized_risk_reward = if average_loss > 0.0 {
        average_gain / average_loss
    } else {
        0.0
    };

    let trades: i32 = total_result
        .clone()
        .into_iter()
        .filter(|&x| x != 0.0)
        .collect::<Vec<_>>()
        .len() as i32;

    // Expectancy
    let expectancy = if total_net_profits.len() + total_net_losses.len() > 0 {
        (average_gain * (hit_ratio / 100.0)) - ((1.0 - (hit_ratio / 100.0)) * average_loss)
    } else {
        0.0
    };

    let max_gain = total_net_profits
        .iter()
        .cloned()
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);
    let max_loss = total_net_losses
        .iter()
        .cloned()
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);

    let buys = side.buy.iter().sum::<i32>();
    let sells = side.sell.iter().sum::<i32>().abs();

    let buy = side.buy.get(len - 1).cloned().unwrap_or(0);
    let sell = side.sell.get(len - 1).cloned().unwrap_or(0);
    let ticker1 = df
        .column("Ticker")
        .unwrap()
        .get(0)
        .unwrap_or("".into())
        .to_string();
    let ticker = ticker1.trim_matches('"').to_string();
    let universe1 = df
        .column("Universe")
        .unwrap()
        .get(0)
        .unwrap_or("".into())
        .to_string();
    let universe = universe1.trim_matches('"').to_string();
    let date1 = df
        .column("Date")
        .unwrap()
        .get(len - 1)
        .unwrap_or("".into())
        .to_string();
    let date = date1.trim_matches('"').to_string();

    // Additional Metrics
    let sharpe_ratio = if total_result.len() > 1 {
        let mean_return = total_result.iter().sum::<f64>() / total_result.len() as f64;
        let std_dev = (total_result
            .iter()
            .map(|x| (x - mean_return).powi(2))
            .sum::<f64>()
            / (total_result.len() as f64 - 1.0))
            .sqrt();
        if std_dev > 0.0 {
            mean_return / std_dev
        } else {
            0.0
        }
    } else {
        0.0
    };

    let sortino_ratio = if total_result.len() > 1 {
        let mean_return = total_result.iter().sum::<f64>() / total_result.len() as f64;
        let downside_deviation = (total_result
            .iter()
            .filter(|&&x| x < 0.0)
            .map(|x| x.powi(2))
            .sum::<f64>()
            / total_result.len() as f64)
            .sqrt();
        if downside_deviation > 0.0 {
            mean_return / downside_deviation
        } else {
            0.0
        }
    } else {
        0.0
    };

    let max_drawdown = {
        let mut peak = total_result[0];
        let mut max_dd = 0.0;
        for &value in &total_result {
            if value > peak {
                peak = value;
            }
            let drawdown = peak - value;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }
        max_dd
    };

    let calmar_ratio = if max_drawdown > 0.0 {
        sum_total_net_profits / max_drawdown
    } else {
        0.0
    };

    let win_loss_ratio = if average_loss > 0.0 {
        average_gain / average_loss
    } else {
        0.0
    };

    let recovery_factor = if max_drawdown > 0.0 {
        sum_total_net_profits / max_drawdown
    } else {
        0.0
    };

    let profit_per_trade = if trades > 0 {
        sum_total_net_profits / trades as f64
    } else {
        0.0
    };

    Ok(Backtest {
        ticker,
        universe,
        strategy: strategy.to_string(),
        expectancy,
        profit_factor,
        hit_ratio,
        realized_risk_reward,
        avg_gain: average_gain,
        avg_loss: average_loss,
        max_gain,
        max_loss,
        sharpe_ratio,
        sortino_ratio,
        max_drawdown,
        calmar_ratio,
        win_loss_ratio,
        recovery_factor,
        profit_per_trade,
        buys,
        sells,
        trades,
        date,
        buy,
        sell,
    })
}

pub fn backtest_performance_sized(
    df: DataFrame,
    side: BuySell,
    strategy: &str,
    entry_amount: f64,
    exit_amount: f64,
) -> Result<Backtest, Box<dyn StdError>> {
    let df = df.clone();
    let len = df.height();

    let mut cash = 100_000.0; // Starting cash
    let mut holdings = 0.0; // Number of shares held
    let mut cash_value = vec![0.0; len];
    let mut holdings_value = vec![0.0; len];
    let mut portfolio_value = vec![0.0; len]; // Portfolio value over time

    let open = df.column("Open").unwrap().f64().unwrap();

    for i in 0..len {
        if side.buy[i] == 1 {
            // Buy entry_amount worth of the ticker
            let price = open.get(i).unwrap_or(0.0);
            if price > 0.0 && cash >= entry_amount {
                let shares_to_buy = entry_amount / price;
                holdings += shares_to_buy;
                cash -= entry_amount;
            }
        }

        if side.sell[i] == -1 {
            // Sell exit_amount worth of the ticker
            let price = open.get(i).unwrap_or(0.0);
            if price > 0.0 && holdings > 0.0 {
                let shares_to_sell = f64::min(exit_amount / price, holdings);
                holdings -= shares_to_sell;
                cash += shares_to_sell * price;
            }
        }

        // Calculate portfolio value at the end of each day
        let price = open.get(i).unwrap_or(0.0);
        cash_value[i] = cash;
        holdings_value[i] = holdings * price;
        portfolio_value[i] = cash + (holdings * price);
        if i < 10 {
            println!(
                "Portfolio value at {i}: cash:{} holdings:{} port:{} buy:{} sell:{}",
                cash_value[i], holdings_value[i], portfolio_value[i], side.buy[i], side.sell[i]
            );
        }
    }

    // Calculate performance metrics
    let total_result: Vec<f64> = portfolio_value.windows(2).map(|w| w[1] - w[0]).collect();

    let total_net_profits: Vec<f64> = total_result.iter().cloned().filter(|&x| x > 0.0).collect();
    let total_net_losses: Vec<f64> = total_result.iter().cloned().filter(|&x| x < 0.0).collect();
    let sum_total_net_profits = total_net_profits.iter().sum::<f64>();
    let sum_total_net_losses = total_net_losses.iter().sum::<f64>().abs();
    let profit_factor = if sum_total_net_losses > 0.0 {
        f64::min(999.0, sum_total_net_profits / sum_total_net_losses)
    } else {
        0.0
    };

    let hit_ratio: f64 = if total_net_losses.len() + total_net_profits.len() > 0 {
        (total_net_profits.len() as f64 / (total_net_losses.len() + total_net_profits.len()) as f64)
            * 100.0
    } else {
        0.0
    };

    let average_gain = if total_net_profits.len() > 0 {
        sum_total_net_profits / total_net_profits.len() as f64
    } else {
        0.0
    };
    let average_loss = if total_net_losses.len() > 0 {
        sum_total_net_losses / total_net_losses.len() as f64
    } else {
        0.0
    };
    let realized_risk_reward = if average_loss > 0.0 {
        average_gain / average_loss
    } else {
        0.0
    };

    let buys: i32 = side.buy.iter().sum::<i32>();
    let sells: i32 = -side.sell.iter().sum::<i32>();
    let trades: i32 = buys + sells;

    let expectancy = if total_net_profits.len() + total_net_losses.len() > 0 {
        (average_gain * (hit_ratio / 100.0)) - ((1.0 - (hit_ratio / 100.0)) * average_loss)
    } else {
        0.0
    };

    let max_gain = total_net_profits
        .iter()
        .cloned()
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);
    let max_loss = total_net_losses
        .iter()
        .cloned()
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);

    let ticker = df
        .column("Ticker")
        .unwrap()
        .get(0)
        .unwrap_or("".into())
        .to_string()
        .trim_matches('"')
        .to_string();
    let universe = df
        .column("Universe")
        .unwrap()
        .get(0)
        .unwrap_or("".into())
        .to_string()
        .trim_matches('"')
        .to_string();
    let date = df
        .column("Date")
        .unwrap()
        .get(len - 1)
        .unwrap_or("".into())
        .to_string()
        .trim_matches('"')
        .to_string();

    // Additional Metrics
    let sharpe_ratio = if total_result.len() > 1 {
        let mean_return = total_result.iter().sum::<f64>() / total_result.len() as f64;
        let std_dev = (total_result
            .iter()
            .map(|x| (x - mean_return).powi(2))
            .sum::<f64>()
            / (total_result.len() as f64 - 1.0))
            .sqrt();
        if std_dev > 0.0 {
            mean_return / std_dev
        } else {
            0.0
        }
    } else {
        0.0
    };

    let sortino_ratio = if total_result.len() > 1 {
        let mean_return = total_result.iter().sum::<f64>() / total_result.len() as f64;
        let downside_deviation = (total_result
            .iter()
            .filter(|&&x| x < 0.0)
            .map(|x| x.powi(2))
            .sum::<f64>()
            / total_result.len() as f64)
            .sqrt();
        if downside_deviation > 0.0 {
            mean_return / downside_deviation
        } else {
            0.0
        }
    } else {
        0.0
    };

    let max_drawdown = {
        let mut peak = total_result[0];
        let mut max_dd = 0.0;
        for &value in &total_result {
            if value > peak {
                peak = value;
            }
            let drawdown = peak - value;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }
        max_dd
    };

    let calmar_ratio = if max_drawdown > 0.0 {
        sum_total_net_profits / max_drawdown
    } else {
        0.0
    };

    let win_loss_ratio = if average_loss > 0.0 {
        average_gain / average_loss
    } else {
        0.0
    };

    let recovery_factor = if max_drawdown > 0.0 {
        sum_total_net_profits / max_drawdown
    } else {
        0.0
    };

    let profit_per_trade = if trades > 0 {
        sum_total_net_profits / trades as f64
    } else {
        0.0
    };

    Ok(Backtest {
        ticker,
        universe,
        strategy: strategy.to_string(),
        expectancy,
        profit_factor,
        hit_ratio,
        realized_risk_reward,
        avg_gain: average_gain,
        avg_loss: average_loss,
        max_gain,
        max_loss,
        sharpe_ratio,
        sortino_ratio,
        max_drawdown,
        calmar_ratio,
        win_loss_ratio,
        recovery_factor,
        profit_per_trade,
        buys,
        sells,
        trades,
        date,
        buy: side.buy.last().cloned().unwrap_or(0),
        sell: side.sell.last().cloned().unwrap_or(0),
    })
}

pub fn showbt(bt: Backtest) -> Result<(), Box<dyn StdError>> {
    println!("");
    println!("Ticker:   {:<20}", bt.ticker);
    println!("Universe: {:<20}", bt.universe);
    println!("Strategy: {:<20}", bt.strategy);
    println!("Profit Factor:    {:>9.1}", bt.profit_factor);
    println!("Hit Ratio:        {:>9.1}", bt.hit_ratio);
    println!("Expectancy:       {:>9.1}", bt.expectancy);
    println!("Risk-Reward:      {:>9.1}", bt.realized_risk_reward);
    println!("Avg Gain:         {:>9.1}", bt.avg_gain);
    println!("Avg Loss:         {:>9.1}", bt.avg_loss);
    println!("Max Gain:         {:>9.1}", bt.max_gain);
    println!("Max Loss:         {:>9.1}", bt.max_loss);
    println!("sharpe_ratio:     {:>9.1}", bt.sharpe_ratio);
    println!("sortino_ratio:    {:>9.1}", bt.sortino_ratio);
    println!("max_drawdown:     {:>9.1}", bt.max_drawdown);
    println!("calmar_ratio:     {:>9.1}", bt.calmar_ratio);
    println!("win_loss_ratio:   {:>9.1}", bt.win_loss_ratio);
    println!("recovery_factor:  {:>9.1}", bt.recovery_factor);
    println!("profit_per_trade: {:>9.1}", bt.profit_per_trade);
    println!("Buys:             {:>9.1}", bt.buys);
    println!("Sells:            {:>9.1}", bt.sells);
    println!("Trades:           {:>9.1}", bt.trades);
    Ok(())
}

pub fn preprocess(df: LazyFrame) -> Result<DataFrame, Box<dyn StdError>> {
    let window_size_5 = RollingOptionsFixedWindow {
        window_size: 5,
        min_periods: 5, // Minimum number of observations in window required to have a value
        center: false,  // Set to true to set the labels at the center of the window
        weights: None,  // Optional weights for the window
        ..Default::default()
    };

    let window_size_20 = RollingOptionsFixedWindow {
        window_size: 20,
        min_periods: 20, // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_50 = RollingOptionsFixedWindow {
        window_size: 50,
        min_periods: 50, // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_100 = RollingOptionsFixedWindow {
        window_size: 100,
        min_periods: 100, // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_200 = RollingOptionsFixedWindow {
        window_size: 200,
        min_periods: 200, // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let window_size_250 = RollingOptionsFixedWindow {
        window_size: 250,
        min_periods: 250, // Minimum number of observations in window required to have a value
        ..Default::default()
    };

    let out = df
        .clone()
        .select([cols([
            "Ticker", "Date", "Open", "High", "Low", "Close", "Volume",
        ])])
        .sort(
            vec!["Date"],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![true],
                ..Default::default()
            },
        )
        .sort(
            vec!["Ticker"],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![true],
                ..Default::default()
            },
        )
        .with_columns([
            (col("Close") / col("Close").shift(lit(1)).over([col("Ticker")]) - lit(1)).alias("Ret"),
            col("Low")
                .rolling_min(window_size_20.clone().into())
                .over([col("Ticker")])
                .alias("min_low_20"),
            col("High")
                .rolling_max(window_size_20.clone().into())
                .over([col("Ticker")])
                .alias("max_high_20"),
            col("Low")
                .rolling_min(window_size_250.clone().into())
                .over([col("Ticker")])
                .alias("min_low_250"),
            col("High")
                .rolling_max(window_size_250.clone().into())
                .over([col("Ticker")])
                .alias("max_high_250"),
            col("Close")
                .rolling_mean(window_size_5.clone().into())
                .over([col("Ticker")])
                .alias("MA_5"),
            col("Close")
                .rolling_mean(window_size_20.clone().into())
                .over([col("Ticker")])
                .alias("MA_20"),
            col("Close")
                .rolling_mean(window_size_50.clone().into())
                .over([col("Ticker")])
                .alias("MA_50"),
            col("Close")
                .rolling_mean(window_size_100.clone().into())
                .over([col("Ticker")])
                .alias("MA_100"),
            col("Close")
                .rolling_mean(window_size_200.clone().into())
                .over([col("Ticker")])
                .alias("MA_200"),
        ])
        .with_columns([(((col("Close") - col("min_low_20"))
            / (col("max_high_20") - col("min_low_20")))
            * lit(100.0))
        .alias("stoch_oscillator_20")])
        .collect()
        .unwrap();

    Ok(out)
}

pub fn postprocess(df: DataFrame) -> Result<DataFrame, Box<dyn StdError>> {
    let _sma = signals::technical::sma(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let _ema = signals::technical::ema(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        0.5,
        20,
    );
    let _smoothed_ma = signals::technical::smoothed_ma(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        0.5,
        20,
    );
    let _vol = signals::technical::volatility(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let _atr = signals::technical::atr(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        10,
    );
    let _rsi = signals::technical::rsi(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let (_out, _stoch, _signal) = signals::technical::stochastic_oscillator(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        250,
        true,
        true,
        3,
        3,
    );
    let _normalized_index = signals::technical::normalized_index(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let (upper_aug_bbands, lower_aug_bbands) = signals::technical::augmented_bollinger_bands(
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        20,
        2.,
    );
    let (_upper_bbands, _lower_bbands) = signals::technical::bollinger_bands(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
        2.,
    );
    let (_upper_kband, _lower_kband, _middle_kband) = signals::technical::k_volatility_band(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        20,
        2.,
    );
    let _rsi_atr = signals::technical::rsi_atr(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        3,
        5,
        7,
    );
    let _trend_intensity = signals::technical::trend_intensity_indicator(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let _kama_10 = signals::technical::kama(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        10,
    );
    let (_fma_high, _fma_low) = signals::technical::fma(
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
    );
    let _frama = signals::technical::fractal_adaptive_ma(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        10,
    );
    let _lwma = signals::technical::lwma(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        10,
    );
    let _hull_ma = signals::technical::hull_ma(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        10,
    );
    let _vama = signals::technical::volatility_adjusted_moving_average(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        3,
        30,
    );
    let _ema = signals::technical::ema(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        2.,
        13,
    );
    let (_macd_diff, _macd_signal) = signals::technical::macd(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        26,
        12,
        9,
    );
    let _elder = signals::technical::elder_impulse(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        250,
    );
    let (_aroon_up, _aroon_down) = signals::technical::aroon(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        25,
    );
    let (_di_plus, _di_minus, _adx, _smoothed_adx) = signals::technical::adx(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        14,
    );
    let _awesome = signals::technical::awesome_oscillator(
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        34,
        5,
    );
    let (_donchian_low, _donchian_high, _donchian_med) = signals::technical::donchian(
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let (_keltner_upper, _keltner_lower) = signals::technical::keltner_channel(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        60,
        60,
        20,
    );
    let _squeeze = signals::technical::squeeze(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        60,
        10.,
        60,
        20,
    );
    let _supertrend = signals::technical::supertrend(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        10,
        2.,
    );
    let _trend_intensity = signals::technical::trend_intensity_indicator(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let _trix = signals::technical::trix(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        20,
    );
    let _vertical_horizontal_ind = signals::technical::vertical_horizontal_indicator(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        60,
    );
    let (_kijun, _tenkan, _senkou_span_a, _senkou_span_b) = signals::technical::ichimoku(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        26,
        9,
        26,
        26,
        52,
    );
    let countdown_indicator = signals::technical::countdown_indicator(
        df.column("Open").unwrap().as_series().unwrap().to_owned(),
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        8,
        3,
    );
    let countdown_indicator_series = Series::new("".into(), &countdown_indicator);
    let (_downward, _upward, _net) =
        signals::technical::extreme_duration(countdown_indicator_series, 5., -5.);
    let _demarker = signals::technical::demarker(
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        14,
    );
    let _disparity = signals::technical::disparity_index(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        14,
    );
    let _fisher = signals::technical::fisher_transform(
        df.column("High").unwrap().as_series().unwrap().to_owned(),
        df.column("Low").unwrap().as_series().unwrap().to_owned(),
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        14,
    );
    let _time_up = signals::technical::time_up(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        1,
    );
    let _tsabm = signals::technical::time_spent_above_below_mean(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        34,
    );
    let (ftp_buy, ftp_sell) = signals::technical::fibonacci_timing_pattern(
        df.column("Close").unwrap().as_series().unwrap().to_owned(),
        8.,
        5,
        3,
        2,
    );

    let out = df
        .lazy()
        .with_columns([
            // lit(Series::new("".into(), &sma)).alias("sma_20"),
            // lit(Series::new("".into(), &ema)).alias("ema_20"),
            // lit(Series::new("".into(), &smoothed_ma)).alias("smoothed_ma_20"),
            // lit(Series::new("".into(), &vol)).alias("vol_20"),
            // lit(Series::new("".into(), &atr)).alias("atr_10"),
            // lit(Series::new("".into(), &rsi)).alias("rsi_20"),
            // lit(Series::new("".into(), &out)).alias("stoch_osc_out"),
            // lit(Series::new("".into(), &stoch)).alias("stoch_osc_stoch"),
            // lit(Series::new("".into(), &signal)).alias("stoch_osc_signal"),
            // lit(Series::new("".into(), &normalized_index)).alias("normalized_index"),
            lit(Series::new("".into(), &upper_aug_bbands)).alias("upper_aug_bbands"),
            lit(Series::new("".into(), &lower_aug_bbands)).alias("lower_aug_bbands"),
            // lit(Series::new("".into(), &upper_kband)).alias("upper_kband"),
            // lit(Series::new("".into(), &lower_kband)).alias("lower_kband"),
            // lit(Series::new("".into(), &middle_kband)).alias("middle_kband"),
            // lit(Series::new("".into(), &rsi_atr)).alias("rsi_atr"),
            // lit(Series::new("".into(), &kama_10)).alias("kama_10"),
            // lit(Series::new("".into(), &fma_high)).alias("fma_high"),
            // lit(Series::new("".into(), &fma_low)).alias("fma_low"),
            // lit(Series::new("".into(), &frama)).alias("frama"),
            // lit(Series::new("".into(), &lwma)).alias("lwma"),
            // lit(Series::new("".into(), &hull_ma)).alias("hull_ma"),
            // lit(Series::new("".into(), &vama)).alias("vama"),
            // lit(Series::new("".into(), &macd_diff)).alias("macd_diff"),
            // lit(Series::new("".into(), &macd_signal)).alias("macd_signal"),
            // lit(Series::new("".into(), &elder)).alias("elder"),
            // lit(Series::new("".into(), &aroon_up)).alias("aroon_up"),
            // lit(Series::new("".into(), &aroon_down)).alias("aroon_down"),
            // lit(Series::new("".into(), &di_plus)).alias("di_plus"),
            // lit(Series::new("".into(), &di_minus)).alias("di_minus"),
            // lit(Series::new("".into(), &adx)).alias("adx"),
            // lit(Series::new("".into(), &smoothed_adx)).alias("smoothed_adx"),
            // lit(Series::new("".into(), &awesome)).alias("awesome_oscillator"),
            // lit(Series::new("".into(), &donchian_low)).alias("donchian_low"),
            // lit(Series::new("".into(), &donchian_high)).alias("donchian_high"),
            // lit(Series::new("".into(), &donchian_med)).alias("donchian_med"),z
            // lit(Series::new("".into(), &squeeze)).alias("squeeze"),
            // lit(Series::new("".into(), &keltner_upper)).alias("keltner_upper"),
            // lit(Series::new("".into(), &keltner_lower)).alias("keltner_lower"),
            // lit(Series::new("".into(), &atr2)).alias("atr2"),
            // lit(Series::new("".into(), &squeeze)).alias("squeeze"),
            // lit(Series::new("".into(), &supertrend)).alias("supertrend"),
            // lit(Series::new("".into(), &trend_intensity)).alias("trend_intensity"),
            // lit(Series::new("".into(), &trix)).alias("trix"),
            // lit(Series::new("".into(), &vertical_horizontal_ind)).alias("vh_indicator"),
            // lit(Series::new("".into(), &kijun)).alias("kijun"),
            // lit(Series::new("".into(), &tenkan)).alias("tenkan"),
            // lit(Series::new("".into(), &senkou_span_a)).alias("senkou_span_a"),
            // lit(Series::new("".into(), &senkou_span_b)).alias("senkou_span_b"),
            // lit(Series::new("".into(), &countdown_indicator)).alias("countdown_indicator"),
            // lit(Series::new("".into(), &downward)).alias("downward"),
            // lit(Series::new("".into(), &upward)).alias("upward"),
            // lit(Series::new("".into(), &net)).alias("net"),
            // lit(Series::new("".into(), &demarker)).alias("demarker"),
            // lit(Series::new("".into(), &disparity)).alias("disparity"),
            // lit(Series::new("".into(), &fisher)).alias("fisher"),
            // lit(Series::new("".into(), &time_up)).alias("time_up"),
            // lit(Series::new("".into(), &tsabm)).alias("tsabm"),
            lit(Series::new("".into(), &ftp_buy)).alias("ftp_buy"),
            lit(Series::new("".into(), &ftp_sell)).alias("ftp_sell"),
        ])
        .collect()
        .unwrap();

    Ok(out)
}

pub async fn save_backtest(
    path: String,
    bt: Vec<Backtest>,
    univ: &str,
    ticker: String,
    production: bool,
) -> Result<(), Box<dyn StdError>> {
    // 2. Jsonify your struct Vec
    let json = serde_json::to_string(&bt)?;
    // 3. Create cursor from json
    let cursor = Cursor::new(json);
    // 4. Create polars DataFrame from reading cursor as json
    let mut df = JsonReader::new(cursor).finish()?;

    let folder = if production {
        "production".to_string()
    } else {
        "testing".to_string()
    };

    let csv_path = match univ {
        "Crypto" => format!("{}/output_crypto/{}/{}.csv", &path, folder, &ticker),
        _ => format!("{}/output/{}/{}.csv", &path, folder, &ticker),
    };
    let mut csvfile = File::create(csv_path.clone())?;
    let _ = CsvWriter::new(&mut csvfile).finish(&mut df);
    Ok(())
}

pub async fn read_price_file(file_path: String) -> Result<LazyFrame, Box<dyn StdError>> {
    // Manually create the schema and add fields
    let mut schema = Schema::with_capacity(8);
    schema.with_column("Date".into(), DataType::Date);
    schema.with_column("Ticker".into(), DataType::String);
    schema.with_column("Universe".into(), DataType::String);
    schema.with_column("Open".into(), DataType::Float64);
    schema.with_column("High".into(), DataType::Float64);
    schema.with_column("Low".into(), DataType::Float64);
    schema.with_column("Close".into(), DataType::Float64);
    schema.with_column("Volume".into(), DataType::Float64);
    let schema = Arc::new(schema);

    let lf = LazyCsvReader::new(file_path)
        .with_schema(Some(schema))
        .with_has_header(true)
        .finish()?;
    Ok(lf)
}

pub fn print_dataframe_vertically(df: &DataFrame) {
    for idx in 0..df.height() {
        match df.get_row(idx) {
            Ok(row) => {
                for (col_name, value) in df.get_column_names().iter().zip(row.0.iter()) {
                    match value {
                        AnyValue::Float64(v) => println!("{:16}: {:4.2}", col_name, v),
                        AnyValue::Float32(v) => println!("{:16}: {:4.2}", col_name, v),
                        AnyValue::Int64(v) => println!("{:16}: {}", col_name, v),
                        AnyValue::Int32(v) => println!("{:16}: {}", col_name, v),
                        AnyValue::String(v) => println!("{:16}: {}", col_name, v),
                        _ => println!("{:16}: {:4.2}", col_name, value),
                    }
                }
            }
            Err(e) => eprintln!("Error retrieving row {}: {}", idx, e),
        }
        println!();
    }
}
