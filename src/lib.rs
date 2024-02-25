
use std::fmt::Debug;
use std::cmp;
use polars::prelude::*;
use polars::prelude::ScanArgsParquet;
use std::env;


use connectorx::prelude::*;

use connectorx::{
    sql::CXQuery,
};

mod signals {
    pub mod technical;    
}

pub fn production_data() -> LazyFrame {

    let startdate = "2000-01-01";
    let enddate = "2024-01-24";
    // let tickers: [&str; 2] = ["US000000048595", "US000000013186"];
    let tickers: [&str; 4] = ["US000000000183", "US000000000250", "US000000000355", "US000000013186"];
    // let tickers: [&str; 1] = ["US000000013186"];

    let mut result = "('".to_string();
    for i in 0..tickers.len() {
        if i > 0 { result.push_str("','") };
        result.push_str(tickers[i]);
    }
    result.push_str("')");

    let txt = format!("SELECT TO_CHAR(date, 'YYYY-MM-DD HH:MM:SS') as date, ticker, \"adjOpen\" as open, \"adjHigh\" as high, \"adjLow\" as low, \"adjClose\" as close, \"adjVolume\" as volume FROM price_history where ticker in {result} and date between '{startdate}' and '{enddate}' order by ticker, date");

    // let txt = format!("SELECT TO_CHAR(date, 'YYYY-MM-DD HH:MM:SS') as date, ticker, \"adjOpen\" as open, \"adjHigh\" as high, \"adjLow\" as low, \"adjClose\" as close, \"adjVolume\" as volume FROM price_history order by ticker, date");

    // Get DB client and connection
    let pg = env::var("PG").unwrap();
    let conn = String::from(format!("postgresql://postgres:{pg}@192.168.86.68/tiingo?cxprotocol=binary"));
    let source_conn = SourceConn::try_from(&*conn).expect("parse conn str failed");
    let queries = &[CXQuery::from(txt.as_str())];
    let destination = get_arrow2(&source_conn, None, queries).expect("query failed");
    let data = destination.polars();

    let df = data.unwrap()
        .rename("ticker", "Ticker").unwrap().clone()
        .rename("date", "Date").unwrap().clone()
        .rename("open", "Open").unwrap().clone()
        .rename("high", "High").unwrap().clone()
        .rename("low", "Low").unwrap().clone()
        .rename("close", "Close").unwrap().clone()
        .rename("volume", "Volume").unwrap().clone()
        .lazy()
        .with_column(col("Date")
            .str()
            .strptime(DataType::Date, StrptimeOptions {
                format: Some("%Y-%m-%d %H:%M:%S".into()),
                use_earliest: Some(false),
                strict: false,
                exact: true,
                cache: true,
            })
            .alias("Date"));
                
    // let mut file = File::create("output2.parquet").expect("could not create file");
    // ParquetWriter::new(&mut file).finish(&mut df.clone().collect().unwrap());
    df
        

}

pub fn demo_data() -> LazyFrame {
    // let path: String = "../backtester/output2.parquet".to_string();
    // let data2 = LazyFrame::scan_parquet(path, ScanArgsParquet::default()).unwrap();
    // println!("output2: {:?}", data2.clone().collect());

    let path: String = "../backtester/history.parquet".to_string();
    let data = LazyFrame::scan_parquet(path, ScanArgsParquet::default()).unwrap();
    let n = data.clone().select([count()]).collect().unwrap();
    
    let count = n.column("count")
             .unwrap()
             .get(0)
             .unwrap()
             .extract::<i64>()
             .unwrap() as usize;    

    let t = Series::new("Ticker", vec!["US000000048595"; count]);
    let df = data
        .with_column(t.lit());
    df
        
}

#[derive(Debug)]
pub struct Backtest {
    pub ticker: String,
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
    pub trades: i32
}

pub struct BuySell {
    pub buy: Vec<i32>,
    pub sell: Vec<i32>
}

pub fn backtest_performance(df: DataFrame, side: BuySell, strategy: &str) -> Backtest {

    let df = df.clone();
    let len = df.height();
    
    let mut long_result = vec![0.0; len];
    let mut short_result = vec![0.0; len];
    
    let open = df.column("Open").unwrap().f64().unwrap(); 

    // Variable holding period
    for i in 0..len {
        if side.buy[i] == 1 {
            for a in i+1..cmp::min(i+1000, len) {
                if side.buy[a] == 1 || side.sell[a] == -1 {
                    long_result[a] = open.get(a).unwrap() - open.get(i).unwrap();
                    break
                }
            }
        }
    }            
    for i in 0..len {
        if side.sell[i] == -1 {
            for a in i+1..cmp::min(i+1000, len) {
                if side.buy[a] == 1 || side.sell[a] == -1 {
                    short_result[a] = open.get(i).unwrap() - open.get(a).unwrap();
                    break
                }
            }
        }
    }   

    // Aggregating the long & short results into one column
    let total_result: Vec<f64> = long_result.iter().zip(short_result.iter()).map(|(&l, &s)| l + s).collect();
    // let trades = buy.iter().sum::<i32>() + sell.iter().sum::<i32>().abs();

    // Profit factor   
    let total_net_profits: Vec<f64> = total_result.clone().into_iter().filter(|&x| x > 0.0).collect();
    let total_net_losses: Vec<f64> = total_result.clone().into_iter().filter(|&x| x < 0.0).collect();
    let sum_total_net_profits = total_net_profits.iter().sum::<f64>();
    let sum_total_net_losses = total_net_losses.iter().sum::<f64>().abs();
    let profit_factor = sum_total_net_profits / sum_total_net_losses;

    // Hit ratio    
    let hit_ratio: f64 = (total_net_profits.len() as f64 / (total_net_losses.len() + total_net_profits.len()) as f64) * 100.0;

    // Risk reward ratio
    let average_gain = sum_total_net_profits / total_net_profits.len() as f64;
    let average_loss = sum_total_net_losses / total_net_losses.len() as f64;
    let realized_risk_reward = average_gain / average_loss;

    let trades = total_result.clone().into_iter().filter(|&x| x != 0.0).collect::<Vec<_>>().len().try_into().unwrap();
        
    // Expectancy
    let expectancy  = (average_gain * hit_ratio) - ((1. - hit_ratio) * average_loss);

    // let name = "test_signal".to_string();
    let max_gain = total_net_profits.into_iter().max_by(|a, b| a.partial_cmp(b).unwrap());
    let max_loss = total_net_losses.into_iter().min_by(|a, b| a.partial_cmp(b).unwrap());
    
    let buys = side.buy.iter().sum::<i32>();
    let sells = side.sell.iter().sum::<i32>().abs();

    Backtest {
        ticker: df.column("Ticker").unwrap().get(0).unwrap().to_string(),
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
        trades: trades }

}

pub fn showbt(bt: Backtest) {

    println!("");
    println!("Ticker:           {}", bt.ticker);
    println!("Strategy:         {}", bt.strategy);
    println!("Profit Factor:    {:.2}", bt.profit_factor);
    println!("Hit Ratio:        {:.2}", bt.hit_ratio);
    println!("Expectancy:       {:.2}", bt.expectancy);
    println!("Risk-Reward:      {:.2}", bt.realized_risk_reward);
    println!("Avg Gain:         {:.2}", bt.avg_gain);
    println!("Avg Loss:         {:.2}", bt.avg_loss);
    println!("Max Gain:         {:.2}", bt.max_gain);
    println!("Max Loss:         {:.2}", bt.max_loss);
    println!("Buys:             {}", bt.buys);
    println!("Sells:            {}", bt.sells);
    println!("Trades:           {}", bt.trades);
}

pub fn records_to_dataframe(backtests: Vec<Backtest>) -> DataFrame {
    let ticker: Vec<String>  = backtests.iter().map(|r| r.ticker.clone()).collect::<Vec<_>>();
    let strategy = backtests.iter().map(|r| r.strategy.clone()).collect::<Vec<_>>();
    let profit_factor = backtests.iter().map(|r| r.profit_factor).collect::<Vec<_>>();
    let expectancy = backtests.iter().map(|r| r.expectancy).collect::<Vec<_>>();
    let hit_ratio = backtests.iter().map(|r| r.hit_ratio).collect::<Vec<_>>();
    let realized_risk_reward = backtests.iter().map(|r| r.realized_risk_reward).collect::<Vec<_>>();
    let avg_gain = backtests.iter().map(|r| r.avg_gain).collect::<Vec<_>>();
    let avg_loss = backtests.iter().map(|r| r.avg_loss).collect::<Vec<_>>();
    let max_gain = backtests.iter().map(|r| r.max_gain).collect::<Vec<_>>();
    let max_loss = backtests.iter().map(|r| r.max_loss).collect::<Vec<_>>();
    let buys = backtests.iter().map(|r| r.buys).collect::<Vec<_>>();
    let sells = backtests.iter().map(|r| r.sells).collect::<Vec<_>>();
    let trades = backtests.iter().map(|r| r.trades).collect::<Vec<_>>();
    
    let df = DataFrame::new(vec![
        Series::new("ticker", ticker),
        Series::new("strategy", strategy),
        Series::new("profit_factor", profit_factor),
        Series::new("expectancy", expectancy),
        Series::new("hit_ratio", hit_ratio),
        Series::new("realized_risk_reward", realized_risk_reward),
        Series::new("avg_gain", avg_gain),
        Series::new("avg_loss", avg_loss),
        Series::new("max_gain", max_gain),
        Series::new("max_loss", max_loss),
        Series::new("buys", buys),
        Series::new("sells", sells),
        Series::new("trades", trades),
    ]).unwrap();

    df.lazy().fill_nan(0).collect().unwrap()
}

pub fn preprocess(df: LazyFrame) -> DataFrame {

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

    df.clone()
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
        .unwrap()

}

pub fn postprocess(df: DataFrame) -> DataFrame {

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

    let new = df.lazy().with_columns([
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
    ]);
    new.collect().unwrap()

}

pub fn summary_performance(df: DataFrame) -> DataFrame {
    df.lazy()
        // .group_by_stable([col("strategy")])
        .groupby_stable([col("strategy")])
        .agg([
            col("profit_factor").count().alias("N"),
            col("expectancy").mean().alias("expect"),
            col("profit_factor").mean().alias("profit"),
            col("hit_ratio").mean().alias("HR"),
            col("realized_risk_reward").mean().alias("RR"),
            col("avg_gain").mean().alias("avg_gain"),
            col("avg_loss").mean().alias("avg_loss"),
            col("max_gain").mean().alias("max_gain"),
            col("max_loss").mean().alias("max_loss"),
            col("buys").mean().alias("buys"),
            col("sells").mean().alias("sells"),
            col("trades").mean().alias("trades"),
        ])
        // .filter(col("trades").gt(lit(0)))
        .sort("profit", SortOptions {descending: true, nulls_last: true, ..Default::default()})
        .collect()
        .expect("strategy performance")
}

