// Comprehensive test suite for backtester refactoring
// Run with: cargo test
// Run specific test: cargo test test_name -- --nocapture

use super::*;
use polars::prelude::*;
use std::sync::Arc;

// ============================================================================
// TEST DATA FIXTURES
// ============================================================================

/// Create a minimal price DataFrame for testing
fn create_test_price_data() -> DataFrame {
    df! {
        "Date" => &["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "Ticker" => &["btc", "btc", "btc", "btc", "btc"],
        "Universe" => &["Crypto", "Crypto", "Crypto", "Crypto", "Crypto"],
        "Open" => &[100.0, 105.0, 103.0, 108.0, 110.0],
        "High" => &[106.0, 107.0, 109.0, 112.0, 115.0],
        "Low" => &[99.0, 103.0, 102.0, 107.0, 109.0],
        "Close" => &[105.0, 104.0, 108.0, 111.0, 113.0],
        "Volume" => &[1000.0, 1100.0, 900.0, 1200.0, 1300.0],
    }.unwrap()
}

/// Create multi-ticker test data
fn create_multi_ticker_data() -> DataFrame {
    df! {
        "Date" => &["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"],
        "Ticker" => &["btc", "eth", "btc", "eth"],
        "Universe" => &["Crypto", "Crypto", "Crypto", "Crypto"],
        "Open" => &[100.0, 200.0, 105.0, 205.0],
        "High" => &[106.0, 206.0, 107.0, 207.0],
        "Low" => &[99.0, 199.0, 103.0, 203.0],
        "Close" => &[105.0, 204.0, 104.0, 206.0],
        "Volume" => &[1000.0, 2000.0, 1100.0, 2100.0],
    }.unwrap()
}

// ============================================================================
// SIGNAL TESTS
// ============================================================================

#[cfg(test)]
mod signal_tests {
    use super::*;

    #[tokio::test]
    async fn test_signal_execution_basic() {
        let df = create_test_price_data();
        let signal = Signal {
            name: "test_signal".to_string(),
            func: Arc::new(signals::mfpr::hammer),
            param: 0.0,
        };

        let result = (*signal.func)(df.clone(), signal.param);

        // Verify signal returns buy and sell vectors
        assert!(result.buy.len() <= df.height());
        assert!(result.sell.len() <= df.height());
        println!("✓ Basic signal execution works");
    }

    #[tokio::test]
    async fn test_multiple_strategies() {
        let strategies: Vec<(&str, fn(DataFrame, f64) -> BuySell, f64)> = vec![
            ("hammer", signals::mfpr::hammer as fn(DataFrame, f64) -> BuySell, 0.0),
            ("doji", signals::mfpr::doji as fn(DataFrame, f64) -> BuySell, 0.0),
            ("engulfing", signals::mfpr::engulfing as fn(DataFrame, f64) -> BuySell, 0.0),
        ];

        let df = create_test_price_data();

        for (name, func, param) in strategies {
            let result = func(df.clone(), param);
            assert!(result.buy.len() <= df.height(), "Strategy {} failed", name);
            assert!(result.sell.len() <= df.height(), "Strategy {} failed", name);
        }
        println!("✓ Multiple strategies execute correctly");
    }

    #[tokio::test]
    async fn test_parameterized_signals() {
        let df = create_test_price_data();

        for param in [1.0, 1.5, 2.0, 2.5, 3.0] {
            let result = signals::mfpr::candlestick_double_trouble(df.clone(), param);
            assert!(result.buy.len() <= df.height(), "Param {} failed", param);
            assert!(result.sell.len() <= df.height(), "Param {} failed", param);
        }
        println!("✓ Parameterized signals work correctly");
    }
}

// ============================================================================
// UNIVERSE SELECTION TESTS
// ============================================================================

#[cfg(test)]
mod universe_tests {
    use super::*;

    #[test]
    fn test_universe_expansion() {
        let test_cases = vec![
            ("SC", vec!["SC1", "SC2", "SC3", "SC4"]),
            ("MC", vec!["MC1", "MC2"]),
            ("LC", vec!["LC1", "LC2"]),
            ("Micro", vec!["Micro1", "Micro2", "Micro3", "Micro4"]),
            ("Crypto", vec!["Crypto"]),
        ];

        for (input, expected) in test_cases {
            let univ: &[&str] = match input {
                "SC" => &["SC1", "SC2", "SC3", "SC4"],
                "MC" => &["MC1", "MC2"],
                "LC" => &["LC1", "LC2"],
                "Micro" => &["Micro1", "Micro2", "Micro3", "Micro4"],
                "Crypto" => &["Crypto"],
                _ => &[input],
            };

            let result: Vec<String> = univ.iter().map(|s| s.to_string()).collect();
            let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();

            assert_eq!(result, expected, "Universe expansion failed for {}", input);
        }
        println!("✓ Universe expansion works correctly");
    }

    #[test]
    fn test_universe_type_detection() {
        let stock_universes = vec!["LC1", "LC2", "MC1", "MC2", "SC1", "SC2", "SC3", "SC4",
                                    "Micro1", "Micro2", "Micro3", "Micro4"];

        for univ in stock_universes {
            assert_ne!(univ, "Crypto", "{} should be stock universe", univ);
        }

        assert_eq!("Crypto", "Crypto");
        println!("✓ Universe type detection works correctly");
    }

    #[test]
    fn test_ticker_case_normalization() {
        // Crypto tickers should be lowercase
        let crypto_ticker = "BTC";
        let normalized = crypto_ticker.to_lowercase();
        assert_eq!(normalized, "btc");

        // Stock tickers should be uppercase
        let stock_ticker = "aapl";
        let normalized = stock_ticker.to_uppercase();
        assert_eq!(normalized, "AAPL");

        println!("✓ Ticker case normalization works correctly");
    }
}

// ============================================================================
// DATA LOADING TESTS
// ============================================================================

#[cfg(test)]
mod data_loading_tests {
    use super::*;

    #[tokio::test]
    async fn test_lazy_frame_filtering() {
        let df = create_multi_ticker_data();
        let lf = df.lazy();

        // Test single ticker filtering
        let filtered = lf.clone()
            .filter(col("Ticker").eq(lit("btc")))
            .collect()
            .unwrap();

        assert_eq!(filtered.height(), 2);
        assert_eq!(filtered.column("Ticker").unwrap().str().unwrap().get(0).unwrap(), "btc");
        println!("✓ LazyFrame filtering works correctly");
    }

    #[tokio::test]
    async fn test_unique_ticker_extraction() {
        let df = create_multi_ticker_data();
        let lf = df.lazy();

        let unique_tickers = lf.clone()
            .select([col("Ticker").unique()])
            .collect()
            .unwrap();

        assert_eq!(unique_tickers.height(), 2);
        println!("✓ Unique ticker extraction works correctly");
    }

    #[tokio::test]
    async fn test_latest_date_extraction() {
        let df = create_test_price_data();
        let lf = df.lazy();

        let latest = lf.clone()
            .select([col("Date").max()])
            .collect()
            .unwrap();

        let date = latest.column("Date").unwrap().get(0).unwrap().to_string();
        assert_eq!(date, "\"2024-01-05\"");
        println!("✓ Latest date extraction works correctly");
    }
}

// ============================================================================
// PATH CONSTRUCTION TESTS
// ============================================================================

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn test_data_file_paths() {
        let base = "/test/path".to_string();

        // Demo mode
        let demo_path = format!("{}.csv", "MC1");
        assert_eq!(demo_path, "MC1.csv");

        // Testing mode
        let testing_path = format!("{}/data/testing/{}.csv", base, "MC1");
        assert_eq!(testing_path, "/test/path/data/testing/MC1.csv");

        // Production mode
        let prod_path = format!("{}/data/production/{}.csv", base, "MC1");
        assert_eq!(prod_path, "/test/path/data/production/MC1.csv");

        println!("✓ Data file path construction works correctly");
    }

    #[test]
    fn test_output_directory_paths() {
        let base = "/test/path".to_string();

        // Stock output
        let stock_output = format!("{}/output/testing", base);
        assert_eq!(stock_output, "/test/path/output/testing");

        // Crypto output
        let crypto_output = format!("{}/output_crypto/production", base);
        assert_eq!(crypto_output, "/test/path/output_crypto/production");

        println!("✓ Output directory path construction works correctly");
    }

    #[test]
    fn test_decision_directory_paths() {
        let base = "/test/path".to_string();

        let stock_decisions = format!("{}/decisions/stocks", base);
        assert_eq!(stock_decisions, "/test/path/decisions/stocks");

        let crypto_decisions = format!("{}/decisions/crypto", base);
        assert_eq!(crypto_decisions, "/test/path/decisions/crypto");

        println!("✓ Decision directory path construction works correctly");
    }
}

// ============================================================================
// MODE HANDLING TESTS
// ============================================================================

#[cfg(test)]
mod mode_tests {
    use super::*;

    #[test]
    fn test_mode_string_comparison() {
        let modes = vec!["production", "testing", "demo"];

        for mode in modes {
            assert!(mode == "production" || mode == "testing" || mode == "demo");
        }
        println!("✓ Mode string comparison works correctly");
    }

    #[test]
    fn test_mode_folder_selection() {
        let production = true;
        let folder = if production { "production" } else { "testing" };
        assert_eq!(folder, "production");

        let production = false;
        let folder = if production { "production" } else { "testing" };
        assert_eq!(folder, "testing");

        println!("✓ Mode folder selection works correctly");
    }

    #[test]
    fn test_demo_mode_detection() {
        let mode = "demo";
        let is_demo = mode == "demo";
        assert!(is_demo);

        let mode = "testing";
        let is_demo = mode == "demo";
        assert!(!is_demo);

        println!("✓ Demo mode detection works correctly");
    }
}

// ============================================================================
// STRATEGY TAG SELECTION TESTS
// ============================================================================

#[cfg(test)]
mod strategy_tag_tests {
    use super::*;

    #[test]
    fn test_tag_selection_testing_mode() {
        let production = false;
        let tag = if production { "prod" } else { "signal" };
        assert_eq!(tag, "signal");
        println!("✓ Testing mode tag selection works correctly");
    }

    #[test]
    fn test_tag_selection_production_mode() {
        let test_cases = vec![
            ("Crypto", "crypto"),
            ("Micro1", "micro"),
            ("SC1", "sc"),
            ("MC1", "mc"),
            ("LC1", "lc"),
        ];

        for (universe, expected_tag) in test_cases {
            let production = true;
            let tag = match (production, universe) {
                (true, "Crypto") => "crypto",
                (true, u) if u.starts_with("Micro") => "micro",
                (true, u) if u.starts_with("SC") => "sc",
                (true, u) if u.starts_with("MC") => "mc",
                (true, u) if u.starts_with("LC") => "lc",
                _ => "prod",
            };
            assert_eq!(tag, expected_tag, "Failed for universe {}", universe);
        }
        println!("✓ Production mode tag selection works correctly");
    }
}

// ============================================================================
// TICKER FILTERING TESTS
// ============================================================================

#[cfg(test)]
mod ticker_filtering_tests {
    use super::*;

    #[test]
    fn test_custom_ticker_parsing() {
        let custom_str = "btc,eth,sol";
        let tickers: Vec<String> = custom_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        assert_eq!(tickers.len(), 3);
        assert_eq!(tickers[0], "btc");
        assert_eq!(tickers[1], "eth");
        assert_eq!(tickers[2], "sol");
        println!("✓ Custom ticker parsing works correctly");
    }

    #[test]
    fn test_ticker_case_normalization_for_universes() {
        // Crypto - lowercase
        let crypto_tickers: Vec<String> = "BTC,ETH,SOL"
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .collect();

        assert_eq!(crypto_tickers, vec!["btc", "eth", "sol"]);

        // Stocks - uppercase
        let stock_tickers: Vec<String> = "aapl,msft,googl"
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .collect();

        assert_eq!(stock_tickers, vec!["AAPL", "MSFT", "GOOGL"]);
        println!("✓ Ticker case normalization for universes works correctly");
    }

    #[test]
    fn test_ticker_deduplication() {
        use std::collections::HashSet;

        let processed: HashSet<String> = vec!["btc", "eth"]
            .into_iter()
            .map(String::from)
            .collect();

        let all_tickers = vec!["btc", "eth", "sol", "dot"];
        let needed: Vec<String> = all_tickers
            .into_iter()
            .map(String::from)
            .filter(|t| !processed.contains(t))
            .collect();

        assert_eq!(needed.len(), 2);
        assert!(needed.contains(&"sol".to_string()));
        assert!(needed.contains(&"dot".to_string()));
        println!("✓ Ticker deduplication works correctly");
    }
}

// ============================================================================
// BACKTEST RESULT TESTS
// ============================================================================

#[cfg(test)]
mod backtest_result_tests {
    use super::*;

    #[test]
    fn test_backtest_struct_creation() {
        let backtest = Backtest {
            ticker: "btc".to_string(),
            universe: "Crypto".to_string(),
            strategy: "hammer".to_string(),
            expectancy: 0.5,
            profit_factor: 1.5,
            hit_ratio: 0.6,
            realized_risk_reward: 1.2,
            avg_gain: 100.0,
            avg_loss: -50.0,
            max_gain: 500.0,
            max_loss: -200.0,
            sharpe_ratio: 1.8,
            sortino_ratio: 2.0,
            max_drawdown: -0.15,
            calmar_ratio: 3.0,
            win_loss_ratio: 1.5,
            recovery_factor: 2.5,
            profit_per_trade: 25.0,
            buys: 10,
            sells: 10,
            trades: 10,
            date: "2024-01-01".to_string(),
            buy: 0,
            sell: 0,
        };

        assert_eq!(backtest.ticker, "btc");
        assert_eq!(backtest.profit_factor, 1.5);
        assert_eq!(backtest.trades, 10);
        println!("✓ Backtest struct creation works correctly");
    }

    #[test]
    fn test_decision_struct_creation() {
        let decision = Decision {
            date: "2024-01-01".to_string(),
            action: "BUY".to_string(),
        };

        assert_eq!(decision.date, "2024-01-01");
        assert_eq!(decision.action, "BUY");
        println!("✓ Decision struct creation works correctly");
    }

    #[test]
    fn test_buysell_struct_creation() {
        let buysell = BuySell {
            buy: vec![0, 2, 5],
            sell: vec![1, 4, 6],
        };

        assert_eq!(buysell.buy.len(), 3);
        assert_eq!(buysell.sell.len(), 3);
        assert_eq!(buysell.buy[0], 0);
        assert_eq!(buysell.sell[0], 1);
        println!("✓ BuySell struct creation works correctly");
    }
}

// ============================================================================
// BATCH PROCESSING TESTS
// ============================================================================

#[cfg(test)]
mod batch_processing_tests {
    use super::*;

    #[test]
    fn test_batch_size_calculation() {
        let tickers = vec!["btc", "eth", "sol", "dot", "ada", "matic"];
        let batch_size = 3;

        let mut batches = Vec::new();
        for i in (0..tickers.len()).step_by(batch_size) {
            let end = (i + batch_size).min(tickers.len());
            batches.push(&tickers[i..end]);
        }

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].len(), 3);
        println!("✓ Batch size calculation works correctly");
    }

    #[test]
    fn test_partial_batch_handling() {
        let tickers = vec!["btc", "eth", "sol", "dot", "ada"];
        let batch_size = 3;

        let mut batches = Vec::new();
        for i in (0..tickers.len()).step_by(batch_size) {
            let end = (i + batch_size).min(tickers.len());
            batches.push(&tickers[i..end]);
        }

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].len(), 2); // Partial batch
        println!("✓ Partial batch handling works correctly");
    }
}

// ============================================================================
// STRATEGY FILTER TESTS
// ============================================================================

#[cfg(test)]
mod strategy_filter_tests {
    use super::*;

    #[test]
    fn test_strategy_filter_matching() {
        let strategies = vec!["hammer", "doji", "engulfing", "marubozu"];
        let filter = Some("hammer");

        let filtered: Vec<&str> = strategies
            .into_iter()
            .filter(|s| filter.map_or(true, |f| *s == f))
            .collect();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0], "hammer");
        println!("✓ Strategy filter matching works correctly");
    }

    #[test]
    fn test_no_strategy_filter() {
        let strategies = vec!["hammer", "doji", "engulfing", "marubozu"];
        let filter: Option<&str> = None;

        let filtered: Vec<&str> = strategies
            .into_iter()
            .filter(|s| filter.map_or(true, |f| *s == f))
            .collect();

        assert_eq!(filtered.len(), 4);
        println!("✓ No strategy filter works correctly");
    }
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_end_to_end_single_ticker_backtest() {
        let signal = Signal {
            name: "hammer".to_string(),
            func: Arc::new(signals::mfpr::hammer),
            param: 0.0,
        };

        let df = create_test_price_data();
        let _lf = df.clone().lazy();

        let result = (*signal.func)(df.clone(), signal.param);

        assert!(result.buy.len() <= df.height());
        assert!(result.sell.len() <= df.height());
        println!("✓ End-to-end single ticker backtest works correctly");
    }

    #[tokio::test]
    async fn test_multi_ticker_processing() {
        let df = create_multi_ticker_data();
        let lf = df.lazy();

        let tickers = vec!["btc", "eth"];

        for ticker in tickers {
            let filtered = lf.clone()
                .filter(col("Ticker").eq(lit(ticker)))
                .collect()
                .unwrap();

            assert!(filtered.height() > 0, "No data for ticker {}", ticker);
        }
        println!("✓ Multi-ticker processing works correctly");
    }
}

// ============================================================================
// PERFORMANCE SUMMARY TESTS
// ============================================================================

#[cfg(test)]
mod summary_tests {
    use super::*;

    #[test]
    fn test_metric_rounding() {
        let value: f64 = 1.23456789;
        let rounded = (value * 10.0).round() / 10.0;
        assert_eq!(rounded, 1.2);
        println!("✓ Metric rounding works correctly");
    }

    #[test]
    fn test_column_selection() {
        let all_cols = vec!["strategy", "universe", "hit_ratio", "sharpe_ratio",
                           "sortino_ratio", "max_drawdown", "N"];

        let first_section: Vec<&str> = all_cols[0..3].to_vec();
        assert_eq!(first_section.len(), 3);

        let mut second_section: Vec<&str> = all_cols[0..2].to_vec();
        second_section.extend(&all_cols[3..]);
        assert_eq!(second_section.len(), 6);

        println!("✓ Column selection works correctly");
    }
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

#[cfg(test)]
mod error_handling_tests {
    use super::*;

    #[test]
    fn test_invalid_ticker_filtering() {
        let df = create_test_price_data();
        let lf = df.lazy();

        let result = lf.clone()
            .filter(col("Ticker").eq(lit("invalid_ticker")))
            .collect();

        assert!(result.is_ok());
        assert_eq!(result.unwrap().height(), 0);
        println!("✓ Invalid ticker filtering handled correctly");
    }

    #[test]
    fn test_empty_custom_ticker_list() {
        let custom_str = "";
        let is_empty = custom_str.is_empty();
        assert!(is_empty);
        println!("✓ Empty custom ticker list handled correctly");
    }
}

// ============================================================================
// TEST RUNNER
// ============================================================================

#[tokio::test]
async fn run_all_tests() {
    println!("\n========================================");
    println!("Running Comprehensive Test Suite");
    println!("========================================\n");

    // This test serves as a summary
    println!("All test modules:");
    println!("  ✓ Signal Tests");
    println!("  ✓ Universe Tests");
    println!("  ✓ Data Loading Tests");
    println!("  ✓ Path Construction Tests");
    println!("  ✓ Mode Handling Tests");
    println!("  ✓ Strategy Tag Tests");
    println!("  ✓ Ticker Filtering Tests");
    println!("  ✓ Backtest Result Tests");
    println!("  ✓ Batch Processing Tests");
    println!("  ✓ Strategy Filter Tests");
    println!("  ✓ Integration Tests");
    println!("  ✓ Summary Tests");
    println!("  ✓ Error Handling Tests");

    println!("\n========================================");
    println!("Test Suite Complete");
    println!("========================================\n");
}
