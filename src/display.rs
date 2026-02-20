/// Display and formatting utilities for backtester output
use chrono::Utc;

/// Format progress message for completed backtest
pub fn format_backtest_progress(
    universe: &str,
    ticker: &str,
    completed: usize,
    total: usize,
) -> String {
    format!(
        "[{}] Finished {} '{}' backtests: {} of {}",
        Utc::now().format("%H:%M:%S"),
        universe,
        ticker,
        completed,
        total
    )
}

/// Format error message for backtest save failure
pub fn format_save_error(error: &dyn std::error::Error) -> String {
    format!(
        "Error saving backtests (check output and decisions folders): {}",
        error
    )
}

/// Format error message for backtest execution failure
pub fn format_execution_error(ticker: &str, error: &dyn std::error::Error) -> String {
    format!("Error running '{}' backtests: {}", ticker, error)
}

/// Format error message for score insertion failure
pub fn format_score_error(error: &dyn std::error::Error) -> String {
    format!("Error inserting scores: {}", error)
}

/// Format message for price file loading
pub fn format_price_loaded(universe: &str, latest_date: &str) -> String {
    format!(
        "Price file loaded for {} - latest date: {}",
        universe, latest_date
    )
}

/// Format message for single backtest result
pub fn format_single_backtest_result(signal_name: &str) -> String {
    format!("Backtest result for signal '{}':", signal_name)
}

/// Format message for single backtest result with sizing
pub fn format_sized_backtest_result(signal_name: &str) -> String {
    format!("Backtest (Sized) result for signal '{}':", signal_name)
}
