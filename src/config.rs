// Configuration module for backtester
// Contains execution mode enum and configuration structs

use std::{env, error::Error as StdError};
use chrono::Local;

/// Execution mode for the backtester
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Production,
    Testing,
    Demo,
}

impl ExecutionMode {
    /// Create ExecutionMode from string
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "production" => Ok(Self::Production),
            "testing" => Ok(Self::Testing),
            "demo" => Ok(Self::Demo),
            _ => Err(format!("Unknown mode: {}", s)),
        }
    }

    /// Get the folder name for this mode (with date suffix for testing)
    pub fn folder_name(&self) -> String {
        match self {
            Self::Production => "production".to_string(),
            Self::Testing => {
                let date_suffix = Local::now().format("%Y%m%d").to_string();
                format!("testing_{}", date_suffix)
            },
            Self::Demo => String::new(),
        }
    }

    /// Get the base folder name without date suffix (for data directories)
    pub fn base_folder_name(&self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Testing => "testing",
            Self::Demo => "",
        }
    }

    /// Check if this is production mode
    pub fn is_production(&self) -> bool {
        matches!(self, Self::Production)
    }

    /// Check if this is testing mode
    pub fn is_testing(&self) -> bool {
        matches!(self, Self::Testing)
    }

    /// Check if this is demo mode
    pub fn is_demo(&self) -> bool {
        matches!(self, Self::Demo)
    }
}

/// Path configuration for consistent file/directory access
#[derive(Debug, Clone)]
pub struct PathConfig {
    pub base: String,
    /// Optional date suffix override for testing mode (e.g., "20260204" -> "testing_20260204")
    pub output_suffix: Option<String>,
}

impl PathConfig {
    /// Create new PathConfig with base directory
    pub fn new(base: String, output_suffix: Option<String>) -> Self {
        Self { base, output_suffix }
    }

    /// Get the folder name for a mode, using the output_suffix override if available
    fn get_folder_name(&self, mode: ExecutionMode) -> String {
        if mode.is_testing() {
            if let Some(ref suffix) = self.output_suffix {
                return format!("testing_{}", suffix);
            }
        }
        mode.folder_name()
    }

    /// Get data file path for a universe
    pub fn data_file(&self, universe: &str, mode: ExecutionMode) -> String {
        if mode.is_demo() {
            format!("{}.csv", universe)
        } else {
            format!("{}/data/{}/{}.csv", self.base, mode.base_folder_name(), universe)
        }
    }

    /// Get output directory for a universe
    pub fn output_dir(&self, universe: &str, mode: ExecutionMode) -> String {
        let output_type = UniverseConfig::output_folder_type(universe);
        format!("{}/{}/{}", self.base, output_type, self.get_folder_name(mode))
    }

    /// Get output file path for a ticker
    pub fn output_file(&self, universe: &str, ticker: &str, mode: ExecutionMode) -> String {
        let folder = self.get_folder_name(mode);
        let output_type = UniverseConfig::output_folder_type(universe);
        format!("{}/{}/{}/{}.csv", self.base, output_type, folder, ticker)
    }

    /// Get decision directory path
    pub fn decision_dir(&self, is_crypto: bool) -> String {
        let asset_type = if is_crypto { "crypto" } else { "stocks" };
        format!("{}/decisions/{}", self.base, asset_type)
    }

    /// Get decision directory path by universe
    pub fn decision_dir_for_universe(&self, universe: &str) -> String {
        let asset_type = UniverseConfig::asset_type_tag(universe);
        format!("{}/decisions/{}", self.base, asset_type)
    }

    /// Get decision file path for a ticker
    pub fn decision_file(&self, universe: &str, ticker: &str) -> String {
        let asset_type = UniverseConfig::asset_type_tag(universe);
        format!("{}/decisions/{}/{}.csv", self.base, asset_type, ticker)
    }

    /// Get data directory path
    pub fn data_dir(&self, mode: ExecutionMode) -> String {
        format!("{}/data/{}", self.base, mode.base_folder_name())
    }

    /// Get performance file path
    pub fn performance_file(&self, tag: &str, datetag: &str, is_production: bool) -> String {
        if is_production {
            format!("{}/performance/{}_all_{}.csv", self.base, tag, datetag)
        } else {
            format!("{}/performance/{}_testing.csv", self.base, tag)
        }
    }

    /// Get buy/sell performance file path
    pub fn buys_file(&self, tag: &str, datetag: &str) -> String {
        format!("{}/performance/{}_buys_{}.csv", self.base, tag, datetag)
    }

    pub fn sells_file(&self, tag: &str, datetag: &str) -> String {
        format!("{}/performance/{}_sells_{}.csv", self.base, tag, datetag)
    }

    /// Get score file path
    pub fn score_file(&self, file_tag: &str, datetag: &str) -> String {
        format!("{}/score/{}_{}.csv", self.base, file_tag, datetag)
    }

    /// Get final testing file path (uses universe label like SC, MC, Crypto)
    pub fn final_testing_file(&self, label: &str) -> String {
        format!("{}/final_testing/{}_testing.csv", self.base, label)
    }
}

/// Main configuration for backtester execution
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub paths: PathConfig,
    pub universes: Vec<String>,
    /// Original universe argument (e.g., "SC", "Crypto", "Stocks")
    pub universe_label: String,
    pub mode: ExecutionMode,
    pub batch_size: usize,
    pub custom_tickers: Option<Vec<String>>,
    pub strategy_filter: Option<String>,
}

impl BacktestConfig {
    /// Create configuration from command-line arguments
    pub fn new(
        path_arg: Option<String>,
        universe: String,
        mode_str: String,
        tickers: Option<String>,
        strategy: Option<String>,
        output_suffix: Option<String>,
    ) -> Result<Self, Box<dyn StdError>> {
        // Get path from args or environment
        let user_path = env::var("CLICKHOUSE_USER_PATH")
            .unwrap_or_else(|_| String::from("/srv"));
        let default_path = format!("{}/rust_home/backtester", user_path);
        let path = path_arg.unwrap_or(default_path);

        // Parse execution mode
        let mode = ExecutionMode::from_str(&mode_str)
            .map_err(|e| format!("Invalid mode: {}", e))?;

        // Expand universe
        let universes = expand_universe(&universe);

        // Parse custom tickers if provided
        let custom_tickers = tickers.map(|t| {
            t.split(',')
                .map(|s| UniverseConfig::normalize_ticker(s.trim(), &universe))
                .collect()
        });

        Ok(Self {
            paths: PathConfig::new(path, output_suffix),
            universes,
            universe_label: universe,
            mode,
            batch_size: 10,
            custom_tickers,
            strategy_filter: strategy,
        })
    }
}

/// Universe configuration and management
pub struct UniverseConfig;

impl UniverseConfig {
    /// Universe definitions mapping shorthand to full list
    const DEFINITIONS: &'static [(&'static str, &'static [&'static str])] = &[
        ("SC", &["SC1", "SC2", "SC3", "SC4"]),
        ("MC", &["MC1", "MC2"]),
        ("LC", &["LC1", "LC2"]),
        ("Micro", &["Micro1", "Micro2", "Micro3", "Micro4"]),
        ("Stocks", &[
            "SC1", "SC2", "SC3", "SC4", "MC1", "MC2", "LC1", "LC2",
            "Micro1", "Micro2", "Micro3", "Micro4",
        ]),
        ("Crypto", &["Crypto"]),
    ];

    /// Expand universe shorthand to full list
    pub fn expand(name: &str) -> Vec<String> {
        Self::DEFINITIONS
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, universes)| universes.iter().map(|s| s.to_string()).collect())
            .unwrap_or_else(|| vec![name.to_string()])
    }

    /// Check if a universe is a stock universe
    pub fn is_stock(universe: &str) -> bool {
        matches!(
            universe,
            "LC1" | "LC2" | "MC1" | "MC2" | "SC1" | "SC2" | "SC3" | "SC4"
                | "Micro1" | "Micro2" | "Micro3" | "Micro4"
        )
    }

    /// Check if a universe is crypto
    pub fn is_crypto(universe: &str) -> bool {
        universe == "Crypto"
    }

    /// Check if any universe in the list is a stock universe
    pub fn any_stock(universes: &[String]) -> bool {
        universes.iter().any(|u| Self::is_stock(u))
    }

    /// Check if any universe in the list is crypto
    pub fn any_crypto(universes: &[String]) -> bool {
        universes.iter().any(|u| Self::is_crypto(u))
    }

    /// Normalize ticker case based on universe type
    /// Crypto tickers are lowercase, stock tickers are uppercase
    pub fn normalize_ticker(ticker: &str, universe: &str) -> String {
        if Self::is_crypto(universe) {
            ticker.to_lowercase()
        } else {
            ticker.to_uppercase()
        }
    }

    /// Get the output folder type for a universe
    pub fn output_folder_type(universe: &str) -> &'static str {
        if Self::is_crypto(universe) {
            "output_crypto"
        } else {
            "output"
        }
    }

    /// Get the asset type tag for a universe
    pub fn asset_type_tag(universe: &str) -> &'static str {
        if Self::is_crypto(universe) {
            "crypto"
        } else {
            "stocks"
        }
    }
}

/// Expand universe shorthand to full list (legacy function for backward compatibility)
fn expand_universe(universe: &str) -> Vec<String> {
    UniverseConfig::expand(universe)
}

/// Check if a universe is a stock universe (legacy function)
pub fn is_stock_universe(universe: &str) -> bool {
    UniverseConfig::is_stock(universe)
}

/// Check if a universe is crypto (legacy function)
pub fn is_crypto_universe(universe: &str) -> bool {
    UniverseConfig::is_crypto(universe)
}

/// Check if any universe in the list is a stock universe (legacy function)
pub fn any_stock_universe(universes: &[String]) -> bool {
    UniverseConfig::any_stock(universes)
}

/// Check if any universe in the list is crypto (legacy function)
pub fn any_crypto_universe(universes: &[String]) -> bool {
    UniverseConfig::any_crypto(universes)
}
