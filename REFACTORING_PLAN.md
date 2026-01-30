# Backtester Refactoring Plan

## Executive Summary

This document outlines a comprehensive refactoring plan for the backtester codebase to improve maintainability, readability, and extensibility. The main goal is to reduce the complexity of the 1400+ line `main.rs` file and separate concerns into logical modules.

## Current State Analysis

### Main Issues

1. **Strategy Configuration Bloat**: 800+ lines of hardcoded strategy lists in `select_backtests()`
2. **Monolithic Main Function**: 230+ line `main()` with complex nested logic
3. **Repeated Path Logic**: Inconsistent file path construction throughout codebase
4. **Mixed Concerns**: Business logic, I/O, and configuration intermingled
5. **Universe Selection**: Hardcoded match statements and repetitive conditional checks

### Code Metrics

- `main.rs`: 1401 lines
- `select_backtests()`: ~800 lines (lines 48-850)
- `backtest_helper()`: 170+ lines (lines 891-1068)
- `main()`: 230+ lines (lines 1072-1305)

## Refactoring Phases

### Phase 1: Strategy Configuration Extraction (HIGH PRIORITY)

**Estimated Effort**: 4-6 hours
**Risk Level**: Medium
**Impact**: High maintainability improvement

#### Goals
- Move all strategy definitions out of `main.rs`
- Create a dedicated `strategy_config.rs` module
- Reduce `select_backtests()` from 800 to ~50 lines
- Make it easy to add/modify strategies per universe

#### Implementation Steps

1. **Create new file**: `src/strategy_config.rs`
2. **Define structure**:
   ```rust
   pub struct StrategyDefinition {
       pub name: &'static str,
       pub function: SignalFunctionWithParam,
       pub param: f64,
   }

   pub fn get_strategies_for_universe(universe: &str) -> Vec<StrategyDefinition>
   ```

3. **Migrate strategy lists**:
   - `prod_signal_functions` → `strategy_config::production_strategies()`
   - `crypto_signal_functions` → `strategy_config::crypto_strategies()`
   - `micro_signal_functions` → `strategy_config::micro_strategies()`
   - `sc_signal_functions` → `strategy_config::small_cap_strategies()`
   - `mc_signal_functions` → `strategy_config::mid_cap_strategies()`
   - `lc_signal_functions` → `strategy_config::large_cap_strategies()`
   - `signal_functions` → `strategy_config::all_strategies()`
   - `param_functions` → `strategy_config::parameterized_strategies()`
   - `testing_functions` → `strategy_config::testing_strategies()`

4. **Update `select_backtests()`**:
   ```rust
   pub async fn select_backtests(
       lf: LazyFrame,
       tag: &str,
       strategy_filter: Option<&str>,
   ) -> Result<Vec<(Backtest, Vec<Decision>)>, Box<dyn StdError>> {
       let strategy_defs = strategy_config::get_strategies_for_universe(tag);

       let signals: Vec<Signal> = strategy_defs
           .into_iter()
           .filter(|def| {
               strategy_filter.map_or(true, |filter| def.name == filter)
           })
           .map(|def| Signal {
               name: def.name.to_string(),
               func: Arc::new(def.function),
               param: def.param,
           })
           .collect();

       run_all_backtests(lf, signals).await
   }
   ```

5. **Testing**: Verify all universe/mode combinations still work

#### Files Modified
- Create: `src/strategy_config.rs`
- Modify: `src/main.rs` (reduce by ~750 lines)
- Modify: `src/lib.rs` (add module declaration)

#### Success Criteria
- ✅ All strategy definitions in dedicated module
- ✅ `select_backtests()` reduced to < 100 lines
- ✅ Existing tests pass
- ✅ No behavioral changes

---

### Phase 2: Execution Mode & Configuration (HIGH PRIORITY)

**Estimated Effort**: 3-4 hours
**Risk Level**: Low
**Impact**: High code clarity

#### Goals
- Replace string-based mode handling with type-safe enum
- Create configuration struct to reduce parameter passing
- Improve type safety and reduce string comparisons

#### Implementation Steps

1. **Create `src/config.rs`**:
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq)]
   pub enum ExecutionMode {
       Production,
       Testing,
       Demo,
   }

   impl ExecutionMode {
       pub fn from_str(s: &str) -> Result<Self, String> {
           match s {
               "production" => Ok(Self::Production),
               "testing" => Ok(Self::Testing),
               "demo" => Ok(Self::Demo),
               _ => Err(format!("Unknown mode: {}", s)),
           }
       }

       pub fn folder_name(&self) -> &'static str {
           match self {
               Self::Production => "production",
               Self::Testing => "testing",
               Self::Demo => "",
           }
       }

       pub fn is_production(&self) -> bool {
           matches!(self, Self::Production)
       }
   }

   pub struct BacktestConfig {
       pub path: String,
       pub universes: Vec<String>,
       pub mode: ExecutionMode,
       pub batch_size: usize,
       pub custom_tickers: Option<Vec<String>>,
       pub strategy_filter: Option<String>,
   }

   impl BacktestConfig {
       pub fn from_args(args: Args) -> Result<Self, Box<dyn StdError>> {
           // ... implementation
       }
   }
   ```

2. **Update function signatures**:
   - Replace `bool production` with `ExecutionMode mode`
   - Replace `bool demo_mode` with `ExecutionMode mode`
   - Pass `&BacktestConfig` instead of individual parameters

3. **Replace string comparisons**:
   - `mode_str == "production"` → `config.mode.is_production()`
   - `mode_str == "demo"` → `config.mode == ExecutionMode::Demo`

#### Files Modified
- Create: `src/config.rs`
- Modify: `src/main.rs`
- Modify: `src/lib.rs`

#### Success Criteria
- ✅ No string-based mode comparisons
- ✅ Type-safe configuration
- ✅ Reduced parameter passing
- ✅ Existing tests pass

---

### Phase 3: Path Management (MEDIUM PRIORITY)

**Estimated Effort**: 2-3 hours
**Risk Level**: Low
**Impact**: Medium code clarity

#### Goals
- Centralize all path construction logic
- Eliminate repeated path building code
- Ensure consistent path handling across modules

#### Implementation Steps

1. **Add to `src/config.rs`**:
   ```rust
   pub struct PathConfig {
       pub base: String,
   }

   impl PathConfig {
       pub fn new(base: String) -> Self {
           Self { base }
       }

       pub fn data_file(&self, universe: &str, mode: ExecutionMode) -> String {
           match mode {
               ExecutionMode::Demo => format!("{}.csv", universe),
               _ => format!("{}/data/{}/{}.csv", self.base, mode.folder_name(), universe),
           }
       }

       pub fn output_dir(&self, universe: &str, mode: ExecutionMode) -> String {
           let output_type = if universe == "Crypto" { "output_crypto" } else { "output" };
           format!("{}/{}/{}", self.base, output_type, mode.folder_name())
       }

       pub fn decision_dir(&self, is_crypto: bool, mode: ExecutionMode) -> String {
           let asset_type = if is_crypto { "crypto" } else { "stocks" };
           format!("{}/decisions/{}", self.base, asset_type)
       }

       pub fn data_dir(&self, mode: ExecutionMode) -> String {
           format!("{}/data/{}", self.base, mode.folder_name())
       }

       pub fn summary_file(&self, is_crypto: bool, mode: ExecutionMode) -> String {
           let output_type = if is_crypto { "output_crypto" } else { "output" };
           format!("{}/{}/{}/summary_performance.csv", self.base, output_type, mode.folder_name())
       }
   }
   ```

2. **Update `BacktestConfig`**:
   ```rust
   pub struct BacktestConfig {
       pub paths: PathConfig,
       pub universes: Vec<String>,
       pub mode: ExecutionMode,
       // ... rest
   }
   ```

3. **Replace all path construction** throughout codebase

#### Files Modified
- Modify: `src/config.rs`
- Modify: `src/main.rs`
- Modify: `src/lib.rs`

#### Success Criteria
- ✅ All paths constructed via `PathConfig`
- ✅ No inline path string formatting
- ✅ Consistent path handling
- ✅ Existing tests pass

---

### Phase 4: Universe Management (MEDIUM PRIORITY)

**Estimated Effort**: 2-3 hours
**Risk Level**: Low
**Impact**: Medium code clarity

#### Goals
- Centralize universe definitions
- Simplify universe expansion logic
- Remove hardcoded universe checks

#### Implementation Steps

1. **Add to `src/config.rs`**:
   ```rust
   pub struct UniverseConfig;

   impl UniverseConfig {
       const DEFINITIONS: &'static [(&'static str, &'static [&'static str])] = &[
           ("SC", &["SC1", "SC2", "SC3", "SC4"]),
           ("MC", &["MC1", "MC2"]),
           ("LC", &["LC1", "LC2"]),
           ("Micro", &["Micro1", "Micro2", "Micro3", "Micro4"]),
           ("Stocks", &["SC1", "SC2", "SC3", "SC4", "MC1", "MC2", "LC1", "LC2",
                        "Micro1", "Micro2", "Micro3", "Micro4"]),
           ("Crypto", &["Crypto"]),
       ];

       pub fn expand(name: &str) -> Vec<String> {
           Self::DEFINITIONS.iter()
               .find(|(n, _)| *n == name)
               .map(|(_, universes)| universes.iter().map(|s| s.to_string()).collect())
               .unwrap_or_else(|| vec![name.to_string()])
       }

       pub fn is_stock(universe: &str) -> bool {
           matches!(universe, "LC1" | "LC2" | "MC1" | "MC2" | "SC1" | "SC2" |
                               "SC3" | "SC4" | "Micro1" | "Micro2" | "Micro3" | "Micro4")
       }

       pub fn is_crypto(universe: &str) -> bool {
           universe == "Crypto"
       }

       pub fn any_stock(universes: &[String]) -> bool {
           universes.iter().any(|u| Self::is_stock(u))
       }

       pub fn any_crypto(universes: &[String]) -> bool {
           universes.iter().any(|u| Self::is_crypto(u))
       }
   }
   ```

2. **Replace universe handling**:
   - Lines 1113-1127: Replace match with `UniverseConfig::expand()`
   - Lines 1228-1244: Replace with `UniverseConfig::any_stock()`

3. **Normalize ticker casing**:
   ```rust
   impl UniverseConfig {
       pub fn normalize_ticker(ticker: &str, universe: &str) -> String {
           if Self::is_crypto(universe) {
               ticker.to_lowercase()
           } else {
               ticker.to_uppercase()
           }
       }
   }
   ```

#### Files Modified
- Modify: `src/config.rs`
- Modify: `src/main.rs`

#### Success Criteria
- ✅ No hardcoded universe lists in main
- ✅ Consistent universe handling
- ✅ Type-safe universe checks
- ✅ Existing tests pass

---

### Phase 5: Simplify Main Function (HIGH PRIORITY)

**Estimated Effort**: 3-4 hours
**Risk Level**: Medium
**Impact**: High code clarity

#### Goals
- Reduce `main()` from 230+ lines to < 50 lines
- Extract workflow logic into focused functions
- Improve readability and testability

#### Implementation Steps

1. **Create workflow functions**:
   ```rust
   async fn setup_environment(args: &Args) -> BacktestConfig {
       setup_logging(args.verbose);
       BacktestConfig::from_args(args.clone())
           .expect("Failed to parse configuration")
   }

   async fn run_backtests(config: &BacktestConfig) -> Result<(), Box<dyn StdError>> {
       if config.mode != ExecutionMode::Demo {
           download_price_data(config).await?;
       }

       for universe in &config.universes {
           run_universe_backtest(config, universe).await?;
       }

       Ok(())
   }

   async fn run_universe_backtest(
       config: &BacktestConfig,
       universe: &str
   ) -> Result<(), Box<dyn StdError>> {
       info!("Backtest starting: {} (mode: {:?})", universe, config.mode);

       let custom_tickers = normalize_custom_tickers(
           config.custom_tickers.clone(),
           universe
       );

       backtest_helper(
           config.paths.base.clone(),
           universe,
           config.batch_size,
           config.mode,
           custom_tickers,
           config.strategy_filter.as_deref(),
       ).await
   }

   async fn generate_summaries(
       config: &BacktestConfig
   ) -> Result<(), Box<dyn StdError>> {
       if config.mode.is_production() {
           generate_production_summaries(config).await?;
       } else if config.mode != ExecutionMode::Demo {
           generate_testing_summaries(config).await?;
       }
       Ok(())
   }
   ```

2. **Simplify main**:
   ```rust
   #[tokio::main]
   async fn main() -> Result<(), Box<dyn StdError>> {
       let args = Args::parse();
       let config = setup_environment(&args).await;

       run_backtests(&config).await?;
       generate_summaries(&config).await?;

       info!("Backtest processing complete");
       Ok(())
   }
   ```

3. **Extract summary logic** (lines 1217-1301):
   ```rust
   async fn generate_production_summaries(
       config: &BacktestConfig
   ) -> Result<(), Box<dyn StdError>> {
       if UniverseConfig::any_crypto(&config.universes) {
           let (datetag, _) = summary_performance_file(
               config.paths.base.clone(),
               config.mode,
               false,
               config.universes.clone()
           ).await?;
           score(&datetag, "Crypto").await?;
       }

       if UniverseConfig::any_stock(&config.universes) {
           let (datetag, _) = summary_performance_file(
               config.paths.base.clone(),
               config.mode,
               true,
               config.universes.clone()
           ).await?;
           score(&datetag, &config.universes[0]).await?;
       }

       Ok(())
   }

   async fn generate_testing_summaries(
       config: &BacktestConfig
   ) -> Result<(), Box<dyn StdError>> {
       let is_stocks = !UniverseConfig::any_crypto(&config.universes);

       let (datetag, mut out) = summary_performance_file(
           config.paths.base.clone(),
           config.mode,
           is_stocks,
           config.universes.clone()
       ).await?;

       save_summary_csv(&config.paths, &mut out, is_stocks)?;
       display_summary_results(out, &datetag)?;

       Ok(())
   }
   ```

#### Files Modified
- Modify: `src/main.rs` (reduce by ~150 lines)

#### Success Criteria
- ✅ `main()` < 50 lines
- ✅ Clear separation of workflow steps
- ✅ Improved testability
- ✅ Existing tests pass

---

### Phase 6: Refactor Backtest Helper (MEDIUM PRIORITY)

**Estimated Effort**: 4-5 hours
**Risk Level**: Medium
**Impact**: Medium code clarity

#### Goals
- Split `backtest_helper()` into focused functions
- Separate concerns: data loading, filtering, execution
- Improve testability

#### Implementation Steps

1. **Extract data loading**:
   ```rust
   async fn load_price_data(
       paths: &PathConfig,
       universe: &str,
       mode: ExecutionMode
   ) -> Result<(LazyFrame, String), Box<dyn StdError>> {
       let file_path = paths.data_file(universe, mode);
       let lf = read_price_file(file_path).await?;

       let latest_date = get_latest_date(&lf).await?;
       info!("Price file loaded for {} - latest date: {}", universe, latest_date);

       Ok((lf, latest_date))
   }
   ```

2. **Extract ticker determination**:
   ```rust
   fn determine_tickers_to_process(
       lf: &LazyFrame,
       paths: &PathConfig,
       universe: &str,
       mode: ExecutionMode,
       custom_tickers: Option<Vec<String>>
   ) -> Result<Vec<String>, Box<dyn StdError>> {
       if let Some(tickers) = custom_tickers {
           return Ok(tickers);
       }

       let all_tickers = extract_unique_tickers(lf)?;
       let processed = load_processed_tickers(paths, universe, mode)?;

       Ok(all_tickers.into_iter()
           .filter(|t| !processed.contains(t))
           .collect())
   }

   fn load_processed_tickers(
       paths: &PathConfig,
       universe: &str,
       mode: ExecutionMode
   ) -> Result<HashSet<String>, Box<dyn StdError>> {
       let dir_path = paths.output_dir(universe, mode);

       let mut processed = HashSet::new();
       for entry in fs::read_dir(dir_path)? {
           let entry = entry?;
           let path = entry.path();
           if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
               if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                   processed.insert(stem.to_owned());
               }
           }
       }

       Ok(processed)
   }
   ```

3. **Simplify backtest_helper**:
   ```rust
   async fn backtest_helper(
       path: String,
       universe: &str,
       batch_size: usize,
       mode: ExecutionMode,
       custom_tickers: Option<Vec<String>>,
       strategy_filter: Option<&str>,
   ) -> Result<(), Box<dyn StdError>> {
       let paths = PathConfig::new(path);
       let (lf, _) = load_price_data(&paths, universe, mode).await?;

       let tickers = determine_tickers_to_process(
           &lf,
           &paths,
           universe,
           mode,
           custom_tickers
       )?;

       run_parallel_backtests(
           lf,
           tickers,
           universe,
           batch_size,
           mode,
           strategy_filter,
           &paths
       ).await?;

       Ok(())
   }
   ```

#### Files Modified
- Modify: `src/main.rs`

#### Success Criteria
- ✅ `backtest_helper()` < 50 lines
- ✅ Each function has single responsibility
- ✅ Improved testability
- ✅ Existing tests pass

---

### Phase 7: Display Logic Extraction (LOW PRIORITY)

**Estimated Effort**: 1-2 hours
**Risk Level**: Low
**Impact**: Low code clarity

#### Goals
- Extract display/formatting logic
- Reduce coupling between business logic and presentation

#### Implementation Steps

1. **Create `src/display.rs`**:
   ```rust
   use polars::prelude::*;

   pub fn display_summary_results(
       df: DataFrame,
       datetag: &str
   ) -> Result<(), PolarsError> {
       let rounded = round_numeric_columns(df)?;

       std::env::set_var("POLARS_FMT_MAX_COLS", "12");

       display_first_section(&rounded, datetag)?;
       display_second_section(&rounded, datetag)?;

       Ok(())
   }

   fn round_numeric_columns(df: DataFrame) -> Result<DataFrame, PolarsError> {
       df.lazy()
           .select([
               col("strategy"),
               col("universe"),
               cols([
                   "hit_ratio", "risk_reward", "avg_gain", "avg_loss",
                   "max_gain", "max_loss", "buys", "sells", "trades",
                   "sharpe_ratio", "sortino_ratio", "max_drawdown",
                   "calmar_ratio", "win_loss_ratio", "recovery_factor",
                   "profit_per_trade", "expectancy", "profit_factor"
               ]).round(1),
               col("N")
           ])
           .collect()
   }

   fn display_first_section(
       df: &DataFrame,
       datetag: &str
   ) -> Result<(), PolarsError> {
       println!("{} Average Performance by Strategy:", datetag);
       let cols: Vec<&str> = df.get_column_names()[0..12].to_vec();
       let section = df.select(cols)?;
       println!("{}", section);
       Ok(())
   }

   fn display_second_section(
       df: &DataFrame,
       datetag: &str
   ) -> Result<(), PolarsError> {
       println!("\n{} Average Performance by Strategy (continued):", datetag);
       let col_names = df.get_column_names();
       let cols: Vec<&str> = col_names[0..2].iter()
           .chain(col_names[12..].iter())
           .copied()
           .collect();
       let section = df.select(cols)?;
       println!("{}", section);
       Ok(())
   }
   ```

2. **Update imports in main.rs**

#### Files Modified
- Create: `src/display.rs`
- Modify: `src/main.rs`
- Modify: `src/lib.rs`

#### Success Criteria
- ✅ Display logic separated
- ✅ Main logic cleaner
- ✅ Existing tests pass

---

## Implementation Order

### Week 1
1. **Phase 1**: Strategy Configuration Extraction (Days 1-2)
2. **Phase 2**: Execution Mode & Configuration (Days 3-4)
3. **Testing & Integration** (Day 5)

### Week 2
1. **Phase 3**: Path Management (Days 1-2)
2. **Phase 4**: Universe Management (Day 3)
3. **Phase 5**: Simplify Main Function (Days 4-5)

### Week 3
1. **Phase 6**: Refactor Backtest Helper (Days 1-3)
2. **Phase 7**: Display Logic Extraction (Day 4)
3. **Final Testing & Documentation** (Day 5)

## Testing Strategy

### Per-Phase Testing
- Run existing test suite after each phase
- Verify no behavioral changes
- Test all universe/mode combinations manually

### Integration Testing
```bash
# Test all universes
cargo run -- -u Crypto -m testing -v
cargo run -- -u MC1 -m testing -v
cargo run -- -u SC -m testing -v
cargo run -- -u LC -m testing -v
cargo run -- -u Micro -m testing -v

# Test custom tickers
cargo run -- -u Crypto -m testing -t btc,eth -v

# Test strategy filtering
cargo run -- -u Crypto -m testing -s hammer -v

# Test demo mode
cargo run -- -u MC1 -m demo -v
```

### Regression Testing
- Compare output files before/after refactoring
- Verify summary statistics match
- Check decision files are identical

## Risk Mitigation

### Backup Strategy
- Create `refactor_2` branch (already done ✅)
- Commit after each phase
- Tag working versions

### Rollback Plan
- Each phase is independently testable
- Can rollback individual phases if needed
- Keep original implementation commented for reference

### Code Review Checkpoints
- After Phase 1 (biggest change)
- After Phase 5 (main function)
- Before merging to main

## Success Metrics

### Code Quality
- **Line Count**: Reduce `main.rs` from 1401 to < 700 lines
- **Function Size**: All functions < 100 lines
- **Cyclomatic Complexity**: Reduce average complexity by 30%

### Maintainability
- **Strategy Addition**: < 5 minutes to add new strategy
- **Universe Addition**: < 10 minutes to add new universe
- **Bug Fix Time**: Reduce average time by 40%

### Performance
- **No Degradation**: Maintain current execution speed
- **Compilation**: No significant increase in compile time

## Post-Refactoring

### Documentation Updates
- [ ] Update README.md with new architecture
- [ ] Update BACKTESTER_COMPREHENSIVE_DOCS.md
- [ ] Add inline documentation for new modules
- [ ] Create architecture diagram

### Future Improvements
- [ ] Add integration tests for new modules
- [ ] Consider extracting signal modules
- [ ] Add configuration file support (TOML/YAML)
- [ ] Implement plugin system for strategies

## Notes

- This refactoring maintains 100% backward compatibility
- No changes to external interfaces (CLI, output files)
- Focus on internal code organization only
- All phases are independently testable and committable
