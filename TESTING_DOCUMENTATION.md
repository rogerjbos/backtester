# Backtester Testing Documentation

## Overview

This document describes the comprehensive test suite created for the backtester codebase before refactoring. The tests ensure that any code restructuring maintains 100% behavioral compatibility.

## Test Execution

### Run All Tests
```bash
cargo test
```

### Run Specific Test Module
```bash
cargo test signal_tests
cargo test universe_tests
cargo test integration_tests
```

### Run Single Test with Output
```bash
cargo test test_signal_execution_basic -- --nocapture
```

### Run Tests in Release Mode (faster)
```bash
cargo test --release
```

## Test Coverage Summary

✅ **34 Tests Passing** across 13 test modules

### Test Modules

| Module | Tests | Coverage |
|--------|-------|----------|
| Signal Tests | 3 | Strategy execution, multiple strategies, parameterized signals |
| Universe Tests | 3 | Universe expansion, type detection, ticker normalization |
| Data Loading Tests | 3 | LazyFrame filtering, unique ticker extraction, date extraction |
| Path Construction Tests | 3 | Data files, output directories, decision directories |
| Mode Handling Tests | 3 | String comparison, folder selection, demo mode detection |
| Strategy Tag Tests | 2 | Testing mode tags, production mode tags |
| Ticker Filtering Tests | 3 | Parsing, case normalization, deduplication |
| Backtest Result Tests | 3 | Backtest struct, Decision struct, BuySell struct |
| Batch Processing Tests | 2 | Batch size calculation, partial batch handling |
| Strategy Filter Tests | 2 | Filter matching, no filter |
| Integration Tests | 2 | End-to-end backtest, multi-ticker processing |
| Summary Tests | 2 | Metric rounding, column selection |
| Error Handling Tests | 2 | Invalid ticker filtering, empty custom list |

## Test Structure

### Unit Tests
Each component is tested in isolation:
- Signal functions return valid BuySell structures
- Path construction produces correct strings
- Mode detection works correctly
- Universe expansion produces expected results

### Integration Tests
End-to-end workflows are tested:
- Complete single-ticker backtest
- Multi-ticker parallel processing
- Data loading and filtering pipeline

### Fixture Data
Tests use minimal but realistic test data:
- `create_test_price_data()`: 5 rows of BTC OHLCV data
- `create_multi_ticker_data()`: Multi-ticker dataset (BTC, ETH)

## Critical Test Cases

### 1. Signal Execution
**Purpose**: Ensure all strategies execute without errors
**Coverage**:
- Basic signal execution (hammer)
- Multiple strategies (hammer, doji, engulfing)
- Parameterized signals with various parameters

### 2. Universe Management
**Purpose**: Verify universe expansion and type detection
**Coverage**:
- Universe abbreviation expansion (SC → SC1-4)
- Stock vs. Crypto universe detection
- Ticker case normalization (lowercase for crypto, uppercase for stocks)

### 3. Data Pipeline
**Purpose**: Validate data loading and filtering
**Coverage**:
- LazyFrame filtering by ticker
- Unique ticker extraction
- Latest date extraction from price data

### 4. Path Construction
**Purpose**: Ensure correct file paths in all modes
**Coverage**:
- Data file paths (demo, testing, production)
- Output directory paths (stock vs. crypto)
- Decision directory paths

### 5. Mode Handling
**Purpose**: Verify mode detection and behavior
**Coverage**:
- Production/testing/demo mode detection
- Folder name selection based on mode
- Demo mode special handling

### 6. Ticker Filtering
**Purpose**: Test custom ticker list handling
**Coverage**:
- Comma-separated ticker parsing
- Case normalization for different universes
- Ticker deduplication (filter already processed)

### 7. Batch Processing
**Purpose**: Validate parallel processing logic
**Coverage**:
- Correct batch size calculation
- Partial batch handling (last batch may be smaller)

## Pre-Refactoring Baseline

### Baseline Test Run (January 30, 2026)
```
Running 34 tests
test result: ok. 34 passed; 0 failed; 0 ignored; 0 measured
Execution time: 0.03s
```

This baseline establishes the expected behavior. After each refactoring phase, tests must pass with identical results.

## Regression Testing Strategy

### After Each Refactoring Phase
1. **Run full test suite**: `cargo test`
2. **Verify all 34 tests pass**: No failures allowed
3. **Compare execution time**: Should not increase significantly
4. **Manual verification** (for critical phases):
   - Run backtests on sample data
   - Compare output files with pre-refactoring versions
   - Verify performance metrics are identical

### Integration Testing Checklist
After completing refactoring phases, run these manual tests:

```bash
# 1. Test basic crypto backtest
cargo run -- -u Crypto -m demo -v

# 2. Test stock universe
cargo run -- -u MC1 -m demo -v

# 3. Test custom tickers
cargo run -- -u Crypto -m demo -t btc,eth -v

# 4. Test strategy filter
cargo run -- -u Crypto -m demo -s hammer -v

# 5. Test multiple universes
cargo run -- -u SC -m demo -v

# 6. Test production mode (careful!)
cargo run -- -u Crypto -m testing -v
```

### Output Validation
Compare critical outputs before/after refactoring:
- **Backtest metrics**: Hit ratio, Sharpe ratio, profit factor
- **Decision files**: BUY/SELL dates should match exactly
- **Summary files**: Strategy rankings should be identical

## Test Maintenance

### Adding Tests During Refactoring

When creating new modules during refactoring, add corresponding tests:

1. **New config module** → Add config tests
2. **New path module** → Add path construction tests
3. **New strategy config** → Add strategy loading tests

### Test File Organization

Current structure:
```
src/
  tests.rs          # All tests in one file for now
  main.rs           # Main binary with test module reference
  lib.rs            # Library code
```

After refactoring with new modules:
```
src/
  tests/
    config_tests.rs
    strategy_tests.rs
    path_tests.rs
    integration_tests.rs
  config.rs
  strategy_config.rs
  ...
```

## Known Limitations

### What's NOT Tested
1. **ClickHouse database interactions**: Requires live database
2. **File system I/O**: Uses in-memory data structures
3. **Parallel execution timing**: Tests don't verify concurrency
4. **Large dataset performance**: Tests use minimal data

### Future Test Improvements
- [ ] Add property-based testing with `proptest`
- [ ] Add benchmark tests with `criterion`
- [ ] Mock ClickHouse for database tests
- [ ] Add file system mocking for I/O tests
- [ ] Test error recovery paths more thoroughly
- [ ] Add performance regression tests

## Continuous Integration

### Recommended CI Setup
```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - run: cargo test
      - run: cargo test --release
```

## Test-Driven Refactoring Workflow

### Phase-by-Phase Approach

For each refactoring phase:

1. **✅ Run baseline tests**
   ```bash
   cargo test > baseline_output.txt
   ```

2. **🔨 Make changes**
   - Implement refactoring
   - Commit frequently

3. **✅ Run tests after changes**
   ```bash
   cargo test > refactored_output.txt
   ```

4. **📊 Compare results**
   ```bash
   diff baseline_output.txt refactored_output.txt
   ```

5. **🎯 Verify functionality**
   - Run manual integration tests
   - Compare output files
   - Check performance

6. **✅ Commit if passing**
   ```bash
   git add .
   git commit -m "Phase X: Description - all tests passing"
   ```

## Rollback Strategy

If tests fail after refactoring:

1. **Check test output**: `cargo test -- --nocapture`
2. **Run specific failing test**: `cargo test <test_name> -- --nocapture`
3. **Compare with previous commit**: `git diff HEAD~1`
4. **Rollback if needed**: `git reset --hard HEAD~1`
5. **Fix incrementally**: Make smaller changes, test frequently

## Success Criteria

Before considering refactoring complete:

- ✅ All 34 baseline tests pass
- ✅ No new warnings or errors
- ✅ Compilation time not significantly increased
- ✅ Test execution time < 1 second
- ✅ Manual integration tests pass
- ✅ Output files identical to pre-refactoring
- ✅ Code coverage maintained or improved

## Conclusion

This comprehensive test suite provides:
1. **Safety net**: Catch breaking changes immediately
2. **Confidence**: Refactor boldly knowing tests will catch issues
3. **Documentation**: Tests show how code is intended to work
4. **Regression prevention**: Ensure new code doesn't break old functionality

The tests establish a baseline that must be maintained throughout the refactoring process. Any phase that causes test failures should be reconsidered or fixed before proceeding.
