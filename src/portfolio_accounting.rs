//! Portfolio Accounting System
//!
//! A standalone accounting library for tracking portfolio cash flows, positions,
//! and performance with flexible position sizing and per-transaction commissions.

use chrono::NaiveDate;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fs::File;

/// Configuration for portfolio accounting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingConfig {
    // Removed target_portfolio_size, commission_per_trade,
    // rebalance_frequency, and rebalance_threshold_pct
    // as these are no longer needed in the simplified version
}

/// Transaction type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum TransactionType {
    Buy,
    Sell,
}

/// Position in the portfolio
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub ticker: String,
    pub shares: f64,
    pub avg_cost_basis: f64,
    pub total_cost: f64,
    pub current_price: f64,
    pub current_value: f64,
    pub unrealized_pnl: f64,
    pub unrealized_pnl_pct: f64,
    pub entry_date: NaiveDate,
    pub last_update_date: NaiveDate,
}

/// Transaction record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub transaction_id: usize,
    pub date: NaiveDate,
    pub ticker: String,
    pub action: TransactionType,
    pub shares: f64,
    pub price: f64,
    pub gross_amount: f64,
    pub commission: f64,
    pub net_amount: f64,
    pub cash_impact: f64,
}

/// Daily portfolio snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailySnapshot {
    pub date: NaiveDate,
    pub cash_balance: f64,
    pub equity_value: f64,
    pub total_value: f64,
    pub position_count: usize,
    // Removed available_slots as we no longer track target portfolio size
    pub total_unrealized_pnl: f64,
    pub total_realized_pnl_to_date: f64,
    pub daily_return_pct: f64,
}

/// Cash flow event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CashFlow {
    pub date: NaiveDate,
    pub description: String,
    pub amount: f64,
    pub cash_balance_after: f64,
}

/// Realized P&L from closed position
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealizedPnL {
    pub close_date: NaiveDate,
    pub ticker: String,
    pub entry_date: NaiveDate,
    pub entry_price: f64,
    pub exit_price: f64,
    pub shares: f64,
    pub gross_proceeds: f64,
    pub total_cost: f64,
    pub sell_commission: f64,
    pub net_pnl: f64,
    pub pnl_pct: f64,
    pub holding_days: i64,
}

/// Unrealized P&L for open position
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnrealizedPnL {
    pub as_of_date: NaiveDate,
    pub ticker: String,
    pub shares: f64,
    pub cost_basis: f64,
    pub current_price: f64,
    pub current_value: f64,
    pub unrealized_pnl: f64,
    pub unrealized_pnl_pct: f64,
}

/// Performance summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSummary {
    pub initial_value: f64,
    pub final_value: f64,
    pub total_return_pct: f64,
    pub cagr: f64,
    pub total_realized_pnl: f64,
    pub total_unrealized_pnl: f64,
    pub total_commissions: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate_pct: f64,
    pub avg_win_pct: f64,
    pub avg_loss_pct: f64,
    pub profit_factor: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub avg_holding_days: f64,
    pub max_holding_days: i64,
    pub min_holding_days: i64,
}

/// Main portfolio accounting system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioAccounting {
    pub config: AccountingConfig,
    pub cash_balance: f64,
    pub initial_cash: f64,
    pub positions: HashMap<String, Position>,
    pub transactions: Vec<Transaction>,
    pub daily_snapshots: Vec<DailySnapshot>,
    pub cash_flows: Vec<CashFlow>,
    pub realized_pnl: Vec<RealizedPnL>,
    // Removed last_rebalance_date as we no longer track rebalancing
}

impl PortfolioAccounting {
    /// Create a new portfolio accounting instance
    pub fn new(initial_cash: f64) -> Self {
        let config = AccountingConfig {};
        Self {
            config,
            cash_balance: initial_cash,
            initial_cash,
            positions: HashMap::new(),
            transactions: Vec::new(),
            daily_snapshots: Vec::new(),
            cash_flows: Vec::new(),
            realized_pnl: Vec::new(),
        }
    }

    // ============================================================================
    // Trade Execution
    // ============================================================================

    /// Execute a buy order with specified shares and commission
    pub fn execute_buy(
        &mut self,
        date: NaiveDate,
        ticker: &str,
        shares: f64,
        price: f64,
        commission: f64,
    ) -> Result<Transaction, String> {
        if shares <= 0.0 {
            return Err("Shares must be positive".to_string());
        }

        if price <= 0.0 {
            return Err("Price must be positive".to_string());
        }

        if commission < 0.0 {
            return Err("Commission cannot be negative".to_string());
        }

        let gross_amount = shares * price;
        let net_amount = gross_amount + commission;

        if net_amount > self.cash_balance {
            return Err(format!(
                "Insufficient cash: need ${:.2}, have ${:.2}",
                net_amount, self.cash_balance
            ));
        }

        // Update cash
        self.cash_balance -= net_amount;

        // Update or create position
        self.update_position_on_buy(ticker, shares, price, commission, date);

        // Record transaction
        let transaction = Transaction {
            transaction_id: self.transactions.len(),
            date,
            ticker: ticker.to_string(),
            action: TransactionType::Buy,
            shares,
            price,
            gross_amount,
            commission,
            net_amount,
            cash_impact: -net_amount,
        };

        self.transactions.push(transaction.clone());

        // Record cash flow
        self.record_cash_flow(
            date,
            format!("Buy {} shares of {} @ ${:.2}", shares, ticker, price),
            -net_amount,
        );

        Ok(transaction)
    }

    /// Execute a sell order (full position)
    pub fn execute_sell(
        &mut self,
        date: NaiveDate,
        ticker: &str,
        price: f64,
        commission: f64,
    ) -> Result<Transaction, String> {
        let position = self
            .positions
            .get(ticker)
            .ok_or_else(|| format!("No position in {}", ticker))?
            .clone();

        self.execute_sell_shares(date, ticker, position.shares, price, commission)
    }

    /// Execute a sell order for specific number of shares
    pub fn execute_sell_shares(
        &mut self,
        date: NaiveDate,
        ticker: &str,
        shares: f64,
        price: f64,
        commission: f64,
    ) -> Result<Transaction, String> {
        if shares <= 0.0 {
            return Err("Shares must be positive".to_string());
        }

        if price <= 0.0 {
            return Err("Price must be positive".to_string());
        }

        if commission < 0.0 {
            return Err("Commission cannot be negative".to_string());
        }

        let position = self
            .positions
            .get(ticker)
            .ok_or_else(|| format!("No position in {}", ticker))?
            .clone();

        if shares > position.shares {
            return Err(format!(
                "Cannot sell {} shares of {}, only have {}",
                shares, ticker, position.shares
            ));
        }

        let gross_amount = shares * price;
        let net_amount = gross_amount - commission;

        // Update cash
        self.cash_balance += net_amount;

        // Calculate realized P&L
        let proportion = shares / position.shares;
        let cost_for_shares = position.total_cost * proportion;

        let realized_pnl = RealizedPnL {
            close_date: date,
            ticker: ticker.to_string(),
            entry_date: position.entry_date,
            entry_price: position.avg_cost_basis,
            exit_price: price,
            shares,
            gross_proceeds: gross_amount,
            total_cost: cost_for_shares,
            sell_commission: commission,
            net_pnl: net_amount - cost_for_shares,
            pnl_pct: ((net_amount / cost_for_shares) - 1.0) * 100.0,
            holding_days: (date - position.entry_date).num_days(),
        };

        self.realized_pnl.push(realized_pnl);

        // Update or remove position
        if shares >= position.shares {
            // Selling entire position
            self.positions.remove(ticker);
        } else {
            // Partial sell - update position
            if let Some(pos) = self.positions.get_mut(ticker) {
                pos.shares -= shares;
                pos.total_cost -= cost_for_shares;
                pos.current_value = pos.shares * price;
                pos.unrealized_pnl = pos.current_value - pos.total_cost;
                pos.unrealized_pnl_pct = (pos.unrealized_pnl / pos.total_cost) * 100.0;
                pos.last_update_date = date;
            }
        }

        // Record transaction
        let transaction = Transaction {
            transaction_id: self.transactions.len(),
            date,
            ticker: ticker.to_string(),
            action: TransactionType::Sell,
            shares,
            price,
            gross_amount,
            commission,
            net_amount,
            cash_impact: net_amount,
        };

        self.transactions.push(transaction.clone());

        // Record cash flow
        self.record_cash_flow(
            date,
            format!("Sell {} shares of {} @ ${:.4}", shares, ticker, price),
            net_amount,
        );

        Ok(transaction)
    }

    // ============================================================================
    // Position Management
    // ============================================================================

    fn update_position_on_buy(
        &mut self,
        ticker: &str,
        shares: f64,
        price: f64,
        commission: f64,
        date: NaiveDate,
    ) {
        let entry = self.positions.entry(ticker.to_string());
        entry
            .and_modify(|pos| {
                // Add to existing position (average cost)
                let old_cost = pos.total_cost;
                let new_cost = shares * price + commission;
                pos.shares += shares;
                pos.total_cost = old_cost + new_cost;
                pos.avg_cost_basis = pos.total_cost / pos.shares;
                pos.current_price = price;
                pos.current_value = pos.shares * price;
                pos.unrealized_pnl = pos.current_value - pos.total_cost;
                pos.unrealized_pnl_pct = (pos.unrealized_pnl / pos.total_cost) * 100.0;
                pos.last_update_date = date;
            })
            .or_insert_with(|| Position {
                ticker: ticker.to_string(),
                shares,
                avg_cost_basis: (shares * price + commission) / shares,
                total_cost: shares * price + commission,
                current_price: price,
                current_value: shares * price,
                unrealized_pnl: -commission,
                unrealized_pnl_pct: -(commission / (shares * price)) * 100.0,
                entry_date: date,
                last_update_date: date,
            });
    }

    /// Update position values with current prices (mark-to-market)
    pub fn mark_to_market(&mut self, date: NaiveDate, prices: &HashMap<String, f64>) {
        for (ticker, position) in self.positions.iter_mut() {
            if let Some(&price) = prices.get(ticker) {
                position.current_price = price;
                position.current_value = position.shares * price;
                position.unrealized_pnl = position.current_value - position.total_cost;
                position.unrealized_pnl_pct = (position.unrealized_pnl / position.total_cost) * 100.0;
                position.last_update_date = date;
            }
        }
    }

    // ============================================================================
    // Daily Snapshot
    // ============================================================================

    /// Take daily snapshot of portfolio state
    pub fn take_daily_snapshot(&mut self, date: NaiveDate) {
        let equity_value = self.get_equity_value();
        let total_value = self.cash_balance + equity_value;
        let position_count = self.positions.len();
        // Sort by ticker before summing for deterministic floating-point result
        let mut pnl_values: Vec<(&String, f64)> = self.positions
            .iter()
            .map(|(k, p)| (k, p.unrealized_pnl))
            .collect();
        pnl_values.sort_by(|a, b| a.0.cmp(b.0));
        let total_unrealized_pnl: f64 = pnl_values.iter().map(|(_, v)| v).sum();
        let total_realized_pnl_to_date: f64 = self.realized_pnl.iter().map(|p| p.net_pnl).sum();

        let daily_return_pct = if let Some(prev) = self.daily_snapshots.last() {
            ((total_value / prev.total_value) - 1.0) * 100.0
        } else {
            ((total_value / self.initial_cash) - 1.0) * 100.0
        };

        let snapshot = DailySnapshot {
            date,
            cash_balance: self.cash_balance,
            equity_value,
            total_value,
            position_count,
            total_unrealized_pnl,
            total_realized_pnl_to_date,
            daily_return_pct,
        };

        self.daily_snapshots.push(snapshot);
    }

    // ============================================================================
    // Cash Flow Tracking
    // ============================================================================

    fn record_cash_flow(&mut self, date: NaiveDate, description: String, amount: f64) {
        self.cash_flows.push(CashFlow {
            date,
            description,
            amount,
            cash_balance_after: self.cash_balance,
        });
    }

    // ============================================================================
    // Query Methods
    // ============================================================================

    pub fn get_cash_balance(&self) -> f64 {
        self.cash_balance
    }

    pub fn get_equity_value(&self) -> f64 {
        // Sort by ticker before summing to ensure deterministic floating-point result
        let mut values: Vec<(&String, f64)> = self.positions
            .iter()
            .map(|(k, p)| (k, p.current_value))
            .collect();
        values.sort_by(|a, b| a.0.cmp(b.0));
        values.iter().map(|(_, v)| v).sum()
    }

    pub fn get_total_value(&self) -> f64 {
        self.cash_balance + self.get_equity_value()
    }

    pub fn get_position_count(&self) -> usize {
        self.positions.len()
    }

    pub fn has_position(&self, ticker: &str) -> bool {
        self.positions.contains_key(ticker)
    }

    pub fn get_position(&self, ticker: &str) -> Option<&Position> {
        self.positions.get(ticker)
    }

    pub fn get_all_positions(&self) -> Vec<&Position> {
        // Sort by ticker for deterministic ordering
        let mut positions: Vec<&Position> = self.positions.values().collect();
        positions.sort_by(|a, b| a.ticker.cmp(&b.ticker));
        positions
    }

    pub fn get_position_weight(&self, ticker: &str) -> Option<f64> {
        let position = self.positions.get(ticker)?;
        let total_value = self.get_total_value();

        if total_value > 0.0 {
            Some(position.current_value / total_value)
        } else {
            None
        }
    }

    pub fn get_all_position_weights(&self) -> HashMap<String, f64> {
        let total_value = self.get_total_value();
        let mut weights = HashMap::new();

        if total_value > 0.0 {
            for (ticker, position) in &self.positions {
                weights.insert(
                    ticker.clone(),
                    position.current_value / total_value
                );
            }
        }

        weights
    }

    pub fn get_unrealized_pnl(&self) -> Vec<UnrealizedPnL> {
        let mut result: Vec<UnrealizedPnL> = self.positions
            .values()
            .map(|p| UnrealizedPnL {
                as_of_date: p.last_update_date,
                ticker: p.ticker.clone(),
                shares: p.shares,
                cost_basis: p.avg_cost_basis,
                current_price: p.current_price,
                current_value: p.current_value,
                unrealized_pnl: p.unrealized_pnl,
                unrealized_pnl_pct: p.unrealized_pnl_pct,
            })
            .collect();
        result.sort_by(|a, b| a.ticker.cmp(&b.ticker));
        result
    }

    // ============================================================================
    // Performance Metrics
    // ============================================================================

    pub fn calculate_performance_summary(&self) -> PerformanceSummary {
        let final_value = self.get_total_value();
        let total_return_pct = ((final_value / self.initial_cash) - 1.0) * 100.0;

        // Calculate CAGR (Compound Annual Growth Rate)
        let cagr = self.calculate_cagr();

        let total_realized_pnl: f64 = self.realized_pnl.iter().map(|p| p.net_pnl).sum();
        // Sort by ticker before summing for deterministic floating-point result
        let mut pnl_values: Vec<(&String, f64)> = self.positions
            .iter()
            .map(|(k, p)| (k, p.unrealized_pnl))
            .collect();
        pnl_values.sort_by(|a, b| a.0.cmp(b.0));
        let total_unrealized_pnl: f64 = pnl_values.iter().map(|(_, v)| v).sum();
        let total_commissions: f64 = self
            .transactions
            .iter()
            .map(|t| t.commission)
            .sum();
        let total_trades = self.realized_pnl.len();
        let winning_trades = self.realized_pnl.iter().filter(|p| p.net_pnl > 0.0).count();
        let losing_trades = self.realized_pnl.iter().filter(|p| p.net_pnl < 0.0).count();
        let win_rate_pct = if total_trades > 0 {
            (winning_trades as f64 / total_trades as f64) * 100.0
        } else {
            0.0
        };

        let winning_pnls: Vec<f64> = self
            .realized_pnl
            .iter()
            .filter(|p| p.net_pnl > 0.0)
            .map(|p| p.pnl_pct)
            .collect();
        let losing_pnls: Vec<f64> = self
            .realized_pnl
            .iter()
            .filter(|p| p.net_pnl < 0.0)
            .map(|p| p.pnl_pct)
            .collect();

        let avg_win_pct = if !winning_pnls.is_empty() {
            winning_pnls.iter().sum::<f64>() / winning_pnls.len() as f64
        } else {
            0.0
        };

        let avg_loss_pct = if !losing_pnls.is_empty() {
            losing_pnls.iter().sum::<f64>() / losing_pnls.len() as f64
        } else {
            0.0
        };

        let profit_factor = if avg_loss_pct != 0.0 && !losing_pnls.is_empty() {
            (avg_win_pct * winning_pnls.len() as f64).abs() / (avg_loss_pct.abs() * losing_pnls.len() as f64)
        } else if winning_pnls.is_empty() {
            0.0
        } else {
            f64::INFINITY
        };

        let max_drawdown_pct = self.calculate_max_drawdown();
        let sharpe_ratio = self.calculate_sharpe_ratio();

        let holding_days: Vec<i64> = self.realized_pnl.iter().map(|p| p.holding_days).collect();
        let avg_holding_days = if !holding_days.is_empty() {
            holding_days.iter().sum::<i64>() as f64 / holding_days.len() as f64
        } else {
            0.0
        };

        let max_holding_days = holding_days.iter().max().copied().unwrap_or(0);
        let min_holding_days = holding_days.iter().min().copied().unwrap_or(0);

        PerformanceSummary {
            initial_value: self.initial_cash,
            final_value,
            total_return_pct,
            cagr,
            total_realized_pnl,
            total_unrealized_pnl,
            total_commissions,
            total_trades,
            winning_trades,
            losing_trades,
            win_rate_pct,
            avg_win_pct,
            avg_loss_pct,
            profit_factor,
            max_drawdown_pct,
            sharpe_ratio,
            avg_holding_days,
            max_holding_days,
            min_holding_days,
        }
    }

    fn calculate_cagr(&self) -> f64 {
        // Need at least 2 snapshots to calculate CAGR meaningfully
        if self.daily_snapshots.len() < 2 {
            return 0.0;
        }

        let first_snapshot = &self.daily_snapshots[0];
        let last_snapshot = &self.daily_snapshots[self.daily_snapshots.len() - 1];

        let initial_value = first_snapshot.total_value;
        let final_value = last_snapshot.total_value;

        // Calculate years as a fraction
        let days = (last_snapshot.date - first_snapshot.date).num_days();
        let years = days as f64 / 365.25;

        // Avoid division by zero and negative values
        if years <= 0.0 || initial_value <= 0.0 {
            return 0.0;
        }

        // CAGR = (Ending Value / Beginning Value)^(1 / Years) - 1
        let cagr = ((final_value / initial_value).powf(1.0 / years) - 1.0) * 100.0;

        cagr
    }

    fn calculate_max_drawdown(&self) -> f64 {
        let mut peak = self.initial_cash;
        let mut max_dd = 0.0;

        for snapshot in &self.daily_snapshots {
            if snapshot.total_value > peak {
                peak = snapshot.total_value;
            }

            let dd = ((peak - snapshot.total_value) / peak) * 100.0;
            if dd > max_dd {
                max_dd = dd;
            }
        }

        max_dd
    }

    fn calculate_sharpe_ratio(&self) -> f64 {
        if self.daily_snapshots.len() <= 1 {
            return 0.0;
        }

        let mut daily_returns = Vec::new();
        for i in 1..self.daily_snapshots.len() {
            let prev = self.daily_snapshots[i - 1].total_value;
            let curr = self.daily_snapshots[i].total_value;
            let ret = (curr / prev - 1.0) * 100.0;
            daily_returns.push(ret);
        }

        let avg_daily_return = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
        let variance = daily_returns
            .iter()
            .map(|r| (r - avg_daily_return).powi(2))
            .sum::<f64>() / daily_returns.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev > 0.0 {
            (avg_daily_return / std_dev) * (252.0_f64).sqrt() // Annualized
        } else {
            0.0
        }
    }

    // ============================================================================
    // Report Generation
    // ============================================================================

    /// Save all reports to files
    pub fn save_all_reports(
        &self,
        output_dir: &str,
        json_only: bool,
    ) -> Result<(), Box<dyn StdError>> {
        std::fs::create_dir_all(output_dir)?;

        // Save transactions
        self.save_transactions_csv(&format!("{}/transactions.csv", output_dir))?;

        // Save positions
        self.save_positions_csv(&format!("{}/positions.csv", output_dir))?;

        // Save realized P&L
        self.save_realized_pnl_csv(&format!("{}/realized_pnl.csv", output_dir))?;

        // Save daily snapshots
        self.save_daily_snapshots_csv(&format!("{}/daily_snapshots.csv", output_dir))?;

        // Save cash flows
        self.save_cash_flows_csv(&format!("{}/cash_flows.csv", output_dir))?;

        // Save performance summary as JSON
        let summary = self.calculate_performance_summary();
        let json = serde_json::to_string_pretty(&summary)?;
        std::fs::write(format!("{}/performance_summary.json", output_dir), json)?;

        if !json_only {
            // Print console-friendly summary
            self.print_performance_summary(&summary);
        }

        Ok(())
    }

    fn save_transactions_csv(&self, path: &str) -> Result<(), Box<dyn StdError>> {
        let tickers: Vec<String> = self.transactions.iter().map(|t| t.ticker.clone()).collect();
        let dates: Vec<String> = self
            .transactions
            .iter()
            .map(|t| t.date.format("%Y-%m-%d").to_string())
            .collect();
        let actions: Vec<String> = self
            .transactions
            .iter()
            .map(|t| format!("{:?}", t.action))
            .collect();
        let shares: Vec<f64> = self.transactions.iter().map(|t| t.shares).collect();
        let prices: Vec<f64> = self.transactions.iter().map(|t| t.price).collect();
        let gross: Vec<f64> = self.transactions.iter().map(|t| t.gross_amount).collect();
        let commissions: Vec<f64> = self.transactions.iter().map(|t| t.commission).collect();
        let net: Vec<f64> = self.transactions.iter().map(|t| t.net_amount).collect();
        let cash_impact: Vec<f64> = self.transactions.iter().map(|t| t.cash_impact).collect();

        let df = df! {
            "date" => dates,
            "ticker" => tickers,
            "action" => actions,
            "shares" => shares,
            "price" => prices,
            "gross_amount" => gross,
            "commission" => commissions,
            "net_amount" => net,
            "cash_impact" => cash_impact,
        }?;

        let mut file = File::create(path)?;
        CsvWriter::new(&mut file).finish(&mut df.clone())?;
        Ok(())
    }

    fn save_positions_csv(&self, path: &str) -> Result<(), Box<dyn StdError>> {
        if self.positions.is_empty() {
            return Ok(());
        }

        // Sort positions by ticker for deterministic output
        let mut positions: Vec<&Position> = self.positions.values().collect();
        positions.sort_by(|a, b| a.ticker.cmp(&b.ticker));
        let tickers: Vec<String> = positions.iter().map(|p| p.ticker.clone()).collect();
        let shares: Vec<f64> = positions.iter().map(|p| p.shares).collect();
        let avg_cost: Vec<f64> = positions.iter().map(|p| p.avg_cost_basis).collect();
        let current_price: Vec<f64> = positions.iter().map(|p| p.current_price).collect();
        let current_value: Vec<f64> = positions.iter().map(|p| p.current_value).collect();
        let unrealized_pnl: Vec<f64> = positions.iter().map(|p| p.unrealized_pnl).collect();
        let unrealized_pct: Vec<f64> = positions.iter().map(|p| p.unrealized_pnl_pct).collect();

        let df = df! {
            "ticker" => tickers,
            "shares" => shares,
            "avg_cost_basis" => avg_cost,
            "current_price" => current_price,
            "current_value" => current_value,
            "unrealized_pnl" => unrealized_pnl,
            "unrealized_pnl_pct" => unrealized_pct,
        }?;

        let mut file = File::create(path)?;
        CsvWriter::new(&mut file).finish(&mut df.clone())?;
        Ok(())
    }

    fn save_realized_pnl_csv(&self, path: &str) -> Result<(), Box<dyn StdError>> {
        if self.realized_pnl.is_empty() {
            return Ok(());
        }

        let tickers: Vec<String> = self.realized_pnl.iter().map(|p| p.ticker.clone()).collect();
        let entry_dates: Vec<String> = self
            .realized_pnl
            .iter()
            .map(|p| p.entry_date.format("%Y-%m-%d").to_string())
            .collect();
        let exit_dates: Vec<String> = self
            .realized_pnl
            .iter()
            .map(|p| p.close_date.format("%Y-%m-%d").to_string())
            .collect();
        let shares: Vec<f64> = self.realized_pnl.iter().map(|p| p.shares).collect();
        let entry_prices: Vec<f64> = self.realized_pnl.iter().map(|p| p.entry_price).collect();
        let exit_prices: Vec<f64> = self.realized_pnl.iter().map(|p| p.exit_price).collect();
        let net_pnl: Vec<f64> = self.realized_pnl.iter().map(|p| p.net_pnl).collect();
        let pnl_pct: Vec<f64> = self.realized_pnl.iter().map(|p| p.pnl_pct).collect();
        let holding_days: Vec<i64> = self.realized_pnl.iter().map(|p| p.holding_days).collect();

        let df = df! {
            "ticker" => tickers,
            "entry_date" => entry_dates,
            "exit_date" => exit_dates,
            "shares" => shares,
            "entry_price" => entry_prices,
            "exit_price" => exit_prices,
            "net_pnl" => net_pnl,
            "pnl_pct" => pnl_pct,
            "holding_days" => holding_days,
        }?;

        let mut file = File::create(path)?;
        CsvWriter::new(&mut file).finish(&mut df.clone())?;
        Ok(())
    }

    fn save_daily_snapshots_csv(&self, path: &str) -> Result<(), Box<dyn StdError>> {
        if self.daily_snapshots.is_empty() {
            return Ok(());
        }

        let dates: Vec<String> = self
            .daily_snapshots
            .iter()
            .map(|s| s.date.format("%Y-%m-%d").to_string())
            .collect();
        let cash: Vec<f64> = self.daily_snapshots.iter().map(|s| s.cash_balance).collect();
        let equity: Vec<f64> = self.daily_snapshots.iter().map(|s| s.equity_value).collect();
        let total: Vec<f64> = self.daily_snapshots.iter().map(|s| s.total_value).collect();
        let positions: Vec<u32> = self
            .daily_snapshots
            .iter()
            .map(|s| s.position_count as u32)
            .collect();

        let df = df! {
            "date" => dates,
            "cash_balance" => cash,
            "equity_value" => equity,
            "total_value" => total,
            "position_count" => positions,
        }?;

        let mut file = File::create(path)?;
        CsvWriter::new(&mut file).finish(&mut df.clone())?;
        Ok(())
    }

    fn save_cash_flows_csv(&self, path: &str) -> Result<(), Box<dyn StdError>> {
        if self.cash_flows.is_empty() {
            return Ok(());
        }

        let dates: Vec<String> = self
            .cash_flows
            .iter()
            .map(|c| c.date.format("%Y-%m-%d").to_string())
            .collect();
        let descriptions: Vec<String> = self.cash_flows.iter().map(|c| c.description.clone()).collect();
        let amounts: Vec<f64> = self.cash_flows.iter().map(|c| c.amount).collect();
        let balances: Vec<f64> = self.cash_flows.iter().map(|c| c.cash_balance_after).collect();

        let df = df! {
            "date" => dates,
            "description" => descriptions,
            "amount" => amounts,
            "cash_balance_after" => balances,
        }?;

        let mut file = File::create(path)?;
        CsvWriter::new(&mut file).finish(&mut df.clone())?;
        Ok(())
    }

    pub fn print_performance_summary(&self, summary: &PerformanceSummary) {
        // Helper to format numbers with commas
        fn fmt_money(val: f64) -> String {
            let abs_val = val.abs();
            let sign = if val < 0.0 { "-" } else { "" };
            if abs_val >= 1_000_000.0 {
                format!("{}{:>3},{:03},{:06.2}", sign, (abs_val / 1_000_000.0) as u64,
                        ((abs_val % 1_000_000.0) / 1_000.0) as u64, abs_val % 1_000.0)
            } else if abs_val >= 1_000.0 {
                format!("{}{:>3},{:06.2}", sign, (abs_val / 1_000.0) as u64, abs_val % 1_000.0)
            } else {
                format!("{}{:>10.2}", sign, abs_val)
            }
        }

        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║              PORTFOLIO PERFORMANCE SUMMARY                   ║");
        println!("╚══════════════════════════════════════════════════════════════╝");

        println!("\n┌─ Portfolio Value ────────────────────────────────────────────┐");
        println!("│  Initial Value:            ${:>17}                │", fmt_money(summary.initial_value));
        println!("│  Final Value:              ${:>17}                │", fmt_money(summary.final_value));
        println!("│  Total Return:              {:>17.2}%               │", summary.total_return_pct);
        println!("│  CAGR:                      {:>17.2}%               │", summary.cagr);
        println!("└──────────────────────────────────────────────────────────────┘");

        println!("\n┌─ P&L Summary ────────────────────────────────────────────────┐");
        println!("│  Realized P&L:             ${:>17}                │", fmt_money(summary.total_realized_pnl));
        println!("│  Unrealized P&L:           ${:>17}                │", fmt_money(summary.total_unrealized_pnl));
        println!("│  Total Commissions:        ${:>17}                │", fmt_money(summary.total_commissions));
        println!("└──────────────────────────────────────────────────────────────┘");

        println!("\n┌─ Trade Statistics ───────────────────────────────────────────┐");
        println!("│  Total Trades:              {:>17}                │", summary.total_trades);
        println!("│  Winning Trades:            {:>17}  ({:>5.1}%)      │", summary.winning_trades, summary.win_rate_pct);
        println!("│  Losing Trades:             {:>17}                │", summary.losing_trades);
        println!("│  Average Win:               {:>16.2}%                │", summary.avg_win_pct);
        println!("│  Average Loss:              {:>16.2}%                │", summary.avg_loss_pct);
        println!("│  Profit Factor:             {:>17.3}                │", summary.profit_factor);
        println!("└──────────────────────────────────────────────────────────────┘");

        println!("\n┌─ Risk Metrics ───────────────────────────────────────────────┐");
        println!("│  Max Drawdown:              {:>16.2}%                │", summary.max_drawdown_pct);
        println!("│  Sharpe Ratio:              {:>17.3}                │", summary.sharpe_ratio);
        println!("└──────────────────────────────────────────────────────────────┘");

        println!("\n┌─ Holding Period ─────────────────────────────────────────────┐");
        println!("│  Average:                   {:>13.1} days               │", summary.avg_holding_days);
        println!("│  Maximum:                   {:>13} days               │", summary.max_holding_days);
        println!("│  Minimum:                   {:>13} days               │", summary.min_holding_days);
        println!("└──────────────────────────────────────────────────────────────┘\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_accounting() {
        let acct = PortfolioAccounting::new(100_000.0);
        assert_eq!(acct.get_cash_balance(), 100_000.0);
        assert_eq!(acct.get_position_count(), 0);
    }

    #[test]
    fn test_buy_sell() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        // Buy 100 shares of AAPL at $50 with $6.95 commission
        let result = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);
        assert!(result.is_ok());
        assert_eq!(acct.get_position_count(), 1);
        assert!(acct.has_position("AAPL"));

        // Check cash reduced correctly
        let expected_cash = 100_000.0 - (100.0 * 50.0 + 6.95);
        assert!((acct.get_cash_balance() - expected_cash).abs() < 0.01);

        // Check position details
        let position = acct.get_position("AAPL").unwrap();
        assert_eq!(position.shares, 100.0);
        assert_eq!(position.avg_cost_basis, (100.0 * 50.0 + 6.95) / 100.0);
        assert_eq!(position.total_cost, 100.0 * 50.0 + 6.95);

        // Check position weight
        let weight = acct.get_position_weight("AAPL").unwrap();
        let expected_weight = (100.0 * 50.0) / acct.get_total_value();
        assert!((weight - expected_weight).abs() < 0.01);

        // Sell all shares at $55 with $6.95 commission
        let result = acct.execute_sell(date, "AAPL", 55.0, 6.95);
        assert!(result.is_ok());
        assert_eq!(acct.get_position_count(), 0);
        assert!(!acct.has_position("AAPL"));

        // Check cash increased correctly
        let expected_cash = 100_000.0 - (100.0 * 50.0 + 6.95) + (100.0 * 55.0 - 6.95);
        assert!((acct.get_cash_balance() - expected_cash).abs() < 0.01);
    }

    #[test]
    fn test_partial_sell() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        // Buy 200 shares
        let _ = acct.execute_buy(date, "AAPL", 200.0, 50.0, 6.95);

        // Sell 100 shares (half)
        let result = acct.execute_sell_shares(date, "AAPL", 100.0, 55.0, 6.95);
        assert!(result.is_ok());
        assert_eq!(acct.get_position_count(), 1);

        // Check remaining position
        let position = acct.get_position("AAPL").unwrap();
        assert_eq!(position.shares, 100.0);

        // Check cash balance
        let expected_cash = 100_000.0 - (200.0 * 50.0 + 6.95) + (100.0 * 55.0 - 6.95);
        assert!((acct.get_cash_balance() - expected_cash).abs() < 0.01);
    }

    #[test]
    fn test_multiple_positions() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        // Buy multiple positions
        let _ = acct.execute_buy(date, "AAPL", 100.0, 150.0, 0.);
        let _ = acct.execute_buy(date, "MSFT", 100.0, 100.0, 0.);
        let _ = acct.execute_buy(date, "GOOG", 100.0, 50.0, 0.);

        assert_eq!(acct.get_position_count(), 3);

        // Check position weights
        let weights = acct.get_all_position_weights();
        assert_eq!(weights.len(), 3);

        // Check that weights sum to 1.0 (or close to it)
        let total_weight: f64 = weights.values().sum();
        assert!(total_weight - 0.3 < 0.00001);

        // Check individual weights
        let aapl_weight = acct.get_position_weight("AAPL").unwrap();
        let msft_weight = acct.get_position_weight("MSFT").unwrap();
        let goog_weight = acct.get_position_weight("GOOG").unwrap();
        // println!("apple wgt {}", aapl_weight);
        // println!("msft wgt {}", msft_weight);
        // println!("goog wgt {}", goog_weight);

        // AAPL should have the largest weight (100 * 50 = 5000)
        assert!(aapl_weight > msft_weight);
        // MSFT should have the second largest weight (50 * 100 = 5000)
        assert!(msft_weight > goog_weight);
    }

    #[test]
    fn test_mark_to_market() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

        // Buy position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);

        // Mark to market with higher price
        let mut prices = HashMap::new();
        prices.insert("AAPL".to_string(), 60.0);
        acct.mark_to_market(date2, &prices);

        // Check updated position
        let position = acct.get_position("AAPL").unwrap();
        assert_eq!(position.current_price, 60.0);
        assert_eq!(position.current_value, 100.0 * 60.0);
        assert_eq!(position.unrealized_pnl, 100.0 * 60.0 - position.total_cost);

        // Check updated weight
        let weight = acct.get_position_weight("AAPL").unwrap();
        let expected_weight = (100.0 * 60.0) / acct.get_total_value();
        assert!((weight - expected_weight).abs() < 0.01);
    }

    #[test]
    fn test_daily_snapshots() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

        // Take initial snapshot
        acct.take_daily_snapshot(date);
        assert_eq!(acct.daily_snapshots.len(), 1);

        // Store the first snapshot value before borrowing mutably again
        let first_snapshot_value = acct.daily_snapshots[0].total_value;

        // Buy position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);

        // Take second snapshot
        acct.take_daily_snapshot(date2);
        assert_eq!(acct.daily_snapshots.len(), 2);

        let snapshot2 = &acct.daily_snapshots[1];
        assert_eq!(snapshot2.position_count, 1);
        assert_eq!(snapshot2.daily_return_pct,
                ((snapshot2.total_value / first_snapshot_value) - 1.0) * 100.0);
    }

    #[test]
    fn test_performance_summary() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

        // Buy position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);

        // Mark to market
        let mut prices = HashMap::new();
        prices.insert("AAPL".to_string(), 60.0);
        acct.mark_to_market(date2, &prices);

        // Take snapshot
        acct.take_daily_snapshot(date2);

        // Sell position
        let _ = acct.execute_sell(date2, "AAPL", 60.0, 6.95);

        // Calculate performance summary
        let summary = acct.calculate_performance_summary();

        // Check basic values
        assert_eq!(summary.initial_value, 100_000.0);
        assert!(summary.final_value > 100_000.0);
        assert!(summary.total_return_pct > 0.0);
        assert_eq!(summary.total_trades, 1);
        assert_eq!(summary.winning_trades, 1);
        assert_eq!(summary.losing_trades, 0);
        assert_eq!(summary.win_rate_pct, 100.0);
        assert!(summary.total_commissions > 0.0);
    }

    #[test]
    fn test_error_handling() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        // Test buying with insufficient cash
        let result = acct.execute_buy(date, "AAPL", 10000.0, 50.0, 6.95);
        assert!(result.is_err());

        // Test buying with zero shares
        let result = acct.execute_buy(date, "AAPL", 0.0, 50.0, 6.95);
        assert!(result.is_err());

        // Test buying with negative price
        let result = acct.execute_buy(date, "AAPL", 100.0, -50.0, 6.95);
        assert!(result.is_err());

        // Test buying with negative commission
        let result = acct.execute_buy(date, "AAPL", 100.0, 50.0, -6.95);
        assert!(result.is_err());

        // Test selling non-existent position
        let result = acct.execute_sell(date, "AAPL", 50.0, 6.95);
        assert!(result.is_err());

        // Test buying a valid position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);

        // Test selling more shares than owned
        let result = acct.execute_sell_shares(date, "AAPL", 200.0, 50.0, 6.95);
        assert!(result.is_err());
    }

    #[test]
    fn test_realized_pnl() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();

        // Buy position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);

        // Sell position at a profit
        let _ = acct.execute_sell(date2, "AAPL", 60.0, 6.95);

        // Check realized P&L
        assert_eq!(acct.realized_pnl.len(), 1);
        let pnl = &acct.realized_pnl[0];

        assert_eq!(pnl.ticker, "AAPL");
        assert_eq!(pnl.entry_date, date);
        assert_eq!(pnl.close_date, date2);
        assert_eq!(pnl.shares, 100.0);
        assert_eq!(pnl.entry_price, (100.0 * 50.0 + 6.95) / 100.0);
        assert_eq!(pnl.exit_price, 60.0);
        assert_eq!(pnl.holding_days, (date2 - date).num_days());

        // Check that P&L is positive (profit)
        assert!(pnl.net_pnl > 0.0);
        assert!(pnl.pnl_pct > 0.0);
    }

    #[test]
    fn test_unrealized_pnl() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

        // Buy position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);

        // Mark to market with higher price
        let mut prices = HashMap::new();
        prices.insert("AAPL".to_string(), 60.0);
        acct.mark_to_market(date2, &prices);

        // Get unrealized P&L
        let unrealized = acct.get_unrealized_pnl();
        assert_eq!(unrealized.len(), 1);

        let pnl = &unrealized[0];
        assert_eq!(pnl.ticker, "AAPL");
        assert_eq!(pnl.as_of_date, date2);
        assert_eq!(pnl.shares, 100.0);
        assert_eq!(pnl.current_price, 60.0);

        // Check that P&L is positive (profit)
        assert!(pnl.unrealized_pnl > 0.0);
        assert!(pnl.unrealized_pnl_pct > 0.0);
    }

    #[test]
    fn test_cash_flows() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

        // Buy position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 6.95);

        // Sell position
        let _ = acct.execute_sell(date2, "AAPL", 60.0, 6.95);

        // Check cash flows
        assert_eq!(acct.cash_flows.len(), 2);

        // First cash flow (buy) should be negative
        let buy_flow = &acct.cash_flows[0];
        assert!(buy_flow.amount < 0.0);
        assert!(buy_flow.description.contains("Buy"));

        // Second cash flow (sell) should be positive
        let sell_flow = &acct.cash_flows[1];
        assert!(sell_flow.amount > 0.0);
        assert!(sell_flow.description.contains("Sell"));
    }

    #[test]
    fn test_save_reports() {
        let mut acct = PortfolioAccounting::new(100_000.0);
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let date2 = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

        // Buy position
        let _ = acct.execute_buy(date, "AAPL", 100.0, 50.0, 0.);

        // Mark to market
        let mut prices = HashMap::new();
        prices.insert("AAPL".to_string(), 60.0);
        let w1 = acct.get_all_position_weights();
        println!("before m2m {:?} ", w1);
        acct.mark_to_market(date2, &prices);
        let w2 = acct.get_all_position_weights();
        println!("after m2m {:?} ", w2);

        // Take snapshot
        let s = acct.take_daily_snapshot(date2);
        if let Some(s) = acct.daily_snapshots.last() {
            println!("snapshot {:?}", s);
        }

        // Save reports
        let result = acct.save_all_reports("test_reports", false); // json_only=true to avoid printing
        assert!(result.is_ok());
    }
}