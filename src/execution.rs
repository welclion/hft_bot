use crate::common::{Action, Signal};
use tokio::time::{sleep, Duration};

pub struct ExecutionBot {
    network_lag_ms: u64,
    fee_rate: f64,
    slippage_bps: f64,
}

impl ExecutionBot {
    pub fn new() -> Self {
        Self {
            network_lag_ms: 50,
            fee_rate: 0.0005, // 0.05%
            slippage_bps: 1.0,
        }
    }

    pub async fn process_signal(&self, signal: Signal) {
        if signal.action == Action::None {
            return;
        }

        // Симуляция задержки сети (RTT)
        sleep(Duration::from_millis(self.network_lag_ms)).await;

        let base_price = 65000.0; // В реальности берется из последнего тика
        let slip_factor = if signal.action == Action::Buy {
            1.0 + (self.slippage_bps / 10000.0)
        } else {
            1.0 - (self.slippage_bps / 10000.0)
        };

        let fill_price = base_price * slip_factor;
        let qty = 0.01;
        let fee = qty * fill_price * self.fee_rate;

        println!(
            "[Exec] T+{}ms | {:?} @ {:.2} | Fee: {:.4}",
            self.network_lag_ms, signal.action, fill_price, fee
        );
    }
}
