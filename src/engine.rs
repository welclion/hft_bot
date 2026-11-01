use crate::common::{Action, Side, Signal, Tick};
use crossbeam_channel::Receiver;
use std::fs::File;
use std::io::Write;

const WINDOW_SIZE: usize = 300_000;

struct RingBuffer {
    data: Vec<(u64, f64)>,
    head: usize,
    tail: usize,
    sum: f64,
}

impl RingBuffer {
    fn new() -> Self {
        Self {
            data: vec![(0, 0.0); WINDOW_SIZE],
            head: 0,
            tail: 0,
            sum: 0.0,
        }
    }

    fn push(&mut self, ts: u64, val: f64) {
        self.data[self.tail] = (ts, val);
        self.sum += val;
        self.tail = (self.tail + 1) % WINDOW_SIZE;
        if self.tail == self.head {
            self.head = (self.head + 1) % WINDOW_SIZE;
        }
    }

    fn prune(&mut self, current_ts: u64, window_ms: u64) {
        let cutoff = current_ts.saturating_sub(window_ms);
        while self.head != self.tail {
            let (ts, val) = self.data[self.head];
            if ts < cutoff {
                self.sum -= val;
                self.head = (self.head + 1) % WINDOW_SIZE;
            } else {
                break;
            }
        }
    }
}

pub fn run_engine(rx: Receiver<Tick>, mut file: File) {
    let mut cvd = RingBuffer::new();
    let mut tick_count = 0u32;
    let mut last_ts = 0u64;
    let mut rate = 0.0;
    let mut flush_counter = 0;

    println!("[Engine] Started metrics calculation");

    loop {
        if let Ok(tick) = rx.recv() {
            let delta = if tick.side == Side::Buy {
                tick.qty
            } else {
                -tick.qty
            };
            cvd.push(tick.ts, delta);
            cvd.prune(tick.ts, 10_000);

            tick_count += 1;
            if tick.ts - last_ts >= 1000 {
                rate = tick_count as f64;
                tick_count = 0;
                last_ts = tick.ts;
            }

            let action = if cvd.sum > 5.0 && rate > 100.0 {
                Action::Buy
            } else if cvd.sum < -5.0 {
                Action::Sell
            } else {
                Action::None
            };

            let signal = Signal {
                timestamp: tick.ts,
                obi: 0.0,
                cvd_10s: cvd.sum,
                tick_rate: rate,
                action,
            };

            let _ = bincode::serialize_into(&mut file, &signal);

            // Оптимизация: flush раз в 100 тиков
            flush_counter += 1;
            if flush_counter % 100 == 0 {
                let _ = file.flush();
            }
        }
    }
}
