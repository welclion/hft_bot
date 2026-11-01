use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Tick {
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub ts: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Signal {
    pub timestamp: u64,
    pub obi: f64,
    pub cvd_10s: f64,
    pub tick_rate: f64,
    pub action: Action,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Action {
    None,
    Buy,
    Sell,
}
