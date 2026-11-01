mod common;
mod engine;
mod execution;
mod ingestion;

use crossbeam_channel::bounded;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::thread;

#[tokio::main]
async fn main() {
    println!("🚀 HFT Bot Starting...");

    // Канал между Ingestion (Async) и Engine (Sync)
    let (tx_tick, rx_tick) = bounded(100_000);

    // Файл для бэктеста
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("market_data.bin")
        .expect("Failed to open file");

    // Запускаем Engine в отдельном OS-потоке
    thread::spawn(move || {
        engine::run_engine(rx_tick, file);
    });

    // Создаем экземпляр исполнителя ордеров
    let bot = Arc::new(execution::ExecutionBot::new());

    // Запускаем Ingestion в Async runtime
    tokio::spawn(async move {
        ingestion::run_ingestion(tx_tick).await;
    });

    // Ждем сигнала выхода (Ctrl+C)
    tokio::signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}
