use crate::common::{Side, Tick};
use crossbeam_channel::Sender;
use futures_util::StreamExt;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// Оптимизированный zero-copy парсер без аллокаций
fn parse_trade_fast(payload: &[u8]) -> Option<Tick> {
    // 1. Ищем цену (ключ "p")
    let p_start = payload.windows(5).position(|w| w == b"\"p\":\"")? + 5;
    let p_end = payload[p_start..].iter().position(|&b| b == b'"')? + p_start;
    let price_str = std::str::from_utf8(&payload[p_start..p_end]).ok()?;
    let price: f64 = price_str.parse().ok()?;

    // 2. Ищем количество (ключ "q")
    let q_start = payload.windows(5).position(|w| w == b"\"q\":\"")? + 5;
    let q_end = payload[q_start..].iter().position(|&b| b == b'"')? + q_start;
    let qty_str = std::str::from_utf8(&payload[q_start..q_end]).ok()?;
    let qty: f64 = qty_str.parse().ok()?;

    // 3. Ищем время (ключ "T")
    let t_start = payload.windows(4).position(|w| w == b"\"T\":")? + 4;
    let t_end = payload[t_start..]
        .iter()
        .position(|&b| b == b',' || b == b'}')?
        + t_start;
    let ts_str = std::str::from_utf8(&payload[t_start..t_end]).ok()?;
    let ts: u64 = ts_str.parse().ok()?;

    // 4. Ищем сторону (ключ "m")
    let is_maker = payload.windows(5).any(|w| w == b"\"m\":t");

    Some(Tick {
        price,
        qty,
        side: if is_maker { Side::Sell } else { Side::Buy },
        ts,
    })
}

pub async fn run_ingestion(tx: Sender<Tick>) {
    // Продажный routed эндпоинт для высокочастотных маркет-данных Binance Futures
    let url = "wss://://binance.com";

    let mut retry_delay = Duration::from_secs(1);
    let max_retry_delay = Duration::from_secs(60);

    loop {
        println!("[Ingestion] Connecting to {}...", url);

        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                println!("[Ingestion] Connected successfully to Binance Futures.");
                // Сбрасываем задержку при успешном подключении
                retry_delay = Duration::from_secs(1);

                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if text.contains("\"e\":\"trade\"") {
                                if let Some(tick) = parse_trade_fast(text.as_bytes()) {
                                    if tx.try_send(tick).is_err() {
                                        // Очередь переполнена — движок не успевает переваривать данные
                                        eprintln!("[Ingestion] Critical Error: Channel full! Engine is lagging.");
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {
                            // Tokio-tungstenite автоматически отвечает на пинги, но логируем для контроля линии
                            // Сервер присылает пинг каждые 3 минуты
                        }
                        Err(e) => {
                            eprintln!("[Ingestion] Stream error encountered: {}", e);
                            break; // Выходим из внутреннего цикла чтения для реконнекта
                        }
                        _ => {}
                    }
                }
                println!("[Ingestion] Connection lost. Reconnecting...");
            }
            Err(e) => {
                eprintln!(
                    "[Ingestion] Connection establishment failed: {}. Retrying in {}s...",
                    e,
                    retry_delay.as_secs()
                );

                sleep(retry_delay).await;
                // Экспоненциальный рост задержки (Exponential Backoff) чтобы не забанили IP
                retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
            }
        }
    }
}
