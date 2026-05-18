use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{broadcast, watch, Mutex, Notify};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type WebSocketSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

pub(super) async fn outbound_sink_loop(
    url: String,
    frame_tx: broadcast::Sender<String>,
    client_count: Arc<Mutex<usize>>,
    client_notify: Arc<Notify>,
    sink_connected: Arc<Mutex<bool>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return;
                }
            }
            connected = tokio::time::timeout(Duration::from_secs(5), connect_async(&url)) => {
                let Ok(Ok((ws_stream, _))) = connected else {
                    wait_before_reconnect(&mut shutdown_rx).await;
                    continue;
                };

                {
                    *sink_connected.lock().await = true;
                    let mut count = client_count.lock().await;
                    *count += 1;
                }
                client_notify.notify_one();

                let mut frame_rx = frame_tx.subscribe();
                let (mut ws_tx, mut ws_rx) = ws_stream.split();
                let mut last_frame_sent_at: Option<Instant> = None;

                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                let _ = ws_tx.send(Message::Close(None)).await;
                                decrement_client_count(&client_count, &sink_connected, &client_notify).await;
                                return;
                            }
                        }
                        frame = frame_rx.recv() => {
                            match frame {
                                Ok(data) => {
                                    if send_latest_messages(&mut frame_rx, &mut ws_tx, &mut shutdown_rx, &mut last_frame_sent_at, data).await.is_err() {
                                      break;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                        msg = ws_rx.next() => {
                            match msg {
                                Some(Ok(Message::Close(_))) | None => break,
                                Some(Err(_)) => break,
                                _ => {}
                            }
                        }
                    }
                }

                decrement_client_count(&client_count, &sink_connected, &client_notify).await;
                wait_before_reconnect(&mut shutdown_rx).await;
                if *shutdown_rx.borrow() {
                    return;
                }
            }
        }
    }
}

fn is_status_message(message: &str) -> bool {
    message.contains(r#""type":"status""#) || message.contains(r#""type": "status""#)
}

fn is_frame_message(message: &str) -> bool {
    message.contains(r#""type":"frame""#) || message.contains(r#""type": "frame""#)
}

enum OutboundMessage {
    Text(String),
    Binary(Vec<u8>),
}

async fn send_latest_messages(
    frame_rx: &mut broadcast::Receiver<String>,
    ws_tx: &mut WebSocketSink,
    shutdown_rx: &mut watch::Receiver<bool>,
    last_frame_sent_at: &mut Option<Instant>,
    first: String,
) -> Result<(), ()> {
    let mut latest_status = None;
    let mut latest_frame = None::<Vec<u8>>;
    let mut latest_other = None;

    classify_message(
        first,
        &mut latest_status,
        &mut latest_frame,
        &mut latest_other,
    );

    loop {
        match frame_rx.try_recv() {
            Ok(next) => classify_message(
                next,
                &mut latest_status,
                &mut latest_frame,
                &mut latest_other,
            ),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Closed) => break,
        }
    }

    if let Some(status) = latest_status {
        send_outbound_message(ws_tx, shutdown_rx, OutboundMessage::Text(status)).await?;
    }

    if let Some(frame) = latest_frame {
        pace_frame_send(shutdown_rx, last_frame_sent_at).await?;
        send_outbound_message(ws_tx, shutdown_rx, OutboundMessage::Binary(frame)).await?;
        *last_frame_sent_at = Some(Instant::now());
    } else if let Some(other) = latest_other {
        send_outbound_message(ws_tx, shutdown_rx, OutboundMessage::Text(other)).await?;
    }

    Ok(())
}

async fn send_outbound_message(
    ws_tx: &mut WebSocketSink,
    shutdown_rx: &mut watch::Receiver<bool>,
    message: OutboundMessage,
) -> Result<(), ()> {
    if *shutdown_rx.borrow() {
        return Err(());
    }

    let message = match message {
        OutboundMessage::Text(data) => Message::Text(data.into()),
        OutboundMessage::Binary(data) => Message::Binary(data.into()),
    };

    tokio::time::timeout(Duration::from_secs(2), ws_tx.send(message))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

async fn pace_frame_send(
    shutdown_rx: &watch::Receiver<bool>,
    last_frame_sent_at: &Option<Instant>,
) -> Result<(), ()> {
    if *shutdown_rx.borrow() {
        return Err(());
    }

    let Some(last_sent_at) = last_frame_sent_at else {
        return Ok(());
    };

    let min_interval = Duration::from_millis(40);
    let elapsed = last_sent_at.elapsed();
    if elapsed < min_interval {
        tokio::time::sleep(min_interval - elapsed).await;
    }

    if *shutdown_rx.borrow() {
        return Err(());
    }

    Ok(())
}

fn classify_message(
    message: String,
    latest_status: &mut Option<String>,
    latest_frame: &mut Option<Vec<u8>>,
    latest_other: &mut Option<String>,
) {
    if is_status_message(&message) {
        *latest_status = Some(message);
    } else if is_frame_message(&message) {
        if let Some(bytes) = frame_bytes_from_message(&message) {
            *latest_frame = Some(bytes);
        }
    } else {
        *latest_other = Some(message);
    }
}

fn frame_bytes_from_message(message: &str) -> Option<Vec<u8>> {
    let value: serde_json::Value = serde_json::from_str(message).ok()?;
    let data = value.get("data")?.as_str()?;
    base64::engine::general_purpose::STANDARD.decode(data).ok()
}

async fn decrement_client_count(
    client_count: &Arc<Mutex<usize>>,
    sink_connected: &Arc<Mutex<bool>>,
    client_notify: &Arc<Notify>,
) {
    {
        *sink_connected.lock().await = false;
        let mut count = client_count.lock().await;
        *count = count.saturating_sub(1);
    }
    client_notify.notify_one();
}

async fn wait_before_reconnect(shutdown_rx: &mut watch::Receiver<bool>) {
    let delay = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(delay);

    tokio::select! {
        _ = &mut delay => {}
        changed = shutdown_rx.changed() => {
            if changed.is_ok() && !*shutdown_rx.borrow() {
                // A non-shutdown notification is still a useful reason to retry
                // immediately, for example after a browser/session state change.
            }
        }
    }
}
