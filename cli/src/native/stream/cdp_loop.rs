use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, watch, Mutex, RwLock};

use crate::native::cdp::client::CdpClient;
use crate::native::network;

use super::timestamp_ms;

async fn stop_preview_screencast(client: &CdpClient, session_id: Option<&str>) {
    let _ = client
        .send_command_with_timeout(
            "Page.stopScreencast",
            None,
            session_id,
            Duration::from_secs(2),
        )
        .await;
}

async fn start_preview_screencast(
    client: &CdpClient,
    session_id: Option<&str>,
    width: u32,
    height: u32,
) {
    let _ = client
        .send_command_with_timeout(
            "Page.startScreencast",
            Some(json!({
                "format": "jpeg",
                "quality": 80,
                "maxWidth": width,
                "maxHeight": height,
                "everyNthFrame": 1,
            })),
            session_id,
            Duration::from_secs(2),
        )
        .await;
}

async fn ack_preview_screencast_frame(
    client: &CdpClient,
    session_id: Option<&str>,
    screencast_session_id: i64,
) {
    let _ = client
        .send_command_no_wait(
            "Page.screencastFrameAck",
            Some(json!({ "sessionId": screencast_session_id })),
            session_id,
        )
        .await;
}

async fn restart_screencast(client: &CdpClient, session_id: Option<&str>, width: u32, height: u32) {
    stop_preview_screencast(client, session_id).await;
    start_preview_screencast(client, session_id, width, height).await;
}

/// Background task that subscribes to CDP events and broadcasts screencast frames in real-time.
/// Also handles auto-start/stop of screencast based on WebSocket client count.
#[allow(clippy::too_many_arguments)]
pub(super) async fn cdp_event_loop(
    frame_tx: broadcast::Sender<String>,
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    client_notify: Arc<tokio::sync::Notify>,
    screencasting: Arc<Mutex<bool>>,
    client_count: Arc<Mutex<usize>>,
    cdp_session_id: Arc<RwLock<Option<String>>>,
    viewport_width: Arc<Mutex<u32>>,
    viewport_height: Arc<Mutex<u32>>,
    last_frame: Arc<RwLock<Option<String>>>,
    last_tabs: Arc<RwLock<Vec<Value>>>,
    last_engine: Arc<RwLock<String>>,
    recording: Arc<Mutex<bool>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    let session_id = cdp_session_id.read().await.clone();
                    if *screencasting.lock().await {
                        if let Some(ref client) = *client_slot.read().await {
                            stop_preview_screencast(client, session_id.as_deref()).await;
                        }
                        let mut sc = screencasting.lock().await;
                        *sc = false;
                    }
                    return;
                }
            }
            _ = client_notify.notified() => {}
        }

        let count = *client_count.lock().await;
        let guard = client_slot.read().await;

        if count > 0 {
            if *recording.lock().await {
                if *screencasting.lock().await {
                    let mut sc = screencasting.lock().await;
                    *sc = false;
                }
                let eng = last_engine.read().await.clone();
                let (vw, vh) = (*viewport_width.lock().await, *viewport_height.lock().await);
                let status = json!({
                    "type": "status",
                    "connected": guard.is_some(),
                    "screencasting": false,
                    "viewportWidth": vw,
                    "viewportHeight": vh,
                    "engine": eng,
                    "recording": true,
                });
                let _ = frame_tx.send(status.to_string());
                drop(guard);
                continue;
            }

            if let Some(ref client) = *guard {
                let mut event_rx = client.subscribe();
                let client_arc = Arc::clone(client);
                drop(guard);

                let session_id = cdp_session_id.read().await.clone();

                let vw = *viewport_width.lock().await;
                let vh = *viewport_height.lock().await;

                let eng = last_engine.read().await.clone();
                let supports_screencast = eng == "chrome";

                if supports_screencast {
                    start_preview_screencast(&client_arc, session_id.as_deref(), vw, vh).await;
                }

                {
                    let mut sc = screencasting.lock().await;
                    *sc = supports_screencast;
                }

                let rec = *recording.lock().await;
                let status = json!({
                    "type": "status",
                    "connected": true,
                    "screencasting": supports_screencast,
                    "viewportWidth": vw,
                    "viewportHeight": vh,
                    "engine": eng,
                    "recording": rec,
                });
                let _ = frame_tx.send(status.to_string());

                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                if supports_screencast {
                                    let session_id = cdp_session_id.read().await.clone();
                                    stop_preview_screencast(&client_arc, session_id.as_deref()).await;
                                }
                                let mut sc = screencasting.lock().await;
                                *sc = false;
                                return;
                            }
                        }
                        event = event_rx.recv() => {
                            match event {
                                Ok(evt) => {
                                    if *recording.lock().await {
                                        if evt.method == "Page.screencastFrame" {
                                            // While recording, preview frames are supplied by
                                            // the recorder. Do not ACK stream-server screencast
                                            // frames here; ACKing keeps Chrome flooding CDP and
                                            // can starve agent control commands.
                                        }
                                        let mut sc = screencasting.lock().await;
                                        *sc = false;
                                        client_notify.notify_one();
                                        break;
                                    }
                                    if evt.method == "Page.frameStoppedLoading" {
                                        if supports_screencast {
                                            tokio::time::sleep(Duration::from_millis(250)).await;
                                            restart_screencast(
                                                &client_arc,
                                                session_id.as_deref(),
                                                vw,
                                                vh,
                                            )
                                            .await;
                                            let mut sc = screencasting.lock().await;
                                            *sc = true;
                                        }
                                    } else if evt.method == "Page.frameNavigated" {
                                        if let Some(frame) = evt.params.get("frame") {
                                            let is_main = frame
                                                .get("parentId")
                                                .and_then(|v| v.as_str())
                                                .is_none_or(|s| s.is_empty());
                                            if is_main {
                                                if let Some(url) = frame.get("url").and_then(|v| v.as_str()) {
                                                    {
                                                        let mut tabs = last_tabs.write().await;
                                                        for tab in tabs.iter_mut() {
                                                            if tab.get("active").and_then(|v| v.as_bool()).unwrap_or(false) {
                                                                tab.as_object_mut().map(|o| o.insert("url".to_string(), json!(url)));
                                                            }
                                                        }
                                                    }
                                                    let msg = json!({
                                                        "type": "url",
                                                        "url": url,
                                                        "timestamp": timestamp_ms(),
                                                    });
                                                    let _ = frame_tx.send(msg.to_string());
                                                }
                                            }
                                        }
                                    } else if evt.method == "Page.screencastFrame" {
                                        if let Some(sid) = evt.params.get("sessionId").and_then(|v| v.as_i64()) {
                                            ack_preview_screencast_frame(&client_arc, evt.session_id.as_deref(), sid).await;
                                        }

                                        if let Some(data) = evt.params.get("data").and_then(|v| v.as_str()) {
                                            let meta = evt.params.get("metadata");
                                            let msg = json!({
                                                "type": "frame",
                                                "data": data,
                                                "metadata": {
                                                    "offsetTop": meta.and_then(|m| m.get("offsetTop")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "pageScaleFactor": meta.and_then(|m| m.get("pageScaleFactor")).and_then(|v| v.as_f64()).unwrap_or(1.0),
                                                    "deviceWidth": vw,
                                                    "deviceHeight": vh,
                                                    "scrollOffsetX": meta.and_then(|m| m.get("scrollOffsetX")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "scrollOffsetY": meta.and_then(|m| m.get("scrollOffsetY")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "timestamp": meta.and_then(|m| m.get("timestamp")).and_then(|v| v.as_u64()).unwrap_or(0),
                                                }
                                            });
                                            let msg_str = msg.to_string();
                                            {
                                                let mut lf = last_frame.write().await;
                                                *lf = Some(msg_str.clone());
                                            }
                                            let _ = frame_tx.send(msg_str);
                                        }
                                    } else if evt.method == "Runtime.consoleAPICalled" {
                                        let level = evt.params.get("type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("log");
                                        let raw_args = evt.params.get("args")
                                            .and_then(|v| v.as_array())
                                            .cloned()
                                            .unwrap_or_default();
                                        let text = network::format_console_args(&raw_args);
                                        if !text.is_empty() {
                                            let mut msg = json!({
                                                "type": "console",
                                                "level": level,
                                                "text": text,
                                                "timestamp": timestamp_ms(),
                                            });
                                            if !raw_args.is_empty() {
                                                msg.as_object_mut().unwrap().insert(
                                                    "args".to_string(),
                                                    Value::Array(raw_args),
                                                );
                                            }
                                            let _ = frame_tx.send(msg.to_string());
                                        }
                                    } else if evt.method == "Runtime.exceptionThrown" {
                                        let text = evt.params.get("exceptionDetails")
                                            .and_then(|d| {
                                                d.get("exception")
                                                    .and_then(|e| e.get("description").and_then(|v| v.as_str()))
                                                    .or_else(|| d.get("text").and_then(|v| v.as_str()))
                                            })
                                            .unwrap_or("Unknown error");
                                        let line = evt.params.get("exceptionDetails")
                                            .and_then(|d| d.get("lineNumber").and_then(|v| v.as_i64()));
                                        let column = evt.params.get("exceptionDetails")
                                            .and_then(|d| d.get("columnNumber").and_then(|v| v.as_i64()));
                                        let msg = json!({
                                            "type": "page_error",
                                            "text": text,
                                            "line": line,
                                            "column": column,
                                            "timestamp": timestamp_ms(),
                                        });
                                        let _ = frame_tx.send(msg.to_string());
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                        _ = client_notify.notified() => {
                            let count = *client_count.lock().await;
                            let new_session_id = cdp_session_id.read().await.clone();
                            if count == 0 {
                                if supports_screencast {
                                    stop_preview_screencast(&client_arc, session_id.as_deref()).await;
                                }
                                let mut sc = screencasting.lock().await;
                                *sc = false;
                                break;
                            }
                            let client_changed = {
                                let guard = client_slot.read().await;
                                let same = guard
                                    .as_ref()
                                    .is_some_and(|c| Arc::ptr_eq(c, &client_arc));
                                !same
                            };
                            let session_changed = new_session_id != session_id;
                            let new_vw = *viewport_width.lock().await;
                            let new_vh = *viewport_height.lock().await;
                            let viewport_changed = new_vw != vw || new_vh != vh;
                            let recording_active = *recording.lock().await;
                            let screencast_stopped = !*screencasting.lock().await;
                            if client_changed
                                || session_changed
                                || viewport_changed
                                || recording_active
                                || screencast_stopped
                            {
                                if supports_screencast && !recording_active {
                                    stop_preview_screencast(&client_arc, session_id.as_deref()).await;
                                }
                                let mut sc = screencasting.lock().await;
                                *sc = false;
                                client_notify.notify_one();
                                break;
                            }
                        }
                    }
                }
            } else {
                drop(guard);
            }
        } else {
            let was_screencasting = *screencasting.lock().await;
            if was_screencasting {
                if let Some(ref client) = *guard {
                    let session_id = cdp_session_id.read().await.clone();
                    stop_preview_screencast(client, session_id.as_deref()).await;
                }
                let mut sc = screencasting.lock().await;
                *sc = false;
            }
            drop(guard);
        }
    }
}

pub async fn start_screencast(
    client: &CdpClient,
    session_id: &str,
    format: &str,
    quality: i32,
    max_width: i32,
    max_height: i32,
) -> Result<(), String> {
    client
        .send_command_with_timeout(
            "Page.startScreencast",
            Some(json!({
                "format": format,
                "quality": quality,
                "maxWidth": max_width,
                "maxHeight": max_height,
                "everyNthFrame": 1,
            })),
            Some(session_id),
            Duration::from_secs(2),
        )
        .await?;
    Ok(())
}

pub async fn stop_screencast(client: &CdpClient, session_id: &str) -> Result<(), String> {
    client
        .send_command_with_timeout(
            "Page.stopScreencast",
            None,
            Some(session_id),
            Duration::from_secs(2),
        )
        .await?;
    Ok(())
}

pub async fn ack_screencast_frame(
    client: &CdpClient,
    session_id: &str,
    screencast_session_id: i64,
) -> Result<(), String> {
    client
        .send_command_no_wait(
            "Page.screencastFrameAck",
            Some(json!({ "sessionId": screencast_session_id })),
            Some(session_id),
        )
        .await?;
    Ok(())
}
