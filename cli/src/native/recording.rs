use serde_json::{json, Value};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use super::cdp::client::CdpClient;
use super::cdp::types::CdpEvent;

const CAPTURE_FPS: u32 = 25;

struct ScreencastFrame {
    bytes: Vec<u8>,
    timestamp_secs: f64,
}

struct EncodedFrame {
    bytes: Vec<u8>,
    timestamp_secs: f64,
    frame_number: u64,
}

struct FrameWriter {
    first_timestamp_secs: Option<f64>,
    first_frame_received_at: Option<Instant>,
    last_frame: Option<EncodedFrame>,
    last_frame_received_at: Instant,
}

impl FrameWriter {
    fn new() -> Self {
        Self {
            first_timestamp_secs: None,
            first_frame_received_at: None,
            last_frame: None,
            last_frame_received_at: Instant::now(),
        }
    }
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub struct RecordingState {
    pub active: bool,
    pub output_path: String,
    pub frame_count: u64,
    pub capture_task: Option<tokio::task::JoinHandle<Result<(), String>>>,
    pub shared_frame_count: Option<Arc<AtomicU64>>,
    pub cancel_tx: Option<oneshot::Sender<()>>,
}

impl RecordingState {
    pub fn new() -> Self {
        Self {
            active: false,
            output_path: String::new(),
            frame_count: 0,
            capture_task: None,
            shared_frame_count: None,
            cancel_tx: None,
        }
    }
}

pub fn recording_start(state: &mut RecordingState, path: &str) -> Result<Value, String> {
    if state.active {
        return Err("Recording already active".to_string());
    }

    state.active = true;
    state.output_path = path.to_string();
    state.frame_count = 0;

    Ok(json!({ "started": true, "path": path }))
}

pub fn recording_stop(state: &mut RecordingState) -> Result<Value, String> {
    if !state.active {
        return Err("No recording in progress".to_string());
    }

    state.active = false;

    if state.frame_count == 0 {
        return Err("No frames captured".to_string());
    }

    Ok(json!({ "path": &state.output_path, "frames": state.frame_count }))
}

pub fn recording_restart(state: &mut RecordingState, path: &str) -> Result<Value, String> {
    let previous = if state.active {
        let stop_result = recording_stop(state);
        stop_result
            .ok()
            .and_then(|v| v.get("path").and_then(|p| p.as_str()).map(String::from))
    } else {
        None
    };

    recording_start(state, path)?;

    Ok(json!({
        "restarted": true,
        "previousPath": previous,
        "path": path,
    }))
}

fn build_ffmpeg_command(output_path: &str) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new("ffmpeg");

    cmd.args(["-loglevel", "error"])
        .args(["-f", "image2pipe"])
        .args(["-avioflags", "direct"])
        .args([
            "-fpsprobesize",
            "0",
            "-probesize",
            "32",
            "-analyzeduration",
            "0",
        ])
        .args(["-c:v", "mjpeg", "-i", "pipe:0"])
        .args(["-y", "-an"])
        .args(["-r", &CAPTURE_FPS.to_string()]);

    if output_path.ends_with(".webm") {
        cmd.args([
            "-c:v",
            "vp8",
            "-qmin",
            "0",
            "-qmax",
            "50",
            "-crf",
            "8",
            "-deadline",
            "realtime",
            "-speed",
            "8",
            "-b:v",
            "1M",
        ]);
    } else {
        cmd.args(["-c:v", "libx264", "-preset", "ultrafast"]);
    }

    cmd.args(["-threads", "1"])
        .args(["-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2"])
        .arg(output_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    cmd
}

async fn start_screencast(client: &CdpClient, session_id: &str) -> Result<(), String> {
    client
        .send_command(
            "Page.startScreencast",
            Some(json!({
                "format": "jpeg",
                "quality": 80,
                "everyNthFrame": 1,
            })),
            Some(session_id),
        )
        .await?;
    Ok(())
}

async fn restart_screencast(client: &CdpClient, session_id: &str) {
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        client.send_command_no_params("Page.stopScreencast", Some(session_id)),
    )
    .await;

    let _ =
        tokio::time::timeout(Duration::from_secs(2), start_screencast(client, session_id)).await;
}

async fn capture_screenshot_frame(client: &CdpClient, session_id: &str) -> Option<ScreencastFrame> {
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        client.send_command(
            "Page.captureScreenshot",
            Some(json!({
                "format": "jpeg",
                "quality": 80,
                "fromSurface": true,
            })),
            Some(session_id),
        ),
    )
    .await
    .ok()?
    .ok()?;

    let data = result.get("data").and_then(|v| v.as_str())?;
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data).ok()?;

    Some(ScreencastFrame {
        bytes,
        timestamp_secs: now_secs(),
    })
}

fn is_main_frame_navigation(event: &CdpEvent) -> bool {
    match event.method.as_str() {
        "Page.frameStartedLoading" => event
            .params
            .get("frameId")
            .and_then(|v| v.as_str())
            .is_some(),
        "Page.frameNavigated" => event
            .params
            .get("frame")
            .and_then(|v| v.get("parentId"))
            .and_then(|v| v.as_str())
            .is_none_or(|s| s.is_empty()),
        "Page.loadEventFired" => true,
        _ => false,
    }
}

async fn collect_screencast_frames(
    client: Arc<CdpClient>,
    session_id: String,
    frame_tx: mpsc::UnboundedSender<ScreencastFrame>,
) {
    let mut event_rx = client.subscribe();
    let mut last_restart = Instant::now();

    loop {
        let event = match event_rx.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        };

        if event
            .session_id
            .as_deref()
            .is_some_and(|event_session_id| event_session_id != session_id)
        {
            continue;
        }

        if is_main_frame_navigation(&event) && last_restart.elapsed() >= Duration::from_millis(250)
        {
            last_restart = Instant::now();
            restart_screencast(&client, &session_id).await;
            continue;
        }

        if event.method != "Page.screencastFrame" {
            continue;
        }

        if let Some(screencast_session_id) = event.params.get("sessionId").and_then(|v| v.as_i64())
        {
            let ack_client = Arc::clone(&client);
            let ack_session_id = event
                .session_id
                .clone()
                .unwrap_or_else(|| session_id.clone());
            tokio::spawn(async move {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    ack_client.send_command(
                        "Page.screencastFrameAck",
                        Some(json!({ "sessionId": screencast_session_id })),
                        Some(&ack_session_id),
                    ),
                )
                .await;
            });
        }

        let Some(data) = event.params.get("data").and_then(|v| v.as_str()) else {
            continue;
        };

        let Ok(bytes) = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)
        else {
            continue;
        };

        let timestamp_secs = event
            .params
            .get("metadata")
            .and_then(|metadata| metadata.get("timestamp"))
            .and_then(|timestamp| timestamp.as_f64())
            .unwrap_or_else(now_secs);

        if frame_tx
            .send(ScreencastFrame {
                bytes,
                timestamp_secs,
            })
            .is_err()
        {
            break;
        }
    }
}

async fn write_bytes(
    stdin: &mut tokio::process::ChildStdin,
    bytes: &[u8],
    shared_count: &AtomicU64,
) -> Result<(), String> {
    stdin
        .write_all(bytes)
        .await
        .map_err(|e| format!("ffmpeg stdin write failed: {}", e))?;
    shared_count.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

async fn write_frame(
    stdin: &mut tokio::process::ChildStdin,
    writer: &mut FrameWriter,
    frame: ScreencastFrame,
    shared_count: &AtomicU64,
) -> Result<(), String> {
    let first_timestamp_secs = match writer.first_timestamp_secs {
        Some(timestamp) => timestamp,
        None => {
            writer.first_timestamp_secs = Some(frame.timestamp_secs);
            writer.first_frame_received_at = Some(Instant::now());
            write_bytes(stdin, &frame.bytes, shared_count).await?;
            writer.last_frame = Some(EncodedFrame {
                bytes: frame.bytes,
                timestamp_secs: frame.timestamp_secs,
                frame_number: 0,
            });
            writer.last_frame_received_at = Instant::now();
            return Ok(());
        }
    };

    let elapsed_secs = (frame.timestamp_secs - first_timestamp_secs).max(0.0);
    let frame_number = (elapsed_secs * f64::from(CAPTURE_FPS)).floor() as u64;
    let mut encoded_frame_number = frame_number;

    if let Some(last_frame) = &writer.last_frame {
        let repeat_count = frame_number.saturating_sub(last_frame.frame_number);
        for _ in 0..repeat_count {
            write_bytes(stdin, &last_frame.bytes, shared_count).await?;
        }
        encoded_frame_number = encoded_frame_number.max(last_frame.frame_number);
    }

    writer.last_frame = Some(EncodedFrame {
        bytes: frame.bytes,
        timestamp_secs: frame.timestamp_secs,
        frame_number: encoded_frame_number,
    });
    writer.last_frame_received_at = Instant::now();
    Ok(())
}

async fn write_realtime_padding(
    stdin: &mut tokio::process::ChildStdin,
    writer: &mut FrameWriter,
    shared_count: &AtomicU64,
    extra_secs: f64,
) -> Result<(), String> {
    let (Some(first_timestamp_secs), Some(first_frame_received_at)) =
        (writer.first_timestamp_secs, writer.first_frame_received_at)
    else {
        return Ok(());
    };

    let Some(last_frame) = writer.last_frame.as_mut() else {
        return Ok(());
    };

    let elapsed_secs = first_frame_received_at.elapsed().as_secs_f64() + extra_secs;
    let target_frame_number = (elapsed_secs * f64::from(CAPTURE_FPS)).floor() as u64;
    let repeat_count = target_frame_number.saturating_sub(last_frame.frame_number);

    for _ in 0..repeat_count {
        write_bytes(stdin, &last_frame.bytes, shared_count).await?;
    }

    if repeat_count > 0 {
        last_frame.frame_number = target_frame_number;
        last_frame.timestamp_secs =
            first_timestamp_secs + (target_frame_number as f64 / f64::from(CAPTURE_FPS));
    }

    Ok(())
}

/// Spawn a background task that records CDP screencast frames and pipes them to ffmpeg.
///
/// Chrome emits screencast frames only when compositor frames arrive. Match
/// Playwright's recorder by using each CDP timestamp to duplicate the previous
/// image into a constant-FPS ffmpeg stream, preserving wall-clock duration even
/// when the page is visually idle or the sandbox delivers frames unevenly.
pub fn spawn_recording_task(
    client: Arc<CdpClient>,
    session_id: String,
    output_path: String,
    shared_count: Arc<AtomicU64>,
    cancel_rx: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let mut cancel_rx = std::pin::pin!(cancel_rx);

        let mut ffmpeg = build_ffmpeg_command(&output_path).spawn().map_err(|e| {
            format!(
                "ffmpeg not found or failed to execute: {}. Install ffmpeg to enable recording.",
                e
            )
        })?;

        let mut stdin = ffmpeg
            .stdin
            .take()
            .ok_or_else(|| "Failed to open ffmpeg stdin".to_string())?;

        let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<ScreencastFrame>();
        let mut frame_writer = FrameWriter::new();
        let mut interval =
            tokio::time::interval(Duration::from_millis(1_000 / u64::from(CAPTURE_FPS)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let _ = client
            .send_command_no_params("Page.stopScreencast", Some(&session_id))
            .await;

        if let Some(frame) = capture_screenshot_frame(&client, &session_id).await {
            let _ = frame_tx.send(frame);
        }

        start_screencast(&client, &session_id).await?;

        let event_task = tokio::spawn(collect_screencast_frames(
            Arc::clone(&client),
            session_id.clone(),
            frame_tx,
        ));

        loop {
            tokio::select! {
                _ = &mut cancel_rx => break,
                Some(frame) = frame_rx.recv() => {
                    if write_frame(&mut stdin, &mut frame_writer, frame, shared_count.as_ref()).await.is_err() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    if write_realtime_padding(&mut stdin, &mut frame_writer, shared_count.as_ref(), 0.0).await.is_err() {
                        break;
                    }
                }
            }
        }

        event_task.abort();
        let _ = event_task.await;

        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            client.send_command_no_params("Page.stopScreencast", Some(&session_id)),
        )
        .await;

        write_realtime_padding(&mut stdin, &mut frame_writer, shared_count.as_ref(), 1.0).await?;

        drop(stdin);

        let output = ffmpeg
            .wait_with_output()
            .await
            .map_err(|e| format!("ffmpeg wait failed: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!(
                "ffmpeg failed: {}",
                stderr.chars().take(300).collect::<String>()
            ));
        }

        Ok(())
    })
}

pub async fn stop_recording_task(state: &mut RecordingState) -> Result<(), String> {
    if let Some(tx) = state.cancel_tx.take() {
        let _ = tx.send(());
    }

    let counter = state.shared_frame_count.take();
    let handle = state.capture_task.take();

    let result = if let Some(h) = handle {
        match h.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(format!("Recording task panicked: {}", e)),
        }
    } else {
        Ok(())
    };

    if let Some(c) = counter {
        state.frame_count = c.load(Ordering::Relaxed);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recording_state_new() {
        let state = RecordingState::new();
        assert!(!state.active);
        assert!(state.output_path.is_empty());
        assert_eq!(state.frame_count, 0);
    }

    #[test]
    fn test_recording_start_sets_active() {
        let mut state = RecordingState::new();
        let result = recording_start(&mut state, "/tmp/test.mp4");
        assert!(result.is_ok());
        assert!(state.active);
        assert_eq!(state.output_path, "/tmp/test.mp4");
        assert_eq!(state.frame_count, 0);
    }

    #[test]
    fn test_recording_start_while_active() {
        let mut state = RecordingState::new();
        recording_start(&mut state, "/tmp/test1.mp4").unwrap();
        let result = recording_start(&mut state, "/tmp/test2.mp4");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already active"));
    }

    #[test]
    fn test_recording_stop_not_active() {
        let mut state = RecordingState::new();
        let result = recording_stop(&mut state);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No recording"));
    }

    #[test]
    fn test_recording_stop_no_frames() {
        let mut state = RecordingState::new();
        recording_start(&mut state, "/tmp/test.mp4").unwrap();
        let result = recording_stop(&mut state);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No frames"));
        assert!(!state.active);
    }

    #[test]
    fn test_recording_restart_while_inactive() {
        let mut state = RecordingState::new();
        let result = recording_restart(&mut state, "/tmp/new.webm");
        assert!(result.is_ok());
        assert!(state.active);
        assert_eq!(state.output_path, "/tmp/new.webm");
    }

    #[test]
    fn test_recording_restart_while_active() {
        let mut state = RecordingState::new();
        recording_start(&mut state, "/tmp/old.webm").unwrap();
        state.frame_count = 10;
        let result = recording_restart(&mut state, "/tmp/new.webm").unwrap();
        assert!(state.active);
        assert_eq!(state.output_path, "/tmp/new.webm");
        assert_eq!(state.frame_count, 0);
        assert_eq!(result["previousPath"], "/tmp/old.webm");
    }

    #[test]
    fn test_build_ffmpeg_command_webm() {
        let cmd = build_ffmpeg_command("/tmp/out.webm");
        let args: Vec<&std::ffi::OsStr> = cmd.as_std().get_args().collect();
        let args_str: Vec<&str> = args.iter().filter_map(|a| a.to_str()).collect();
        assert!(args_str.contains(&"vp8"));
        assert!(args_str.contains(&"-r"));
        assert!(args_str.contains(&"25"));
        assert!(args_str.contains(&"/tmp/out.webm"));
    }

    #[test]
    fn test_build_ffmpeg_command_mp4() {
        let cmd = build_ffmpeg_command("/tmp/out.mp4");
        let args: Vec<&std::ffi::OsStr> = cmd.as_std().get_args().collect();
        let args_str: Vec<&str> = args.iter().filter_map(|a| a.to_str()).collect();
        assert!(args_str.contains(&"libx264"));
        assert!(args_str.contains(&"/tmp/out.mp4"));
    }
}
