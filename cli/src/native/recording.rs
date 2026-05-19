use serde_json::{json, Value};
use std::env;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, Notify};

use super::browser::should_track_target;
use super::cdp::client::CdpClient;
use super::cdp::types::{
    AttachToTargetParams, AttachToTargetResult, CdpEvent, GetTargetsResult, TargetInfo,
};
use super::stream::{FrameMetadata, StreamServer};

const CAPTURE_FPS: u32 = 25;
const DEFAULT_SCREENSHOT_CAPTURE_FPS: u32 = 8;

struct ScreencastFrame {
    bytes: Vec<u8>,
    base64_data: String,
    metadata: FrameMetadata,
    timestamp_secs: f64,
}

struct EncodedFrame {
    bytes: Vec<u8>,
    base64_data: String,
    metadata: FrameMetadata,
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
    pub control: Option<Arc<RecordingControl>>,
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
            control: None,
        }
    }
}

pub struct RecordingControl {
    paused: AtomicBool,
    restart_requested: AtomicBool,
    capture_should_run: AtomicBool,
    capture_suspended: AtomicBool,
    capture_notify: Notify,
    screencast_session_id: Mutex<Option<String>>,
    requested_target_id: Mutex<Option<String>>,
}

impl Default for RecordingControl {
    fn default() -> Self {
        Self {
            paused: AtomicBool::new(false),
            restart_requested: AtomicBool::new(false),
            capture_should_run: AtomicBool::new(true),
            capture_suspended: AtomicBool::new(false),
            capture_notify: Notify::new(),
            screencast_session_id: Mutex::new(None),
            requested_target_id: Mutex::new(None),
        }
    }
}

impl RecordingControl {
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub fn request_restart(&self) {
        self.restart_requested.store(true, Ordering::SeqCst);
    }

    pub fn take_restart_requested(&self) -> bool {
        self.restart_requested.swap(false, Ordering::SeqCst)
    }

    pub async fn suspend_capture(&self, timeout: Duration) -> bool {
        self.capture_should_run.store(false, Ordering::SeqCst);
        self.capture_notify.notify_waiters();
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if self.capture_suspended.load(Ordering::SeqCst) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        self.capture_suspended.load(Ordering::SeqCst)
    }

    pub async fn resume_capture(&self, timeout: Duration) -> bool {
        self.capture_should_run.store(true, Ordering::SeqCst);
        self.capture_notify.notify_waiters();
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            if !self.capture_suspended.load(Ordering::SeqCst) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        !self.capture_suspended.load(Ordering::SeqCst)
    }

    fn capture_should_run(&self) -> bool {
        self.capture_should_run.load(Ordering::SeqCst)
    }

    fn set_capture_suspended(&self, suspended: bool) {
        self.capture_suspended.store(suspended, Ordering::SeqCst);
    }

    fn is_capture_suspended(&self) -> bool {
        self.capture_suspended.load(Ordering::SeqCst)
    }

    async fn wait_for_capture_change(&self) {
        self.capture_notify.notified().await;
    }

    pub fn switch_capture_target(&self, target_id: String) {
        if let Ok(mut guard) = self.requested_target_id.lock() {
            *guard = Some(target_id);
        }
        self.capture_notify.notify_waiters();
    }

    fn take_requested_target_id(&self) -> Option<String> {
        self.requested_target_id
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }

    pub fn set_screencast_session_id(&self, session_id: String) {
        if let Ok(mut guard) = self.screencast_session_id.lock() {
            *guard = Some(session_id);
        }
    }

    fn clear_screencast_session_id(&self) {
        if let Ok(mut guard) = self.screencast_session_id.lock() {
            *guard = None;
        }
    }

    pub fn screencast_session_id(&self) -> Option<String> {
        self.screencast_session_id
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
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
    let quality = env::var("AGENT_BROWSER_RECORDING_QUALITY")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| (1..=100).contains(value))
        .unwrap_or(70);
    let every_nth_frame = env::var("AGENT_BROWSER_RECORDING_EVERY_NTH_FRAME")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(6);
    let max_width = env::var("AGENT_BROWSER_RECORDING_MAX_WIDTH")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1280);
    let max_height = env::var("AGENT_BROWSER_RECORDING_MAX_HEIGHT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(720);

    client
        .send_command_with_timeout(
            "Page.startScreencast",
            Some(json!({
                "format": "jpeg",
                "quality": quality,
                "everyNthFrame": every_nth_frame,
                "maxWidth": max_width,
                "maxHeight": max_height,
            })),
            Some(session_id),
            Duration::from_secs(2),
        )
        .await?;
    Ok(())
}

async fn restart_screencast(client: &CdpClient, session_id: &str) {
    let _ = client
        .send_command_with_timeout(
            "Page.stopScreencast",
            None,
            Some(session_id),
            Duration::from_secs(2),
        )
        .await;

    let _ = start_screencast(client, session_id).await;
}

pub async fn start_recording_screencast(
    client: &CdpClient,
    session_id: &str,
) -> Result<(), String> {
    start_screencast(client, session_id).await
}

pub async fn stop_recording_screencast(client: &CdpClient, session_id: &str) -> Result<(), String> {
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

async fn ack_screencast_frame(client: &CdpClient, session_id: &str, screencast_session_id: i64) {
    let _ = client
        .send_command_no_wait(
            "Page.screencastFrameAck",
            Some(json!({ "sessionId": screencast_session_id })),
            Some(session_id),
        )
        .await;
}

async fn capture_screenshot_frame(client: &CdpClient, session_id: &str) -> Option<ScreencastFrame> {
    let result = client
        .send_command_with_timeout(
            "Page.captureScreenshot",
            Some(json!({
                "format": "jpeg",
                "quality": 80,
                "fromSurface": true,
            })),
            Some(session_id),
            Duration::from_secs(3),
        )
        .await
        .ok()?;

    let data = result.get("data").and_then(|v| v.as_str())?;
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data).ok()?;
    Some(ScreencastFrame {
        bytes,
        base64_data: data.to_string(),
        metadata: FrameMetadata {
            timestamp: (now_secs() * 1000.0) as u64,
            ..FrameMetadata::default()
        },
        timestamp_secs: now_secs(),
    })
}

fn is_main_frame_load_start(event: &CdpEvent) -> bool {
    event.method == "Page.frameStartedLoading"
        && event
            .params
            .get("frameId")
            .and_then(|v| v.as_str())
            .is_some()
}

fn is_main_frame_navigation_ready(event: &CdpEvent) -> bool {
    match event.method.as_str() {
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

fn target_event_matches(
    event: &CdpEvent,
    method: &str,
    field: &str,
    expected: Option<&str>,
) -> bool {
    event.method == method
        && expected.is_some_and(|value| {
            event
                .params
                .get(field)
                .and_then(|v| v.as_str())
                .is_some_and(|event_value| event_value == value)
        })
}

fn should_recover_recording_target(
    event: &CdpEvent,
    session_id: &str,
    target_id: Option<&str>,
) -> bool {
    (event.session_id.as_deref() == Some(session_id) && event.method == "Target.detachedFromTarget")
        || target_event_matches(
            event,
            "Target.detachedFromTarget",
            "sessionId",
            Some(session_id),
        )
        || target_event_matches(event, "Target.targetDestroyed", "targetId", target_id)
}

struct ReplacementTarget {
    target_id: String,
    session_id: String,
}

fn choose_replacement_target(
    targets: Vec<TargetInfo>,
    current_target_id: Option<&str>,
    browser_context_id: Option<&str>,
) -> Option<TargetInfo> {
    let candidates: Vec<TargetInfo> = targets
        .into_iter()
        .filter(should_track_target)
        .filter(|target| {
            current_target_id
                .map(|id| target.target_id != id)
                .unwrap_or(true)
        })
        .collect();

    candidates
        .iter()
        .find(|target| {
            browser_context_id
                .map(|context_id| target.browser_context_id.as_deref() == Some(context_id))
                .unwrap_or(true)
        })
        .cloned()
        .or_else(|| candidates.into_iter().next())
}

async fn enable_recording_domains(client: &CdpClient, session_id: &str) -> Result<(), String> {
    client
        .send_command_with_timeout(
            "Page.enable",
            None,
            Some(session_id),
            Duration::from_secs(2),
        )
        .await?;
    Ok(())
}

async fn attach_to_replacement_target(
    client: &CdpClient,
    current_target_id: Option<&str>,
    browser_context_id: Option<&str>,
) -> Result<ReplacementTarget, String> {
    let result: GetTargetsResult = client
        .send_command_typed("Target.getTargets", &json!({}), None)
        .await?;

    let target =
        choose_replacement_target(result.target_infos, current_target_id, browser_context_id)
            .ok_or_else(|| "No replacement page target found".to_string())?;

    let attach: AttachToTargetResult = client
        .send_command_typed(
            "Target.attachToTarget",
            &AttachToTargetParams {
                target_id: target.target_id.clone(),
                flatten: true,
            },
            None,
        )
        .await?;

    enable_recording_domains(client, &attach.session_id).await?;
    start_screencast(client, &attach.session_id).await?;

    Ok(ReplacementTarget {
        target_id: target.target_id,
        session_id: attach.session_id,
    })
}

async fn attach_recording_session(
    client: &CdpClient,
    target_id: Option<&str>,
) -> Result<String, String> {
    let target_id = target_id.ok_or_else(|| "No active recording target id".to_string())?;
    let attach: AttachToTargetResult = client
        .send_command_typed(
            "Target.attachToTarget",
            &AttachToTargetParams {
                target_id: target_id.to_string(),
                flatten: true,
            },
            None,
        )
        .await?;
    enable_recording_domains(client, &attach.session_id).await?;
    Ok(attach.session_id)
}

async fn detach_recording_session(client: &CdpClient, session_id: &str) {
    let params = Some(json!({ "sessionId": session_id }));

    if client
        .send_command_with_timeout(
            "Target.detachFromTarget",
            params.clone(),
            None,
            Duration::from_millis(750),
        )
        .await
        .is_err()
    {
        let _ = client
            .send_command_no_wait("Target.detachFromTarget", params, None)
            .await;
    }
}

async fn suspend_recording_session(client: &CdpClient, session_id: &str) {
    let _ = client
        .send_command_with_timeout(
            "Page.stopScreencast",
            None,
            Some(session_id),
            Duration::from_millis(500),
        )
        .await;

    detach_recording_session(client, session_id).await;
}

async fn suspend_recording_session_no_wait(client: &CdpClient, session_id: &str) {
    let _ = client
        .send_command_no_wait("Page.stopScreencast", None, Some(session_id))
        .await;
    let _ = client
        .send_command_no_wait(
            "Target.detachFromTarget",
            Some(json!({ "sessionId": session_id })),
            None,
        )
        .await;
}

async fn attach_and_start_recording_session(
    client: &CdpClient,
    target_id: Option<&str>,
    current_target_id: Option<&str>,
    browser_context_id: Option<&str>,
) -> Result<ReplacementTarget, String> {
    if let Some(target_id) = target_id {
        if let Ok(session_id) = attach_recording_session(client, Some(target_id)).await {
            start_screencast(client, &session_id).await?;
            return Ok(ReplacementTarget {
                target_id: target_id.to_string(),
                session_id,
            });
        }
    }

    attach_to_replacement_target(client, current_target_id, browser_context_id).await
}

async fn collect_screencast_frames(
    client: Arc<CdpClient>,
    initial_session_id: String,
    mut target_id: Option<String>,
    browser_context_id: Option<String>,
    frame_tx: mpsc::UnboundedSender<ScreencastFrame>,
    preview_stream: Option<Arc<StreamServer>>,
    control: Arc<RecordingControl>,
) {
    let mut event_rx = client.subscribe();
    let mut session_id = Some(initial_session_id);
    control.set_capture_suspended(false);
    if let Some(session_id) = session_id.as_deref() {
        let _ = start_screencast(&client, session_id).await;
    }

    loop {
        if let Some(requested_target_id) = control.take_requested_target_id() {
            if target_id.as_deref() != Some(requested_target_id.as_str()) {
                if let Some(active_session_id) = session_id.take() {
                    control.clear_screencast_session_id();
                    suspend_recording_session_no_wait(&client, &active_session_id).await;
                }
                target_id = Some(requested_target_id);
                control.set_capture_suspended(true);
            }
        }

        if !control.capture_should_run() {
            if let Some(active_session_id) = session_id.take() {
                control.clear_screencast_session_id();
                suspend_recording_session(&client, &active_session_id).await;
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            control.set_capture_suspended(true);

            tokio::select! {
                _ = control.wait_for_capture_change() => continue,
                event = event_rx.recv() => {
                    match event {
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }

        if session_id.is_none() {
            match attach_and_start_recording_session(
                &client,
                target_id.as_deref(),
                target_id.as_deref(),
                browser_context_id.as_deref(),
            )
            .await
            {
                Ok(replacement) => {
                    control.set_screencast_session_id(replacement.session_id.clone());
                    control.set_capture_suspended(false);
                    session_id = Some(replacement.session_id);
                    target_id = Some(replacement.target_id);
                }
                Err(_) => {
                    control.set_capture_suspended(true);
                    tokio::select! {
                        _ = control.wait_for_capture_change() => continue,
                        _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
                    }
                }
            }
        }

        let active_session_id = match session_id.clone() {
            Some(session_id) => session_id,
            None => continue,
        };

        let event = tokio::select! {
            _ = control.wait_for_capture_change() => continue,
            event = event_rx.recv() => {
                match event {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        };

        if should_recover_recording_target(&event, &active_session_id, target_id.as_deref()) {
            if let Ok(replacement) = attach_to_replacement_target(
                &client,
                target_id.as_deref(),
                browser_context_id.as_deref(),
            )
            .await
            {
                control.set_screencast_session_id(replacement.session_id.clone());
                control.set_capture_suspended(false);
                session_id = Some(replacement.session_id);
                target_id = Some(replacement.target_id);
            }
            continue;
        }

        if event
            .session_id
            .as_deref()
            .is_some_and(|event_session_id| event_session_id != active_session_id)
        {
            continue;
        }

        if is_main_frame_navigation_ready(&event) || event.method == "Page.frameStoppedLoading" {
            tokio::time::sleep(Duration::from_millis(750)).await;
            restart_screencast(&client, &active_session_id).await;
            if let Some(frame) = capture_screenshot_frame(&client, &active_session_id).await {
                if let Some(ref stream) = preview_stream {
                    stream.broadcast_screencast_frame(&frame.base64_data, &frame.metadata);
                }
                if frame_tx.send(frame).is_err() {
                    break;
                }
            }
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
                .unwrap_or_else(|| active_session_id.clone());
            tokio::spawn(async move {
                ack_screencast_frame(&ack_client, &ack_session_id, screencast_session_id).await;
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

        let meta = event.params.get("metadata");
        let metadata = FrameMetadata {
            offset_top: meta
                .and_then(|m| m.get("offsetTop"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            page_scale_factor: meta
                .and_then(|m| m.get("pageScaleFactor"))
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0),
            device_width: meta
                .and_then(|m| m.get("deviceWidth"))
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(1280),
            device_height: meta
                .and_then(|m| m.get("deviceHeight"))
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(720),
            scroll_offset_x: meta
                .and_then(|m| m.get("scrollOffsetX"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            scroll_offset_y: meta
                .and_then(|m| m.get("scrollOffsetY"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            timestamp: (timestamp_secs * 1000.0) as u64,
        };

        if let Some(ref stream) = preview_stream {
            stream.broadcast_screencast_frame(data, &metadata);
        }

        if frame_tx
            .send(ScreencastFrame {
                bytes,
                base64_data: data.to_string(),
                metadata,
                timestamp_secs,
            })
            .is_err()
        {
            break;
        }
    }
}

async fn collect_screenshot_frames(
    client: Arc<CdpClient>,
    session_id: String,
    frame_tx: mpsc::UnboundedSender<ScreencastFrame>,
    preview_stream: Option<Arc<StreamServer>>,
    control: Arc<RecordingControl>,
) {
    let screenshot_fps = env::var("AGENT_BROWSER_SCREENSHOT_CAPTURE_FPS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| (1..=CAPTURE_FPS).contains(value))
        .unwrap_or(DEFAULT_SCREENSHOT_CAPTURE_FPS);
    let mut interval =
        tokio::time::interval(Duration::from_millis(1_000 / u64::from(screenshot_fps)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        if control.is_paused() {
            continue;
        }

        let Some(frame) = capture_screenshot_frame(&client, &session_id).await else {
            continue;
        };

        if let Some(ref stream) = preview_stream {
            stream.broadcast_screencast_frame(&frame.base64_data, &frame.metadata);
        }

        if frame_tx.send(frame).is_err() {
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

fn broadcast_encoded_frame(preview_stream: Option<&Arc<StreamServer>>, frame: &EncodedFrame) {
    if let Some(stream) = preview_stream {
        let mut metadata = frame.metadata.clone();
        metadata.timestamp = (now_secs() * 1000.0) as u64;
        stream.broadcast_screencast_frame(&frame.base64_data, &metadata);
    }
}

async fn write_frame(
    stdin: &mut tokio::process::ChildStdin,
    writer: &mut FrameWriter,
    frame: ScreencastFrame,
    shared_count: &AtomicU64,
    preview_stream: Option<&Arc<StreamServer>>,
) -> Result<(), String> {
    let first_timestamp_secs = match writer.first_timestamp_secs {
        Some(timestamp) => timestamp,
        None => {
            writer.first_timestamp_secs = Some(frame.timestamp_secs);
            writer.first_frame_received_at = Some(Instant::now());
            write_bytes(stdin, &frame.bytes, shared_count).await?;
            writer.last_frame = Some(EncodedFrame {
                bytes: frame.bytes,
                base64_data: frame.base64_data,
                metadata: frame.metadata,
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
            broadcast_encoded_frame(preview_stream, last_frame);
        }
        encoded_frame_number = encoded_frame_number.max(last_frame.frame_number);
    }

    writer.last_frame = Some(EncodedFrame {
        bytes: frame.bytes,
        base64_data: frame.base64_data,
        metadata: frame.metadata,
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
    preview_stream: Option<&Arc<StreamServer>>,
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
        broadcast_encoded_frame(preview_stream, last_frame);
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
    target_id: Option<String>,
    browser_context_id: Option<String>,
    output_path: String,
    shared_count: Arc<AtomicU64>,
    cancel_rx: oneshot::Receiver<()>,
    preview_stream: Option<Arc<StreamServer>>,
    control: Arc<RecordingControl>,
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

        let use_screencast = matches!(
            env::var("AGENT_BROWSER_RECORDING_SOURCE")
                .unwrap_or_else(|_| "screencast".to_string())
                .as_str(),
            "screencast"
        );

        let recording_session_id = if use_screencast {
            attach_recording_session(&client, target_id.as_deref())
                .await
                .unwrap_or_else(|_| session_id.clone())
        } else {
            session_id.clone()
        };
        control.set_screencast_session_id(recording_session_id.clone());

        let _ = stop_recording_screencast(&client, &recording_session_id).await;

        if let Some(frame) = capture_screenshot_frame(&client, &recording_session_id).await {
            if let Some(ref stream) = preview_stream {
                stream.broadcast_screencast_frame(&frame.base64_data, &frame.metadata);
            }
            write_frame(
                &mut stdin,
                &mut frame_writer,
                frame,
                shared_count.as_ref(),
                preview_stream.as_ref(),
            )
            .await?;
        }

        let event_task = if use_screencast {
            // Subscribe before starting screencast. Idle pages can emit a
            // single initial frame immediately, and missing it leaves ffmpeg
            // with no seed frame until the page changes again.
            let task = tokio::spawn(collect_screencast_frames(
                Arc::clone(&client),
                recording_session_id.clone(),
                target_id.clone(),
                browser_context_id.clone(),
                frame_tx.clone(),
                preview_stream.clone(),
                control.clone(),
            ));
            task
        } else {
            tokio::spawn(collect_screenshot_frames(
                Arc::clone(&client),
                recording_session_id.clone(),
                frame_tx.clone(),
                preview_stream.clone(),
                control.clone(),
            ))
        };

        loop {
            tokio::select! {
                _ = &mut cancel_rx => break,
                Some(frame) = frame_rx.recv() => {
                    if write_frame(
                        &mut stdin,
                        &mut frame_writer,
                        frame,
                        shared_count.as_ref(),
                        preview_stream.as_ref(),
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
                _ = interval.tick() => {
                    if control.is_capture_suspended() {
                        if let Some(last_frame) = frame_writer.last_frame.as_ref() {
                            broadcast_encoded_frame(preview_stream.as_ref(), last_frame);
                        }
                        continue;
                    }
                    if write_realtime_padding(
                        &mut stdin,
                        &mut frame_writer,
                        shared_count.as_ref(),
                        0.0,
                        preview_stream.as_ref(),
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
            }
        }

        event_task.abort();
        let _ = event_task.await;

        if let Some(session_id) = control.screencast_session_id() {
            let _ = stop_recording_screencast(&client, &session_id).await;
            detach_recording_session(&client, &session_id).await;
            control.clear_screencast_session_id();
        }

        write_realtime_padding(
            &mut stdin,
            &mut frame_writer,
            shared_count.as_ref(),
            1.0,
            preview_stream.as_ref(),
        )
        .await?;

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
    if let Some(control) = state.control.as_ref() {
        control.resume();
    }

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
    state.control = None;

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
