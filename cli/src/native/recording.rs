use serde_json::{json, Value};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

use super::cdp::client::CdpClient;
use super::cdp::types::{
    AttachToTargetParams, AttachToTargetResult, CaptureScreenshotParams, CaptureScreenshotResult,
    GetTargetsResult, TargetInfo,
};

const CAPTURE_INTERVAL_MS: u64 = 100;
const CAPTURE_FPS: u32 = 10;

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

    cmd.args(["-y"])
        .args(["-avioflags", "direct"])
        .args([
            "-fpsprobesize",
            "0",
            "-probesize",
            "32",
            "-analyzeduration",
            "0",
        ])
        .args([
            "-f",
            "image2pipe",
            "-c:v",
            "mjpeg",
            "-framerate",
            &CAPTURE_FPS.to_string(),
            "-i",
            "pipe:0",
        ])
        .args(["-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2"]);

    if output_path.ends_with(".webm") {
        cmd.args(["-c:v", "libvpx", "-crf", "30", "-b:v", "1M"]);
    } else {
        cmd.args(["-c:v", "libx264", "-preset", "ultrafast"]);
    }

    cmd.args(["-pix_fmt", "yuv420p", "-threads", "1"])
        .arg(output_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    cmd
}

/// Spawn a background task that captures screenshots at a fixed interval
/// and pipes them to ffmpeg in real-time.
pub fn spawn_recording_task(
    client: Arc<CdpClient>,
    session_id: String,
    target_id: Option<String>,
    browser_context_id: Option<String>,
    output_path: String,
    shared_count: Arc<AtomicU64>,
    cancel_rx: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let mut session_id = session_id;
        let mut target_id = target_id;
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

        let mut interval = tokio::time::interval(Duration::from_millis(CAPTURE_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let params = CaptureScreenshotParams {
            format: Some("jpeg".to_string()),
            quality: Some(80),
            clip: None,
            from_surface: Some(true),
            capture_beyond_viewport: None,
        };

        loop {
            tokio::select! {
                _ = &mut cancel_rx => break,
                _ = interval.tick() => {}
            }

            let result: Result<CaptureScreenshotResult, _> = client
                .send_command_typed("Page.captureScreenshot", &params, Some(&session_id))
                .await;

            let screenshot = match result {
                Ok(s) => s,
                Err(_) => {
                    if let Ok(replacement) = attach_to_replacement_target(
                        &client,
                        target_id.as_deref(),
                        browser_context_id.as_deref(),
                    )
                    .await
                    {
                        target_id = Some(replacement.target_id);
                        session_id = replacement.session_id;
                    }
                    continue;
                }
            };

            let bytes = match base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                &screenshot.data,
            ) {
                Ok(b) => b,
                Err(_) => continue,
            };

            if stdin.write_all(&bytes).await.is_err() {
                break;
            }
            shared_count.fetch_add(1, Ordering::Relaxed);
        }

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

struct ReplacementTarget {
    target_id: String,
    session_id: String,
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

    Ok(ReplacementTarget {
        target_id: target.target_id,
        session_id: attach.session_id,
    })
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

fn should_track_target(target: &TargetInfo) -> bool {
    (target.target_type == "page" || target.target_type == "webview")
        && !is_internal_chrome_target(&target.url)
}

fn is_internal_chrome_target(url: &str) -> bool {
    url.starts_with("chrome://")
        || url.starts_with("chrome-extension://")
        || url.starts_with("devtools://")
}

async fn enable_recording_domains(client: &CdpClient, session_id: &str) -> Result<(), String> {
    client
        .send_command_no_params("Page.enable", Some(session_id))
        .await?;
    client
        .send_command_no_params("Runtime.enable", Some(session_id))
        .await?;
    let _ = client
        .send_command_no_params("Runtime.runIfWaitingForDebugger", Some(session_id))
        .await;
    client
        .send_command_no_params("Network.enable", Some(session_id))
        .await?;
    Ok(())
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

    fn target(target_id: &str, url: &str, browser_context_id: Option<&str>) -> TargetInfo {
        TargetInfo {
            target_id: target_id.to_string(),
            target_type: "page".to_string(),
            title: String::new(),
            url: url.to_string(),
            attached: None,
            browser_context_id: browser_context_id.map(str::to_string),
        }
    }

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
    fn test_choose_replacement_target_prefers_same_browser_context() {
        let result = choose_replacement_target(
            vec![
                target("old", "https://example.com", Some("ctx-a")),
                target("other", "https://example.org", Some("ctx-b")),
                target("new", "https://example.com/done", Some("ctx-a")),
            ],
            Some("old"),
            Some("ctx-a"),
        )
        .expect("replacement target");

        assert_eq!(result.target_id, "new");
    }

    #[test]
    fn test_choose_replacement_target_ignores_internal_chrome_targets() {
        let result = choose_replacement_target(
            vec![
                target("old", "https://example.com", Some("ctx-a")),
                target(
                    "devtools",
                    "devtools://devtools/bundled/inspector.html",
                    Some("ctx-a"),
                ),
                target("new", "https://example.com/done", Some("ctx-a")),
            ],
            Some("old"),
            Some("ctx-a"),
        )
        .expect("replacement target");

        assert_eq!(result.target_id, "new");
    }

    #[test]
    fn test_choose_replacement_target_falls_back_when_context_id_missing() {
        let result = choose_replacement_target(
            vec![
                target("old", "https://example.com", Some("ctx-a")),
                target("new", "https://example.com/done", None),
            ],
            Some("old"),
            Some("ctx-a"),
        )
        .expect("replacement target");

        assert_eq!(result.target_id, "new");
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
        assert!(args_str.contains(&"libvpx"));
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
