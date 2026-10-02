use std::{
    collections::BTreeSet,
    net::TcpStream,
    process::{Child, Command, ExitStatus},
    thread::sleep,
    time::Duration,
};

use anyhow::{anyhow, Result};
use serde_json::Value as JsonValue;
use tracing::{info, warn};

use crate::setup::{isolate_python_child_environment, venv_python};
use crate::{launcher_trust_secret, TRUST_SECRET_ENV};
use crate::window_util::CreateNoWindow as _;

const BACKEND_STARTUP_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub(crate) struct BackendStartupTimeout {
    port: u16,
}

impl std::fmt::Display for BackendStartupTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Timeout waiting for port {} to be ready",
            self.port
        )
    }
}

impl std::error::Error for BackendStartupTimeout {}

pub(crate) fn is_backend_startup_timeout(error: &anyhow::Error) -> bool {
    error.downcast_ref::<BackendStartupTimeout>().is_some()
}

#[derive(Clone, Debug)]
pub struct WebuiLaunchConfig {
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub cdn: bool,
    pub ssl_key: Option<String>,
    pub ssl_cert: Option<String>,
    pub run: Vec<String>,
}

impl WebuiLaunchConfig {
    pub fn from_deploy_config(config: Option<&JsonValue>) -> Self {
        let webui = config
            .and_then(|config| config.get("Deploy"))
            .and_then(|deploy| deploy.get("Webui"));

        Self {
            host: webui
                .and_then(|webui| webui.get("WebuiHost"))
                .and_then(value_as_string)
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "127.0.0.1".to_owned()),
            port: webui
                .and_then(|webui| webui.get("WebuiPort"))
                .and_then(value_as_u16)
                .unwrap_or(22267),
            password: webui
                .and_then(|webui| webui.get("Password"))
                .and_then(value_as_string)
                .filter(|value| !value.trim().is_empty()),
            cdn: webui
                .and_then(|webui| webui.get("CDN"))
                .and_then(value_as_bool)
                .unwrap_or(false),
            ssl_key: webui
                .and_then(|webui| webui.get("WebuiSSLKey"))
                .and_then(value_as_string)
                .filter(|value| !value.trim().is_empty()),
            ssl_cert: webui
                .and_then(|webui| webui.get("WebuiSSLCert"))
                .and_then(value_as_string)
                .filter(|value| !value.trim().is_empty()),
            run: webui
                .and_then(|webui| webui.get("Run"))
                .map(value_as_string_list)
                .unwrap_or_default(),
        }
    }

    fn args(&self) -> Vec<String> {
        let mut args = vec![
            "gui.py".to_owned(),
            "--host".to_owned(),
            self.host.clone(),
            "--port".to_owned(),
            self.port.to_string(),
        ];

        if let Some(password) = &self.password {
            args.push("--key".to_owned());
            args.push(password.clone());
        }
        if self.cdn {
            args.push("--cdn".to_owned());
        }
        if let Some(ssl_key) = &self.ssl_key {
            args.push("--ssl-key".to_owned());
            args.push(ssl_key.clone());
        }
        if let Some(ssl_cert) = &self.ssl_cert {
            args.push("--ssl-cert".to_owned());
            args.push(ssl_cert.clone());
        }
        if !self.run.is_empty() {
            args.push("--run".to_owned());
            args.extend(self.run.iter().cloned());
        }

        args
    }
}

fn value_as_string(value: &JsonValue) -> Option<String> {
    if let Some(value) = value.as_str() {
        Some(value.to_owned())
    } else if value.is_null() {
        None
    } else {
        Some(value.to_string())
    }
}

fn value_as_u16(value: &JsonValue) -> Option<u16> {
    if let Some(value) = value.as_u64() {
        u16::try_from(value).ok()
    } else {
        value.as_str()?.parse::<u16>().ok()
    }
}

fn value_as_bool(value: &JsonValue) -> Option<bool> {
    if let Some(value) = value.as_bool() {
        Some(value)
    } else {
        match value.as_str()?.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        }
    }
}

fn value_as_string_list(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .filter_map(value_as_string)
            .filter(|value| !value.trim().is_empty())
            .collect(),
        _ => value_as_string(value)
            .filter(|value| !value.trim().is_empty())
            .into_iter()
            .collect(),
    }
}

/// 启动器承载的 gui.py 进程。
///
/// 这里**刻意不用进程组 / Job Object**：command-group 创建的 Job 只设
/// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`、未设 `BREAKAWAY_OK`，于是
/// 1) 关闭启动器时 `TerminateJobObject` 会连坐杀掉 Job 内的一切进程，
///    包括 gui.py 用 `cmd /c start` 拉起的模拟器（`start` 只脱离父子链，
///    不脱离 Job）；
/// 2) 因为没设 `BREAKAWAY_OK`，子进程也无法用 `CREATE_BREAKAWAY_FROM_JOB`
///    自保（实测被拒，ERROR_ACCESS_DENIED）。
/// 实测（2026-10-02）：MuMu 的 MuMuNxMain.exe 正因此被一并终止。
/// 现在只记录 PID、只终止这一个进程；漏网的 worker 由 Drop 里的
/// `ALAS_LAUNCHER_PID` 扫描兜底，残留端口占用由启动时的
/// `kill_processes_using_port` 清理。
pub struct ManagedBackend {
    child: Option<Child>,
}

impl ManagedBackend {
    pub fn new(config: &WebuiLaunchConfig) -> Result<Self> {
        std::env::set_var("ALAS_LAUNCHER_PID", format!("{}", std::process::id()));
        kill_processes_using_port(config.port)?;

        let mut command = Command::new(venv_python());
        command.args(config.args());
        // 注入启动器信任密钥，WebUI 据此为启动器窗口开启免密；子进程缺省继承，
        // isolate_python_child_environment 不清理该键。
        command.env(TRUST_SECRET_ENV, launcher_trust_secret());
        isolate_python_child_environment(&mut command);
        let child = command.create_no_window().spawn()?;
        let mut res = Self { child: Some(child) };

        let address = format!("127.0.0.1:{}", config.port).parse().unwrap();
        let start_time = std::time::Instant::now();
        while start_time.elapsed() < BACKEND_STARTUP_TIMEOUT {
            if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
                return Ok(res);
            }
            if let Some(child) = res.child.as_mut() {
                if let Some(status) = child.try_wait()? {
                    return Err(anyhow!(
                        "Backend exited before port {} was ready: {}",
                        config.port,
                        status
                    ));
                }
            }
            sleep(Duration::from_millis(100));
        }
        res.terminate().map_err(|error| {
            anyhow!(
                "Failed to stop timed out backend on port {} before recovery: {error:#}",
                config.port
            )
        })?;
        Err(BackendStartupTimeout { port: config.port }.into())
    }

    pub fn terminate(&mut self) -> Result<ExitStatus> {
        if let Some(mut child) = self.child.take() {
            #[cfg(unix)]
            {
                // 只对 backend 自身发信号，不再像 Job 那样连坐它的后代进程
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;

                let _ = kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM);
                let start_time = std::time::Instant::now();
                while start_time.elapsed() < Duration::from_millis(500) {
                    if let Ok(Some(exit_status)) = child.try_wait() {
                        return Ok(exit_status);
                    }
                    sleep(Duration::from_millis(100));
                }
                warn!("gui.py didn't exit, killing it...");
            }
            child.kill()?;
            Ok(child.wait()?)
        } else {
            Ok(ExitStatus::default())
        }
    }
}

fn kill_processes_using_port(port: u16) -> Result<()> {
    let pids = match pids_using_tcp_port(port) {
        Ok(pids) => pids,
        Err(e) => {
            warn!("Unable to scan processes using port {}: {}", port, e);
            return Ok(());
        }
    };
    if pids.is_empty() {
        return Ok(());
    }

    let current_pid = std::process::id();
    let sys = sysinfo::System::new_all();
    for pid in pids {
        if pid == 0 || pid == current_pid {
            continue;
        }

        let sys_pid = sysinfo::Pid::from_u32(pid);
        match sys.process(sys_pid) {
            Some(process) => {
                info!(
                    "Killing process {} ({}) using configured WebUI port {}",
                    pid,
                    process.name().to_string_lossy(),
                    port
                );
                if !process.kill() {
                    warn!("Failed to kill process {} using port {}", pid, port);
                }
            }
            None => {
                warn!(
                    "Process {} was using port {}, but exited before it could be killed",
                    pid, port
                );
            }
        }
    }

    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(5) {
        match pids_using_tcp_port(port) {
            Ok(pids) if pids.is_empty() => return Ok(()),
            Ok(_) => sleep(Duration::from_millis(100)),
            Err(e) => {
                warn!("Unable to verify port {} was released: {}", port, e);
                return Ok(());
            }
        }
    }

    warn!("Timed out waiting for port {} to be released", port);
    Ok(())
}

#[cfg(windows)]
fn pids_using_tcp_port(port: u16) -> Result<BTreeSet<u32>> {
    let output = Command::new("netstat")
        .args(["-ano", "-p", "tcp"])
        .create_no_window()
        .output()?;
    if !output.status.success() {
        return Err(anyhow!("netstat failed with status {}", output.status));
    }

    Ok(parse_windows_netstat_pids(&output.stdout, port))
}

#[cfg(windows)]
fn parse_windows_netstat_pids(output: &[u8], port: u16) -> BTreeSet<u32> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() < 5
                || !parts[0].eq_ignore_ascii_case("TCP")
                || !parts[3].eq_ignore_ascii_case("LISTENING")
                || !local_address_uses_port(parts[1], port)
            {
                return None;
            }
            parts.last()?.parse::<u32>().ok()
        })
        .collect()
}

#[cfg(unix)]
fn pids_using_tcp_port(port: u16) -> Result<BTreeSet<u32>> {
    let output = Command::new("lsof")
        .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-t"])
        .create_no_window()
        .output()?;
    if !output.status.success() && output.stdout.is_empty() {
        return Ok(BTreeSet::new());
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.trim().parse::<u32>().ok())
        .collect())
}

#[cfg(windows)]
fn local_address_uses_port(address: &str, port: u16) -> bool {
    address
        .rsplit_once(':')
        .and_then(|(_, port_part)| port_part.parse::<u16>().ok())
        == Some(port)
}

/// 绝不误杀的进程名片段（小写）：模拟器主程序 / 管理工具 / 设备进程。
///
/// 它们由 Alas 用 `cmd /c start` 拉起，会继承启动器注入的 `ALAS_LAUNCHER_PID`，
/// 但它们不是启动器的 worker —— 关掉启动器不该关掉模拟器。
const EMULATOR_PROCESS_NAME_HINTS: [&str; 8] = [
    "mumu",
    "nemu",
    "dnplayer",
    "ldconsole",
    "bluestacks",
    "hd-player",
    "nox",
    "memu",
];

impl Drop for ManagedBackend {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            match child.kill() {
                Ok(_) => {}
                Err(e) => warn!("Failed to kill gui.py process: {:?}", e),
            }
        }
        // 清理漏网的 backend worker：按注入的环境变量识别。
        // 刻意跳过模拟器进程（见 EMULATOR_PROCESS_NAME_HINTS），
        // 否则会重演"关启动器连坐关掉模拟器"的老问题。
        let sys = sysinfo::System::new_all();
        for (pid, process) in sys.processes() {
            if pid.as_u32() == std::process::id() {
                continue;
            }
            let name = process.name().to_string_lossy().to_ascii_lowercase();
            if EMULATOR_PROCESS_NAME_HINTS
                .iter()
                .any(|hint| name.contains(hint))
            {
                continue;
            }
            for var in process.environ() {
                if var.to_str().unwrap_or_default()
                    == format!("ALAS_LAUNCHER_PID={}", std::process::id())
                {
                    process.kill();
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_startup_timeout_is_five_minutes() {
        assert_eq!(BACKEND_STARTUP_TIMEOUT, Duration::from_secs(5 * 60));
    }

    #[test]
    fn backend_startup_timeout_is_identified_without_matching_other_errors() {
        let timeout: anyhow::Error = BackendStartupTimeout { port: 22267 }.into();

        assert!(is_backend_startup_timeout(&timeout));
        assert!(!is_backend_startup_timeout(&anyhow!("other startup error")));
    }
}
