use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen: String,
    #[serde(default = "default_metrics_listen")]
    pub metrics_listen: String,
    #[serde(default = "default_admin_listen")]
    pub admin_listen: String,
    #[serde(default)]
    pub strategy: Strategy,
    pub backends: Vec<BackendConfig>,
    #[serde(default)]
    pub health_check: HealthCheckConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    #[default]
    RoundRobin,
    LeastConnections,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendConfig {
    pub addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_health_path")]
    pub path: String,
    /// consecutive failures before a backend is marked down
    #[serde(default = "default_fall")]
    pub fall: u32,
    /// consecutive successes before a backend is marked up again
    #[serde(default = "default_rise")]
    pub rise: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_interval_secs(),
            timeout_secs: default_timeout_secs(),
            path: default_health_path(),
            fall: default_fall(),
            rise: default_rise(),
        }
    }
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9090".into()
}
fn default_admin_listen() -> String {
    "0.0.0.0:50051".into()
}
fn default_interval_secs() -> u64 {
    5
}
fn default_timeout_secs() -> u64 {
    2
}
fn default_health_path() -> String {
    "/health".into()
}
fn default_fall() -> u32 {
    3
}
fn default_rise() -> u32 {
    2
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path.as_ref())?;
        let cfg: Config = serde_yaml::from_str(&raw)?;
        if cfg.backends.is_empty() {
            anyhow::bail!("config needs at least one backend");
        }
        Ok(cfg)
    }
}
