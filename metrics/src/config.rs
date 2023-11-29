use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The name of the service. Should be the same as the Cargo package name.
    pub service_name: String,
    /// A unique identifier for this instance of the service.
    pub instance_id: String,
    /// The build version of the service (commit hash).
    pub build: String,
    /// The version of the service. Should be the same as the Cargo package version.
    pub version: String,
    /// The environment of the service.
    pub service_env: String,
    /// Flag to enable metrics export.
    pub export: bool,
    /// Flag to enable tracing collection.
    pub tracing: bool,
    /// The endpoint of the trace collector.
    pub collector_endpoint: String,
    /// The endpoint of the prometheus push gateway.
    #[serde(alias = "prom_gateway_endpoint")]
    pub prom_gateway_endpoint: String,
    #[cfg(feature = "tokio-console")]
    /// Enables tokio console debugging.
    pub tokio_console: bool,
    /// How to print log lines to STDOUT
    pub log_format: LogFormat,
}

/// Format of log events
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LogFormat {
    // Print log lines on multiple lines
    #[default]
    MultiLine,
    // Print log lines on a single line
    SingleLine,
    // Print logs a newline delimited JSON
    Json,
}

impl Config {
    pub fn with_service_name(mut self, name: String) -> Self {
        self.service_name = name;
        self
    }
    pub fn with_build(mut self, build: String) -> Self {
        self.build = build;
        self
    }
    pub fn with_version(mut self, version: String) -> Self {
        self.version = version;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            service_name: "unknown".to_string(),
            instance_id: names::Generator::default().next().unwrap(),
            build: "unknown".to_string(),
            version: "unknown".to_string(),
            service_env: "dev".to_string(),
            export: true,
            tracing: false,
            collector_endpoint: "http://localhost:4317".to_string(),
            prom_gateway_endpoint: "http://localhost:9091".to_string(),
            #[cfg(feature = "tokio-console")]
            tokio_console: false,
            log_format: LogFormat::default(),
        }
    }
}
