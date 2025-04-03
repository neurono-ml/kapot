// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

//! kapot configuration

use std::collections::HashMap;
use std::result;

use crate::error::{KapotError, Result};

use datafusion::{
    arrow::datatypes::DataType, common::config_err, config::ConfigExtension,
};

pub const KAPOT_JOB_NAME: &str = "kapot.job.name";
pub const KAPOT_STANDALONE_PARALLELISM: &str = "kapot.standalone.parallelism";
/// max message size for gRPC clients
pub const KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE: &str =
    "kapot.grpc_client_max_message_size";
pub const KAPOT_SHUFFLE_READER_MAX_REQUESTS: &str =
    "kapot.shuffle.max_concurrent_read_requests";

pub type ParseResult<T> = result::Result<T, String>;
use std::sync::LazyLock;

static CONFIG_ENTRIES: LazyLock<HashMap<String, ConfigEntry>> = LazyLock::new(|| {
    let entries = vec![
        ConfigEntry::new(KAPOT_JOB_NAME.to_string(),
                         "Sets the job name that will appear in the web user interface for any submitted jobs".to_string(),
                         DataType::Utf8, None),
        ConfigEntry::new(KAPOT_STANDALONE_PARALLELISM.to_string(),
                        "Standalone processing parallelism ".to_string(),
                        DataType::UInt16, Some(std::thread::available_parallelism().map(|v| v.get()).unwrap_or(1).to_string())),
        ConfigEntry::new(KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE.to_string(),
                         "Configuration for max message size in gRPC clients".to_string(),
                         DataType::UInt64,
                         Some((16 * 1024 * 1024).to_string())),
        ConfigEntry::new(KAPOT_SHUFFLE_READER_MAX_REQUESTS.to_string(),
                         "Maximum concurrent requests shuffle reader can process".to_string(),
                         DataType::UInt64,
                         Some((64).to_string())),
    ];
    entries
        .into_iter()
        .map(|e| (e.name.clone(), e))
        .collect::<HashMap<_, _>>()
});

/// Configuration option meta-data
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    name: String,
    description: String,
    data_type: DataType,
    default_value: Option<String>,
}

impl ConfigEntry {
    fn new(
        name: String,
        description: String,
        data_type: DataType,
        default_value: Option<String>,
    ) -> Self {
        Self {
            name,
            description,
            data_type,
            default_value,
        }
    }
}

/// kapot configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KapotConfig {
    /// Settings stored in map for easy serde
    settings: HashMap<String, String>,
}

impl Default for KapotConfig {
    fn default() -> Self {
        Self::with_settings(HashMap::new()).unwrap()
    }
}

impl KapotConfig {
    /// Create a new configuration based on key-value pairs
    fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        let supported_entries = KapotConfig::valid_entries();
        for (name, entry) in supported_entries {
            if let Some(v) = settings.get(name) {
                // validate that we can parse the user-supplied value
                Self::parse_value(v.as_str(), entry.data_type.clone()).map_err(|e| KapotError::General(format!("Failed to parse user-supplied value '{name}' for configuration setting '{v}': {e}")))?;
            } else if let Some(v) = entry.default_value.clone() {
                Self::parse_value(v.as_str(), entry.data_type.clone()).map_err(|e| KapotError::General(format!("Failed to parse default value '{name}' for configuration setting '{v}': {e}")))?;
            } else if entry.default_value.is_none() {
                // optional config
            } else {
                return Err(KapotError::General(format!(
                    "No value specified for mandatory configuration setting '{name}'"
                )));
            }
        }

        Ok(Self { settings })
    }

    pub fn parse_value(val: &str, data_type: DataType) -> ParseResult<()> {
        match data_type {
            DataType::UInt16 => {
                val.to_string()
                    .parse::<usize>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::UInt32 => {
                val.to_string()
                    .parse::<usize>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::UInt64 => {
                val.to_string()
                    .parse::<usize>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::Boolean => {
                val.to_string()
                    .parse::<bool>()
                    .map_err(|e| format!("{e:?}"))?;
            }
            DataType::Utf8 => {
                val.to_string();
            }
            _ => {
                return Err(format!("not support data type: {data_type}"));
            }
        }

        Ok(())
    }

    // All available configuration options
    pub fn valid_entries() -> &'static HashMap<String, ConfigEntry> {
        &CONFIG_ENTRIES
    }

    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    pub fn default_grpc_client_max_message_size(&self) -> usize {
        self.get_usize_setting(KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE)
    }

    pub fn default_standalone_parallelism(&self) -> usize {
        self.get_usize_setting(KAPOT_STANDALONE_PARALLELISM)
    }

    pub fn shuffle_reader_maximum_concurrent_requests(&self) -> usize {
        self.get_usize_setting(KAPOT_SHUFFLE_READER_MAX_REQUESTS)
    }

    fn get_usize_setting(&self, key: &str) -> usize {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.parse().unwrap()
        } else {
            let entries = Self::valid_entries();
            // infallible because we validate all configs in the constructor
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.parse().unwrap()
        }
    }

    #[allow(dead_code)]
    fn get_bool_setting(&self, key: &str) -> bool {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.parse::<bool>().unwrap()
        } else {
            let entries = Self::valid_entries();
            // infallible because we validate all configs in the constructor
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.parse::<bool>().unwrap()
        }
    }
    #[allow(dead_code)]
    fn get_string_setting(&self, key: &str) -> String {
        if let Some(v) = self.settings.get(key) {
            // infallible because we validate all configs in the constructor
            v.to_string()
        } else {
            let entries = Self::valid_entries();
            // infallible because we validate all configs in the constructor
            let v = entries.get(key).unwrap().default_value.as_ref().unwrap();
            v.to_string()
        }
    }
}

impl datafusion::config::ExtensionOptions for KapotConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn datafusion::config::ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        let entries = Self::valid_entries();
        let k = format!("{}.{key}", KapotConfig::PREFIX);

        if entries.contains_key(&k) {
            self.settings.insert(k, value.to_string());
            Ok(())
        } else {
            config_err!("configuration key `{}` does not exist", key)
        }
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        Self::valid_entries()
            .iter()
            .map(|(key, value)| datafusion::config::ConfigEntry {
                key: key.clone(),
                value: self
                    .settings
                    .get(key)
                    .cloned()
                    .or(value.default_value.clone()),
                description: &value.description,
            })
            .collect()
    }
}

impl datafusion::config::ConfigExtension for KapotConfig {
    const PREFIX: &'static str = "kapot";
}

// an enum used to configure the scheduler policy
// needs to be visible to code generated by configure_me

/// kapot supports both push-based and pull-based task scheduling.
/// It is recommended that you try both to determine which is the best for your use case.
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum TaskSchedulingPolicy {
    /// Pull-based scheduling works in a similar way to Apache Spark
    #[default]
    PullStaged,
    /// push-based scheduling can result in lower latency.
    PushStaged,
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for TaskSchedulingPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}
#[cfg(feature = "build-binary")]
impl configure_me::parse_arg::ParseArgFromStr for TaskSchedulingPolicy {
    fn describe_type<W: core::fmt::Write>(mut writer: W) -> core::fmt::Result {
        write!(writer, "The scheduler policy for the scheduler")
    }
}

// an enum used to configure the log rolling policy
// needs to be visible to code generated by configure_me
#[derive(Clone, Copy, Debug, serde::Deserialize, Default)]
#[cfg_attr(feature = "build-binary", derive(clap::ValueEnum))]
pub enum LogRotationPolicy {
    Minutely,
    Hourly,
    Daily,
    #[default]
    Never,
}

#[cfg(feature = "build-binary")]
impl std::str::FromStr for LogRotationPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

#[cfg(feature = "build-binary")]
impl configure_me::parse_arg::ParseArgFromStr for LogRotationPolicy {
    fn describe_type<W: core::fmt::Write>(mut writer: W) -> core::fmt::Result {
        write!(writer, "The log rotation policy")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() -> Result<()> {
        let config = KapotConfig::default();
        assert_eq!(16777216, config.default_grpc_client_max_message_size());
        Ok(())
    }
}
