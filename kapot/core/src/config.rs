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

use clap::ValueEnum;
use core::fmt;
use std::collections::HashMap;
use std::result;

use crate::error::{KapotError, Result};

use datafusion::arrow::datatypes::DataType;

pub const KAPOT_JOB_NAME: &str = "kapot.job.name";
pub const KAPOT_DEFAULT_SHUFFLE_PARTITIONS: &str = "kapot.shuffle.partitions";
pub const KAPOT_HASH_JOIN_SINGLE_PARTITION_THRESHOLD: &str =
    "kapot.optimizer.hash_join_single_partition_threshold";
pub const KAPOT_DEFAULT_BATCH_SIZE: &str = "kapot.batch.size";
pub const KAPOT_REPARTITION_JOINS: &str = "kapot.repartition.joins";
pub const KAPOT_REPARTITION_AGGREGATIONS: &str = "kapot.repartition.aggregations";
pub const KAPOT_REPARTITION_WINDOWS: &str = "kapot.repartition.windows";
pub const KAPOT_PARQUET_PRUNING: &str = "kapot.parquet.pruning";
pub const KAPOT_COLLECT_STATISTICS: &str = "kapot.collect_statistics";
/// Indicate whether to enable to data cache for a task
pub const KAPOT_DATA_CACHE_ENABLED: &str = "kapot.data_cache.enabled";

pub const KAPOT_WITH_INFORMATION_SCHEMA: &str = "kapot.with_information_schema";
/// give a plugin files dir, and then the dynamic library files in this dir will be load when scheduler state init.
pub const KAPOT_PLUGIN_DIR: &str = "kapot.plugin_dir";
/// max message size for gRPC clients
pub const KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE: &str =
    "kapot.grpc_client_max_message_size";

pub type ParseResult<T> = result::Result<T, String>;

/// Configuration option meta-data
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    name: String,
    _description: String,
    _data_type: DataType,
    default_value: Option<String>,
}

impl ConfigEntry {
    fn new(
        name: String,
        _description: String,
        _data_type: DataType,
        default_value: Option<String>,
    ) -> Self {
        Self {
            name,
            _description,
            _data_type,
            default_value,
        }
    }
}

/// kapot configuration builder
pub struct KapotConfigBuilder {
    settings: HashMap<String, String>,
}

impl Default for KapotConfigBuilder {
    /// Create a new config builder
    fn default() -> Self {
        Self {
            settings: HashMap::new(),
        }
    }
}

impl KapotConfigBuilder {
    /// Create a new config with an additional setting
    pub fn set(&self, k: &str, v: &str) -> Self {
        let mut settings = self.settings.clone();
        settings.insert(k.to_owned(), v.to_owned());
        Self { settings }
    }

    pub fn build(&self) -> Result<KapotConfig> {
        KapotConfig::with_settings(self.settings.clone())
    }
}

/// kapot configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KapotConfig {
    /// Settings stored in map for easy serde
    settings: HashMap<String, String>,
}

impl KapotConfig {
    /// Create a default configuration
    pub fn new() -> Result<Self> {
        Self::with_settings(HashMap::new())
    }

    /// Create a configuration builder
    pub fn builder() -> KapotConfigBuilder {
        KapotConfigBuilder::default()
    }

    /// Create a new configuration based on key-value pairs
    pub fn with_settings(settings: HashMap<String, String>) -> Result<Self> {
        let supported_entries = KapotConfig::valid_entries();
        for (name, entry) in &supported_entries {
            if let Some(v) = settings.get(name) {
                // validate that we can parse the user-supplied value
                Self::parse_value(v.as_str(), entry._data_type.clone()).map_err(|e| KapotError::General(format!("Failed to parse user-supplied value '{name}' for configuration setting '{v}': {e}")))?;
            } else if let Some(v) = entry.default_value.clone() {
                Self::parse_value(v.as_str(), entry._data_type.clone()).map_err(|e| KapotError::General(format!("Failed to parse default value '{name}' for configuration setting '{v}': {e}")))?;
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

    /// All available configuration options
    pub fn valid_entries() -> HashMap<String, ConfigEntry> {
        let entries = vec![
            ConfigEntry::new(KAPOT_JOB_NAME.to_string(),
                             "Sets the job name that will appear in the web user interface for any submitted jobs".to_string(),
                             DataType::Utf8, None),
            ConfigEntry::new(KAPOT_DEFAULT_SHUFFLE_PARTITIONS.to_string(),
                             "Sets the default number of partitions to create when repartitioning query stages".to_string(),
                             DataType::UInt16, Some("16".to_string())),
            ConfigEntry::new(KAPOT_DEFAULT_BATCH_SIZE.to_string(),
                             "Sets the default batch size".to_string(),
                             DataType::UInt16, Some("8192".to_string())),
            ConfigEntry::new(KAPOT_REPARTITION_JOINS.to_string(),
                             "Configuration for repartition joins".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(KAPOT_REPARTITION_AGGREGATIONS.to_string(),
                             "Configuration for repartition aggregations".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(KAPOT_REPARTITION_WINDOWS.to_string(),
                             "Configuration for repartition windows".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(KAPOT_PARQUET_PRUNING.to_string(),
                             "Configuration for parquet prune".to_string(),
                             DataType::Boolean, Some("true".to_string())),
            ConfigEntry::new(KAPOT_WITH_INFORMATION_SCHEMA.to_string(),
                             "Sets whether enable information_schema".to_string(),
                             DataType::Boolean, Some("false".to_string())),
            ConfigEntry::new(KAPOT_HASH_JOIN_SINGLE_PARTITION_THRESHOLD.to_string(),
                "Sets threshold in bytes for collecting the smaller side of the hash join in memory".to_string(),
                DataType::UInt64, Some((1024 * 1024).to_string())),
            ConfigEntry::new(KAPOT_COLLECT_STATISTICS.to_string(),
                "Configuration for collecting statistics during scan".to_string(),
                DataType::Boolean, Some("false".to_string())
            ),
            ConfigEntry::new(KAPOT_PLUGIN_DIR.to_string(),
                             "Sets the plugin dir".to_string(),
                             DataType::Utf8, Some("".to_string())),
            ConfigEntry::new(KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE.to_string(),
                             "Configuration for max message size in gRPC clients".to_string(),
                             DataType::UInt64,
                             Some((128 * 1024 * 1024).to_string())),
        ];
        entries
            .iter()
            .map(|e| (e.name.clone(), e.clone()))
            .collect::<HashMap<_, _>>()
    }

    pub fn settings(&self) -> &HashMap<String, String> {
        &self.settings
    }

    pub fn default_shuffle_partitions(&self) -> usize {
        self.get_usize_setting(KAPOT_DEFAULT_SHUFFLE_PARTITIONS)
    }

    pub fn default_plugin_dir(&self) -> String {
        self.get_string_setting(KAPOT_PLUGIN_DIR)
    }

    pub fn default_batch_size(&self) -> usize {
        self.get_usize_setting(KAPOT_DEFAULT_BATCH_SIZE)
    }

    pub fn hash_join_single_partition_threshold(&self) -> usize {
        self.get_usize_setting(KAPOT_HASH_JOIN_SINGLE_PARTITION_THRESHOLD)
    }

    pub fn default_grpc_client_max_message_size(&self) -> usize {
        self.get_usize_setting(KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE)
    }

    pub fn repartition_joins(&self) -> bool {
        self.get_bool_setting(KAPOT_REPARTITION_JOINS)
    }

    pub fn repartition_aggregations(&self) -> bool {
        self.get_bool_setting(KAPOT_REPARTITION_AGGREGATIONS)
    }

    pub fn repartition_windows(&self) -> bool {
        self.get_bool_setting(KAPOT_REPARTITION_WINDOWS)
    }

    pub fn parquet_pruning(&self) -> bool {
        self.get_bool_setting(KAPOT_PARQUET_PRUNING)
    }

    pub fn collect_statistics(&self) -> bool {
        self.get_bool_setting(KAPOT_COLLECT_STATISTICS)
    }

    pub fn default_with_information_schema(&self) -> bool {
        self.get_bool_setting(KAPOT_WITH_INFORMATION_SCHEMA)
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

// an enum used to configure the scheduler policy
// needs to be visible to code generated by configure_me
#[derive(Clone, ValueEnum, Copy, Debug, serde::Deserialize)]
pub enum TaskSchedulingPolicy {
    PullStaged,
    PushStaged,
}

impl std::str::FromStr for TaskSchedulingPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ValueEnum::from_str(s, true)
    }
}

impl parse_arg::ParseArgFromStr for TaskSchedulingPolicy {
    fn describe_type<W: fmt::Write>(mut writer: W) -> fmt::Result {
        write!(writer, "The scheduler policy for the scheduler")
    }
}

// an enum used to configure the log rolling policy
// needs to be visible to code generated by configure_me
#[derive(Clone, ValueEnum, Copy, Debug, serde::Deserialize)]
pub enum LogRotationPolicy {
    Minutely,
    Hourly,
    Daily,
    Never,
}

impl std::str::FromStr for LogRotationPolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ValueEnum::from_str(s, true)
    }
}

impl parse_arg::ParseArgFromStr for LogRotationPolicy {
    fn describe_type<W: fmt::Write>(mut writer: W) -> fmt::Result {
        write!(writer, "The log rotation policy")
    }
}

// an enum used to configure the source data cache policy
// needs to be visible to code generated by configure_me
#[derive(Clone, ValueEnum, Copy, Debug, serde::Deserialize)]
pub enum DataCachePolicy {
    LocalDiskFile,
}

impl std::str::FromStr for DataCachePolicy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ValueEnum::from_str(s, true)
    }
}

impl parse_arg::ParseArgFromStr for DataCachePolicy {
    fn describe_type<W: fmt::Write>(mut writer: W) -> fmt::Result {
        write!(writer, "The data cache policy")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() -> Result<()> {
        let config = KapotConfig::new()?;
        assert_eq!(16, config.default_shuffle_partitions());
        assert!(!config.default_with_information_schema());
        assert_eq!(16777216, config.default_grpc_client_max_message_size());
        Ok(())
    }

    #[test]
    fn custom_config() -> Result<()> {
        let config = KapotConfig::builder()
            .set(KAPOT_DEFAULT_SHUFFLE_PARTITIONS, "123")
            .set(KAPOT_WITH_INFORMATION_SCHEMA, "true")
            .set(
                KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE,
                (8 * 1024 * 1024).to_string().as_str(),
            )
            .build()?;
        assert_eq!(123, config.default_shuffle_partitions());
        assert!(config.default_with_information_schema());
        assert_eq!(8388608, config.default_grpc_client_max_message_size());
        Ok(())
    }

    #[test]
    fn custom_config_invalid() -> Result<()> {
        let config = KapotConfig::builder()
            .set(KAPOT_DEFAULT_SHUFFLE_PARTITIONS, "true")
            .set(KAPOT_PLUGIN_DIR, "test_dir")
            .build();
        assert!(config.is_err());
        assert_eq!("General(\"Failed to parse user-supplied value 'kapot.shuffle.partitions' for configuration setting 'true': ParseIntError { kind: InvalidDigit }\")", format!("{:?}", config.unwrap_err()));

        let config = KapotConfig::builder()
            .set(KAPOT_WITH_INFORMATION_SCHEMA, "123")
            .build();
        assert!(config.is_err());
        assert_eq!("General(\"Failed to parse user-supplied value 'kapot.with_information_schema' for configuration setting '123': ParseBoolError\")", format!("{:?}", config.unwrap_err()));
        Ok(())
    }
}
