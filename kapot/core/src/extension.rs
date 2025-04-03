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

use crate::config::{
    KapotConfig, KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE, KAPOT_JOB_NAME,
    KAPOT_SHUFFLE_READER_MAX_REQUESTS, KAPOT_STANDALONE_PARALLELISM,
};
use crate::planner::KapotQueryPlanner;
use crate::serde::protobuf::KeyValuePair;
use crate::serde::{KapotLogicalExtensionCodec, KapotPhysicalExtensionCodec};
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionState};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::LogicalPlanNode;
use std::sync::Arc;

/// Provides methods which adapt [SessionState]
/// for kapot usage
pub trait SessionStateExt {
    /// Setups new [SessionState] for kapot usage
    ///
    /// State will be created with appropriate [SessionConfig] configured
    fn new_kapot_state(
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState>;
    /// Upgrades [SessionState] for kapot usage
    ///
    /// State will be upgraded to appropriate [SessionConfig]
    fn upgrade_for_kapot(
        self,
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState>;
}

/// [SessionConfig] extension with methods needed
/// for kapot configuration
pub trait SessionConfigExt {
    /// Creates session config which has
    /// kapot configuration initialized
    fn new_with_kapot() -> SessionConfig;

    /// Overrides kapot's [LogicalExtensionCodec]
    fn with_kapot_logical_extension_codec(
        self,
        codec: Arc<dyn LogicalExtensionCodec>,
    ) -> SessionConfig;

    /// Overrides kapot's [PhysicalExtensionCodec]
    fn with_kapot_physical_extension_codec(
        self,
        codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> SessionConfig;

    /// returns [LogicalExtensionCodec] if set
    /// or default kapot codec if not
    fn kapot_logical_extension_codec(&self) -> Arc<dyn LogicalExtensionCodec>;

    /// returns [PhysicalExtensionCodec] if set
    /// or default kapot codec if not
    fn kapot_physical_extension_codec(&self) -> Arc<dyn PhysicalExtensionCodec>;

    /// Overrides kapot's [QueryPlanner]
    fn with_kapot_query_planner(
        self,
        planner: Arc<dyn QueryPlanner + Send + Sync + 'static>,
    ) -> SessionConfig;

    /// Returns kapot's [QueryPlanner] if overridden
    fn kapot_query_planner(
        &self,
    ) -> Option<Arc<dyn QueryPlanner + Send + Sync + 'static>>;

    /// Returns parallelism of standalone cluster
    fn kapot_standalone_parallelism(&self) -> usize;
    /// Sets parallelism of standalone cluster
    ///
    /// This option to be used to configure standalone session context
    fn with_kapot_standalone_parallelism(self, parallelism: usize) -> Self;

    /// retrieves grpc client max message size
    fn kapot_grpc_client_max_message_size(&self) -> usize;

    /// sets grpc client max message size
    fn with_kapot_grpc_client_max_message_size(self, max_size: usize) -> Self;

    /// Sets kapot job name
    fn with_kapot_job_name(self, job_name: &str) -> Self;

    /// get maximum in flight requests for shuffle reader
    fn kapot_shuffle_reader_maximum_concurrent_requests(&self) -> usize;

    /// Sets maximum in flight requests for shuffle reader
    fn with_kapot_shuffle_reader_maximum_concurrent_requests(
        self,
        max_requests: usize,
    ) -> Self;
}

/// [SessionConfigHelperExt] is set of [SessionConfig] extension methods
/// which are used internally (not exposed in client)
pub trait SessionConfigHelperExt {
    /// converts [SessionConfig] to proto
    fn to_key_value_pairs(&self) -> Vec<KeyValuePair>;
    /// updates [SessionConfig] from proto
    fn update_from_key_value_pair(self, key_value_pairs: &[KeyValuePair]) -> Self;
    /// updates mut [SessionConfig] from proto
    fn update_from_key_value_pair_mut(&mut self, key_value_pairs: &[KeyValuePair]);
    /// changes some of default datafusion configuration
    /// in order to make it suitable for kapot
    fn kapot_restricted_configuration(self) -> Self;
}

impl SessionStateExt for SessionState {
    fn new_kapot_state(
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState> {
        let session_config = SessionConfig::new_with_kapot();
        let planner = KapotQueryPlanner::<LogicalPlanNode>::new(
            scheduler_url,
            KapotConfig::default(),
        );

        let runtime_env = RuntimeEnvBuilder::new().build()?;
        let session_state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(session_config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_query_planner(Arc::new(planner))
            .with_session_id(session_id)
            .build();

        Ok(session_state)
    }

    fn upgrade_for_kapot(
        self,
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<SessionState> {
        let codec_logical = self.config().kapot_logical_extension_codec();
        let planner_override = self.config().kapot_query_planner();

        let new_config = self
            .config()
            .options()
            .extensions
            .get::<KapotConfig>()
            .cloned()
            .unwrap_or_else(KapotConfig::default);

        let session_config = self
            .config()
            .clone()
            .with_option_extension(new_config.clone())
            .kapot_restricted_configuration();

        let builder = SessionStateBuilder::new_from_existing(self)
            .with_config(session_config)
            .with_session_id(session_id);

        let builder = match planner_override {
            Some(planner) => builder.with_query_planner(planner),
            None => {
                let planner = KapotQueryPlanner::<LogicalPlanNode>::with_extension(
                    scheduler_url,
                    new_config,
                    codec_logical,
                );
                builder.with_query_planner(Arc::new(planner))
            }
        };

        Ok(builder.build())
    }
}

impl SessionConfigExt for SessionConfig {
    fn new_with_kapot() -> SessionConfig {
        SessionConfig::new()
            .with_option_extension(KapotConfig::default())
            .with_information_schema(true)
            .with_target_partitions(16)
            .kapot_restricted_configuration()
    }
    fn with_kapot_logical_extension_codec(
        self,
        codec: Arc<dyn LogicalExtensionCodec>,
    ) -> SessionConfig {
        let extension = KapotConfigExtensionLogicalCodec::new(codec);
        self.with_extension(Arc::new(extension))
    }
    fn with_kapot_physical_extension_codec(
        self,
        codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> SessionConfig {
        let extension = KapotConfigExtensionPhysicalCodec::new(codec);
        self.with_extension(Arc::new(extension))
    }

    fn kapot_logical_extension_codec(&self) -> Arc<dyn LogicalExtensionCodec> {
        self.get_extension::<KapotConfigExtensionLogicalCodec>()
            .map(|c| c.codec())
            .unwrap_or_else(|| Arc::new(KapotLogicalExtensionCodec::default()))
    }
    fn kapot_physical_extension_codec(&self) -> Arc<dyn PhysicalExtensionCodec> {
        self.get_extension::<KapotConfigExtensionPhysicalCodec>()
            .map(|c| c.codec())
            .unwrap_or_else(|| Arc::new(KapotPhysicalExtensionCodec::default()))
    }

    fn with_kapot_query_planner(
        self,
        planner: Arc<dyn QueryPlanner + Send + Sync + 'static>,
    ) -> SessionConfig {
        let extension = KapotQueryPlannerExtension::new(planner);
        self.with_extension(Arc::new(extension))
    }

    fn kapot_query_planner(
        &self,
    ) -> Option<Arc<dyn QueryPlanner + Send + Sync + 'static>> {
        self.get_extension::<KapotQueryPlannerExtension>()
            .map(|c| c.planner())
    }

    fn kapot_standalone_parallelism(&self) -> usize {
        self.options()
            .extensions
            .get::<KapotConfig>()
            .map(|c| c.default_standalone_parallelism())
            .unwrap_or_else(|| KapotConfig::default().default_standalone_parallelism())
    }

    fn kapot_grpc_client_max_message_size(&self) -> usize {
        self.options()
            .extensions
            .get::<KapotConfig>()
            .map(|c| c.default_grpc_client_max_message_size())
            .unwrap_or_else(|| {
                KapotConfig::default().default_grpc_client_max_message_size()
            })
    }

    fn with_kapot_job_name(self, job_name: &str) -> Self {
        if self.options().extensions.get::<KapotConfig>().is_some() {
            self.set_str(KAPOT_JOB_NAME, job_name)
        } else {
            self.with_option_extension(KapotConfig::default())
                .set_str(KAPOT_JOB_NAME, job_name)
        }
    }

    fn with_kapot_grpc_client_max_message_size(self, max_size: usize) -> Self {
        if self.options().extensions.get::<KapotConfig>().is_some() {
            self.set_usize(KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE, max_size)
        } else {
            self.with_option_extension(KapotConfig::default())
                .set_usize(KAPOT_GRPC_CLIENT_MAX_MESSAGE_SIZE, max_size)
        }
    }

    fn with_kapot_standalone_parallelism(self, parallelism: usize) -> Self {
        if self.options().extensions.get::<KapotConfig>().is_some() {
            self.set_usize(KAPOT_STANDALONE_PARALLELISM, parallelism)
        } else {
            self.with_option_extension(KapotConfig::default())
                .set_usize(KAPOT_STANDALONE_PARALLELISM, parallelism)
        }
    }

    fn kapot_shuffle_reader_maximum_concurrent_requests(&self) -> usize {
        self.options()
            .extensions
            .get::<KapotConfig>()
            .map(|c| c.shuffle_reader_maximum_concurrent_requests())
            .unwrap_or_else(|| {
                KapotConfig::default().shuffle_reader_maximum_concurrent_requests()
            })
    }

    fn with_kapot_shuffle_reader_maximum_concurrent_requests(
        self,
        max_requests: usize,
    ) -> Self {
        if self.options().extensions.get::<KapotConfig>().is_some() {
            self.set_usize(KAPOT_SHUFFLE_READER_MAX_REQUESTS, max_requests)
        } else {
            self.with_option_extension(KapotConfig::default())
                .set_usize(KAPOT_SHUFFLE_READER_MAX_REQUESTS, max_requests)
        }
    }
}

impl SessionConfigHelperExt for SessionConfig {
    fn to_key_value_pairs(&self) -> Vec<KeyValuePair> {
        self.options()
            .entries()
            .iter()
            .map(|datafusion::config::ConfigEntry { key, value, .. }| {
                log::trace!("sending configuration key: `{}`, value`{:?}`", key, value);
                KeyValuePair {
                    key: key.to_owned(),
                    value: value.clone(),
                }
            })
            .collect()
    }

    fn update_from_key_value_pair(self, key_value_pairs: &[KeyValuePair]) -> Self {
        let mut s = self;
        s.update_from_key_value_pair_mut(key_value_pairs);
        s
    }

    fn update_from_key_value_pair_mut(&mut self, key_value_pairs: &[KeyValuePair]) {
        for KeyValuePair { key, value } in key_value_pairs {
            match value {
                Some(value) => {
                    log::trace!(
                        "setting up configuration key: `{}`, value: `{:?}`",
                        key,
                        value
                    );
                    if let Err(e) = self.options_mut().set(key, value) {
                        // there is not much we can do about this error at the moment.
                        // it used to be warning but it gets very verbose
                        // as even datafusion properties can't be parsed
                        log::debug!(
                            "could not set configuration key: `{}`, value: `{:?}`, reason: {}",
                            key,
                            value,
                            e.to_string()
                        )
                    }
                }
                None => {
                    log::trace!(
                        "can't set up configuration key: `{}`, as value is None",
                        key,
                    )
                }
            }
        }
    }

    fn kapot_restricted_configuration(self) -> Self {
        self
            // round robbin repartition does not work well with kapot.
            // this setting it will also be enforced by the scheduler
            // thus user will not be able to override it.
            .with_round_robin_repartition(false)
            // There is issue with Utv8View(s) where Arrow IPC will generate
            // frames which would be too big to send using Arrow Flight.
            //
            // This configuration option will be disabled temporary.
            //
            // This configuration is not enforced by the scheduler, thus
            // user could override this setting using `SET` operation.
            //
            // TODO: enable this option once we get to root of the problem
            //       between `IpcWriter` and `ViewTypes`
            .set_bool(
                "datafusion.execution.parquet.schema_force_view_types",
                false,
            )
    }
}

/// Wrapper for [SessionConfig] extension
/// holding [LogicalExtensionCodec] if overridden
struct KapotConfigExtensionLogicalCodec {
    codec: Arc<dyn LogicalExtensionCodec>,
}

impl KapotConfigExtensionLogicalCodec {
    fn new(codec: Arc<dyn LogicalExtensionCodec>) -> Self {
        Self { codec }
    }
    fn codec(&self) -> Arc<dyn LogicalExtensionCodec> {
        self.codec.clone()
    }
}

/// Wrapper for [SessionConfig] extension
/// holding [PhysicalExtensionCodec] if overridden
struct KapotConfigExtensionPhysicalCodec {
    codec: Arc<dyn PhysicalExtensionCodec>,
}

impl KapotConfigExtensionPhysicalCodec {
    fn new(codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        Self { codec }
    }
    fn codec(&self) -> Arc<dyn PhysicalExtensionCodec> {
        self.codec.clone()
    }
}

/// Wrapper for [SessionConfig] extension
/// holding overridden [QueryPlanner]
struct KapotQueryPlannerExtension {
    planner: Arc<dyn QueryPlanner + Send + Sync + 'static>,
}

impl KapotQueryPlannerExtension {
    fn new(planner: Arc<dyn QueryPlanner + Send + Sync + 'static>) -> Self {
        Self { planner }
    }
    fn planner(&self) -> Arc<dyn QueryPlanner + Send + Sync + 'static> {
        self.planner.clone()
    }
}

#[cfg(test)]
mod test {
    use datafusion::{
        execution::{SessionState, SessionStateBuilder},
        prelude::SessionConfig,
    };

    use crate::{
        config::KAPOT_JOB_NAME,
        extension::{SessionConfigExt, SessionConfigHelperExt, SessionStateExt},
    };

    // kapot disables round robin repatriations
    #[tokio::test]
    async fn should_disable_round_robin_repartition() {
        let state = SessionState::new_kapot_state(
            "scheduler_url".to_string(),
            "session_id".to_string(),
        )
        .unwrap();

        assert!(!state.config().round_robin_repartition());

        let state = SessionStateBuilder::new().build();

        assert!(state.config().round_robin_repartition());
        let state = state
            .upgrade_for_kapot("scheduler_url".to_string(), "session_id".to_string())
            .unwrap();

        assert!(!state.config().round_robin_repartition());
    }
    #[test]
    fn should_convert_to_key_value_pairs() {
        // key value pairs should contain datafusion and kapot values

        let config =
            SessionConfig::new_with_kapot().with_kapot_job_name("job_name");
        let pairs = config.to_key_value_pairs();

        assert!(pairs.iter().any(|p| p.key == KAPOT_JOB_NAME));
        assert!(pairs
            .iter()
            .any(|p| p.key == "datafusion.catalog.information_schema"))
    }
}
