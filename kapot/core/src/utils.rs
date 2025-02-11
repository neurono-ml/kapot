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

use crate::config::KapotConfig;
use crate::error::{KapotError, Result};
use crate::execution_plans::{
    DistributedQueryExec, ShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::object_store_registry::KapotObjectStoreRegistry;
use crate::serde::scheduler::PartitionStats;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::IpcWriteOptions;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::physical_plan::{CsvExec, ParquetExec};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{
    QueryPlanner, SessionConfig, SessionContext, SessionState,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{metrics, ExecutionPlan, RecordBatchStream};
use datafusion_proto::logical_plan::{
    AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use futures::StreamExt;
use log::error;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs::File, pin::Pin};
use tonic::codegen::StdError;
use tonic::transport::{Channel, Error, Server};

/// Default session builder using the provided configuration
pub fn default_session_builder(config: SessionConfig) -> SessionState {
    //TODO: Adapt to Result and remove this unwrap
    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(KapotObjectStoreRegistry::new()))
        .build()
        .unwrap();
    
    SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime))
        .build()
}

/// Stream data to disk in Arrow IPC format
pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &metrics::Time,
) -> Result<PartitionStats> {
    let file = File::create(path).map_err(|e| {
        error!("Failed to create partition file at {}: {:?}", path, e);
        KapotError::IoError(e)
    })?;

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;

    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

    let mut writer =
        StreamWriter::try_new_with_options(file, stream.schema().as_ref(), options)?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch.get_array_memory_size();
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;

        let timer = disk_write_metric.timer();
        writer.write(&batch)?;
        timer.done();
    }
    let timer = disk_write_metric.timer();
    writer.finish()?;
    timer.done();
    Ok(PartitionStats::new(
        Some(num_rows as u64),
        Some(num_batches),
        Some(num_bytes as u64),
    ))
}

pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn produce_diagram(filename: &str, stages: &[Arc<ShuffleWriterExec>]) -> Result<()> {
    let write_file = File::create(filename)?;
    let mut w = BufWriter::new(&write_file);
    writeln!(w, "digraph G {{")?;

    // draw stages and entities
    for stage in stages {
        writeln!(w, "\tsubgraph cluster{} {{", stage.stage_id())?;
        writeln!(w, "\t\tlabel = \"Stage {}\";", stage.stage_id())?;
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(
            &mut w,
            stage.children()[0].as_ref(),
            stage.stage_id(),
            &mut id,
            true,
        )?;
        writeln!(w, "\t}}")?;
    }

    // draw relationships
    for stage in stages {
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(
            &mut w,
            stage.children()[0].as_ref(),
            stage.stage_id(),
            &mut id,
            false,
        )?;
    }

    write!(w, "}}")?;
    Ok(())
}

fn build_exec_plan_diagram(
    w: &mut BufWriter<&File>,
    plan: &dyn ExecutionPlan,
    stage_id: usize,
    id: &mut AtomicUsize,
    draw_entity: bool,
) -> Result<usize> {
    let operator_str = if plan.as_any().downcast_ref::<AggregateExec>().is_some() {
        "AggregateExec"
    } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
        "SortExec"
    } else if plan.as_any().downcast_ref::<ProjectionExec>().is_some() {
        "ProjectionExec"
    } else if plan.as_any().downcast_ref::<HashJoinExec>().is_some() {
        "HashJoinExec"
    } else if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
        "ParquetExec"
    } else if plan.as_any().downcast_ref::<CsvExec>().is_some() {
        "CsvExec"
    } else if plan.as_any().downcast_ref::<FilterExec>().is_some() {
        "FilterExec"
    } else if plan.as_any().downcast_ref::<ShuffleWriterExec>().is_some() {
        "ShuffleWriterExec"
    } else if plan
        .as_any()
        .downcast_ref::<UnresolvedShuffleExec>()
        .is_some()
    {
        "UnresolvedShuffleExec"
    } else if plan
        .as_any()
        .downcast_ref::<CoalesceBatchesExec>()
        .is_some()
    {
        "CoalesceBatchesExec"
    } else if plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .is_some()
    {
        "CoalescePartitionsExec"
    } else {
        println!("Unknown: {plan:?}");
        "Unknown"
    };

    let node_id = id.load(Ordering::SeqCst);
    id.store(node_id + 1, Ordering::SeqCst);

    if draw_entity {
        writeln!(
            w,
            "\t\tstage_{stage_id}_exec_{node_id} [shape=box, label=\"{operator_str}\"];"
        )?;
    }
    for child in plan.children() {
        if let Some(shuffle) = child.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            if !draw_entity {
                writeln!(
                    w,
                    "\tstage_{}_exec_1 -> stage_{}_exec_{};",
                    shuffle.stage_id, stage_id, node_id
                )?;
            }
        } else {
            // relationships within same entity
            let child_id =
                build_exec_plan_diagram(w, child.as_ref(), stage_id, id, draw_entity)?;
            if draw_entity {
                writeln!(
                    w,
                    "\t\tstage_{stage_id}_exec_{child_id} -> stage_{stage_id}_exec_{node_id};"
                )?;
            }
        }
    }
    Ok(node_id)
}

/// Create a client DataFusion context that uses the kapotQueryPlanner to send logical plans
/// to a kapot scheduler
pub fn create_df_ctx_with_kapot_query_planner<T: 'static + AsLogicalPlan>(
    scheduler_url: String,
    session_id: String,
    config: &KapotConfig,
) -> SessionContext {
    let planner: Arc<KapotQueryPlanner<T>> =
        Arc::new(KapotQueryPlanner::new(scheduler_url, config.clone()));

    let session_config = SessionConfig::new()
        .with_target_partitions(config.default_shuffle_partitions())
        .with_information_schema(true);
    let object_store_registry = Arc::new(KapotObjectStoreRegistry::new());
    // TODO: Convert this function into result, then remove the unwrap
    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(object_store_registry)
        .build().unwrap();
    let session_state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .with_runtime_env(Arc::new(runtime))
        .with_query_planner(planner)
        .with_session_id(session_id)
        .build();
    
    // the SessionContext created here is the client side context, but the session_id is from server side.
    let session_context = SessionContext::new_with_state(session_state);
    session_context
}

#[derive(Debug)]
pub struct KapotQueryPlanner<T: AsLogicalPlan> {
    scheduler_url: String,
    config: KapotConfig,
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    plan_repr: PhantomData<T>,
}

impl<T: 'static + AsLogicalPlan> KapotQueryPlanner<T> {
    pub fn new(scheduler_url: String, config: KapotConfig) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
        }
    }

    pub fn with_extension(
        scheduler_url: String,
        config: KapotConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            plan_repr: PhantomData,
        }
    }

    pub fn with_repr(
        scheduler_url: String,
        config: KapotConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        plan_repr: PhantomData<T>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            plan_repr,
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan> QueryPlanner for KapotQueryPlanner<T> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match logical_plan {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(_)) => {
                // table state is managed locally in the kapotContext, not in the scheduler
                Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))))
            }
            _ => Ok(Arc::new(DistributedQueryExec::with_repr(
                self.scheduler_url.clone(),
                self.config.clone(),
                logical_plan.clone(),
                self.extension_codec.clone(),
                self.plan_repr,
                session_state.session_id().to_string(),
            ))),
        }
    }
}

pub async fn create_grpc_client_connection<D>(
    dst: D,
) -> std::result::Result<Channel, Error>
where
    D: std::convert::TryInto<tonic::transport::Endpoint>,
    D::Error: Into<StdError>,
{
    let endpoint = tonic::transport::Endpoint::new(dst)?
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);
    endpoint.connect().await
}

pub fn create_grpc_server() -> Server {
    Server::builder()
        .timeout(Duration::from_secs(20))
        // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_nodelay(true)
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keepalive_interval(Option::Some(Duration::from_secs(300)))
        .http2_keepalive_timeout(Option::Some(Duration::from_secs(20)))
}

pub fn collect_plan_metrics(plan: &dyn ExecutionPlan) -> Vec<MetricsSet> {
    let mut metrics_array = Vec::<MetricsSet>::new();
    if let Some(metrics) = plan.metrics() {
        metrics_array.push(metrics);
    }
    plan.children().iter().for_each(|c| {
        collect_plan_metrics(c.as_ref())
            .into_iter()
            .for_each(|e| metrics_array.push(e))
    });
    metrics_array
}

/// Given an interval in seconds, get the time in seconds before now
pub fn get_time_before(interval_seconds: u64) -> u64 {
    let now_epoch_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    now_epoch_ts
        .checked_sub(Duration::from_secs(interval_seconds))
        .unwrap_or_else(|| Duration::from_secs(0))
        .as_secs()
}
