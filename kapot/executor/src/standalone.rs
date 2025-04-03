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

use crate::metrics::LoggingMetricsCollector;
use crate::{execution_loop, executor::Executor, flight_service::KapotFlightService};
use arrow_flight::flight_service_server::FlightServiceServer;
use kapot_core::config::KapotConfig;
use kapot_core::extension::SessionConfigExt;
use kapot_core::object_store::dynamic_store_registry::DynamicObjectStoreRegistry;
use kapot_core::registry::KapotFunctionRegistry;
use kapot_core::utils::default_config_producer;
use kapot_core::{
    error::Result,
    serde::protobuf::{scheduler_grpc_client::SchedulerGrpcClient, ExecutorRegistration},
    serde::scheduler::ExecutorSpecification,
    serde::KapotCodec,
    utils::create_grpc_server,
    KAPOT_VERSION,
};
use kapot_core::{ConfigProducer, RuntimeProducer};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionState, SessionStateBuilder};
use log::info;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use uuid::Uuid;

/// Creates new standalone executor based on
/// session_state provided.
///
/// This provides flexible way of configuring underlying
/// components.
pub async fn new_standalone_executor_from_state(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    session_state: &SessionState,
) -> Result<()> {
    let logical = session_state.config().kapot_logical_extension_codec();
    let physical = session_state.config().kapot_physical_extension_codec();

    let codec: KapotCodec<
        datafusion_proto::protobuf::LogicalPlanNode,
        datafusion_proto::protobuf::PhysicalPlanNode,
    > = KapotCodec::new(logical, physical);

    let config = session_state
        .config()
        .clone()
        .with_option_extension(KapotConfig::default()) // TODO: do we need this statement
        ;

    let runtime = session_state.runtime_env().clone();

    let config_producer: ConfigProducer = Arc::new(move || config.clone());
    let runtime_producer: RuntimeProducer = Arc::new(move |_| Ok(runtime.clone()));

    new_standalone_executor_from_builder(
        scheduler,
        concurrent_tasks,
        config_producer,
        runtime_producer,
        codec,
        session_state.into(),
    )
    .await
}


/// Creates standalone executor with most values
/// set as default.
pub async fn new_standalone_executor(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    codec: KapotCodec,
) -> Result<()> {
    let object_store_registry = DynamicObjectStoreRegistry::new();
    let runtime_env = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(object_store_registry))
        .build()?;

    let session_state = SessionStateBuilder::new()
        .with_runtime_env(Arc::new(runtime_env))
        .build();
    
    let runtime = session_state.runtime_env().clone();
    let runtime_producer: RuntimeProducer = Arc::new(move |_| Ok(runtime.clone()));

    new_standalone_executor_from_builder(
        scheduler,
        concurrent_tasks,
        Arc::new(default_config_producer),
        runtime_producer,
        codec,
        (&session_state).into(),
    )
    .await
}


pub async fn new_standalone_executor_from_builder(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    config_producer: ConfigProducer,
    runtime_producer: RuntimeProducer,
    codec: KapotCodec,
    function_registry: KapotFunctionRegistry,
) -> Result<()> {
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("localhost:0").await?;
    let address = listener.local_addr()?;
    info!(
        "kapot v{} Rust Executor listening on {:?}",
        KAPOT_VERSION, address
    );

    let executor_meta = ExecutorRegistration {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        host: Some("0.0.0.0".to_string()),
        port: address.port() as u32,
        // TODO Make it configurable
        grpc_port: 50020,
        specification: Some(
            ExecutorSpecification {
                task_slots: concurrent_tasks as u32,
            }
            .into(),
        ),
    };

    let config = config_producer();
    let max_message_size = config.kapot_grpc_client_max_message_size();

    let work_dir = TempDir::new()?
        .into_path()
        .into_os_string()
        .into_string()
        .unwrap();

    info!("work_dir: {}", work_dir);

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        Arc::new(function_registry),
        Arc::new(LoggingMetricsCollector::default()),
        concurrent_tasks,
        None,
    ));

    let service = KapotFlightService::new();
    let server = FlightServiceServer::new(service)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);

    tokio::spawn(
        create_grpc_server()
            .add_service(server)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                listener,
            )),
    );

    tokio::spawn(execution_loop::poll_loop(scheduler, executor, codec));
    Ok(())
}