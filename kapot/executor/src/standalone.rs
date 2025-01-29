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
use kapot_core::{
    error::Result,
    serde::protobuf::executor_registration::OptionalHost,
    serde::protobuf::{scheduler_grpc_client::SchedulerGrpcClient, ExecutorRegistration},
    serde::scheduler::ExecutorSpecification,
    serde::KapotCodec,
    utils::create_grpc_server,
    KAPOT_VERSION,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use log::info;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use uuid::Uuid;

pub async fn new_standalone_executor<
    T: 'static + AsLogicalPlan,
    U: 'static + AsExecutionPlan,
>(
    scheduler: SchedulerGrpcClient<Channel>,
    concurrent_tasks: usize,
    codec: KapotCodec<T, U>,
) -> Result<()> {
    // Let the OS assign a random, free port
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    info!(
        "kapot v{} Rust Executor listening on {:?}",
        KAPOT_VERSION, addr
    );

    let executor_meta = ExecutorRegistration {
        id: Uuid::new_v4().to_string(), // assign this executor a unique ID
        optional_host: Some(OptionalHost::Host("0.0.0.0".to_string())),
        port: addr.port() as u32,
        // TODO Make it configurable
        grpc_port: 50020,
        specification: Some(
            ExecutorSpecification {
                task_slots: concurrent_tasks as u32,
            }
            .into(),
        ),
    };
    let work_dir = TempDir::new()?
        .into_path()
        .into_os_string()
        .into_string()
        .unwrap();
    info!("work_dir: {}", work_dir);

    let runtime = RuntimeEnvBuilder::new()
        .with_temp_file_path(work_dir.clone())
        .build()?;

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        Arc::new(runtime),
        None,
        Arc::new(LoggingMetricsCollector::default()),
        concurrent_tasks,
        None,
    ));

    let service = KapotFlightService::new();
    let server = FlightServiceServer::new(service);
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
