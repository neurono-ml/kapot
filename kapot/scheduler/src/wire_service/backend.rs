use std::{pin::Pin, sync::Arc};

use async_stream::stream;
use datafusion::{arrow::{array::{Datum, RecordBatch}, datatypes::DataType}, common::DFSchema, execution::RecordBatchStream, parquet::data_type::DataType, prelude::SessionContext, sql::sqlparser::dialect::SQLiteDialect};
use futures::{pin_mut, Stream, StreamExt};
use pgwire::{api::{query::SimpleQueryHandler, results::{FieldFormat, FieldInfo, QueryResponse, Response}, ClientInfo}, error::PgWireResult, messages::data::DataRow};

use super::error_extension::PgWireErrorExtension;
pub struct WireBackend {
    session_context: Arc<SessionContext>,
    query_parser: Arc<SQLiteDialect>,
}

#[async_trait::async_trait]
impl SimpleQueryHandler for WireBackend {
    async fn do_query<'a, C>(&self, client: &mut C, sql: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync
    {
        let context = self.session_context.clone();
        let output_dataframe = self.session_context.sql(sql).await.map_err(PgWireErrorExtension::from)?;

        let field_definitions = Arc::new(extract_field_definitions_from_schema(output_dataframe.schema()));
        let row_stream = output_dataframe.execute_stream().await.map(as_row_stream).map_err(PgWireErrorExtension::from)?;

        Ok(vec![Response::Query(QueryResponse::new(field_definitions, row_stream))])
    }
}

async fn as_row_stream(record_batch_stream: Pin<Box<dyn RecordBatchStream + std::marker::Send>>) -> impl Stream<Item = PgWireResult<DataRow>>
{    
    stream! {
        pin_mut!(record_batch_stream);

        while let Some(Ok(record_batch))  = record_batch_stream.next().await {
            let data_row_results = extract_data_rows(record_batch);
            for data_row_result in data_row_results {
                yield data_row_result;
            }
        }
    }
}

fn extract_field_definitions_from_schema(schema: &DFSchema) -> Vec<FieldInfo> {
    let fields =
        schema.fields().iter().map(|arrow_field|{
            let data_type = convert_data_type(arrow_field.data_type());
            FieldInfo::new(arrow_field.name().to_owned(), None, None, data_type, FieldFormat::Text)
        })
        .collect();

    fields
}

fn extract_data_rows(record_batch: RecordBatch) -> Vec<PgWireResult<DataRow>> {
    // Assuming PgWireResult and DataRow are defined as:
    // struct PgWireResult<T>(T);
    // struct DataRow { /* fields */ }

    let mut rows = Vec::new();
    for column in record_batch.columns().iter() {
        let data_row = DataRow { /* initialize with row data */ };
        rows.push(PgWireResult(data_row));
    }
    rows
}


fn convert_data_type(data_type: &DataType) -> pgwire::api::Type {
    match data_type {
        DataType::Utf8 => pgwire::api::Type::TEXT,
        DataType::Int32 => pgwire::api::Type::INT8,
        DataType::Int16 => pgwire::api::Type::INT4,
        // Add more cases as needed
        _ => pgwire::api::Type::ANY,
    }
}