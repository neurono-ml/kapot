use datafusion::error::DataFusionError;
use pgwire::error::PgWireError;


pub trait PgWireErrorExtension {
    fn from(error: DataFusionError) -> PgWireError {
        PgWireError::IoError(error.into())
    }
}

impl PgWireErrorExtension for PgWireError {}