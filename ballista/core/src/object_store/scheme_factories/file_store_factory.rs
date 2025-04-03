use std::sync::Arc;

use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use url::Url;

pub const FILE_PROTOCOL: &str = "file";
pub const FACTORY_NAME: &str = "Local File";


pub fn make_local_file_factory(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
    let protocol = url.scheme();

    if protocol == FILE_PROTOCOL {
        let store = Arc::new(LocalFileSystem::new());
        Ok(store)
    } else {
        let message = format!("Invalid protocol {} for {} object store Factory", protocol, FACTORY_NAME);
        return Err(datafusion::error::DataFusionError::Internal(message));
    }
}