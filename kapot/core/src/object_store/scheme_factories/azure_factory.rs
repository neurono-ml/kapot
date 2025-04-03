use std::sync::Arc;

use object_store::{azure::MicrosoftAzureBuilder, ObjectStore};
use url::Url;

pub const AZURE_PROTOCOL: &str = "azure";
pub const AZ_PROTOCOL: &str = "az";
pub const FACTORY_NAME: &str = "Azure";


pub fn make_azure_az_factory(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
    let protocol = url.scheme();
    if protocol == AZURE_PROTOCOL || protocol == AZ_PROTOCOL {
        if let Some(bucket_name) = url.host_str() {
            let store = Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_container_name(bucket_name)
                    .build()?,
            );

            return Ok(store);
        } else {
            let message = format!("Invalid Url {}, no bucket name present", url);
            return Err(datafusion::error::DataFusionError::Internal(message));
        }
    } else {
        let message = format!("Invalid protocol {} for {} object store Factory", protocol, FACTORY_NAME);
        return Err(datafusion::error::DataFusionError::Internal(message));
    }
}