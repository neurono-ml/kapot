use std::sync::Arc;

use object_store::{aws::AmazonS3Builder, ObjectStore};
use url::Url;

pub const S3_PROTOCOL: &str = "s3";
pub const S3A_PROTOCOL: &str = "s3a";
pub const FACTORY_NAME: &str = "Amazon S3";


pub fn make_aws_s3_factory(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
    let protocol = url.scheme();
    if protocol == S3_PROTOCOL || protocol == S3A_PROTOCOL {
        if let Some(bucket_name) = url.host_str() {
            let store = Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
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