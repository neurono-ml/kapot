use std::sync::Arc;
use datafusion::{execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry}, prelude::SessionConfig};

use object_store::ObjectStore;
use url::Url;

#[cfg(feature = "oss")]
use crate::object_store::scheme_factories::alibaba_oss_factory::{make_alibaba_oss_factory, OSS_PROTOCOL};
#[cfg(feature = "s3")]
use crate::object_store::scheme_factories::aws_s3_factory::{make_aws_s3_factory, S3A_PROTOCOL, S3_PROTOCOL};
#[cfg(feature = "azure")]
use crate::object_store::scheme_factories::azure_factory::{make_azure_az_factory, AZURE_PROTOCOL, AZ_PROTOCOL};
use crate::object_store::scheme_factories::file_store_factory::{make_local_file_factory, FILE_PROTOCOL};
#[cfg(feature = "gcs")]
use crate::object_store::scheme_factories::google_gcs_factory::{make_google_gcs_factory, GCS_PROTOCOL, GS_PROTOCOL};
#[cfg(feature = "mgc")]
use crate::object_store::scheme_factories::magalu_mgc_factory::{make_magalu_mgc_factory, MGC_PROTOCOL, MG_PROTOCOL};

#[derive(Debug, Default)]
pub struct DynamicObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
}

impl DynamicObjectStoreRegistry {
    pub fn new_with_config(_config: &SessionConfig) -> Self {
        Default::default()
    }

    pub fn new() -> Self {
        Default::default()
    }

    fn get_feature_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        log::debug!("Selecting an object store for url {}", url);

        let object_store=
            match url.scheme() {
                #[cfg(feature = "s3")]
                S3_PROTOCOL => make_aws_s3_factory(url)?,
                #[cfg(feature = "s3")]
                S3A_PROTOCOL => make_aws_s3_factory(url)?,
                #[cfg(feature = "oss")]
                OSS_PROTOCOL => make_alibaba_oss_factory(url)?,
                #[cfg(feature = "azure")]
                AZURE_PROTOCOL => make_azure_az_factory(url)?,
                #[cfg(feature = "azure")]
                AZ_PROTOCOL => make_azure_az_factory(url)?,
                #[cfg(feature = "gcs")]
                GCS_PROTOCOL => make_google_gcs_factory(url)?,
                #[cfg(feature = "gcs")]
                GS_PROTOCOL => make_google_gcs_factory(url)?,
                #[cfg(feature = "mgc")]
                MGC_PROTOCOL => make_magalu_mgc_factory(url)?,
                #[cfg(feature = "mgc")]
                MG_PROTOCOL => make_magalu_mgc_factory(url)?,
                FILE_PROTOCOL => make_local_file_factory(url)?,

                protocol => {
                    let message = format!("No object store factory available for protocol {}", protocol);
                    return Err(datafusion::error::DataFusionError::Internal(message));
                }
            };

        Ok(object_store)
    }
}


impl ObjectStoreRegistry for DynamicObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let scheme = url.scheme();
        if let Some(domain) = url.host_str() {
            log::trace!("Registering object store for {}", url);

            log::debug!("Registering object store for {}://{}", scheme, domain);
            self.inner.register_store(url, store)
        } else {
            log::error!("Invalid URL for object: url `{}` has no domain/bucket name", url);
            None
        }
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        log::info!("Getting object store for url {}", url);

        self.inner.get_store(url).or_else(|_| {
            let store = self.get_feature_store(url)?;
            self.inner.register_store(url, store.clone());

            Ok(store)
        })
    }
}