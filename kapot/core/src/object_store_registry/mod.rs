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

#[cfg(not(windows))]
pub mod cache;

use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry,
};

#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "azure")]
use object_store::azure::MicrosoftAzureBuilder;
#[cfg(feature = "gcs")]
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

/// An object store detector based on which features are enable for different kinds of object stores
#[derive(Debug, Default)]
pub struct KapotObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
}

impl KapotObjectStoreRegistry {
    pub fn new() -> Self {
        Default::default()
    }

    /// Find a suitable object store based on its url and enabled features if possible
    fn get_feature_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {

        log::debug!("Selecting object store for url {}", url);
        
        let url_str = url.as_str();

        #[cfg(feature = "s3")]
        {
            if url_str.starts_with("s3://") || url_str.starts_with("s3a://") {
                log::debug!("Selected S3 object store for url {}", url);

                if let Some(bucket_name) = url.host_str() {
                    log::debug!("Bucket is {} for url {}", bucket_name, url);

                    let store = Arc::new(
                        AmazonS3Builder::from_env()
                            .with_bucket_name(bucket_name)
                            .build()?,
                    );

                    log::debug!("Object store for {} successfully created", url);

                    return Ok(store);
                }
                // Support Alibaba Cloud OSS
                // Use S3 compatibility mode to access Alibaba Cloud OSS
                // The `AWS_ENDPOINT` should have bucket name included
            } else if url_str.starts_with("oss://") || url_str.starts_with("oci://") {
                if let Some(bucket_name) = url.host_str() {
                    let store = Arc::new(
                        AmazonS3Builder::from_env()
                            .with_virtual_hosted_style_request(true)
                            .with_bucket_name(bucket_name)
                            .build()?,
                    );
                    return Ok(store);
                }
            }
        }

        #[cfg(feature = "azure")]
        {
            if url_str.starts_with("azure://") || url_str.starts_with("az://") {
                if let Some(bucket_name) = url.host_str() {
                    let store = Arc::new(
                        MicrosoftAzureBuilder::from_env()
                            .with_container_name(bucket_name)
                            .build()?,
                    );
                    return Ok(store);
                }
            }
        }

        #[cfg(feature = "gcs")]
        {
            if url_str.starts_with("gs://") || url.to_string().starts_with("gcs://")
            {
                if let Some(bucket_name) = url.host_str() {
                    let store = Arc::new(
                        GoogleCloudStorageBuilder::from_env()
                            .with_bucket_name(bucket_name)
                            .build()?,
                    );
                    return Ok(store);
                }
            }
        }

        if url.to_string().starts_with("file://") {
            let store = Arc::new(LocalFileSystem::new());
            return Ok(store)
        }

        Err(DataFusionError::Execution(format!(
            "No object store available for: {url}"
        )))
    }
}

impl ObjectStoreRegistry for KapotObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url).or_else(|_| {
            let store = self.get_feature_store(url)?;
            self.inner.register_store(url, store.clone());

            Ok(store)
        })
    }
}
