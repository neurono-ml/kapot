use std::sync::Arc;

use std::time::Duration;
use object_store::{aws::AmazonS3Builder, ClientOptions, ObjectStore, RetryConfig};
use url::Url;

pub const MGC_PROTOCOL: &str = "mgc";
pub const MG_PROTOCOL: &str = "mg";
pub const FACTORY_NAME: &str = "Magalu Cloud MGC";


pub fn make_magalu_mgc_factory(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
    let protocol = url.scheme();
    if protocol == MGC_PROTOCOL || protocol == MG_PROTOCOL {
        if let Some(bucket_name) = url.host_str() {
            let store = build_mgc_object_store(url, bucket_name)?;
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

fn build_mgc_object_store(url: &Url, bucket_name: &str) -> datafusion::error::Result<Arc<object_store::aws::AmazonS3>> {
    log::debug!("Bucket is {} for url {}", bucket_name, url);
    const MGC_DEFAULT_REGION: &str = "br-se1";

    let retry_config = RetryConfig{
        max_retries: 50,
        ..RetryConfig::default()
    };

    let http_timeout_int =
        std::env::var("MGC_HTTP_TIMEOUT")
            .unwrap_or("120".to_owned())
            .parse::<u64>()
            .unwrap_or(120);
    let http_timeout_duration = Duration::from_secs(http_timeout_int);

    let client_options =
        ClientOptions::new()
            .with_timeout(http_timeout_duration)
            .with_http2_keep_alive_while_idle()
            .with_allow_invalid_certificates(true);
            
    let mut store_builder =
        AmazonS3Builder::from_env()
            .with_client_options(client_options)
            .with_retry(retry_config)
            .with_bucket_name(bucket_name);

    if let Ok(access_key) = std::env::var("MGC_ACCESS_KEY") {
        store_builder = store_builder.with_access_key_id(access_key);
    } else if let Ok(access_key) = std::env::var("MGC_ACCESS_KEY_ID") {
        store_builder = store_builder.with_access_key_id(access_key);
    } else if let Ok(access_key) = std::env::var("ACCESS_KEY_ID") {
        store_builder = store_builder.with_access_key_id(access_key);
    } else if let Ok(access_key) = std::env::var("ACCESS_KEY") {
        store_builder = store_builder.with_access_key_id(access_key);
    }

    if let Ok(secret_key) = std::env::var("MGC_SECRET_ACCESS_KEY") {
        store_builder = store_builder.with_secret_access_key(secret_key);
    } else if let Ok(secret_key) = std::env::var("MGC_SECRET_KEY") {
        store_builder = store_builder.with_secret_access_key(secret_key);
    } else if let Ok(secret_key) = std::env::var("SECRET_ACCESS_KEY") {
        store_builder = store_builder.with_secret_access_key(secret_key);
    } else if let Ok(secret_key) = std::env::var("SECRET_KEY") {
        store_builder = store_builder.with_secret_access_key(secret_key);
    }

    let region =
        if let Ok(region) = std::env::var("MGC_REGION") {
            store_builder = store_builder.with_region(&region);
            region
        } else if let Ok(region) = std::env::var("REGION") {
            store_builder = store_builder.with_region(&region);
            region
        } else {
            store_builder = store_builder.with_region(MGC_DEFAULT_REGION);
            MGC_DEFAULT_REGION.to_owned()
        };

    if let Ok(endpoint_url) = std::env::var("MGC_ENDPOINT_URL") {
        store_builder = store_builder.with_endpoint(endpoint_url);
    } else if let Ok(endpoint_url) = std::env::var("MGC_ENDPOINT") {
        store_builder = store_builder.with_endpoint(endpoint_url);
    } else if let Ok(endpoint_url) = std::env::var("ENDPOINT_URL") {
        store_builder = store_builder.with_endpoint(endpoint_url);
    } else if let Ok(endpoint_url) = std::env::var("ENDPOINT") {
        store_builder = store_builder.with_endpoint(endpoint_url);
    } else {
        let endpoint_for_region = format!("https://{region}.magaluobjects.com");
        store_builder = store_builder.with_endpoint(endpoint_for_region);
    }

    let store = Arc::new(store_builder.build()?);

    Ok(store)
}