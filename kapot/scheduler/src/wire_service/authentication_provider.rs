use pgwire::{api::auth::{AuthSource, LoginInfo, Password}, error::PgWireResult};
use pgwire::api::auth::md5pass::hash_md5_password;


pub struct WireAuthenticationProvider {
    salt: Vec<u8>,

}

#[async_trait::async_trait]
impl AuthSource for WireAuthenticationProvider {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        let username = login_info.user();
        let hostname = login_info.host();
        let database = login_info.database();
        
        log::info!("Login request from {}@{}/{}", username.unwrap_or_default(), hostname, database.unwrap_or_default());

        if let Some(password) = self.get_password_for_user(username).await? {
            let hash_password = hash_md5_password(username.unwrap_or_default(), &password, self.salt.as_ref());
            
            Ok(Password::new(Some(self.salt.clone()), hash_password.as_bytes().to_vec()))
        } else {
            // TODO: Use the correct code for non authorized user
            Err(pgwire::error::PgWireError::InvalidAuthenticationMessageCode(0))
        }

    }
}

impl WireAuthenticationProvider {
    pub async fn get_password_for_user(&self, maybe_username: Option<&str>) -> PgWireResult<Option<String>> {
        let maybe_password = maybe_username.map(|username|{
            username.to_owned()
        });

        Ok(maybe_password)
    }
}