use reqwest::Client;
use serde::Serialize;

pub(super) struct ClickHouse {
    pub(super) client: Client,
    pub(super) url: String,
    pub(super) database: String,
    pub(super) user: Option<String>,
    pub(super) password: Option<String>,
}

impl ClickHouse {
    pub(super) async fn insert<T: Serialize>(
        &self,
        table: &str,
        columns: &str,
        rows: &[T],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut body, row)?;
            body.push(b'\n');
        }
        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT JSONEachRow",
            self.database, table, columns
        );
        let mut request = self
            .client
            .post(&self.url)
            .query(&[("query", query)])
            .header("content-type", "application/json")
            .body(body);
        if let Some(user) = &self.user {
            request = request.basic_auth(user, self.password.as_deref());
        }
        if !request.send().await?.status().is_success() {
            return Err(std::io::Error::other("ClickHouse asset insert failed").into());
        }
        Ok(())
    }
}
