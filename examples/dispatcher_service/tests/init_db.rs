//! Initialize a Sqlite database

pub(crate) const DB_CONN: &str = "sqlite://ds.sqlite";

#[actix_web::test]
async fn main() -> anyhow::Result<()> {
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
    use sqlx::ConnectOptions;
    use std::str::FromStr;

    let conn = SqliteConnectOptions::from_str(DB_CONN)?
        .journal_mode(SqliteJournalMode::Wal)
        .create_if_missing(true)
        .connect()
        .await;

    assert!(conn.is_ok(), "connection should not fail");

    Ok(())
}
