use anyhow::{Context, Ok};
use hyli_net::clock::TimestampMsClock;
use sqlx::Connection;

/// Ensures that `database_url` points to a usable Postgres database.
///
/// If `database_url` contains the literal placeholder `{db}`, this function will:
/// - Check `data_directory/database_name.txt` for a persisted database name to reuse.
/// - If no persisted name is found, generate a new unique DB name and create it, then
///   persist that to `data_directory/database_name.txt`.
pub async fn use_fresh_db(
    data_directory: &std::path::Path,
    database_url: &mut String,
) -> anyhow::Result<()> {
    if database_url.contains("{db}") {
        // Check for pre-existing DB name
        let db_file_path = data_directory.join("database_name.txt");
        if let Result::Ok(name) = std::fs::read_to_string(&db_file_path) {
            *database_url = database_url.replace("{db}", name.trim());
            tracing::info!(
                "Reusing existing database '{}' for the indexer and explorer.",
                name.trim()
            );
            return Ok(());
        }

        // Generate a probably unique db name based on timestamp
        let timestamp = TimestampMsClock::now().0;
        let name = format!("hyli_{timestamp}");
        *database_url = database_url.replace("{db}", &name);

        #[allow(clippy::unwrap_used, reason = "definitely should never fail")]
        let mut conn =
            sqlx::postgres::PgConnection::connect(database_url.rsplit_once('/').unwrap().0)
                .await
                .context("Failed to connect to Postgres to create a new database")?;

        sqlx::query(
            format!(
                "CREATE DATABASE {}",
                database_url.split('/').next_back().unwrap_or("postgres")
            )
            .as_str(),
        )
        .execute(&mut conn)
        .await
        .context("Failed to create new database")?;

        // Store file with db name to persist between restarts
        std::fs::write(&db_file_path, &name).context("writing database name to file")?;

        tracing::warn!(
            "Using new database '{}' for the indexer and explorer.",
            name
        );
    }

    Ok(())
}
