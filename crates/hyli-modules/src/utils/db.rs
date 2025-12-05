use anyhow::{Context, Ok};

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
        let timestamp = chrono::Utc::now().timestamp();
        let name = format!("hyli_{timestamp}");
        *database_url = database_url.replace("{db}", &name);

        #[allow(clippy::unwrap_used, reason = "definitely should never fail")]
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect(database_url.rsplit_once('/').unwrap().0)
            .await
            .context("Failed to connect to Postgres to create a new database")?;

        sqlx::query(
            format!(
                "CREATE DATABASE {}",
                database_url.split('/').next_back().unwrap_or("postgres")
            )
            .as_str(),
        )
        .execute(&pool)
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
