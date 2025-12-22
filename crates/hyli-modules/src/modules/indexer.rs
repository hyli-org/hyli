pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./src/modules/indexer/migrations");
