use sea_orm::*;

// Replace with your database URL and database name
const DATABASE_URL: &str = "mysql://root:root@localhost:3306";
const DB_NAME: &str = "bakeries_db";

pub(super) async fn set_up_db() -> Result<DatabaseConnection, DbErr> {
    let db = Database::connect(DATABASE_URL).await?;

    let db = match db.get_database_backend() {
        DbBackend::MySql => {
            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!("CREATE DATABASE IF NOT EXISTS `{}`;", DB_NAME),
            ))
            .await?;

            let url = format!("{}/{}", DATABASE_URL, DB_NAME);
            Database::connect(&url).await?
        }
        DbBackend::Postgres => {
            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!("DROP DATABASE IF EXISTS \"{}\";", DB_NAME),
            ))
            .await?;
            db.execute(Statement::from_string(
                db.get_database_backend(),
                format!("CREATE DATABASE \"{}\";", DB_NAME),
            ))
            .await?;

            let url = format!("{}/{}", DATABASE_URL, DB_NAME);
            Database::connect(&url).await?
        }
        DbBackend::Sqlite => {
            // let mut conn = db.begin().await?;
            // let c = conn.transaction(callback)
            // sqlx::migrate!("../migrations/sqlite")
            //     .run_direct(&mut conn)
            //     .await
            //     .unwrap();
            // conn.commit().await?;
            db
        }
    };

    Ok(db)
}
