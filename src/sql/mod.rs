//! # SQLite wrapper

use async_std::prelude::*;
use async_std::sync::RwLock;

use std::collections::HashSet;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use sqlx::{pool::PoolOptions, sqlite::*, Done, Execute, Executor, Row};

use crate::chat::{update_device_icon, update_saved_messages_icon};
use crate::constants::DC_CHAT_ID_TRASH;
use crate::context::Context;
use crate::dc_tools::*;
use crate::ephemeral::start_ephemeral_timers;
use crate::param::*;
use crate::peerstate::*;

mod error;
mod migrations;

pub use self::error::*;

/// A wrapper around the underlying Sqlite3 object.
#[derive(Debug)]
pub struct Sql {
    sql: RwLock<Option<SqlitePool>>,
}

impl Default for Sql {
    fn default() -> Self {
        Self {
            sql: RwLock::new(None),
        }
    }
}

impl Sql {
    pub fn new() -> Sql {
        Self::default()
    }

    pub async fn is_open(&self) -> bool {
        self.sql.read().await.is_some()
    }

    pub async fn close(&self) {
        if let Some(sql) = self.sql.write().await.take() {
            sql.close().await;
        }

        // drop closes the connection
    }

    pub async fn open<T: AsRef<Path>>(
        &self,
        context: &Context,
        dbfile: T,
        readonly: bool,
    ) -> crate::error::Result<()> {
        if self.is_open().await {
            error!(
                context,
                "Cannot open, database \"{:?}\" already opened.",
                dbfile.as_ref(),
            );
            return Err(Error::SqlAlreadyOpen.into());
        }

        let config = SqliteConnectOptions::new()
            .filename(dbfile.as_ref())
            .read_only(readonly)
            .create_if_missing(!readonly);
        let pool = PoolOptions::<Sqlite>::new()
            .after_connect(|conn| {
                Box::pin(async move {
                    let q = format!(
                        r#"
PRAGMA secure_delete=on;
PRAGMA busy_timeout = {};
PRAGMA temp_store=memory; -- Avoid SQLITE_IOERR_GETTEMPPATH errors on Android
"#,
                        Duration::from_secs(10).as_millis()
                    );
                    conn.execute_many(sqlx::query(&q))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .await?;
                    Ok(())
                })
            })
            .connect_with(config)
            .await?;
        {
            *self.sql.write().await = Some(pool);
        }

        if !readonly {
            // journal_mode is persisted, it is sufficient to change it only for one handle.
            // (nb: execute() always returns errors for this PRAGMA call, just discard it.
            // but even if execute() would handle errors more gracefully, we should continue on errors -
            // systems might not be able to handle WAL, in which case the standard-journal is used.
            // that may be not optimal, but better than not working at all :)
            self.execute("PRAGMA journal_mode=WAL;").await.ok();

            // (1) update low-level database structure.
            // this should be done before updates that use high-level objects that
            // rely themselves on the low-level structure.
            // --------------------------------------------------------------------

            let (recalc_fingerprints, update_icons) = migrations::run(context, &self).await?;

            // (2) updates that require high-level objects
            // (the structure is complete now and all objects are usable)
            // --------------------------------------------------------------------

            if recalc_fingerprints {
                info!(context, "[migration] recalc fingerprints");
                let mut rows = self.fetch("SELECT addr FROM acpeerstates;").await?;

                while let Some(row) = rows.next().await {
                    let row = row?;
                    let addr = row.try_get(0)?;
                    if let Some(ref mut peerstate) = Peerstate::from_addr(context, addr).await? {
                        peerstate.recalc_fingerprint();
                        peerstate.save_to_db(&self, false).await?;
                    }
                }
            }
            if update_icons {
                update_saved_messages_icon(context).await?;
                update_device_icon(context).await?;
            }
        }

        info!(context, "Opened {:?}.", dbfile.as_ref(),);

        Ok(())
    }

    pub async fn execute<'e, 'q, E>(&self, query: E) -> Result<u64>
    where
        'q: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        let lock = self.sql.read().await;
        let pool = lock.as_ref().ok_or(Error::SqlNoConnection)?;

        let rows = pool.execute(query).await?;
        Ok(rows.rows_affected())
    }

    pub async fn fetch<'e, 'q, E>(
        &self,
        query: E,
    ) -> Result<impl Stream<Item = sqlx::Result<<Sqlite as sqlx::Database>::Row>> + 'e + Send>
    where
        'q: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        let lock = self.sql.read().await;
        let pool = lock.as_ref().ok_or(Error::SqlNoConnection)?;

        let rows = pool.fetch(query);
        Ok(rows)
    }

    pub async fn fetch_one<'e, 'q, E>(&self, query: E) -> Result<<Sqlite as sqlx::Database>::Row>
    where
        'q: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        let lock = self.sql.read().await;
        let pool = lock.as_ref().ok_or(Error::SqlNoConnection)?;

        let row = pool.fetch_one(query).await?;
        Ok(row)
    }

    pub async fn fetch_optional<'e, 'q, E>(
        &self,
        query: E,
    ) -> Result<Option<<Sqlite as sqlx::Database>::Row>>
    where
        'q: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        let lock = self.sql.read().await;
        let pool = lock.as_ref().ok_or(Error::SqlNoConnection)?;

        let row = pool.fetch_optional(query).await?;
        Ok(row)
    }

    pub async fn count<'e, 'q, E>(&self, query: E) -> Result<usize>
    where
        'q: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        use std::convert::TryFrom;

        let row = self.fetch_one(query).await?;
        let count: i64 = row.try_get(0)?;

        Ok(usize::try_from(count).unwrap())
    }

    pub async fn exists<'e, 'q, E>(&self, query: E) -> Result<bool>
    where
        'q: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        let count = self.count(query).await?;
        Ok(count > 0)
    }

    /// Execute the function inside a transaction.
    ///
    /// If the function returns an error, the transaction will be rolled back. If it does not return an error, the transaction will be committed.
    pub async fn transaction<F, R>(&self, callback: F) -> Result<R>
    where
        F: for<'c> FnOnce(
                &'c mut sqlx::Transaction<'_, Sqlite>,
            ) -> Pin<Box<dyn Future<Output = Result<R>> + 'c + Send>>
            + 'static
            + Send
            + Sync,
        R: Send,
    {
        let lock = self.sql.read().await;
        let pool = lock.as_ref().ok_or(Error::SqlNoConnection)?;

        let mut transaction = pool.begin().await?;
        let ret = callback(&mut transaction).await;

        match ret {
            Ok(ret) => {
                transaction.commit().await?;

                Ok(ret)
            }
            Err(err) => {
                transaction.rollback().await?;

                Err(err)
            }
        }
    }

    pub async fn table_exists(&self, name: impl AsRef<str>) -> Result<bool> {
        let q = format!("PRAGMA table_info(\"{}\")", name.as_ref());

        let lock = self.sql.read().await;
        let pool = lock.as_ref().ok_or(Error::SqlNoConnection)?;

        let mut rows = pool.fetch(sqlx::query(&q));
        let first_row = rows.next().await;

        Ok(first_row.is_some() && first_row.unwrap().is_ok())
    }

    /// Executes a query which is expected to return one row and one
    /// column. If the query does not return a value or returns SQL
    /// `NULL`, returns `Ok(None)`.
    pub async fn query_get_value<'e, 'q, E, T>(&self, query: E) -> Result<Option<T>>
    where
        'q: 'e,
        E: 'q + Execute<'q, Sqlite>,
        T: for<'r> sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite>,
    {
        let res = self
            .fetch_optional(query)
            .await?
            .map(|row| row.get::<T, _>(0));
        Ok(res)
    }

    /// Set private configuration options.
    ///
    /// Setting `None` deletes the value.  On failure an error message
    /// will already have been logged.
    pub async fn set_raw_config(&self, key: impl AsRef<str>, value: Option<&str>) -> Result<()> {
        if !self.is_open().await {
            return Err(Error::SqlNoConnection);
        }

        let key = key.as_ref();
        if let Some(ref value) = value {
            let exists = self
                .exists(sqlx::query("SELECT COUNT(*) FROM config WHERE keyname=?;").bind(key))
                .await?;

            if exists {
                self.execute(
                    sqlx::query("UPDATE config SET value=? WHERE keyname=?;")
                        .bind(value)
                        .bind(key),
                )
                .await?;
            } else {
                self.execute(
                    sqlx::query("INSERT INTO config (keyname, value) VALUES (?, ?);")
                        .bind(key)
                        .bind(value),
                )
                .await?;
            }
        } else {
            self.execute(sqlx::query("DELETE FROM config WHERE keyname=?;").bind(key))
                .await?;
        }

        Ok(())
    }

    /// Get configuration options from the database.
    pub async fn get_raw_config(&self, key: impl AsRef<str>) -> Result<Option<String>> {
        if !self.is_open().await || key.as_ref().is_empty() {
            return Err(Error::SqlNoConnection);
        }
        self.query_get_value(
            sqlx::query("SELECT value FROM config WHERE keyname=?;").bind(key.as_ref()),
        )
        .await
    }

    pub async fn set_raw_config_int(&self, key: impl AsRef<str>, value: i32) -> Result<()> {
        self.set_raw_config(key, Some(&format!("{}", value))).await
    }

    pub async fn get_raw_config_int(&self, key: impl AsRef<str>) -> Result<Option<i32>> {
        self.get_raw_config(key)
            .await
            .map(|s| s.and_then(|s| s.parse().ok()))
    }

    pub async fn get_raw_config_bool(&self, key: impl AsRef<str>) -> Result<bool> {
        // Not the most obvious way to encode bool as string, but it is matter
        // of backward compatibility.
        let res = self.get_raw_config_int(key).await?;
        Ok(res.unwrap_or_default() > 0)
    }

    pub async fn set_raw_config_bool<T>(&self, key: T, value: bool) -> Result<()>
    where
        T: AsRef<str>,
    {
        let value = if value { Some("1") } else { None };
        self.set_raw_config(key, value).await
    }

    pub async fn set_raw_config_int64(&self, key: impl AsRef<str>, value: i64) -> Result<()> {
        self.set_raw_config(key, Some(&format!("{}", value))).await
    }

    pub async fn get_raw_config_int64(&self, key: impl AsRef<str>) -> Result<Option<i64>> {
        self.get_raw_config(key)
            .await
            .map(|s| s.and_then(|r| r.parse().ok()))
    }

    /// Alternative to sqlite3_last_insert_rowid() which MUST NOT be used due to race conditions, see comment above.
    /// the ORDER BY ensures, this function always returns the most recent id,
    /// eg. if a Message-ID is split into different messages.
    pub async fn get_rowid(
        &self,
        table: impl AsRef<str>,
        field: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<i64> {
        // alternative to sqlite3_last_insert_rowid() which MUST NOT be used due to race conditions, see comment above.
        // the ORDER BY ensures, this function always returns the most recent id,
        // eg. if a Message-ID is split into different messages.
        let query = format!(
            "SELECT id FROM {} WHERE {}=? ORDER BY id DESC",
            table.as_ref(),
            field.as_ref(),
        );

        self.query_get_value(sqlx::query(&query).bind(value.as_ref()))
            .await
            .map(|id| id.unwrap_or_default())
    }

    /// Fetches the rowid by restricting the rows through two different key, value settings.
    pub async fn get_rowid2(
        &self,
        table: impl AsRef<str>,
        field: impl AsRef<str>,
        value: i64,
        field2: impl AsRef<str>,
        value2: i64,
    ) -> Result<i64> {
        let query = format!(
            "SELECT id FROM {} WHERE {}={} AND {}={} ORDER BY id DESC",
            table.as_ref(),
            field.as_ref(),
            value,
            field2.as_ref(),
            value2,
        );

        self.query_get_value(sqlx::query(&query))
            .await
            .map(|id| id.unwrap_or_default())
    }
}

pub async fn housekeeping(context: &Context) -> Result<()> {
    let mut files_in_use = HashSet::new();
    let mut unreferenced_count = 0;

    info!(context, "Start housekeeping...");
    maybe_add_from_param(
        &context.sql,
        &mut files_in_use,
        "SELECT param FROM msgs  WHERE chat_id!=3   AND type!=10;",
        Param::File,
    )
    .await?;
    maybe_add_from_param(
        &context.sql,
        &mut files_in_use,
        "SELECT param FROM jobs;",
        Param::File,
    )
    .await?;
    maybe_add_from_param(
        &context.sql,
        &mut files_in_use,
        "SELECT param FROM chats;",
        Param::ProfileImage,
    )
    .await?;
    maybe_add_from_param(
        &context.sql,
        &mut files_in_use,
        "SELECT param FROM contacts;",
        Param::ProfileImage,
    )
    .await?;

    let mut rows = context.sql.fetch("SELECT value FROM config;").await?;
    while let Some(row) = rows.next().await {
        let row: String = row?.try_get(0)?;
        maybe_add_file(&mut files_in_use, row);
    }

    info!(context, "{} files in use.", files_in_use.len(),);
    /* go through directory and delete unused files */
    let p = context.get_blobdir();
    match async_std::fs::read_dir(p).await {
        Ok(mut dir_handle) => {
            /* avoid deletion of files that are just created to build a message object */
            let diff = std::time::Duration::from_secs(60 * 60);
            let keep_files_newer_than = std::time::SystemTime::now().checked_sub(diff).unwrap();

            while let Some(entry) = dir_handle.next().await {
                if entry.is_err() {
                    break;
                }
                let entry = entry.unwrap();
                let name_f = entry.file_name();
                let name_s = name_f.to_string_lossy();

                if is_file_in_use(&files_in_use, None, &name_s)
                    || is_file_in_use(&files_in_use, Some(".increation"), &name_s)
                    || is_file_in_use(&files_in_use, Some(".waveform"), &name_s)
                    || is_file_in_use(&files_in_use, Some("-preview.jpg"), &name_s)
                {
                    continue;
                }

                unreferenced_count += 1;

                if let Ok(stats) = async_std::fs::metadata(entry.path()).await {
                    let recently_created =
                        stats.created().is_ok() && stats.created().unwrap() > keep_files_newer_than;
                    let recently_modified = stats.modified().is_ok()
                        && stats.modified().unwrap() > keep_files_newer_than;
                    let recently_accessed = stats.accessed().is_ok()
                        && stats.accessed().unwrap() > keep_files_newer_than;

                    if recently_created || recently_modified || recently_accessed {
                        info!(
                            context,
                            "Housekeeping: Keeping new unreferenced file #{}: {:?}",
                            unreferenced_count,
                            entry.file_name(),
                        );
                        continue;
                    }
                }
                info!(
                    context,
                    "Housekeeping: Deleting unreferenced file #{}: {:?}",
                    unreferenced_count,
                    entry.file_name()
                );
                let path = entry.path();
                dc_delete_file(context, path).await;
            }
        }
        Err(err) => {
            warn!(
                context,
                "Housekeeping: Cannot open {}. ({})",
                context.get_blobdir().display(),
                err
            );
        }
    }

    if let Err(err) = start_ephemeral_timers(context).await {
        warn!(
            context,
            "Housekeeping: cannot start ephemeral timers: {}", err
        );
    }

    if let Err(err) = prune_tombstones(&context.sql).await {
        warn!(
            context,
            "Housekeeping: Cannot prune message tombstones: {}", err
        );
    }

    info!(context, "Housekeeping done.");
    Ok(())
}

#[allow(clippy::indexing_slicing)]
fn is_file_in_use(files_in_use: &HashSet<String>, namespc_opt: Option<&str>, name: &str) -> bool {
    let name_to_check = if let Some(namespc) = namespc_opt {
        let name_len = name.len();
        let namespc_len = namespc.len();
        if name_len <= namespc_len || !name.ends_with(namespc) {
            return false;
        }
        &name[..name_len - namespc_len]
    } else {
        name
    };
    files_in_use.contains(name_to_check)
}

fn maybe_add_file(files_in_use: &mut HashSet<String>, file: impl AsRef<str>) {
    if let Some(file) = file.as_ref().strip_prefix("$BLOBDIR/") {
        files_in_use.insert(file.to_string());
    }
}

async fn maybe_add_from_param(
    sql: &Sql,
    files_in_use: &mut HashSet<String>,
    query: &str,
    param_id: Param,
) -> Result<()> {
    let mut rows = sql.fetch(query).await?;
    while let Some(row) = rows.next().await {
        let row: String = row?.try_get(0)?;
        let param: Params = row.parse().unwrap_or_default();
        if let Some(file) = param.get(param_id) {
            maybe_add_file(files_in_use, file);
        }
    }
    Ok(())
}

/// Removes from the database locally deleted messages that also don't
/// have a server UID.
async fn prune_tombstones(sql: &Sql) -> Result<()> {
    sql.execute(
        sqlx::query(
            "DELETE FROM msgs \
         WHERE (chat_id = ? OR hidden) \
         AND server_uid = 0",
        )
        .bind(DC_CHAT_ID_TRASH),
    )
    .await?;
    Ok(())
}

/// Returns the SQLite version as a string; e.g., `"3.16.2"` for version 3.16.2.
pub fn version() -> &'static str {
    #[allow(unsafe_code)]
    let cstr = unsafe { std::ffi::CStr::from_ptr(libsqlite3_sys::sqlite3_libversion()) };
    cstr.to_str()
        .expect("SQLite version string is not valid UTF8 ?!")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_maybe_add_file() {
        let mut files = Default::default();
        maybe_add_file(&mut files, "$BLOBDIR/hello");
        maybe_add_file(&mut files, "$BLOBDIR/world.txt");
        maybe_add_file(&mut files, "world2.txt");
        maybe_add_file(&mut files, "$BLOBDIR");

        assert!(files.contains("hello"));
        assert!(files.contains("world.txt"));
        assert!(!files.contains("world2.txt"));
        assert!(!files.contains("$BLOBDIR"));
    }

    #[test]
    fn test_is_file_in_use() {
        let mut files = Default::default();
        maybe_add_file(&mut files, "$BLOBDIR/hello");
        maybe_add_file(&mut files, "$BLOBDIR/world.txt");
        maybe_add_file(&mut files, "world2.txt");

        assert!(is_file_in_use(&files, None, "hello"));
        assert!(!is_file_in_use(&files, Some(".txt"), "hello"));
        assert!(is_file_in_use(&files, Some("-suffix"), "world.txt-suffix"));
    }
}
