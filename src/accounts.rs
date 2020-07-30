use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll};

use async_std::fs;
use async_std::path::PathBuf;
use async_std::sync::{Arc, RwLock};
use uuid::Uuid;

use anyhow::{ensure, Context as _};
use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::error::Result;

/// Account manager, that can handle multiple accounts in a single place.
#[derive(Debug, Clone)]
pub struct Accounts {
    dir: PathBuf,
    config: Config,
    accounts: Arc<RwLock<HashMap<u64, Context>>>,
}

impl Accounts {
    /// Loads or creates an accounts folder at the given `dir`.
    pub async fn new(os_name: String, dir: PathBuf) -> Result<Self> {
        if !dir.exists().await {
            Accounts::create(os_name, &dir).await?;
        }

        Accounts::open(dir).await
    }

    /// Creates a new default structure, including a default account.
    pub async fn create(os_name: String, dir: &PathBuf) -> Result<()> {
        fs::create_dir_all(dir)
            .await
            .context("failed to create folder")?;

        // create default account
        let config = Config::new(os_name.clone(), dir).await?;
        let account_config = config.new_account(dir).await?;

        Context::new(os_name, account_config.dbfile().into())
            .await
            .context("failed to create default account")?;

        Ok(())
    }

    /// Opens an existing accounts structure. Will error if the folder doesn't exist,
    /// no account exists and no config exists.
    pub async fn open(dir: PathBuf) -> Result<Self> {
        ensure!(dir.exists().await, "directory does not exist");

        let config_file = dir.join(CONFIG_NAME);
        ensure!(config_file.exists().await, "accounts.toml does not exist");

        let config = Config::from_file(config_file).await?;
        let accounts = config.load_accounts().await?;

        Ok(Self {
            dir,
            config,
            accounts: Arc::new(RwLock::new(accounts)),
        })
    }

    /// Get an account by its `id`:
    pub async fn get_account(&self, id: u64) -> Option<Context> {
        self.accounts.read().await.get(&id).cloned()
    }

    /// Get the currently selected account.
    pub async fn get_selected_account(&self) -> Context {
        let id = self.config.get_selected_account().await;
        self.accounts
            .read()
            .await
            .get(&id)
            .cloned()
            .expect("inconsistent state")
    }

    /// Select the given account.
    pub async fn select_account(&self, id: u64) -> Result<()> {
        self.config.select_account(id).await?;

        Ok(())
    }

    /// Add a new account.
    pub async fn add_account(&self) -> Result<u64> {
        let os_name = self.config.os_name().await;
        let account_config = self.config.new_account(&self.dir).await?;

        let ctx = Context::new(os_name, account_config.dbfile().into()).await?;
        self.accounts.write().await.insert(account_config.id, ctx);

        Ok(account_config.id)
    }

    /// Remove an account.
    pub async fn remove_account(&self, id: u64) -> Result<()> {
        let ctx = self.accounts.write().await.remove(&id);
        ensure!(ctx.is_some(), "no account with this id: {}", id);
        let ctx = ctx.unwrap();
        ctx.stop_io().await;
        drop(ctx);

        if let Some(cfg) = self.config.get_account(id).await {
            fs::remove_dir_all(async_std::path::PathBuf::from(&cfg.dir))
                .await
                .context("failed to remove account data")?;
        }
        self.config.remove_account(id).await?;

        Ok(())
    }

    /// Migrate an existing account into this structure.
    pub fn migrate_account(source: PathBuf) -> Result<u64> {
        todo!()
    }

    /// Get a list of all account ids.
    pub async fn get_all(&self) -> Vec<u64> {
        self.accounts.read().await.keys().copied().collect()
    }

    /// Import a backup using a new account and selects it.
    pub async fn import_account(&self, file: PathBuf) -> Result<u64> {
        let old_id = self.config.get_selected_account().await;

        let id = self.add_account().await?;
        let ctx = self.get_account(id).await.expect("just added");

        match crate::imex::imex(&ctx, crate::imex::ImexMode::ImportBackup, Some(file)).await {
            Ok(_) => Ok(id),
            Err(err) => {
                // remove temp account
                self.remove_account(id).await?;
                // set selection back
                self.select_account(old_id).await?;
                Err(err)
            }
        }
    }

    pub async fn start_io(&self) {
        let accounts = &*self.accounts.read().await;
        for account in accounts.values() {
            account.start_io().await;
        }
    }

    pub async fn stop_io(&self) {
        let accounts = &*self.accounts.read().await;
        for account in accounts.values() {
            account.stop_io().await;
        }
    }

    pub async fn maybe_network(&self) {
        let accounts = &*self.accounts.read().await;
        for account in accounts.values() {
            account.maybe_network().await;
        }
    }

    /// Unified event emitter.
    pub async fn get_event_emitter(&self) -> EventEmitter {
        let emitters = self
            .accounts
            .read()
            .await
            .iter()
            .map(|(id, a)| EmitterWrapper {
                id: *id,
                emitter: a.get_event_emitter(),
                done: AtomicBool::new(false),
            })
            .collect();

        EventEmitter(emitters)
    }
}

impl EventEmitter {
    /// Blocking recv of an event. Return `None` if the `Sender` has been droped.
    pub fn recv_sync(&self) -> Option<Event> {
        async_std::task::block_on(self.recv())
    }

    /// Async recv of an event. Return `None` if the `Sender` has been droped.
    pub async fn recv(&self) -> Option<Event> {
        futures::future::poll_fn(|cx| Pin::new(self).recv_poll(cx)).await
    }

    fn recv_poll(self: Pin<&Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Event>> {
        for e in &*self.0 {
            if e.done.load(Ordering::Acquire) {
                continue;
            }

            match e.emitter.try_recv() {
                Ok(event) => return Poll::Ready(Some(Event { event, id: e.id })),
                Err(async_std::sync::TryRecvError::Disconnected) => {
                    e.done.store(false, Ordering::Release);
                }
                Err(async_std::sync::TryRecvError::Empty) => {}
            }
        }

        Poll::Pending
    }
}

#[derive(Debug)]
pub struct EventEmitter(Vec<EmitterWrapper>);

#[derive(Debug)]
struct EmitterWrapper {
    id: u64,
    emitter: crate::events::EventEmitter,
    done: AtomicBool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The id of the account that emitted the event.
    pub id: u64,
    pub event: crate::events::Event,
}

pub const CONFIG_NAME: &str = "accounts.toml";
pub const DB_NAME: &str = "dc.db";

#[derive(Debug, Clone)]
pub struct Config {
    file: PathBuf,
    inner: Arc<RwLock<InnerConfig>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct InnerConfig {
    pub os_name: String,
    /// The currently selected account.
    pub selected_account: u64,
    pub next_id: u64,
    pub accounts: Vec<AccountConfig>,
}

impl Config {
    pub async fn new(os_name: String, dir: &PathBuf) -> Result<Self> {
        let cfg = Config {
            file: dir.join(CONFIG_NAME),
            inner: Arc::new(RwLock::new(InnerConfig {
                os_name,
                accounts: Vec::new(),
                selected_account: 0,
                next_id: 0,
            })),
        };

        cfg.sync().await?;

        Ok(cfg)
    }

    pub async fn os_name(&self) -> String {
        self.inner.read().await.os_name.clone()
    }

    /// Sync the inmemory representation to disk.
    async fn sync(&self) -> Result<()> {
        fs::write(
            &self.file,
            toml::to_string_pretty(&*self.inner.read().await)?,
        )
        .await
        .context("failed to write config")
    }

    /// Read a configuration from the given file into memory.
    pub async fn from_file(file: PathBuf) -> Result<Self> {
        let bytes = fs::read(&file).await.context("failed to read file")?;
        let inner: InnerConfig = toml::from_slice(&bytes).context("failed to parse config")?;

        Ok(Config {
            file,
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn load_accounts(&self) -> Result<HashMap<u64, Context>> {
        let cfg = &*self.inner.read().await;
        let mut accounts = HashMap::with_capacity(cfg.accounts.len());
        for account_config in &cfg.accounts {
            let ctx = Context::new(cfg.os_name.clone(), account_config.dbfile().into()).await?;
            accounts.insert(account_config.id, ctx);
        }

        Ok(accounts)
    }

    /// Create a new account in the given root directory.
    pub async fn new_account(&self, dir: &PathBuf) -> Result<AccountConfig> {
        let id = {
            let inner = &mut self.inner.write().await;
            let id = inner.next_id;
            let uuid = Uuid::new_v4();
            let target_dir = dir.join(uuid.to_simple_ref().to_string());

            inner.accounts.push(AccountConfig {
                id,
                name: String::new(),
                dir: target_dir.into(),
                uuid,
            });
            inner.next_id += 1;
            id
        };

        self.sync().await?;

        self.select_account(id).await.expect("just added");
        let cfg = self.get_account(id).await.expect("just added");
        Ok(cfg)
    }

    /// Removes an existing acccount entirely.
    pub async fn remove_account(&self, id: u64) -> Result<()> {
        {
            let inner = &mut *self.inner.write().await;
            if let Some(idx) = inner.accounts.iter().position(|e| e.id == id) {
                // remove account from the configs
                inner.accounts.remove(idx);
            }
            if inner.selected_account == id {
                // reset selected account
                inner.selected_account = inner.accounts.get(0).map(|e| e.id).unwrap_or_default();
            }
        }

        self.sync().await
    }

    pub async fn get_account(&self, id: u64) -> Option<AccountConfig> {
        self.inner
            .read()
            .await
            .accounts
            .iter()
            .find(|e| e.id == id)
            .cloned()
    }

    pub async fn get_selected_account(&self) -> u64 {
        self.inner.read().await.selected_account
    }

    pub async fn select_account(&self, id: u64) -> Result<()> {
        {
            let inner = &mut *self.inner.write().await;
            ensure!(
                inner.accounts.iter().any(|e| e.id == id),
                "invalid account id: {}",
                id
            );

            inner.selected_account = id;
        }

        self.sync().await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AccountConfig {
    /// Unique id.
    pub id: u64,
    /// Display name
    pub name: String,
    /// Root directory for all data for this account.
    pub dir: std::path::PathBuf,
    pub uuid: Uuid,
}

impl AccountConfig {
    /// Get the canoncial dbfile name for this configuration.
    pub fn dbfile(&self) -> std::path::PathBuf {
        self.dir.join(DB_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn test_account_new_open() {
        let dir = tempfile::tempdir().unwrap();
        let p: PathBuf = dir.path().join("accounts1").into();

        let accounts1 = Accounts::new("my_os".into(), p.clone()).await.unwrap();
        let accounts2 = Accounts::open(p).await.unwrap();

        assert_eq!(accounts1.accounts.read().await.len(), 1);
        assert_eq!(accounts1.config.get_selected_account().await, 0);

        assert_eq!(accounts1.dir, accounts2.dir);
        assert_eq!(
            &*accounts1.config.inner.read().await,
            &*accounts2.config.inner.read().await,
        );
        assert_eq!(
            accounts1.accounts.read().await.len(),
            accounts2.accounts.read().await.len()
        );
    }

    #[async_std::test]
    async fn test_account_new_add_remove() {
        let dir = tempfile::tempdir().unwrap();
        let p: PathBuf = dir.path().join("accounts").into();

        let accounts = Accounts::new("my_os".into(), p.clone()).await.unwrap();

        assert_eq!(accounts.accounts.read().await.len(), 1);
        assert_eq!(accounts.config.get_selected_account().await, 0);

        let id = accounts.add_account().await.unwrap();
        assert_eq!(id, 1);
        assert_eq!(accounts.config.get_selected_account().await, id);
        assert_eq!(accounts.accounts.read().await.len(), 2);

        accounts.select_account(0).await.unwrap();
        assert_eq!(accounts.config.get_selected_account().await, 0);

        accounts.remove_account(0).await.unwrap();
        assert_eq!(accounts.config.get_selected_account().await, 1);
        assert_eq!(accounts.accounts.read().await.len(), 1);
    }
}
