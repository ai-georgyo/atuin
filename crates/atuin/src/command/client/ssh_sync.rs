use clap::Subcommand;
use eyre::{Context, Result};

use atuin_client::{
    database::Database,
    encryption,
    history::store::HistoryStore,
    record::{
        sqlite_store::SqliteStore,
        ssh_client::SshClient,
        ssh_server,
        store::Store,
        sync,
    },
    settings::Settings,
};

#[derive(Subcommand, Debug)]
#[command(infer_subcommands = true)]
pub enum Cmd {
    /// Sync with a remote machine over SSH
    Run {
        /// SSH destination (e.g., user@hostname)
        destination: String,
    },

    /// SSH sync server (called automatically by the remote end)
    #[command(hide = true)]
    Serve,
}

impl Cmd {
    pub async fn run(
        self,
        settings: Settings,
        db: &impl Database,
        store: SqliteStore,
    ) -> Result<()> {
        match self {
            Self::Run { destination } => run_sync(&settings, db, store, &destination).await,
            Self::Serve => run_serve(&settings, db, store).await,
        }
    }
}

async fn run_sync(
    settings: &Settings,
    db: &impl Database,
    store: SqliteStore,
    destination: &str,
) -> Result<()> {
    println!("Connecting to {destination} over SSH...");

    let client = SshClient::connect(destination)
        .await
        .context("failed to connect over SSH")?;

    let (uploaded, downloaded) = sync::sync_with_remote(&client, &store).await?;

    println!("{uploaded}/{} up/down to record store", downloaded.len());

    // Tell the remote to materialize its records
    client
        .build_remote()
        .await
        .context("failed to build on remote")?;

    // Materialize records locally
    crate::sync::build(settings, &store, db, Some(&downloaded)).await?;

    // Check if local history needs re-init (same logic as regular sync)
    let encryption_key: [u8; 32] = encryption::load_key(settings)
        .context("could not load encryption key")?
        .into();
    let host_id = Settings::host_id().await?;
    let history_store = HistoryStore::new(store.clone(), host_id, encryption_key);

    let history_length = db.history_count(true).await?;
    let store_history_length = store.len_tag("history").await?;

    #[allow(clippy::cast_sign_loss)]
    if history_length as u64 > store_history_length {
        println!(
            "{history_length} in history index, but {store_history_length} in history store"
        );
        println!("Running automatic history store init...");
        history_store.init_store(db).await?;

        println!("Re-running sync due to new records locally");
        let (uploaded, downloaded) = sync::sync_with_remote(&client, &store).await?;
        crate::sync::build(settings, &store, db, Some(&downloaded)).await?;
        println!("{uploaded}/{} up/down to record store", downloaded.len());
    }

    // Clean shutdown
    client.close().await.context("failed to close SSH session")?;

    println!(
        "SSH sync complete! {} items in history database",
        db.history_count(true).await?
    );

    Ok(())
}

async fn run_serve(
    settings: &Settings,
    db: &impl Database,
    store: SqliteStore,
) -> Result<()> {
    ssh_server::serve(&store, || async {
        crate::sync::build(settings, &store, db, None).await
    })
    .await
}
