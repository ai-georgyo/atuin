use clap::Subcommand;
use eyre::{Context, Result, bail};

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
    /// Sync with a remote machine over SSH (or a custom command)
    Run {
        /// SSH destination (e.g., user@hostname)
        #[arg(required_unless_present = "exec")]
        destination: Option<String>,

        /// Custom command to start the remote sync server.
        /// The command is passed to `sh -c` and must run `atuin ssh-sync serve`
        /// with its stdio connected.
        ///
        /// Examples:
        ///   --exec "ssh -p 2222 user@host atuin ssh-sync serve"
        ///   --exec "docker exec -i mycontainer atuin ssh-sync serve"
        #[arg(long, conflicts_with = "destination")]
        exec: Option<String>,
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
            Self::Run { destination, exec } => {
                run_sync(&settings, db, store, destination.as_deref(), exec.as_deref()).await
            }
            Self::Serve => run_serve(&settings, db, store).await,
        }
    }
}

async fn run_sync(
    settings: &Settings,
    db: &impl Database,
    store: SqliteStore,
    destination: Option<&str>,
    exec: Option<&str>,
) -> Result<()> {
    let client = match (destination, exec) {
        (Some(dest), None) => {
            println!("Connecting to {dest} over SSH...");
            SshClient::connect(dest)
                .await
                .context("failed to connect over SSH")?
        }
        (None, Some(cmd)) => {
            println!("Running: {cmd}");
            SshClient::connect_exec(cmd)
                .await
                .context("failed to start sync command")?
        }
        _ => bail!("provide either a destination or --exec"),
    };

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
    client.close().await.context("failed to close session")?;

    println!(
        "Sync complete! {} items in history database",
        db.history_count(true).await?
    );

    Ok(())
}

async fn run_serve(
    settings: &Settings,
    db: &impl Database,
    store: SqliteStore,
) -> Result<()> {
    // Ensure local history is materialized into the record store before serving,
    // so the remote gets all our records (same preflight as regular sync).
    let encryption_key: [u8; 32] = encryption::load_key(settings)
        .context("could not load encryption key")?
        .into();
    let host_id = Settings::host_id().await?;
    let history_store = HistoryStore::new(store.clone(), host_id, encryption_key);

    let history_length = db.history_count(true).await?;
    let store_history_length = store.len_tag("history").await?;

    #[allow(clippy::cast_sign_loss)]
    if history_length as u64 > store_history_length {
        history_store.init_store(db).await?;
    }

    ssh_server::serve(&store, || async {
        crate::sync::build(settings, &store, db, None).await
    })
    .await
}
