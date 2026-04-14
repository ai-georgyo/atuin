use std::time::Duration;

use eyre::{Result, bail};
use tokio::io::{BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::Mutex;

use atuin_common::record::{EncryptedData, HostId, Record, RecordIdx, RecordStatus};

use super::ssh_protocol::{Request, Response, read_message, write_message};
use super::store::RemoteStore;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

struct SshIo {
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

pub struct SshClient {
    child: Child,
    io: Mutex<SshIo>,
}

impl Drop for SshClient {
    fn drop(&mut self) {
        // Ensure the child process is killed if we're dropped without close()
        let _ = self.child.start_kill();
    }
}

impl SshClient {
    /// Spawn `ssh <destination> atuin ssh-sync serve` and return a client connected over stdio.
    pub async fn connect(destination: &str) -> Result<Self> {
        let child = tokio::process::Command::new("ssh")
            .arg("-o")
            .arg("ConnectTimeout=10")
            .arg("--")
            .arg(destination)
            .arg("atuin")
            .arg("ssh-sync")
            .arg("serve")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()?;

        Self::from_child(child).await
    }

    /// Spawn an arbitrary command that runs `atuin ssh-sync serve` on its stdio.
    ///
    /// The command is passed to `sh -c`, so shell syntax (pipes, env vars, etc.) works.
    /// Examples:
    ///   "ssh -p 2222 user@host atuin ssh-sync serve"
    ///   "docker exec -i mycontainer atuin ssh-sync serve"
    pub async fn connect_exec(command: &str) -> Result<Self> {
        let child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()?;

        Self::from_child(child).await
    }

    async fn from_child(mut child: Child) -> Result<Self> {
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| eyre::eyre!("failed to open stdin to child process"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| eyre::eyre!("failed to open stdout from child process"))?;

        let client = Self {
            child,
            io: Mutex::new(SshIo {
                stdin: BufWriter::new(stdin),
                stdout: BufReader::new(stdout),
            }),
        };

        // Verify the remote is responsive with a timeout
        match tokio::time::timeout(CONNECT_TIMEOUT, client.request(&Request::Status)).await {
            Ok(Ok(Response::Status { .. })) => {}
            Ok(Ok(other)) => bail!("unexpected response during handshake: {other:?}"),
            Ok(Err(e)) => {
                return Err(e.wrap_err("remote atuin failed to respond — is atuin installed on the remote?"))
            }
            Err(_) => bail!("timed out waiting for remote atuin to respond ({}s)", CONNECT_TIMEOUT.as_secs()),
        }

        Ok(client)
    }

    async fn request(&self, req: &Request) -> Result<Response> {
        let mut io = self.io.lock().await;
        write_message(&mut io.stdin, req).await?;
        let resp: Response = read_message(&mut io.stdout).await?;

        if let Response::Error { ref message } = resp {
            bail!("remote error: {message}");
        }

        Ok(resp)
    }

    /// Tell the remote side to run build() to materialize records.
    pub async fn build_remote(&self) -> Result<()> {
        self.request(&Request::BuildRemote).await?;
        Ok(())
    }

    /// Send Goodbye and wait for the child process to exit gracefully.
    pub async fn close(mut self) -> Result<()> {
        // Send goodbye, ignoring errors (remote may have already closed)
        {
            let mut io = self.io.lock().await;
            let _ = write_message(&mut io.stdin, &Request::Goodbye).await;
        }
        self.child.wait().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl RemoteStore for SshClient {
    async fn status(&self) -> Result<RecordStatus> {
        match self.request(&Request::Status).await? {
            Response::Status { status } => Ok(status),
            other => bail!("unexpected response to Status: {other:?}"),
        }
    }

    async fn push(&self, records: &[Record<EncryptedData>]) -> Result<()> {
        match self.request(&Request::Push {
            records: records.to_vec(),
        })
        .await?
        {
            Response::PushOk => Ok(()),
            other => bail!("unexpected response to Push: {other:?}"),
        }
    }

    async fn fetch(
        &self,
        host: HostId,
        tag: String,
        start: RecordIdx,
        count: u64,
    ) -> Result<Vec<Record<EncryptedData>>> {
        match self
            .request(&Request::Fetch {
                host,
                tag,
                start,
                count,
            })
            .await?
        {
            Response::Records { records } => Ok(records),
            other => bail!("unexpected response to Fetch: {other:?}"),
        }
    }
}
