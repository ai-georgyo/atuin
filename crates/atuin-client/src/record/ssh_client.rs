use eyre::{Result, bail};
use tokio::io::{BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::Mutex;

use atuin_common::record::{EncryptedData, HostId, Record, RecordIdx, RecordStatus};

use super::ssh_protocol::{Request, Response, read_message, write_message};
use super::store::RemoteStore;

struct SshIo {
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

pub struct SshClient {
    child: Mutex<Child>,
    io: Mutex<SshIo>,
}

impl SshClient {
    /// Spawn `ssh <destination> atuin ssh-sync serve` and return a client connected over stdio.
    pub async fn connect(destination: &str) -> Result<Self> {
        let mut child = tokio::process::Command::new("ssh")
            .arg(destination)
            .arg("--")
            .arg("atuin")
            .arg("ssh-sync")
            .arg("serve")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| eyre::eyre!("failed to open stdin to ssh process"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| eyre::eyre!("failed to open stdout from ssh process"))?;

        Ok(Self {
            child: Mutex::new(child),
            io: Mutex::new(SshIo {
                stdin: BufWriter::new(stdin),
                stdout: BufReader::new(stdout),
            }),
        })
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

    /// Send Goodbye and wait for the SSH process to exit.
    pub async fn close(self) -> Result<()> {
        // Send goodbye, ignoring errors (remote may have already closed)
        {
            let mut io = self.io.lock().await;
            let _ = write_message(&mut io.stdin, &Request::Goodbye).await;
        }
        let mut child = self.child.into_inner();
        child.wait().await?;
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
