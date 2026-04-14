use eyre::{Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use atuin_common::record::{EncryptedData, HostId, Record, RecordIdx, RecordStatus};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Request {
    Status,
    Push {
        records: Vec<Record<EncryptedData>>,
    },
    Fetch {
        host: HostId,
        tag: String,
        start: RecordIdx,
        count: u64,
    },
    BuildRemote,
    Goodbye,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Response {
    Status {
        status: RecordStatus,
    },
    PushOk,
    Records {
        records: Vec<Record<EncryptedData>>,
    },
    BuildOk,
    Error {
        message: String,
    },
}

/// Read a length-prefixed JSON message from a reader.
/// Wire format: [4 bytes big-endian u32 length][JSON payload]
pub async fn read_message<T: DeserializeOwned>(
    reader: &mut (impl AsyncReadExt + Unpin),
) -> Result<T> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 64 * 1024 * 1024 {
        bail!("ssh-sync message too large: {len} bytes");
    }

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    let msg = serde_json::from_slice(&buf)?;
    Ok(msg)
}

/// Write a length-prefixed JSON message to a writer.
/// Wire format: [4 bytes big-endian u32 length][JSON payload]
pub async fn write_message<T: Serialize>(
    writer: &mut (impl AsyncWriteExt + Unpin),
    msg: &T,
) -> Result<()> {
    let json = serde_json::to_vec(msg)?;
    let len = (json.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}
