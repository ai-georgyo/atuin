use eyre::{Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use atuin_common::record::{EncryptedData, HostId, Record, RecordIdx, RecordStatus};

const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

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

    if len > MAX_MESSAGE_SIZE {
        bail!("ssh-sync message too large: {len} bytes (max {MAX_MESSAGE_SIZE})");
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

    if json.len() > MAX_MESSAGE_SIZE {
        bail!(
            "ssh-sync message too large to send: {} bytes (max {})",
            json.len(),
            MAX_MESSAGE_SIZE
        );
    }

    let len = (json.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&json).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use atuin_common::record::Host;

    #[tokio::test]
    async fn roundtrip_request() {
        let req = Request::Fetch {
            host: HostId(atuin_common::utils::uuid_v7()),
            tag: "history".into(),
            start: 42,
            count: 100,
        };

        let mut buf = Vec::new();
        write_message(&mut buf, &req).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded: Request = read_message(&mut cursor).await.unwrap();

        match decoded {
            Request::Fetch {
                tag, start, count, ..
            } => {
                assert_eq!(tag, "history");
                assert_eq!(start, 42);
                assert_eq!(count, 100);
            }
            other => panic!("expected Fetch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_response_status() {
        let mut hosts = HashMap::new();
        let host_id = HostId(atuin_common::utils::uuid_v7());
        let mut tags = HashMap::new();
        tags.insert("history".into(), 10u64);
        hosts.insert(host_id, tags);

        let resp = Response::Status {
            status: RecordStatus { hosts },
        };

        let mut buf = Vec::new();
        write_message(&mut buf, &resp).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded: Response = read_message(&mut cursor).await.unwrap();

        match decoded {
            Response::Status { status } => {
                assert_eq!(*status.hosts.get(&host_id).unwrap().get("history").unwrap(), 10);
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn roundtrip_multiple_messages() {
        let messages: Vec<Request> = vec![
            Request::Status,
            Request::BuildRemote,
            Request::Goodbye,
        ];

        let mut buf = Vec::new();
        for msg in &messages {
            write_message(&mut buf, msg).await.unwrap();
        }

        let mut cursor = std::io::Cursor::new(buf);
        let r1: Request = read_message(&mut cursor).await.unwrap();
        let r2: Request = read_message(&mut cursor).await.unwrap();
        let r3: Request = read_message(&mut cursor).await.unwrap();

        assert!(matches!(r1, Request::Status));
        assert!(matches!(r2, Request::BuildRemote));
        assert!(matches!(r3, Request::Goodbye));
    }

    #[tokio::test]
    async fn read_rejects_oversized_length() {
        // Craft a message with a length prefix claiming 128 MiB
        let fake_len = (128u32 * 1024 * 1024).to_be_bytes();
        let mut cursor = std::io::Cursor::new(fake_len.to_vec());
        let result: Result<Request> = read_message(&mut cursor).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too large"));
    }

    #[tokio::test]
    async fn roundtrip_push_with_records() {
        let record = Record::builder()
            .host(Host::new(HostId(atuin_common::utils::uuid_v7())))
            .version("v1".into())
            .tag("history".into())
            .data(EncryptedData {
                data: "encrypted_payload".into(),
                content_encryption_key: "wrapped_key".into(),
            })
            .idx(0)
            .build();

        let req = Request::Push {
            records: vec![record.clone()],
        };

        let mut buf = Vec::new();
        write_message(&mut buf, &req).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded: Request = read_message(&mut cursor).await.unwrap();

        match decoded {
            Request::Push { records } => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].tag, "history");
                assert_eq!(records[0].data.data, "encrypted_payload");
            }
            other => panic!("expected Push, got {other:?}"),
        }
    }
}
