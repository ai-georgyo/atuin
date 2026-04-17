use std::future::Future;

use eyre::Result;
use tokio::io::{BufReader, BufWriter};

use super::encryption::PASETO_V4;
use super::ssh_protocol::{Request, Response, read_message, write_message};
use super::store::Store;

/// Run the SSH sync server loop, reading requests from stdin and writing responses to stdout.
///
/// The `encryption_key` is used to filter incoming records — only records encrypted with
/// a matching key are accepted. This prevents foreign-key records from poisoning the store.
///
/// The `on_build` callback is invoked when the client requests BuildRemote. This allows the
/// caller (in the CLI crate) to run the full build() that spans multiple crates.
pub async fn serve<F>(
    store: &impl Store,
    encryption_key: &[u8; 32],
    on_build: impl FnOnce() -> F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = BufWriter::new(tokio::io::stdout());

    let mut on_build = Some(on_build);

    loop {
        let req: Request = match read_message(&mut stdin).await {
            Ok(req) => req,
            Err(e) => {
                // EOF or broken pipe means the client disconnected
                debug!("ssh-sync serve: read error (client disconnected?): {e}");
                break;
            }
        };

        let resp = match req {
            Request::Status => match store.status().await {
                Ok(status) => Response::Status { status },
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },

            Request::Push { records } => {
                // Filter out records encrypted with a different key
                let (ours, foreign): (Vec<_>, Vec<_>) = records
                    .iter()
                    .partition(|r| PASETO_V4::key_matches(&r.data, encryption_key));

                if !foreign.is_empty() {
                    debug!(
                        "ssh-sync serve: rejected {} records with non-matching key",
                        foreign.len()
                    );
                }

                match store.push_batch(ours.into_iter()).await {
                    Ok(()) => Response::PushOk,
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }

            Request::Fetch {
                host,
                tag,
                start,
                count,
            } => match store.next(host, tag.as_str(), start, count).await {
                Ok(records) => Response::Records { records },
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },

            Request::BuildRemote => {
                if let Some(build_fn) = on_build.take() {
                    match build_fn().await {
                        Ok(()) => Response::BuildOk,
                        Err(e) => Response::Error {
                            message: e.to_string(),
                        },
                    }
                } else {
                    // Already built once this session
                    Response::BuildOk
                }
            }

            Request::Goodbye => break,
        };

        if let Err(e) = write_message(&mut stdout, &resp).await {
            debug!("ssh-sync serve: write error: {e}");
            break;
        }
    }

    Ok(())
}
