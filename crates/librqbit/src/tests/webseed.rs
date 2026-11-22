//! A torrent whose only source is a web seed (BEP 19) downloads completely.
use std::{path::PathBuf, time::Duration};

use bytes::Bytes;
use librqbit_core::torrent_metainfo::UrlList;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    AddTorrent, AddTorrentOptions, Session, SessionOptions, create_torrent,
    spawn_utils::BlockingSpawner,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
};

/// A tiny HTTP server for `root` that answers `Range: bytes=a-b` requests.
async fn serve(listener: TcpListener, root: PathBuf) {
    loop {
        let Ok((mut conn, _)) = listener.accept().await else {
            return;
        };
        let root = root.clone();
        tokio::spawn(async move {
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = conn.read(&mut tmp).await.unwrap();
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            let head = String::from_utf8_lossy(&buf).into_owned();
            let path = head
                .split_whitespace()
                .nth(1)
                .unwrap()
                .trim_start_matches('/');
            let range = head.lines().find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("range: bytes=")
                    .map(str::to_owned)
            });
            let response = match std::fs::read(root.join(path)) {
                Err(_) => {
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_vec()
                }
                Ok(data) => {
                    let (from, to) = range
                        .and_then(|r| {
                            let (a, b) = r.trim().split_once('-')?;
                            Some((a.parse::<usize>().ok()?, b.parse::<usize>().ok()?))
                        })
                        .unwrap();
                    let body = &data[from..=to];
                    let mut r = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {from}-{to}/{}\r\nConnection: close\r\n\r\n",
                        body.len(),
                        data.len()
                    )
                    .into_bytes();
                    r.extend_from_slice(body);
                    r
                }
            };
            let _ = conn.write_all(&response).await;
        });
    }
}

#[tokio::test]
async fn test_webseed_only_download() -> anyhow::Result<()> {
    setup_test_logging();
    // Files whose sizes are not a multiple of the piece length, so pieces span two files.
    let files = create_default_random_dir_with_torrents(3, 100_003, Some("rqbit_webseed"));
    let name = files
        .path()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let mut torrent =
        create_torrent(files.path(), Default::default(), &BlockingSpawner::new(1)).await?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(serve(listener, files.path().parent().unwrap().to_owned()));
    torrent.meta.url_list = Some(UrlList::One(
        Bytes::from(format!("http://127.0.0.1:{port}/")).into(),
    ));
    let bytes = torrent.as_bytes()?;

    let out = tempfile::TempDir::with_prefix("rqbit_webseed_out")?;
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            listen: None,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(bytes),
            Some(AddTorrentOptions {
                output_folder: Some(out.path().to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .unwrap();
    assert_eq!(handle.shared().web_seeds().len(), 1);
    tokio::time::timeout(Duration::from_secs(30), handle.wait_until_completed()).await??;

    for f in 0..3 {
        let file = format!("{f}.data");
        let got = std::fs::read(out.path().join(&file))?;
        let want = std::fs::read(files.path().join(&file))?;
        assert!(got == want, "{name}/{file} differs");
    }
    Ok(())
}
