//! Web seeds (BEP 19): download pieces from HTTP servers listed in the torrent's `url-list`.
//!
//! Each web seed runs as one task. It reserves a piece like a peer would, fetches the piece's
//! bytes with HTTP range requests (a piece can span several files), writes them and checks the
//! hash. Peers may steal a piece from a slow web seed; the write then notices and drops it.
use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    num::NonZeroU32,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, bail};
use librqbit_core::lengths::ValidPieceIndex;
use peer_binary_protocol::Piece;
use tracing::{debug, trace, warn};

use super::{TorrentStateLive, TorrentStateLocked};
use crate::{
    Error,
    chunk_tracker::ChunkMarkingResult,
    piece_tracker::{AcquireRequest, AcquireResult},
    type_aliases::PeerHandle,
};

/// A made-up peer address for web seed number `i`, from the discard-only prefix 100::/64
/// (RFC 6666) so that it never matches a real peer.
pub(crate) fn webseed_addr(i: usize) -> PeerHandle {
    #[allow(clippy::cast_possible_truncation)]
    let ip = Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, (i >> 16) as u16, i as u16);
    SocketAddr::new(IpAddr::V6(ip), 0)
}

/// The URL of each file of the torrent on the web seed `base`, as BEP 19 lays them out.
/// `None` for padding files, which are not on the server.
pub(crate) fn file_urls(
    base: &url::Url,
    name: &str,
    single_file: bool,
    files: impl Iterator<Item = (Vec<String>, bool)>,
) -> Vec<Option<url::Url>> {
    files
        .map(|(components, padding)| {
            if padding {
                return None;
            }
            let mut url = base.clone();
            if single_file && !base.path().ends_with('/') {
                return Some(url);
            }
            {
                let mut segments = url.path_segments_mut().ok()?;
                segments.pop_if_empty().push(name);
                if !single_file {
                    segments.extend(components.iter().map(String::as_str));
                }
            }
            Some(url)
        })
        .collect()
}

const MAX_BACKOFF: Duration = Duration::from_secs(600);

impl TorrentStateLive {
    pub(crate) async fn task_webseed(
        self: Arc<Self>,
        index: usize,
        base: url::Url,
    ) -> crate::Result<()> {
        let addr = webseed_addr(index);
        let client = match self.shared.session.upgrade() {
            Some(s) => s.reqwest_client.clone(),
            None => return Ok(()),
        };
        let info = &self.metadata.info;
        let name = info.name_or_else(|| "torrent-content").into_owned();
        let single_file = info.info().files.is_none();
        let urls = file_urls(
            &base,
            &name,
            single_file,
            info.iter_file_details()
                .map(|d| (d.filename.to_vec(), d.attrs().padding)),
        );

        let mut failures = 0u32;
        loop {
            let piece = match self.webseed_acquire(addr)? {
                Some(p) => p,
                None => {
                    tokio::select! {
                        _ = self.new_pieces_notify.notified() => {},
                        _ = tokio::time::sleep(Duration::from_secs(15)) => {},
                    }
                    continue;
                }
            };
            let result = async {
                let data = self.webseed_fetch(&client, &urls, piece).await?;
                let state = self.clone();
                self.shared
                    .spawner
                    .block_in_place_with_semaphore(move || state.webseed_write(addr, piece, &data))
                    .await
            }
            .await;
            match result {
                Ok(()) => failures = 0,
                Err(e) => {
                    self.webseed_release(addr);
                    failures = failures.saturating_add(1);
                    let wait = Duration::from_secs(5)
                        .saturating_mul(1 << failures.min(8))
                        .min(MAX_BACKOFF);
                    debug!(url = %base, ?wait, "web seed failed on piece {piece}: {e:#}");
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    /// Reserves a piece for the web seed. Web seeds have every piece and never steal.
    fn webseed_acquire(&self, addr: PeerHandle) -> crate::Result<Option<ValidPieceIndex>> {
        let mut g = self.lock_write("webseed_acquire");
        let TorrentStateLocked {
            pieces,
            file_priorities,
            ..
        } = &mut **g;
        let pieces = pieces.as_mut().ok_or(Error::ChunkTrackerEmpty)?;
        let result = pieces.acquire_piece(AcquireRequest {
            peer: addr,
            peer_avg_time: None,
            priority_pieces: self.streams.iter_next_pieces(&self.lengths),
            file_priorities,
            file_infos: &self.metadata.file_infos,
            peer_has_piece: |_| true,
            can_steal: |_| false,
        });
        Ok(match result {
            AcquireResult::Reserved(p) => Some(p),
            AcquireResult::Stolen { piece, .. } => Some(piece),
            AcquireResult::NoneAvailable => None,
        })
    }

    fn webseed_release(&self, addr: PeerHandle) {
        if let Ok(pieces) = self.lock_write("webseed_release").get_pieces_mut() {
            pieces.release_pieces_owned_by(addr);
        }
        self.new_pieces_notify.notify_waiters();
    }

    /// The bytes of `piece`, fetched from the files it spans.
    async fn webseed_fetch(
        &self,
        client: &reqwest::Client,
        urls: &[Option<url::Url>],
        piece: ValidPieceIndex,
    ) -> anyhow::Result<Vec<u8>> {
        let len = self.lengths.piece_length(piece);
        let mut chunk = len;
        while chunk > 0 {
            let n = chunk.min(librqbit_core::constants::CHUNK_SIZE);
            let n = NonZeroU32::new(n).context("bug")?;
            self.shared.ratelimits.prepare_for_download(n).await?;
            if let Some(session) = self.shared.session.upgrade() {
                session.ratelimits.prepare_for_download(n).await?;
            }
            chunk -= n.get();
        }

        let start = self.lengths.piece_offset(piece);
        let end = start + len as u64;
        let mut data = Vec::with_capacity(len as usize);
        for (fi, url) in self.metadata.file_infos.iter().zip(urls) {
            let (f_start, f_end) = (fi.offset_in_torrent, fi.offset_in_torrent + fi.len);
            if f_end <= start || f_start >= end {
                continue;
            }
            let from = start.max(f_start) - f_start;
            let to = end.min(f_end) - f_start; // exclusive
            #[allow(clippy::cast_possible_truncation)] // at most one piece
            let want = (to - from) as usize;
            let Some(url) = url else {
                data.resize(data.len() + want, 0);
                continue;
            };
            let response = client
                .get(url.clone())
                .header(reqwest::header::RANGE, format!("bytes={}-{}", from, to - 1))
                .timeout(Duration::from_secs(60))
                .send()
                .await
                .with_context(|| format!("GET {url}"))?;
            let status = response.status();
            let whole_file = from == 0 && to == fi.len;
            match status.as_u16() {
                206 => {}
                200 if whole_file => {}
                200 => bail!("{url}: the server does not support byte ranges"),
                _ => bail!("{url}: HTTP {status}"),
            }
            if response.content_length().is_some_and(|l| l != want as u64) {
                bail!(
                    "{url}: expected {want} bytes, server sends {:?}",
                    response.content_length()
                );
            }
            let body = response
                .bytes()
                .await
                .with_context(|| format!("reading {url}"))?;
            if body.len() != want {
                bail!("{url}: expected {want} bytes, got {}", body.len());
            }
            data.extend_from_slice(&body);

            self.stats
                .fetched_bytes
                .fetch_add(want as u64, Ordering::Relaxed);
            self.session_stats
                .counters
                .fetched_bytes
                .fetch_add(want as u64, Ordering::Relaxed);
        }
        if data.len() != len as usize {
            bail!("piece {piece}: assembled {} of {len} bytes", data.len());
        }
        Ok(data)
    }

    /// Writes a fetched piece and checks its hash, as `on_received_piece` does for peers.
    fn webseed_write(
        &self,
        addr: PeerHandle,
        piece: ValidPieceIndex,
        data: &[u8],
    ) -> anyhow::Result<()> {
        // While we hold the per-piece lock no peer can steal the piece.
        let ppl = {
            let g = self.lock_read("webseed_check_steal");
            let ppl = self.per_piece_locks[piece.get_usize()].read();
            match g.get_pieces()?.get_inflight(piece) {
                Some(inflight) if inflight.peer == addr => {}
                _ => {
                    trace!("web seed piece {piece} was taken by a peer, dropping it");
                    return Ok(());
                }
            }
            ppl
        };

        let mut download_time = None;
        for chunk in self.lengths.iter_chunk_infos(piece) {
            let begin = chunk.offset as usize;
            let bytes = &data[begin..begin + chunk.size as usize];
            let p = Piece::from_data(piece.get(), chunk.offset, bytes);
            if let Err(e) = self.file_ops().write_chunk(addr, &p, &chunk) {
                return self.on_fatal_error(e);
            }
            let mut g = self.lock_write("webseed_mark_chunk");
            match g.get_pieces_mut()?.mark_chunk_downloaded(&p) {
                Some(ChunkMarkingResult::Completed) => {
                    download_time = g.get_pieces_mut()?.take_inflight(piece);
                }
                Some(ChunkMarkingResult::NotCompleted) => {}
                Some(ChunkMarkingResult::PreviouslyCompleted) => return Ok(()),
                None => bail!("bug: web seed chunk {chunk:?} does not map to the piece"),
            }
        }
        drop(ppl);
        let Some(download_time) = download_time else {
            return Ok(());
        };

        if !self.file_ops().check_piece(piece)? {
            warn!(?addr, "web seed piece {piece} failed the hash check");
            self.lock_write("webseed_piece_broken")
                .get_pieces_mut()?
                .mark_piece_hash_failed(piece);
            self.new_pieces_notify.notify_waiters();
            bail!("piece {piece} failed the hash check");
        }
        self.lock_write("webseed_piece_ok")
            .get_pieces_mut()?
            .mark_piece_hash_ok(piece);
        let piece_len = self.lengths.piece_length(piece) as u64;
        self.stats
            .downloaded_and_checked_bytes
            .fetch_add(piece_len, Ordering::Release);
        self.stats
            .downloaded_and_checked_pieces
            .fetch_add(1, Ordering::Release);
        self.stats
            .have_bytes
            .fetch_add(piece_len, Ordering::Relaxed);
        #[allow(clippy::cast_possible_truncation)]
        self.stats
            .total_piece_download_ms
            .fetch_add(download_time.as_millis() as u64, Ordering::Relaxed);
        self.on_piece_completed(piece)?;
        self.transmit_haves(piece);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(base: &str, single: bool, files: &[(&[&str], bool)]) -> Vec<Option<String>> {
        let base = url::Url::parse(base).unwrap();
        let files = files
            .iter()
            .map(|(c, pad)| (c.iter().map(|s| s.to_string()).collect(), *pad));
        file_urls(&base, "My Show", single, files)
            .into_iter()
            .map(|u| u.map(|u| u.to_string()))
            .collect()
    }

    #[test]
    fn single_file_urls() {
        assert_eq!(
            urls(
                "http://x.test/files/show.mkv",
                true,
                &[(&["My Show"], false)]
            ),
            vec![Some("http://x.test/files/show.mkv".to_owned())]
        );
        assert_eq!(
            urls("http://x.test/files/", true, &[(&["My Show"], false)]),
            vec![Some("http://x.test/files/My%20Show".to_owned())]
        );
    }

    #[test]
    fn multi_file_urls() {
        assert_eq!(
            urls(
                "http://x.test/files",
                false,
                &[(&["a", "b#1.mkv"], false), (&[".pad", "100"], true)]
            ),
            vec![
                Some("http://x.test/files/My%20Show/a/b%231.mkv".to_owned()),
                None
            ]
        );
    }

    #[test]
    fn addrs_differ() {
        assert_ne!(webseed_addr(0), webseed_addr(1));
        assert!(webseed_addr(70000).ip().is_ipv6());
    }
}
