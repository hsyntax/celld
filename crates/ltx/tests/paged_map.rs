//! The paged-restore page map: which planned object serves each page of a cut,
//! and how a fault reads a page's run.

use celld_ltx::ltx::{encode_file_v0_5_2, Header, HEADER_FLAG_NO_CHECKSUM, VERSION};
use celld_ltx::paged::{build_page_map, FileRangeReader, PageMap};
use celld_ltx::{FileInfo, FileReplicaClient, ReplicaClient, TXID};

const PAGE_SIZE: u32 = 4096;
const MIB: u64 = 1024 * 1024;

/// A page's bytes name the object that wrote it (its txid) and the page.
fn page(txid: u64, pgno: u32) -> Vec<u8> {
    let mut data = vec![txid as u8; PAGE_SIZE as usize];
    data[..4].copy_from_slice(&pgno.to_be_bytes());
    data
}

/// Writes a one-transaction object holding `pgnos` at `commit` pages.
async fn put(client: &FileReplicaClient, txid: u64, commit: u32, pgnos: &[u32]) -> FileInfo {
    let header = Header {
        version: VERSION,
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size: PAGE_SIZE,
        commit,
        min_txid: TXID(txid),
        max_txid: TXID(txid),
        ..Header::default()
    };
    let pages: Vec<(u32, Vec<u8>)> = pgnos.iter().map(|p| (*p, page(txid, *p))).collect();
    let bytes = encode_file_v0_5_2(&header, &pages, 0).unwrap();
    client
        .write_ltx_file(0, TXID(txid), TXID(txid), &bytes)
        .await
        .unwrap()
}

/// A snapshot of pages 1..=8, then a transaction rewriting 3, 4 and 7, then
/// one that rewrites 2 and 5 and shrinks the database to 6 pages.
async fn shrinking_chain(client: &FileReplicaClient) -> PageMap {
    let snapshot = put(client, 1, 8, &[1, 2, 3, 4, 5, 6, 7, 8]).await;
    let rewrite = put(client, 2, 8, &[3, 4, 7]).await;
    let shrink = put(client, 3, 6, &[2, 5]).await;
    build_page_map(client, &[snapshot, rewrite, shrink])
        .await
        .unwrap()
}

#[tokio::test]
async fn newest_object_serves_each_page_up_to_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let client = FileReplicaClient::new(dir.path().to_str().unwrap());
    let map = shrinking_chain(&client).await;
    assert_eq!(map.page_size, PAGE_SIZE);
    assert_eq!(map.commit, 6);

    for (pgno, txid) in [(1, 1), (2, 3), (3, 2), (4, 2), (5, 3), (6, 1)] {
        assert_eq!(
            map.read_page(&client, pgno).await.unwrap(),
            Some(page(txid, pgno)),
            "page {pgno}"
        );
    }
    // Pages the shrink cut off, and pages no object holds.
    for pgno in [0, 7, 8, 9, u32::MAX] {
        assert_eq!(
            map.read_page(&client, pgno).await.unwrap(),
            None,
            "page {pgno}"
        );
    }
}

#[tokio::test]
async fn a_run_stays_inside_one_object() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let client = FileReplicaClient::new(root);
    let map = shrinking_chain(&client).await;
    let reader = FileRangeReader::new(root);

    // Page 2 comes from a newer object, so page 1's run is page 1 alone; 3
    // and 4 are adjacent frames of one object, and 5 belongs to another.
    for (pgno, expected) in [
        (1, vec![(1, 1)]),
        (3, vec![(3, 2), (4, 2)]),
        (6, vec![(6, 1)]),
    ] {
        let run = map.read_run_sync(&reader, pgno, MIB).unwrap();
        let expected: Vec<(u32, Vec<u8>)> = expected
            .into_iter()
            .map(|(p, txid)| (p, page(txid, p)))
            .collect();
        assert_eq!(run, expected, "run from page {pgno}");
    }
    assert!(map.read_run_sync(&reader, 7, MIB).unwrap().is_empty());
}

#[tokio::test]
async fn a_run_stops_at_the_byte_cap() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let client = FileReplicaClient::new(root);
    let pgnos: Vec<u32> = (1..=40).collect();
    let snapshot = put(&client, 1, 40, &pgnos).await;
    let map = build_page_map(&client, &[snapshot]).await.unwrap();
    let reader = FileRangeReader::new(root);

    let pages_of = |run: Vec<(u32, Vec<u8>)>| -> Vec<u32> {
        run.into_iter()
            .map(|(pgno, data)| {
                assert_eq!(data, page(1, pgno));
                pgno
            })
            .collect()
    };
    let capped = map
        .read_run_sync(&reader, 10, 4 * u64::from(PAGE_SIZE))
        .unwrap();
    assert_eq!(pages_of(capped), (10..=13).collect::<Vec<_>>());
    let whole = map.read_run_sync(&reader, 10, MIB).unwrap();
    assert_eq!(pages_of(whole), (10..=40).collect::<Vec<_>>());
}
