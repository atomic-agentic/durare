//! Opening one new SQLite file from several connections at once.
//!
//! `connect` switches the file to WAL journaling, which takes a brief
//! exclusive lock; two openers racing for it on a fresh file could get
//! SQLITE_BUSY straight back. Opening must succeed for every caller.

#![cfg(feature = "sqlite")]

use durare::SqliteProvider;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_openers_of_one_new_file_all_succeed() {
    for round in 0..30 {
        let db = std::env::temp_dir().join(format!(
            "durare-concurrent-open-{}-{round}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&db);
        let url = format!("sqlite://{}?mode=rwc", db.display());
        let openers: Vec<_> = (0..8)
            .map(|_| {
                let url = url.clone();
                tokio::spawn(async move { SqliteProvider::connect(&url).await.map(|_| ()) })
            })
            .collect();
        for (i, o) in openers.into_iter().enumerate() {
            o.await
                .expect("task")
                .unwrap_or_else(|e| panic!("round {round}, opener {i}: {e}"));
        }
        let _ = std::fs::remove_file(&db);
    }
}
