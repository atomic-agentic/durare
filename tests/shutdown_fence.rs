use durare::{
    DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions, STATUS_PENDING,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test]
async fn timed_out_shutdown_fences_old_body_before_same_process_recovery() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let old_completed = Arc::new(AtomicUsize::new(0));
    let checkpoint_calls = Arc::new(AtomicUsize::new(0));
    let mut old = DurableEngine::new(provider.clone()).await?;
    {
        let entered = entered.clone();
        let release = release.clone();
        let old_completed = old_completed.clone();
        let checkpoint_calls = checkpoint_calls.clone();
        old.register("parked", move |ctx: DurableContext, (): ()| {
            let entered = entered.clone();
            let release = release.clone();
            let old_completed = old_completed.clone();
            let checkpoint_calls = checkpoint_calls.clone();
            async move {
                ctx.step("checkpoint", move || async move {
                    checkpoint_calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, durare::Error>(5_i32)
                })
                .await?;
                let release_wait = release.notified();
                tokio::pin!(release_wait);
                release_wait.as_mut().enable();
                entered.notify_one();
                release_wait.await;
                old_completed.fetch_add(1, Ordering::SeqCst);
                Ok::<_, durare::Error>(1_i32)
            }
        });
    }
    old.launch().await?;
    let _old_handle = old
        .start::<_, i32>("parked", (), WorkflowOptions::with_id("fenced-run"))
        .await?;
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("old body never parked");

    old.shutdown(Duration::ZERO).await?;
    assert_eq!(old_completed.load(Ordering::SeqCst), 0);
    assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        old.retrieve_workflow::<i32>("fenced-run")
            .await?
            .get_status()
            .await?
            .status,
        STATUS_PENDING
    );

    let mut reopened = DurableEngine::new(provider.clone()).await?;
    let recovery_checkpoint_calls = checkpoint_calls.clone();
    reopened.register("parked", move |ctx: DurableContext, (): ()| {
        let checkpoint_calls = recovery_checkpoint_calls.clone();
        async move {
            let checkpoint: i32 = ctx
                .step("checkpoint", move || async move {
                    checkpoint_calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, durare::Error>(99_i32)
                })
                .await?;
            Ok::<_, durare::Error>(checkpoint + 2)
        }
    });
    reopened.launch().await?;
    let recovered = reopened
        .recover_pending_for(&[old.executor_id().to_string()])
        .await?;
    assert_eq!(recovered, vec!["fenced-run"]);
    let value = reopened
        .retrieve_workflow::<i32>("fenced-run")
        .await?
        .result()
        .await?;
    assert_eq!(value, 7);
    assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 1);
    release.notify_waiters();
    tokio::task::yield_now().await;
    assert_eq!(old_completed.load(Ordering::SeqCst), 0);
    reopened.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
