use async_trait::async_trait;
use mockall::mock;
use shutdown::Shutdown;
use tokio::task::JoinHandle;

use crate::ConclusionEvent;

mock! {
    #[derive(Debug)]
    pub ConclusionFeed {}
    #[async_trait]
    impl crate::ConclusionFeed for ConclusionFeed {
        async fn max_highwater_mark(&self) -> anyhow::Result<Option<u64>>;
        async fn conclusion_events_since(
            &self,
            highwater_mark: i64,
            limit: i64,
        ) -> anyhow::Result<Vec<ConclusionEvent>>;
    }
}

// A context that ensures all tasks for the actor have terminated.
// Without the assurance we cannot be sure that the mock actor drop logic has run its
// assertions.
//
// This struct implements drop logic that panics if proper cleanup wasn't performed.
// However double panics (i.e. panic while panicing) make test output hard to read.
// Therefore if we are already panicing the drop logic does not panic.
// This makes it so that the panic in the cleanup logic doesn't hide the real reason for the
// test failure. However in order for this to work tests must fail by panicing not returning a
// result.
#[must_use]
pub struct TestContext<A> {
    shutdown: Shutdown,
    handles: Vec<JoinHandle<()>>,
    pub actor_handle: A,
    is_shutdown: bool,
}

impl<A> TestContext<A> {
    pub fn new(shutdown: Shutdown, handles: Vec<JoinHandle<()>>, actor_handle: A) -> Self {
        Self {
            shutdown,
            handles,
            actor_handle,
            is_shutdown: false,
        }
    }

    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.shutdown.shutdown();
        while let Some(h) = self.handles.pop() {
            // if the task paniced this will report and error.
            h.await?;
        }
        self.is_shutdown = true;
        Ok(())
    }
}
impl<A> Drop for TestContext<A> {
    fn drop(&mut self) {
        if !self.is_shutdown {
            if std::thread::panicking() {
                // Do not double panic. Test is going to fail anyways.
            } else {
                panic!("TestContext shutdown must be called");
            }
        }
    }
}
