use futures::future::BoxFuture;
use tokio::sync::broadcast;

/// A shutdown signal is a future that resolve to unit.
pub type ShutdownSignal = BoxFuture<'static, ()>;

/// Shutdown can be used to signal shutdown across many different tasks.
/// Shutdown is cheaply clonable so it can be shared with as many tasks as needed.
#[derive(Clone)]
pub struct Shutdown {
    tx: broadcast::Sender<()>,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(1);
        Self { tx }
    }
    /// Signal that all listeners should shutdown.
    /// Shutdown can be called from any clone.
    pub fn shutdown(&self) {
        let _ = self.tx.send(());
    }
    /// Construct a future that resolves when the shutdown signal is sent.
    ///
    /// The future is cancel safe.
    pub fn wait_fut(&self) -> ShutdownSignal {
        let mut sub = self.tx.subscribe();
        Box::pin(async move {
            let _ = sub.recv().await;
        })
    }
}
