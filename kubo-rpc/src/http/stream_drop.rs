use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{Future, Stream};
use pin_project::{pin_project, pinned_drop};

/// Wraps a stream and on drop spawns a future as its own task.
/// The future is not gauranteed to complete.
#[pin_project(PinnedDrop)]
pub struct StreamDrop<S, D>
where
    D: Future<Output = ()> + Send + 'static,
{
    #[pin]
    stream: S,
    drop_fut: Option<D>,
}

impl<S, D> StreamDrop<S, D>
where
    D: Future<Output = ()> + Send + 'static,
{
    pub fn new(stream: S, drop_fn: D) -> Self {
        Self {
            stream,
            drop_fut: Some(drop_fn),
        }
    }
}

impl<S, D> Stream for StreamDrop<S, D>
where
    S: Stream,
    D: Future<Output = ()> + Send + 'static,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        Stream::poll_next(this.stream, cx)
    }
}

#[pinned_drop]
impl<S, D> PinnedDrop for StreamDrop<S, D>
where
    D: Future<Output = ()> + Send + 'static,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if let Some(drop_fn) = this.drop_fut.take() {
            tokio::spawn(drop_fn);
        }
    }
}
