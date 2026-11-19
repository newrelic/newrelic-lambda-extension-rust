// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Run a list of futures at the same time. The project has no `futures` dependency, so this
//! is a small `join_all`: it polls every future until all are done.

/// A future that is boxed so a list of different async blocks can be run together.
pub(crate) type BoxedFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Run every future at the same time and return once all have finished, with the results in
/// input order.
pub(crate) async fn join_all_concurrently<'a, T>(futures: Vec<BoxedFuture<'a, T>>) -> Vec<T> {
    let mut slots: Vec<Option<BoxedFuture<'a, T>>> = futures.into_iter().map(Some).collect();
    let mut results: Vec<Option<T>> = (0..slots.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (index, slot) in slots.iter_mut().enumerate() {
            let finished = match slot.as_mut() {
                Some(future) => match future.as_mut().poll(cx) {
                    std::task::Poll::Ready(value) => Some(value),
                    std::task::Poll::Pending => {
                        pending = true;
                        None
                    }
                },
                None => None,
            };
            if let Some(value) = finished {
                results[index] = Some(value);
                *slot = None;
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    results.into_iter().flatten().collect()
}
