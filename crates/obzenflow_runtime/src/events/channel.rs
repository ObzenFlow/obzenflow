// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::{mpsc, Notify};

/// `source_channel` makes a bounded queue that holds at most `capacity` items. The
/// [`SourceReceiver`] goes to the system that a source reads, and the
/// [`SourceSender`] goes to the code that supplies the input.
pub fn source_channel<T>(capacity: usize) -> (SourceSender<T>, SourceReceiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    let arrivals = Arc::new(Notify::new());
    (
        SourceSender {
            tx: Some(tx),
            arrivals: arrivals.clone(),
        },
        SourceReceiver {
            rx: Mutex::new(rx),
            arrivals,
        },
    )
}

/// `SourceSender` puts input into a source channel. The channel closes when the code
/// drops every sender.
pub struct SourceSender<T> {
    tx: Option<mpsc::Sender<T>>,
    arrivals: Arc<Notify>,
}

impl<T> SourceSender<T> {
    fn tx(&self) -> &mpsc::Sender<T> {
        self.tx.as_ref().expect("a live sender holds its channel")
    }

    /// `send` waits for space in the channel and puts `value` in it.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.tx().send(value).await?;
        self.arrivals.notify_waiters();
        Ok(())
    }

    /// `blocking_send` waits for space in the channel and puts `value` in it, from a
    /// thread outside the async runtime.
    pub fn blocking_send(&self, value: T) -> Result<(), SendError<T>> {
        self.tx().blocking_send(value)?;
        self.arrivals.notify_waiters();
        Ok(())
    }

    /// `try_send` puts `value` in the channel if it has space.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.tx().try_send(value)?;
        self.arrivals.notify_waiters();
        Ok(())
    }

    /// `try_reserve` holds space for one value if the channel has space.
    pub fn try_reserve(&self) -> Result<SourcePermit<'_, T>, TrySendError<()>> {
        let permit = self.tx().try_reserve()?;
        Ok(SourcePermit {
            permit,
            arrivals: &self.arrivals,
        })
    }

    /// `capacity` returns the space that is free in the channel now.
    pub fn capacity(&self) -> usize {
        self.tx().capacity()
    }

    /// `is_closed` tells whether the receiver is gone.
    pub fn is_closed(&self) -> bool {
        self.tx().is_closed()
    }
}

impl<T> Clone for SourceSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            arrivals: self.arrivals.clone(),
        }
    }
}

impl<T> Drop for SourceSender<T> {
    fn drop(&mut self) {
        drop(self.tx.take());
        self.arrivals.notify_waiters();
    }
}

impl<T> std::fmt::Debug for SourceSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSender").finish_non_exhaustive()
    }
}

/// `SourcePermit` is space that [`SourceSender::try_reserve`] holds for one value.
pub struct SourcePermit<'a, T> {
    permit: mpsc::Permit<'a, T>,
    arrivals: &'a Notify,
}

impl<T> SourcePermit<'_, T> {
    /// `send` puts `value` in the space that the permit holds.
    pub fn send(self, value: T) {
        self.permit.send(value);
        self.arrivals.notify_waiters();
    }
}

/// `SourceReceiver` is the end of a source channel that a system reads.
pub struct SourceReceiver<T> {
    rx: Mutex<mpsc::Receiver<T>>,
    arrivals: Arc<Notify>,
}

/// `SourceClosed` tells that a source channel is empty and every sender is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceClosed;

impl<T> SourceReceiver<T> {
    /// `ready` resolves when the channel holds input. It takes nothing from the
    /// channel, so the input stays in the channel until a read takes it.
    pub async fn ready(&self) -> Result<(), SourceClosed> {
        loop {
            let arrival = self.arrivals.notified();
            tokio::pin!(arrival);
            arrival.as_mut().enable();
            {
                let rx = self.rx.lock().unwrap_or_else(PoisonError::into_inner);
                if !rx.is_empty() {
                    return Ok(());
                }
                if rx.is_closed() {
                    return Err(SourceClosed);
                }
            }
            arrival.await;
        }
    }

    /// `take` removes up to `limit` items that the channel holds now, in order.
    pub fn take(&self, limit: usize) -> Vec<T> {
        let mut rx = self.rx.lock().unwrap_or_else(PoisonError::into_inner);
        let mut items = Vec::new();
        while items.len() < limit {
            match rx.try_recv() {
                Ok(item) => items.push(item),
                Err(_) => break,
            }
        }
        items
    }

    /// `len` returns the number of items that the channel holds now.
    pub fn len(&self) -> usize {
        self.rx.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    /// `is_empty` tells whether the channel holds no items now.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> std::fmt::Debug for SourceReceiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceReceiver").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn ready_leaves_input_queued() {
        let (tx, rx) = source_channel::<u32>(2);
        tx.send(7).await.unwrap();
        rx.ready().await.unwrap();
        rx.ready().await.unwrap();
        assert_eq!(rx.len(), 1);
        assert_eq!(tx.capacity(), 1);
        assert_eq!(rx.take(10), vec![7]);
    }

    #[tokio::test]
    async fn ready_waits_for_send() {
        let (tx, rx) = source_channel::<u32>(1);
        let waiting = tokio::time::timeout(Duration::from_millis(20), rx.ready()).await;
        assert!(waiting.is_err(), "an empty channel is not ready");
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tx.send(1).await.unwrap();
            tx
        });
        tokio::time::timeout(Duration::from_secs(5), rx.ready())
            .await
            .expect("a send wakes the wait")
            .unwrap();
        drop(sender.await.unwrap());
        assert_eq!(rx.take(1), vec![1]);
    }

    #[tokio::test]
    async fn ready_drained_closed_fails() {
        let (tx, rx) = source_channel::<u32>(1);
        tx.send(1).await.unwrap();
        drop(tx);
        rx.ready().await.unwrap();
        assert_eq!(rx.take(1), vec![1]);
        assert_eq!(rx.ready().await, Err(SourceClosed));
    }
}
