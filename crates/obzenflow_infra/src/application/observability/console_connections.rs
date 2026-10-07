// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tonic owns its connection tasks; the application retains authority over their
//! sockets so a non-reading client cannot prolong diagnostic shutdown.

use std::collections::HashMap;
use std::io;
use std::net::{Shutdown, TcpStream as StdTcpStream};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tonic::transport::server::{Connected, TcpConnectInfo};

#[derive(Default)]
struct State {
    closing: bool,
    next_id: u64,
    sockets: HashMap<u64, StdTcpStream>,
}

#[derive(Default)]
pub(super) struct ConsoleConnections {
    state: Mutex<State>,
    closed: Notify,
}

impl ConsoleConnections {
    pub(super) fn register(self: &Arc<Self>, stream: TcpStream) -> io::Result<ConsoleConnection> {
        let socket = stream.into_std()?;
        let retained = socket.try_clone()?;
        let stream = TcpStream::from_std(socket)?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing {
            let _ = retained.shutdown(Shutdown::Both);
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Console is closing",
            ));
        }
        let id = state.next_id;
        state.next_id += 1;
        state.sockets.insert(id, retained);
        Ok(ConsoleConnection {
            stream: Some(stream),
            owner: self.clone(),
            id,
        })
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        // Registration and closure share this latch: a racing accept can never
        // install an open socket after this snapshot has been shut down.
        state.closing = true;
        for socket in state.sockets.values() {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    pub(super) async fn wait_closed(&self) {
        loop {
            let notified = self.closed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .sockets
                .is_empty()
            {
                return;
            }
            notified.await;
        }
    }

    #[cfg(all(test, tokio_unstable))]
    pub(super) fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sockets
            .len()
    }
}

pub(super) struct ConsoleConnection {
    stream: Option<TcpStream>,
    owner: Arc<ConsoleConnections>,
    id: u64,
}

impl Drop for ConsoleConnection {
    fn drop(&mut self) {
        // Release both socket handles before announcing closure. An empty
        // registry proves released transport IO, not a join of Tonic internals.
        self.stream.take();
        self.owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sockets
            .remove(&self.id);
        self.owner.closed.notify_waiters();
    }
}

impl Connected for ConsoleConnection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream
            .as_ref()
            .expect("connection is live")
            .connect_info()
    }
}

impl AsyncRead for ConsoleConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("connection is live")).poll_read(cx, buf)
    }
}

impl AsyncWrite for ConsoleConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.stream.as_mut().expect("connection is live")).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("connection is live")).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("connection is live")).poll_shutdown(cx)
    }
}
