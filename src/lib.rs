use std::{
    pin::Pin,
    task::{Context, Poll},
};

use axum::body::{Body, Bytes};
use flume::r#async::SendSink;
use futures_concurrency::future::TryJoin;
use futures_io::AsyncWrite;
use futures_sink::Sink;
use genawaiter_try_stream::try_stream;

pub struct AxumWrite {
    send: SendSink<'static, Bytes>,
}

impl AsyncWrite for AxumWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.send.sender().send(Bytes::copy_from_slice(buf)).ok();
        let _ = (&mut self, cx);
        // core::task::ready!(Pin::new(&mut self.send).poll_ready(cx))
        //     .map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))?;
        // Pin::new(&mut self.send)
        //     .start_send(Bytes::copy_from_slice(buf))
        //     .map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send)
            .poll_flush(cx)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send)
            .poll_close(cx)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))
    }
}

pub fn with_write<F: 'static + Send + Future<Output = std::io::Result<()>>>(
    f: impl 'static + Send + FnOnce(AxumWrite) -> F,
) -> Body {
    let (send, recv) = flume::unbounded::<Bytes>();
    Body::from_stream(try_stream(async move |co| {
        (
            async {
                while let Ok(bytes) = recv.recv_async().await {
                    tracing::info!("chunk len: {}", bytes.len());
                    co.yield_(bytes).await;
                }
                tracing::info!("chunks ended");
                Ok(())
            },
            async {
                f(AxumWrite {
                    send: send.into_sink(),
                })
                .await
            },
        )
            .try_join()
            .await?;
        std::io::Result::Ok(())
    }))
}
