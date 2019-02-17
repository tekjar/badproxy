#![feature(async_await, await_macro, futures_api)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate tokio;

use std::io;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::codec::{BytesCodec, Decoder};


fn main() {
    pretty_env_logger::init();
    tokio::run_async(async move {
        match await!(listen()) {
            Ok(_) => info!("done."),
            Err(e) => error!("server failed; error = {:?}", e),
        }
    });
}


async fn listen() -> Result<(), io::Error> {
    let listener = TcpListener::bind(&"127.0.0.1:7878".parse().unwrap())?;
    let mut incoming = listener.incoming();

    while let Some(stream) = await!(incoming.next()) {
        let source_stream = stream?;
        let addr = source_stream.peer_addr()?;
        let destination_addr = "127.0.0.1:1883".parse().unwrap();
        let destination_stream = await!(TcpStream::connect(&destination_addr))?;

        tokio::spawn_async(async move {
            info!("Accepting stream from: {}", addr);
            await!(regulate(source_stream, destination_stream)).unwrap();
            info!("Closing stream from: {}", addr);
        });
    }

    Ok(())
}

async fn regulate(source: TcpStream, destination: TcpStream) -> Result<(), io::Error> {
    let source = BytesCodec::new().framed(source);
    let destination = BytesCodec::new().framed(destination);

    let (mut source_sink, mut source_stream) = source.split();
    let (mut dest_sink, mut dest_stream) = destination.split();

    let (close_tx, mut close_rx) = tokio::sync::mpsc::channel::<bool>(1);
    let close_tx_1 = close_tx.clone();
    let close_tx_2 = close_tx.clone();

    tokio::spawn_async(async move {
        while let Some(message) = await!(source_stream.next()) {
            let message = match message {
                Ok(m) => m.freeze(),
                Err(e) => {
                    error!("Error = {:?}", e);
                    break
                }
            };
            await!(dest_sink.send_async(message)).unwrap();
        }

        let _ = await!(close_tx_1.send(true));
    });

    tokio::spawn_async(async move {
        while let Some(message) = await!(dest_stream.next()) {
            let message = match message {
                Ok(m) => m.freeze(),
                Err(e) => {
                    error!("Error = {:?}", e);
                    break
                }
            };

            await!(source_sink.send_async(message)).unwrap();
        }

        let _ = await!(close_tx_2.send(true));
    });

    await!(close_rx.next());
    Ok(())
}
