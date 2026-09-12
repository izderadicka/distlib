//! What has to happen before a peer that moved can be reached again.
//!
//! The behaviour underneath P1-23's last mile, pinned at the level it lives at:
//! two iroh endpoints and nothing else of ours. `distlib-consensus` reacts to a
//! `CoreGroupChanged` by closing the connections it holds to the member that
//! moved, and this is the test that says why that is not housekeeping.
//!
//! **A node that is killed sends no close frame.** Its peers go on holding a
//! connection that still reads as open, to a socket with nothing behind it —
//! and while they do, iroh will not reach that endpoint id at any other
//! address. The dial is sent to the pinned path and times out, however many
//! times it is retried and whatever address it is handed. Letting go of the
//! connection *after* the move is what frees it.
//!
//! Cheap and deterministic — loopback, no processes, milliseconds — so it lives
//! in the fast lane even though what it is about is a three-node cluster.

#![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::atomic::{AtomicU16, Ordering},
    time::Duration,
};

use iroh::{
    Endpoint, EndpointAddr, EndpointId, SecretKey, TransportAddr,
    endpoint::{Connection, RelayMode, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};

const ALPN: &[u8] = b"distlib-test/moved/0";

/// Long enough that a reachable peer is certainly reached, short enough that an
/// unreachable one does not hold the suite up. The working case takes
/// milliseconds; the broken one never completes at all.
const PATIENCE: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
struct Echo;

impl ProtocolHandler for Echo {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let said = recv.read_to_end(64).await.map_err(AcceptError::from_err)?;
        send.write_all(&said).await.map_err(AcceptError::from_err)?;
        send.finish().map_err(AcceptError::from_err)?;
        connection.closed().await;
        Ok(())
    }
}

/// A port this test can pin, from a range nothing else will be given.
///
/// Pinned rather than left to the OS because the whole point is to bind *two*
/// addresses for one identity and know they differ — with `:0` the kernel may
/// hand back the port just released, and the test would quietly stop being
/// about a move at all.
///
/// 30_000–30_999: above `distlib`'s `founding.rs` range, which ends at 30_000,
/// and below the kernel's ephemeral range, which starts at 32768 on Linux.
/// Disjoint on purpose — the two suites run concurrently under `test-all`, and
/// `founding.rs` probes while this pins, so an overlap would be a flake that
/// only ever appeared in the full run. Probed anyway, and offset by the process
/// id, for two runs on one machine.
fn a_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(0);
    const SPAN: u16 = 1_000;
    let offset = (std::process::id() as u16).wrapping_mul(16);

    for _ in 0..SPAN {
        let step = offset.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed));
        let port = 30_000 + step % SPAN;
        // Probed with UDP, which is what QUIC binds.
        if std::net::UdpSocket::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free port in 30000..31000");
}

async fn serve(secret: SecretKey, port: u16) -> Router {
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .secret_key(secret)
        .alpns(vec![ALPN.to_vec()])
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
        .unwrap()
        .bind()
        .await
        .unwrap();
    Router::builder(endpoint).accept(ALPN, Echo).spawn()
}

fn at(id: EndpointId, port: u16) -> EndpointAddr {
    let mut addr = EndpointAddr::new(id);
    addr.addrs.insert(TransportAddr::Ip(SocketAddr::from((
        Ipv4Addr::LOCALHOST,
        port,
    ))));
    addr
}

/// One exchange, keeping the connection so the caller decides its fate.
async fn speak_to(from: &Endpoint, to: EndpointAddr) -> Option<Connection> {
    let connection = from.connect(to, ALPN).await.ok()?;
    let (mut send, mut recv) = connection.open_bi().await.ok()?;
    send.write_all(b"hello").await.ok()?;
    send.finish().ok()?;
    assert_eq!(recv.read_to_end(64).await.ok()?, b"hello");
    Some(connection)
}

/// Whether `from` can reach a peer at `port` within [`PATIENCE`].
async fn can_reach(from: &Endpoint, id: EndpointId, port: u16) -> bool {
    tokio::time::timeout(PATIENCE, speak_to(from, at(id, port)))
        .await
        .is_ok_and(|connection| connection.is_some())
}

#[tokio::test]
async fn a_peer_that_moved_is_out_of_reach_until_the_old_connection_is_let_go() {
    let mover_key = SecretKey::generate();
    let mover_id = mover_key.public();

    let peer = serve(SecretKey::generate(), a_port()).await;
    let was = a_port();
    let mover = serve(mover_key.clone(), was).await;

    // A connection, held — as every peer of a core node holds one, across
    // several protocols, for as long as the group is running.
    let held = speak_to(peer.endpoint(), at(mover_id, was))
        .await
        .expect("a peer that has not moved is reachable");

    // The move. Dropped rather than shut down, because that is what this is
    // about: a killed process sends no close frame, and a peer renumbering its
    // machine is not going to ask politely first.
    drop(mover);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let now = a_port();
    let mover = serve(mover_key, now).await;

    // The address is known, correct, and supplied at the call site. It is not
    // enough, and *that* is the finding: nothing about knowing where a peer
    // went makes it reachable while a connection to where it was is still open.
    assert!(
        !can_reach(peer.endpoint(), mover_id, now).await,
        "if this passes, iroh no longer pins the old path and \
         `let_go_of_the_moved` may have become unnecessary — check before deleting it. \
         (This is the one assertion here that waits out a timeout rather than \
         observing something, but the margin is enormous: the working case takes \
         milliseconds and the broken one was measured at thirty seconds and never \
         completing. A failure here is the behaviour changing, not PATIENCE being short.)"
    );

    // Letting go is the whole of the cure.
    held.close(0u32.into(), b"moved");
    assert!(
        can_reach(peer.endpoint(), mover_id, now).await,
        "once the old connection is closed, the new address works"
    );

    let _ = mover.shutdown().await;
    let _ = peer.shutdown().await;
}
