//! What each subcommand actually does.

use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{Context, Result};
use distlib_core::{
    Config, DataDir, MemberId,
    identity::{create_secret_key, load_or_create_secret_key, member_id},
};
use distlib_net::{AllowlistHooks, Node, allowlist, build_endpoint, ping};
use iroh::{EndpointAddr, RelayUrl, Watcher as _};

/// Everything a command needs to find this node's files.
///
/// Resolved before any configuration is read: the config file lives inside the
/// data directory, so the directory cannot come from it.
pub struct Paths {
    pub data_dir: DataDir,
    pub config_file: std::path::PathBuf,
}

impl Paths {
    pub fn secret_key_file(&self) -> std::path::PathBuf {
        self.data_dir.secret_key_file()
    }
}

/// `distlib init`
pub fn init(paths: &Paths, force: bool) -> Result<()> {
    paths
        .data_dir
        .create()
        .context("could not create the data directory")?;

    let key_file = paths.secret_key_file();
    let existed = key_file.exists();

    let secret = if force {
        // Replacing the key discards the node's identity, and with it its
        // membership of any group. Say so plainly rather than in the logs.
        if existed {
            println!("replacing the existing identity; this node leaves any group it was in");
        }
        create_secret_key(&key_file, true)?
    } else {
        load_or_create_secret_key(&key_file)?
    };

    if !paths.config_file.exists() {
        std::fs::write(&paths.config_file, Config::default().to_starter_toml())
            .with_context(|| format!("could not write {}", paths.config_file.display()))?;
        println!("wrote      {}", paths.config_file.display());
    }

    println!(
        "identity   {} ({})",
        member_id(&secret),
        if existed && !force { "existing" } else { "new" }
    );
    println!("data dir   {}", paths.data_dir.root().display());
    Ok(())
}

/// `distlib run`
pub async fn run(paths: &Paths) -> Result<()> {
    let config = load_config(&paths.config_file)?;
    let secret = load_or_create_secret_key(&paths.secret_key_file())?;
    let me = member_id(&secret);

    // The writer is unused in phase 0 but must outlive the endpoint: from
    // phase 1 the Raft state machine drives it to expel members at runtime.
    let (_allowlist_writer, allowed) = allowlist(me, config.net.allowlist.iter().copied());
    let endpoint = build_endpoint(secret, &config.net, AllowlistHooks::new(allowed)).await?;
    let node = Node::spawn(endpoint);

    tracing::info!(member = %me, "node started");
    for addr in node.endpoint().bound_sockets() {
        tracing::info!(%addr, "listening");
    }
    if config.net.allowlist.is_empty() {
        tracing::warn!("the allowlist is empty; this node will talk to nobody");
    }

    // Reaching a relay takes a moment and may never happen offline, so report
    // it when it arrives instead of blocking startup on it.
    let watcher = node.endpoint().clone();
    let relay_task = tokio::spawn(async move {
        watcher.online().await;
        for relay in watcher.home_relay_status().get() {
            tracing::info!(url = %relay.url(), "home relay established");
        }
    });

    tokio::signal::ctrl_c()
        .await
        .context("could not listen for ctrl-c")?;

    tracing::info!("shutting down");
    relay_task.abort();
    node.shutdown().await;
    Ok(())
}

/// `distlib status`
pub async fn status(paths: &Paths, online: bool) -> Result<()> {
    println!("data dir   {}", paths.data_dir.root().display());
    println!("config     {}", display_path(&paths.config_file));

    let key_file = paths.secret_key_file();
    if !key_file.exists() {
        println!("identity   not initialised — run `distlib init`");
        return Ok(());
    }

    let config = load_config(&paths.config_file)?;
    let secret = load_or_create_secret_key(&key_file)?;
    let me = member_id(&secret);

    println!("identity   {me}");
    println!("relay mode {:?}", config.net.relay_mode);
    println!("allowlist  {} member(s)", config.net.allowlist.len());

    if online {
        let (_writer, allowed) = allowlist(me, config.net.allowlist.iter().copied());
        let endpoint = build_endpoint(secret, &config.net, AllowlistHooks::new(allowed)).await?;
        endpoint.online().await;
        let addr = endpoint.addr();
        for transport in &addr.addrs {
            println!("address    {transport:?}");
        }
        endpoint.close().await;
    }
    Ok(())
}

/// `distlib ping`
pub async fn ping(
    paths: &Paths,
    member: MemberId,
    addrs: Vec<SocketAddr>,
    relay: Option<String>,
    payload: &str,
    timeout: Duration,
) -> Result<()> {
    let config = load_config(&paths.config_file)?;
    let secret = load_or_create_secret_key(&paths.secret_key_file())?;
    let me = member_id(&secret);

    let (_writer, allowed) = allowlist(me, config.net.allowlist.iter().copied());
    let endpoint = build_endpoint(secret, &config.net, AllowlistHooks::new(allowed)).await?;

    let result = ping::ping_with_timeout(
        &endpoint,
        target_addr(member, addrs, relay)?,
        payload.as_bytes(),
        timeout,
    )
    .await;
    endpoint.close().await;

    let echo = result.with_context(|| format!("ping to {member} failed"))?;
    println!("{}", String::from_utf8_lossy(&echo));
    Ok(())
}

/// Assembles the address to dial from whatever the caller supplied.
///
/// With neither `--addr` nor `--relay`, the address carries only the member id
/// and the endpoint's address lookup has to find it — which works with the
/// default relay mode and not at all without it.
fn target_addr(
    member: MemberId,
    addrs: Vec<SocketAddr>,
    relay: Option<String>,
) -> Result<EndpointAddr> {
    let mut addr = EndpointAddr::new(member.endpoint_id());
    for socket in addrs {
        addr = addr.with_ip_addr(socket);
    }
    if let Some(url) = relay {
        let parsed: RelayUrl = url
            .parse()
            .with_context(|| format!("{url} is not a valid relay url"))?;
        addr = addr.with_relay_url(parsed);
    }
    Ok(addr)
}

fn load_config(path: &Path) -> Result<Config> {
    Config::load(path).with_context(|| format!("could not load {}", path.display()))
}

fn display_path(path: &Path) -> String {
    if path.exists() {
        path.display().to_string()
    } else {
        format!("{} (absent, using defaults)", path.display())
    }
}
