//! What each subcommand actually does.

use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use distlib_consensus::{
    MemberRecord, MembershipNode, MembershipState, NodeAddr, RAFT_DB, StateMachineStore,
};
use distlib_core::{
    Config, CoreMember, DataDir, MemberId,
    identity::{create_secret_key, load_or_create_secret_key, member_id},
};
use distlib_net::{AllowlistHooks, allowlist, build_endpoint, ping};
use iroh::{Endpoint, EndpointAddr, RelayUrl, SecretKey, Watcher as _};

/// How long `status --online` waits to reach a relay before giving up.
const ONLINE_TIMEOUT: Duration = Duration::from_secs(5);

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

    fn raft_db(&self) -> std::path::PathBuf {
        self.data_dir.root().join(RAFT_DB)
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
pub async fn run(paths: &Paths, found_group: bool) -> Result<()> {
    let config = load_config(&paths.config_file)?;
    let secret = load_or_create_secret_key(&paths.secret_key_file())?;
    let me = member_id(&secret);

    // The bootstrap seed, and the last time configuration has anything to say
    // about who this node talks to. Once `GroupFounded` is applied the node
    // replaces it with the log's membership and never reads this again.
    let (writer, allowed) = allowlist(me, config.consensus.core.iter().map(|core| core.member));
    let hooks = AllowlistHooks::new(allowed);
    let endpoint = build_endpoint(
        secret.clone(),
        &config.net,
        hooks.clone(),
        distlib_consensus::alpns(),
    )
    .await?;

    tracing::info!(member = %me, "node started");
    for addr in endpoint.bound_sockets() {
        tracing::info!(%addr, "listening");
    }

    let node = MembershipNode::start(endpoint, hooks, writer, paths.data_dir.root()).await?;

    if found_group {
        if let Err(error) = found(&node, &config, &secret, me).await {
            // Shut down rather than propagating straight out. Dropping a live
            // node leaves iroh complaining that the endpoint was never closed,
            // and that complaint lands after the error it should not bury.
            node.shutdown().await;
            return Err(error);
        }
    } else if node.membership().group_id().is_none() {
        tracing::warn!(
            "this node is in no group; found one with `distlib run --found-group`, or wait \
             for a founder to admit it"
        );
    }

    // A running node holds the database exclusively, so nothing else can read
    // the membership while it is up. Logging every change is how the group is
    // observed.
    let memberships = tokio::spawn(report_membership(node.subscribe()));

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
    memberships.abort();
    node.shutdown().await;
    Ok(())
}

/// `distlib members`
pub fn members(paths: &Paths) -> Result<()> {
    let membership = stored_membership(paths)?.unwrap_or_default();
    if !print_group(&membership) {
        println!("no group yet — found one with `distlib run --found-group`");
        return Ok(());
    }
    for member in membership.members() {
        let core = if membership.core().contains(&member.member_id) {
            "  core"
        } else {
            ""
        };
        println!("  {}{core}", display(member));
    }
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
    println!("core seed  {} member(s)", config.consensus.core.len());

    // Degrades rather than fails: a locked database is the normal state while
    // the node is running, and `status` is what people run when confused. It
    // should still tell them where their data and identity are.
    match stored_membership(paths) {
        Ok(membership) => {
            let membership = membership.unwrap_or_default();
            if print_group(&membership) {
                let standing = if membership.core().contains(&me) {
                    "core member"
                } else if membership.is_member(&me) {
                    "member"
                } else {
                    "not a member of this group"
                };
                println!("standing   {standing}");
            } else {
                println!("group      none yet");
            }
        }
        Err(error) => {
            // Almost always the lock rather than a broken file, and the chain
            // beneath says so three times. Keep the line readable and put the
            // detail where someone debugging a real failure will look.
            tracing::debug!(?error, "could not read the membership");
            println!("group      unavailable — the database is in use by a running node");
        }
    }

    if online {
        let (_writer, allowed) = allowlist(me, config.consensus.core.iter().map(|c| c.member));
        let endpoint = build_endpoint(
            secret,
            &config.net,
            AllowlistHooks::new(allowed),
            distlib_consensus::alpns(),
        )
        .await?;
        // Bounded, because "online" means a relay has been reached and with
        // `relay_mode = "disabled"` there is no relay to reach — the wait would
        // never end. The direct addresses are known either way.
        if tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online())
            .await
            .is_err()
        {
            println!("address    (no relay reached; direct addresses only)");
        }
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

    // The target is allowed whether or not it is in the config: naming a member
    // on the command line is the consent the allowlist exists to record, and a
    // ping that refused to dial the id it was handed would be useless for
    // exactly the case it is for — checking whether someone is reachable before
    // they are in a group with you.
    let seed = config
        .consensus
        .core
        .iter()
        .map(|core| core.member)
        .chain(std::iter::once(member));
    let (_writer, allowed) = allowlist(me, seed);
    let endpoint = build_endpoint(
        secret,
        &config.net,
        AllowlistHooks::new(allowed),
        distlib_net::alpn::registered(),
    )
    .await?;

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

/// Founds the group described by `[consensus] core`.
///
/// The founders' addresses come from configuration rather than from the log,
/// because the log is what they are needed to fetch — see
/// [`distlib_core::ConsensusConfig::core`].
async fn found(
    node: &MembershipNode,
    config: &Config,
    secret: &SecretKey,
    me: MemberId,
) -> Result<()> {
    if node.membership().group_id().is_some() {
        bail!("this node is already in a group; founding again would abandon it");
    }

    // Founding writes this node's address into the log, and every other founder
    // dials what is written there. An OS-chosen port is a different port after
    // the next restart, and nothing rewrites the entry.
    if config.net.bind_addr_v4.port() == 0 {
        tracing::warn!(
            "bind_addr_v4 has no fixed port; the address recorded for this node in the \
             founding entry will be wrong after a restart"
        );
    }

    if config.consensus.core.is_empty() {
        tracing::info!("no core group configured; founding with this node alone");
    }
    let founders = founders(&config.consensus.core, me, || local_addr(node.endpoint()))?;

    let names: Vec<_> = founders.iter().map(|(r, _)| display(r)).collect();
    tracing::info!(founders = names.join(", "), "founding the group");
    node.init_group(founders, secret).await?;
    Ok(())
}

/// The founding members, from the configured core group.
///
/// `mine` supplies this node's own address, and is only called if it is needed:
/// an empty core group means founding alone, and a core entry for this node that
/// states no address means using what we actually bound to rather than making
/// the operator repeat it.
fn founders(
    core: &[CoreMember],
    me: MemberId,
    mine: impl Fn() -> NodeAddr,
) -> Result<Vec<(MemberRecord, NodeAddr)>> {
    if core.is_empty() {
        return Ok(vec![(record(me, String::new()), mine())]);
    }
    if !core.iter().any(|member| member.member == me) {
        bail!(
            "[consensus] core does not list this node ({me}); a founder has to be one of \
             the founding members"
        );
    }

    Ok(core
        .iter()
        .map(|member| {
            let addr = if member.member == me && member.addrs.is_empty() {
                mine()
            } else {
                NodeAddr {
                    relay: member.relay.clone(),
                    direct: member.addrs.iter().copied().collect(),
                }
            };
            (record(member.member, member.name.clone()), addr)
        })
        .collect())
}

/// This node's own dialable address.
fn local_addr(endpoint: &Endpoint) -> NodeAddr {
    NodeAddr {
        relay: endpoint
            .home_relay_status()
            .get()
            .first()
            .map(|relay| relay.url().to_string()),
        direct: endpoint.bound_sockets().into_iter().collect(),
    }
}

/// A founding member's record.
///
/// `pledge_bytes` is zero because nothing reads it before phase 3, where
/// storage pledges get a command of their own.
fn record(member_id: MemberId, display_name: String) -> MemberRecord {
    MemberRecord {
        member_id,
        display_name,
        pledge_bytes: 0,
    }
}

/// Logs the membership on every change, for as long as the node runs.
async fn report_membership(mut memberships: tokio::sync::watch::Receiver<MembershipState>) {
    loop {
        // Read before waiting, so the membership a node restarts with is
        // reported at once rather than at the next change — which for a settled
        // group could be never.
        let membership = memberships.borrow_and_update().clone();
        if let Some(group) = membership.group_id() {
            tracing::info!(
                %group,
                members = membership.len(),
                core = membership.core().len(),
                who = render(&membership),
                "membership"
            );
        }

        if memberships.changed().await.is_err() {
            return;
        }
    }
}

/// The membership held on disk, or `None` if this node has no database yet.
///
/// An error here is usually not corruption: redb takes an exclusive lock on the
/// file, so a second process cannot open it even to read while `distlib run`
/// holds it.
fn stored_membership(paths: &Paths) -> Result<Option<MembershipState>> {
    let path = paths.raft_db();
    if !path.exists() {
        return Ok(None);
    }
    let store = StateMachineStore::open(&path).with_context(|| {
        format!(
            "could not open {}; a running `distlib run` holds it exclusively",
            path.display()
        )
    })?;
    Ok(Some(store.membership()))
}

/// The group line and the member count, shared by `members` and `status`.
///
/// Returns whether there was a group to describe.
fn print_group(membership: &MembershipState) -> bool {
    let Some(group) = membership.group_id() else {
        return false;
    };
    println!("group      {group}");
    println!(
        "members    {} ({} core)",
        membership.len(),
        membership.core().len()
    );
    true
}

/// One member, named if they have a name.
fn display(member: &MemberRecord) -> String {
    if member.display_name.is_empty() {
        member.member_id.to_string()
    } else {
        format!("{} ({})", member.display_name, member.member_id)
    }
}

fn render(membership: &MembershipState) -> String {
    membership
        .members()
        .map(display)
        .collect::<Vec<_>>()
        .join(", ")
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

#[cfg(test)]
mod tests {
    use super::*;

    fn an_id(byte: u8) -> MemberId {
        MemberId::from(iroh::SecretKey::from_bytes(&[byte; 32]).public())
    }

    fn bound() -> NodeAddr {
        NodeAddr {
            relay: None,
            direct: ["127.0.0.1:11204".parse().expect("a literal address")]
                .into_iter()
                .collect(),
        }
    }

    fn configured(member: MemberId, addrs: &[&str]) -> CoreMember {
        CoreMember {
            member,
            name: "someone".to_owned(),
            addrs: addrs
                .iter()
                .map(|addr| addr.parse().expect("a literal address"))
                .collect(),
            relay: None,
        }
    }

    #[test]
    fn an_empty_core_group_founds_alone() {
        let me = an_id(1);

        let founders = founders(&[], me, bound).expect("founding alone is allowed");

        assert_eq!(founders.len(), 1);
        assert_eq!(founders[0].0.member_id, me);
        assert_eq!(
            founders[0].1,
            bound(),
            "its own address, since none is given"
        );
    }

    #[test]
    fn a_core_group_without_this_node_is_refused() {
        // Otherwise the founder writes a group it is not in, then cannot reach
        // its own log: it would be refused by every member it just admitted.
        let error = founders(&[configured(an_id(2), &[])], an_id(1), bound)
            .expect_err("a founder has to be a founding member");

        assert!(
            error.to_string().contains("[consensus] core"),
            "the error should name the key to fix; got {error}"
        );
    }

    #[test]
    fn a_configured_address_wins_over_the_bound_one() {
        // The bound address is a fallback, not an override. A node behind NAT
        // binds to something no peer can dial, and states the reachable address
        // in its config.
        let me = an_id(1);
        let core = [configured(me, &["203.0.113.7:11204"])];

        let founders = founders(&core, me, bound).expect("this node is a founder");

        assert_eq!(
            founders[0].1.direct,
            ["203.0.113.7:11204".parse().expect("a literal address")]
                .into_iter()
                .collect(),
        );
    }

    #[test]
    fn every_founder_is_recorded_with_its_own_address() {
        let me = an_id(1);
        let other = an_id(2);
        let core = [
            configured(me, &[]),
            configured(other, &["198.51.100.4:11205"]),
        ];

        let founders = founders(&core, me, bound).expect("this node is a founder");

        assert_eq!(founders.len(), 2);
        assert_eq!(founders[0].1, bound(), "ours, filled in from the endpoint");
        assert_eq!(
            founders[1].1.direct,
            ["198.51.100.4:11205".parse().expect("a literal address")]
                .into_iter()
                .collect(),
        );
    }
}
