//! What each subcommand actually does.

use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use distlib_api::{Api, Client, ClientError, Server};
use distlib_consensus::{
    MemberRecord, MembershipNode, MembershipState, RAFT_DB, StateMachineStore,
};
use distlib_core::{
    Config, CoreMember, DataDir, MemberId, NodeAddr, Ticket,
    identity::{create_secret_key, load_or_create_secret_key, member_id},
    token,
};
use distlib_net::{AllowlistHooks, allowlist, build_endpoint, ping};
use iroh::{Endpoint, EndpointAddr, RelayUrl, SecretKey, TransportAddr, Watcher as _};
use serde_json::{Value, json};

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
        println!("wrote       {}", paths.config_file.display());
    }

    println!(
        "identity    {} ({})",
        member_id(&secret),
        if existed && !force { "existing" } else { "new" }
    );
    println!("data dir    {}", paths.data_dir.root().display());
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
    // What this node expects to serve. Decided from configuration because the
    // endpoint is built before the log is opened, and the log gets the final
    // say over the role inside `start` — but that mismatch cannot escape:
    // iroh's router calls `set_alpns` with what it actually accepts, so a node
    // the group was founded without stops advertising raft the moment its
    // router comes up. This is the same answer arrived at earlier, which is
    // what makes it worth stating.
    let core = core_group(&config);
    let is_core = core.iter().any(|(member, _)| *member == me);
    let endpoint = build_endpoint(
        secret.clone(),
        &config.net,
        hooks.clone(),
        distlib_consensus::alpns(is_core),
    )
    .await?;

    tracing::info!(member = %me, "node started");
    for addr in endpoint.bound_sockets() {
        tracing::info!(%addr, "listening");
    }

    let node = Arc::new(
        MembershipNode::start(endpoint, hooks, writer, paths.data_dir.root(), core).await?,
    );

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

    // The local API. Started after founding, so a caller that reaches it finds
    // a node that has finished deciding what it is.
    let api = if config.api.enabled {
        Some(serve_api(paths, &config, Arc::clone(&node), secret.clone()).await?)
    } else {
        tracing::info!("the local api is disabled");
        None
    };

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
    if let Some(api) = api {
        api.shutdown();
    }
    node.shutdown().await;
    Ok(())
}

/// `distlib whoami`
///
/// What a founder needs from everyone else before they can found the group.
/// Founding writes each member's id and address into the log, and the log is
/// what the group is — so those have to be collected out of band, once, before
/// there is any group to ask.
pub async fn whoami(paths: &Paths) -> Result<()> {
    let config = load_config(&paths.config_file)?;
    let secret = load_or_create_secret_key(&paths.secret_key_file())?;
    let me = member_id(&secret);

    println!("identity    {me}");
    println!("data dir    {}", paths.data_dir.root().display());

    // Only ever this node's own entry, so the name is left for whoever pastes
    // it: they are the one who has to recognise it in `distlib members`.
    let mut entry = CoreMember {
        member: me,
        name: String::new(),
        addrs: Vec::new(),
        relay: None,
    };

    let pinned = config.net.bind_addr_v4.port() != 0;
    // Deliberately no address rather than a wrong one. An OS-chosen port is a
    // different port next time, and the whole point of this line is that the
    // founder pastes it once and it keeps working.
    let mut note = if pinned {
        Vec::new()
    } else {
        vec![
            "No address shown: bind_addr_v4 has no fixed port, so any address here".to_owned(),
            "would be wrong after the next restart. Pin a port in config.toml and run".to_owned(),
            "this again — a founder needs one on a LAN, or with relay_mode = \"disabled\"."
                .to_owned(),
        ]
    };

    if pinned {
        match build_endpoint(
            secret,
            &config.net,
            AllowlistHooks::new(allowlist(me, []).1),
            distlib_net::alpn::registered(),
        )
        .await
        {
            Ok(endpoint) => {
                let addr = local_addr(&endpoint);
                entry.addrs = addr.direct.into_iter().collect();
                entry.relay = addr.relay;
                endpoint.close().await;
            }
            Err(error) => {
                tracing::debug!(?error, "could not bind to discover addresses");
                note = vec![
                    format!(
                        "No address shown: {} is already bound, most likely by this",
                        config.net.bind_addr_v4
                    ),
                    "node already running. Stop it and run this again.".to_owned(),
                ];
            }
        }
    }

    println!();
    println!("Send this to whoever is founding the group, for their [consensus] core:");
    println!();
    println!("  {}", entry.to_toml_inline());
    println!();
    println!("They set `name` to whatever they want you called. Every founder needs the same");
    println!("`core` list, so they send the finished block back to all of you.");
    if entry.addrs.len() > 1 {
        println!();
        println!("Several addresses are listed because this machine has several interfaces.");
        println!("Keep the one your friends can actually reach and delete the rest.");
    }
    if !note.is_empty() {
        println!();
        for line in note {
            println!("{line}");
        }
    }
    Ok(())
}

/// A client for this node's running API, if there is a token to use.
fn api(paths: &Paths, config: &Config) -> Result<Client> {
    let token_file = paths.data_dir.api_token_file();
    if !token_file.exists() {
        bail!(
            "no api token at {}; the node writes one the first time it runs",
            token_file.display()
        );
    }
    Ok(Client::new(
        config.api.bind_addr,
        token::load_or_create(&token_file)?,
    ))
}

/// Calls `method` on this node, turning the failures into advice.
async fn ask(paths: &Paths, method: &str, params: Value) -> Result<Value> {
    let config = load_config(&paths.config_file)?;
    api(paths, &config)?
        .call(method, params)
        .await
        .map_err(|error| match error {
            ClientError::Unreachable { .. } => anyhow::anyhow!(
                "{error}\n\nthe node holds its log while it runs, so changing membership \
                 goes through it: start it with `distlib run`"
            ),
            other => anyhow::Error::from(other),
        })
}

/// `distlib admit`
pub async fn admit(paths: &Paths, member: MemberId, name: Option<String>) -> Result<()> {
    let params = match name {
        Some(name) => json!({ "member": member, "name": name }),
        None => json!({ "member": member }),
    };
    ask(paths, "group.propose_add", params).await?;
    println!("admitted    {member}");
    Ok(())
}

/// `distlib expel`
pub async fn expel(paths: &Paths, member: MemberId, reason: String) -> Result<()> {
    ask(
        paths,
        "group.propose_expel",
        json!({ "member": member, "reason": reason }),
    )
    .await?;
    println!("expelled    {member}");
    Ok(())
}

/// `distlib pledge`
pub async fn pledge(paths: &Paths, bytes: u64) -> Result<()> {
    ask(paths, "group.pledge_set", json!({ "bytes": bytes })).await?;
    println!("pledged     {bytes} bytes");
    Ok(())
}

/// `distlib ticket`
pub async fn ticket(paths: &Paths) -> Result<()> {
    let answer = ask(paths, "group.ticket", Value::Null).await?;
    let ticket = answer["ticket"]
        .as_str()
        .context("the api did not return a ticket")?;

    println!("{ticket}");
    println!();
    println!("Send this to somebody you have already admitted with `distlib admit`.");
    println!("It says where the group is, not who may join it — until their");
    println!("`MemberAdded` is committed, a ticket gets them nowhere.");
    Ok(())
}

/// `distlib join`
///
/// Writes the ticket's directions into this node's configuration and stops
/// there. Starting the node is a separate step on purpose: `join` is a
/// configuration change, and a command that silently began serving would be a
/// surprising thing to have run.
pub fn join(paths: &Paths, ticket: &str) -> Result<()> {
    let ticket: Ticket = ticket.parse()?;
    let mut config = load_config(&paths.config_file)?;

    // Refuse rather than overwrite: this node already follows a log, and
    // replacing its bootstrap set with another group's would leave it enforcing
    // one membership while fetching another.
    if let Some(existing) = stored_membership(paths)?.and_then(|m| m.group_id())
        && existing != ticket.group
    {
        bail!(
            "this node already belongs to group {existing}; joining {} would mean starting \
             over, so delete {} first if that is what you want",
            ticket.group,
            paths.data_dir.root().display()
        );
    }

    config.net.relay_mode = ticket.relay_mode;
    config.net.relay_urls = ticket.relay_urls;
    config.consensus.core = ticket
        .core
        .iter()
        .map(|(member, addr)| CoreMember {
            member: *member,
            // The log carries the names; this list only has to be reachable.
            name: String::new(),
            addrs: addr.direct.iter().copied().collect(),
            relay: addr.relay.clone(),
        })
        .collect();

    // Rendered rather than patched. The starter renderer is the one place that
    // knows how to write every field with its explanation, and patching a
    // section into a file by hand is how the two shapes drift apart.
    std::fs::write(&paths.config_file, config.to_starter_toml())
        .with_context(|| format!("could not write {}", paths.config_file.display()))?;

    println!("group       {}", ticket.group);
    println!("core        {} node(s)", config.consensus.core.len());
    println!("wrote       {}", paths.config_file.display());
    println!();
    println!("Start the node with `distlib run` and it will fetch the log.");
    Ok(())
}

/// `distlib members`
pub async fn members(paths: &Paths) -> Result<()> {
    // The running node first, its database second. Only one of the two can
    // answer at a time — the node holds the database exclusively — so this
    // works whether or not it is up, which is the whole point of asking twice.
    let listing = match live_members(paths).await {
        Ok(listing) => listing,
        Err(error) => {
            tracing::debug!(?error, "the api did not answer; reading the database");
            stored_members(paths)?
        }
    };

    let Some(group) = &listing.group else {
        println!("no group yet — found one with `distlib run --found-group`");
        return Ok(());
    };
    println!("group       {group}");
    println!(
        "members     {} ({} core)",
        listing.members.len(),
        listing.members.iter().filter(|member| member.core).count()
    );
    for member in &listing.members {
        println!(
            "  {}{}",
            member.name,
            if member.core { "  core" } else { "" }
        );
    }
    Ok(())
}

/// What a running node says about itself.
fn print_live_status(live: &Value) {
    match live["group"].as_str() {
        Some(group) => println!("group       {group}"),
        None => println!("group       none yet"),
    }
    println!(
        "members     {} ({} core)",
        live["members"].as_u64().unwrap_or(0),
        live["core_group"].as_array().map_or(0, Vec::len)
    );
    println!(
        "role        {}",
        if live["core"].as_bool().unwrap_or(false) {
            "core member"
        } else {
            "member"
        }
    );
    // Only a running node has these, which is the reason to have asked it. A
    // follower has no Raft state to report — it does not vote — so it says how
    // far it has read instead of pretending to a term it has no part in.
    match live["raft"].as_str() {
        Some(state) => {
            println!("Raft role   {state}");
            if let Some(leader) = live["leader"].as_str() {
                println!("Raft leader {leader}");
            }
        }
        None => println!(
            "follows     the log to index {}",
            live["followed_upto"].as_u64().unwrap_or(0)
        ),
    }
    println!("node        running");
}

/// What this node's files say, when the node itself is not up to answer.
fn print_stored_status(paths: &Paths, me: MemberId) {
    match stored_membership(paths) {
        Ok(membership) => {
            let membership = membership.unwrap_or_default();
            if print_group(&membership) {
                let role = if membership.core().contains(&me) {
                    "core member"
                } else if membership.is_member(&me) {
                    "member"
                } else {
                    "not a member of this group"
                };
                println!("role        {role}");
            } else {
                println!("group       none yet");
            }
        }
        Err(error) => {
            // Reaching here means neither source answered: the api is down and
            // the database will not open. Almost always a stale lock or a
            // half-written directory, and the chain beneath says so three
            // times, so keep the line readable and log the detail.
            tracing::debug!(?error, "could not read the membership");
            println!("group       unavailable — could not read the database");
        }
    }
    println!("node        not running");
}

/// A membership, however it was obtained.
///
/// The two sources describe the same thing in different shapes; converting both
/// to this is what keeps one renderer rather than two that drift.
#[derive(Default)]
struct Listing {
    group: Option<String>,
    members: Vec<Listed>,
}

struct Listed {
    name: String,
    core: bool,
}

/// The membership as the running node has it.
async fn live_members(paths: &Paths) -> Result<Listing> {
    let answer = ask(paths, "group.members", Value::Null).await?;
    let members = answer["members"]
        .as_array()
        .context("the api did not return a member list")?
        .iter()
        .map(|member| Listed {
            name: named(
                member["member"].as_str().unwrap_or_default(),
                member["name"].as_str().unwrap_or_default(),
            ),
            core: member["core"].as_bool().unwrap_or(false),
        })
        .collect();

    Ok(Listing {
        group: answer["group"].as_str().map(str::to_owned),
        members,
    })
}

/// The membership as this node's database has it.
fn stored_members(paths: &Paths) -> Result<Listing> {
    let membership = stored_membership(paths)?.unwrap_or_default();
    Ok(Listing {
        group: membership.group_id().map(|group| group.to_string()),
        members: membership
            .members()
            .map(|member| Listed {
                name: display(member),
                core: membership.core().contains(&member.member_id),
            })
            .collect(),
    })
}

/// `distlib status`
pub async fn status(paths: &Paths, online: bool) -> Result<()> {
    println!("data dir    {}", paths.data_dir.root().display());
    println!("config      {}", display_path(&paths.config_file));

    let key_file = paths.secret_key_file();
    if !key_file.exists() {
        println!("identity    not initialised — run `distlib init`");
        return Ok(());
    }

    let config = load_config(&paths.config_file)?;
    let secret = load_or_create_secret_key(&key_file)?;
    let me = member_id(&secret);

    println!("identity    {me}");
    println!("relay mode  {:?}", config.net.relay_mode);
    println!("core seed   {} member(s)", config.consensus.core.len());

    // The running node knows more than its database does — it can say which
    // Raft state it is in and who the leader is — so ask it first and read the
    // files only when nothing answers.
    match ask(paths, "node.status", Value::Null).await {
        Ok(live) => print_live_status(&live),
        Err(error) => {
            tracing::debug!(?error, "the api did not answer; reading the database");
            print_stored_status(paths, me);
        }
    }

    if online {
        let (_writer, allowed) = allowlist(me, config.consensus.core.iter().map(|c| c.member));
        let endpoint = match build_endpoint(
            secret,
            &config.net,
            AllowlistHooks::new(allowed),
            distlib_consensus::alpns(false),
        )
        .await
        {
            Ok(endpoint) => endpoint,
            // Binding a pinned port fails while the node holds it — which is
            // the normal state now that core nodes pin one. Say so instead of
            // discarding everything printed above.
            Err(error) => {
                tracing::debug!(?error, "could not bind for --online");
                println!(
                    "address    unavailable — {} is already bound",
                    config.net.bind_addr_v4
                );
                return Ok(());
            }
        };
        // Bounded, because "online" means a relay has been reached and with
        // `relay_mode = "disabled"` there is no relay to reach — the wait would
        // never end. The direct addresses are known either way.
        if tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online())
            .await
            .is_err()
        {
            println!("address     (no relay reached; direct addresses only)");
        }
        let addr = endpoint.addr();
        for transport in &addr.addrs {
            println!("address     {transport:?}");
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
                member.addr()
            };
            (record(member.member, member.name.clone()), addr)
        })
        .collect())
}

/// The configured core group, with an address for each.
///
/// The whole list, not just this node's own entry: a follower needs the others'
/// addresses to fetch the log from them, and a core node needs their ids to
/// know who may speak Raft to it before there is a log to say.
fn core_group(config: &Config) -> Vec<(MemberId, NodeAddr)> {
    config
        .consensus
        .core
        .iter()
        .map(|member| (member.member, member.addr()))
        .collect()
}

/// Starts the local API.
///
/// The token is created on first run rather than at `init`, so a data
/// directory made before this existed grows one when it is next started.
async fn serve_api(
    paths: &Paths,
    config: &Config,
    node: Arc<MembershipNode>,
    secret: SecretKey,
) -> Result<Server> {
    let token_file = paths.data_dir.api_token_file();
    let token = token::load_or_create(&token_file)?;

    let api = Api {
        node,
        secret,
        net: config.net.clone(),
    };
    let server = distlib_api::serve(config.api.bind_addr, api, token)
        .await
        .with_context(|| {
            format!(
                "could not serve the api on {}; a second node on this machine needs its own \
                 `[api] bind_addr`, just as it needs its own `[net] bind_addr_v4`",
                config.api.bind_addr
            )
        })?;

    // Bound somewhere other than loopback, the token is the only thing between
    // the network and this node's membership, and there is no TLS in front of
    // it. Serving remotely is a legitimate thing to want — a node on a server
    // or in a container — so this warns rather than refuses.
    if !server.addr().ip().is_loopback() {
        tracing::warn!(
            addr = %server.addr(),
            "the api is not on loopback and has no tls; put it behind a reverse proxy"
        );
    }

    // The address, never the token: it is a secret, and it is one command away
    // for anyone entitled to it.
    tracing::info!(addr = %server.addr(), token = %token_file.display(), "local api listening");
    Ok(server)
}

/// This node's own dialable address.
///
/// From `addr()` rather than `bound_sockets()`, which is the difference between
/// an address a peer can use and one it cannot: a node bound to `0.0.0.0:11204`
/// reports exactly that as its bound socket, and no peer can dial a wildcard.
/// `addr()` resolves it to the machine's actual interface addresses, and does so
/// immediately — there is nothing to wait for.
///
/// The relay comes from `home_relay_status` instead, because `addr()` carries a
/// relay only once one has been established, which takes a moment and never
/// happens with `relay_mode = "disabled"`.
fn local_addr(endpoint: &Endpoint) -> NodeAddr {
    NodeAddr {
        relay: endpoint
            .home_relay_status()
            .get()
            .first()
            .map(|relay| relay.url().to_string()),
        direct: endpoint
            .addr()
            .addrs
            .into_iter()
            .filter_map(|addr| match addr {
                TransportAddr::Ip(socket) => Some(socket),
                _ => None,
            })
            .collect(),
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
    println!("group       {group}");
    println!(
        "members     {} ({} core)",
        membership.len(),
        membership.core().len()
    );
    true
}

/// One member, named if they have a name.
fn display(member: &MemberRecord) -> String {
    named(&member.member_id.to_string(), &member.display_name)
}

/// `name (id)`, or just the id for a member with no name.
fn named(id: &str, name: &str) -> String {
    if name.is_empty() {
        id.to_owned()
    } else {
        format!("{name} ({id})")
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

    #[tokio::test]
    async fn a_founder_never_records_a_wildcard_address() {
        // Every other founder dials what founding writes into the log, and
        // nobody can dial 0.0.0.0. A node bound to a wildcard reports exactly
        // that as its bound socket, so the address has to be resolved to the
        // machine's real interfaces before it goes in.
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::endpoint::RelayMode::Disabled)
            .bind_addr(
                "0.0.0.0:0"
                    .parse::<SocketAddr>()
                    .expect("a literal address"),
            )
            .expect("a bindable address")
            .bind()
            .await
            .expect("binding a wildcard address");

        let recorded = local_addr(&endpoint);

        assert!(
            recorded
                .direct
                .iter()
                .all(|addr| !addr.ip().is_unspecified()),
            "a wildcard is not dialable; got {:?}",
            recorded.direct
        );
        endpoint.close().await;
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
