//! Building the iroh endpoint from configuration.

use distlib_core::{NetConfig, RelayMode as ConfigRelayMode};
use iroh::{
    Endpoint, RelayUrl, SecretKey,
    endpoint::{Builder, RelayMode, presets},
};

use crate::{error::NetError, error::Result, hooks::AllowlistHooks};

/// Binds an endpoint for this node.
///
/// The relay mode decides which preset is used, and the preset is what
/// determines whether the endpoint reaches the network at all:
///
/// * [`ConfigRelayMode::Default`] uses [`presets::N0`] — n0's relays plus DNS
///   and pkarr address lookup.
/// * The other modes build on [`presets::Minimal`], which sets only the crypto
///   provider. No address lookup is configured, so an endpoint in these modes
///   makes no DNS or pkarr requests and reaches only peers it is given explicit
///   addresses for.
///
/// `alpns` must be exactly what the caller's router will serve — see
/// [`configure`]. [`crate::alpn::registered`] is the right answer only for a node
/// serving nothing but [`crate::Node`]'s protocols.
pub async fn build_endpoint(
    secret_key: SecretKey,
    config: &NetConfig,
    hooks: AllowlistHooks,
    alpns: Vec<Vec<u8>>,
) -> Result<Endpoint> {
    let builder = match config.relay_mode {
        ConfigRelayMode::Default => Endpoint::builder(presets::N0),
        ConfigRelayMode::Disabled => {
            Endpoint::builder(presets::Minimal).relay_mode(RelayMode::Disabled)
        }
        ConfigRelayMode::Custom => {
            Endpoint::builder(presets::Minimal).relay_mode(custom_relays(&config.relay_urls)?)
        }
    };

    Ok(configure(builder, secret_key, hooks, alpns)
        .bind_addr(config.bind_addr_v4)
        .map_err(|source| NetError::InvalidRelayUrl {
            url: source.to_string(),
        })?
        .bind()
        .await?)
}

/// Applies the settings every distlib endpoint shares, whatever its relay mode.
///
/// Kept separate so tests can build endpoints with their own transport and
/// relay setup while still getting — importantly — the same membership
/// enforcement as production. A test that bypassed the hooks would be testing a
/// configuration nothing ships.
///
/// `alpns` is a parameter rather than always [`crate::alpn::registered`] because the
/// endpoint must advertise exactly what its router serves, and that depends on
/// the caller: a node running consensus serves [`crate::alpn::RAFT`] too, which
/// `distlib-net` cannot handle itself.
///
/// The hooks are passed in rather than built here so the caller can keep a
/// clone and run [`AllowlistHooks::evict_expelled`] on it. Both halves need the
/// same connection table, and the endpoint takes ownership of its copy.
pub fn configure(
    builder: Builder,
    secret_key: SecretKey,
    hooks: AllowlistHooks,
    alpns: Vec<Vec<u8>>,
) -> Builder {
    builder.secret_key(secret_key).alpns(alpns).hooks(hooks)
}

fn custom_relays(urls: &[String]) -> Result<RelayMode> {
    if urls.is_empty() {
        return Err(NetError::NoCustomRelays);
    }
    let parsed = urls
        .iter()
        .map(|url| {
            url.parse::<RelayUrl>()
                .map_err(|_| NetError::InvalidRelayUrl { url: url.clone() })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RelayMode::Custom(parsed.into_iter().collect()))
}
