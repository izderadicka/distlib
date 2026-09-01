//! Configuration: a TOML file overlaid with `DISTLIB_*` environment variables.
//!
//! Precedence, lowest to highest: built-in defaults, the config file, the
//! environment. The command line is applied last by the binary, on the value
//! this module returns.
//!
//! Nested keys use a double underscore, so `[net] bind_addr_v4` is
//! `DISTLIB_NET__BIND_ADDR_V4`.

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::Path,
};

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

use crate::{
    addr::NodeAddr,
    error::{CoreError, Result},
    id::MemberId,
};

/// Prefix for every environment override.
const ENV_PREFIX: &str = "DISTLIB_";
/// Separator for nested keys in environment variables.
const ENV_NESTED_SEPARATOR: &str = "__";
/// The one key resolved before configuration is read, and so forbidden in it.
const DATA_DIR_KEY: &str = "data_dir";

/// The whole configuration of a node.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Transport settings.
    pub net: NetConfig,
    /// The group this node belongs to.
    pub consensus: ConsensusConfig,
    /// The local control API.
    pub api: ApiConfig,
}

/// How this node talks to the network.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetConfig {
    /// Local socket to bind. Port 0 asks the OS to choose.
    pub bind_addr_v4: SocketAddr,
    /// Which relay servers to use for connectivity assistance.
    pub relay_mode: RelayMode,
    /// Relay URLs, used only when `relay_mode` is [`RelayMode::Custom`].
    pub relay_urls: Vec<String>,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr_v4: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            relay_mode: RelayMode::default(),
            relay_urls: Vec::new(),
        }
    }
}

/// The local JSON-RPC API (§7.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiConfig {
    /// Whether to serve it at all.
    pub enabled: bool,

    /// Where to listen.
    ///
    /// Loopback by default, and that is the security boundary rather than a
    /// convenience: the token is the only other thing between a caller and
    /// making this node propose membership changes as itself. Moving this off
    /// `127.0.0.1` exposes that to the network, with no TLS in front of it.
    pub bind_addr: SocketAddr,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 11280)),
        }
    }
}

/// The group this node founds or belongs to.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsensusConfig {
    /// The founding core group, this node included.
    ///
    /// The one piece of membership that cannot come from the log, because it is
    /// what makes reading the log possible: core nodes have to reach each other
    /// to replicate the founding entry, and they will not connect to anyone
    /// outside the allowlist. So the founding set is stated once, here, and
    /// from the moment `GroupFounded` is applied the log is authoritative and
    /// this is never consulted again.
    ///
    /// Empty means no group has been founded. `distlib run --found-group` with
    /// an empty list founds one with this node alone.
    pub core: Vec<CoreMember>,
}

/// A founding core member.
///
/// Carries an address, unlike anything else that names a member: the log that
/// would otherwise supply it is exactly what these addresses are needed to
/// fetch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreMember {
    /// The member's id — their public key.
    pub member: MemberId,

    /// Display name recorded in the founding event. Metadata, not identity.
    #[serde(default)]
    pub name: String,

    /// Socket addresses to try directly.
    ///
    /// Required with `relay_mode = "disabled"` or on a LAN, where there is no
    /// address lookup to fall back on.
    #[serde(default)]
    pub addrs: Vec<SocketAddr>,

    /// A relay to reach this member through.
    #[serde(default)]
    pub relay: Option<String>,
}

/// Relay selection.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayMode {
    /// The n0-operated default relays.
    #[default]
    Default,
    /// No relays; direct connectivity only.
    Disabled,
    /// The relays listed in [`NetConfig::relay_urls`].
    Custom,
}

impl RelayMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Disabled => "disabled",
            Self::Custom => "custom",
        }
    }
}

impl Config {
    /// Loads configuration for a node whose config file is at `config_file`.
    ///
    /// A missing file is not an error: defaults plus environment overrides are
    /// a valid configuration.
    pub fn load(config_file: &Path) -> Result<Self> {
        // The data directory is resolved before this file can be found, so a
        // `data_dir` key here would be silently ignored. Say so instead.
        if Figment::from(Toml::file(config_file))
            .find_value(DATA_DIR_KEY)
            .is_ok()
        {
            return Err(CoreError::DataDirInConfig {
                path: config_file.to_path_buf(),
            });
        }

        Ok(Figment::from(Serialized::defaults(Self::default()))
            .merge(Toml::file(config_file))
            .merge(Self::env())
            .extract()?)
    }

    /// The environment provider, with `DISTLIB_DATA_DIR` excluded.
    ///
    /// The data directory is a legitimate environment override, but it is
    /// consumed before configuration is loaded. Leaving it in would make
    /// `deny_unknown_fields` reject a variable that is documented to work.
    fn env() -> Env {
        Env::prefixed(ENV_PREFIX)
            .ignore(&[DATA_DIR_KEY])
            .split(ENV_NESTED_SEPARATOR)
    }

    /// Renders a starter config file.
    ///
    /// Written out by hand rather than through a serialiser so the file carries
    /// the comments that explain each field — the point of a starter file.
    pub fn to_starter_toml(&self) -> String {
        let quoted = |values: &[String]| {
            values
                .iter()
                .map(|value| format!("\"{value}\""))
                .collect::<Vec<_>>()
                .join(", ")
        };

        format!(
            "# distlib configuration.\n\
             #\n\
             # Any key can be overridden by an environment variable: prefix with\n\
             # DISTLIB_ and separate nested keys with a double underscore, e.g.\n\
             # DISTLIB_NET__BIND_ADDR_V4=0.0.0.0:11204\n\
             #\n\
             # The data directory is not configured here — it is where this file\n\
             # lives. Use --data-dir or DISTLIB_DATA_DIR.\n\
             \n\
             [net]\n\
             # Local socket to bind; port 0 lets the OS choose.\n\
             #\n\
             # Pin a port on a core node. Founding records this node's address in\n\
             # the log, and an OS-chosen port is different after every restart.\n\
             bind_addr_v4 = \"{bind}\"\n\
             \n\
             # \"default\" (n0 relays), \"disabled\", or \"custom\" with relay_urls below.\n\
             relay_mode = \"{relay_mode}\"\n\
             relay_urls = [{relay_urls}]\n\
             \n\
             [consensus]\n\
             # The founding core group, this node included — the members who vote\n\
             # on the membership log. Everything else about membership comes from\n\
             # the log itself; this is the one thing that cannot, because reaching\n\
             # the log means connecting to these nodes first.\n\
             #\n\
             # Run `distlib whoami` on each founder; it prints the line to put\n\
             # here. Every founder needs the same list.\n\
             #\n\
             # Leave it empty to found a group with this node alone:\n\
             #\n\
             #   distlib run --found-group\n\
             #\n\
             # To found with others, list every founder here — the same list in\n\
             # every founder's config — and have one of them run --found-group.\n\
             # `addrs` is required unless relays are enabled and can find them.\n\
             #\n\
             #   core = [\n\
             #     {{ member = \"<their id>\", name = \"alice\", addrs = [\"192.168.1.10:11204\"] }},\n\
             #     {{ member = \"<your id>\", name = \"bob\", addrs = [\"192.168.1.11:11204\"] }},\n\
             #   ]\n\
             core = [{core}]\n\
             \n\
             [api]\n\
             # The local JSON-RPC API: what `distlib admit`, `distlib expel` and\n\
             # the web UI talk to. Loopback only — the token in\n\
             # <data-dir>/api.token is the only thing guarding it, and there is\n\
             # no TLS, so do not move this off 127.0.0.1.\n\
             enabled = {api_enabled}\n\
             bind_addr = \"{api_bind}\"\n",
            bind = self.net.bind_addr_v4,
            relay_mode = self.net.relay_mode.as_str(),
            relay_urls = quoted(&self.net.relay_urls),
            api_enabled = self.api.enabled,
            api_bind = self.api.bind_addr,
            core = self
                .consensus
                .core
                .iter()
                .map(CoreMember::to_toml_inline)
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

impl CoreMember {
    /// Where to reach this member.
    ///
    /// The config file keeps `addrs` and `relay` as flat keys rather than
    /// nesting a [`NodeAddr`], because this is a file people edit by hand and
    /// `addrs = ["1.2.3.4:11204"]` reads better than a nested table. This is
    /// the one place the two shapes meet.
    pub fn addr(&self) -> NodeAddr {
        NodeAddr {
            relay: self.relay.clone(),
            direct: self.addrs.iter().copied().collect(),
        }
    }

    /// This member as a TOML inline table, ready to paste into `[consensus] core`.
    ///
    /// Public so `distlib whoami` prints exactly what `init` writes: one
    /// renderer, so the line a founder is handed cannot drift from the file it
    /// goes into.
    pub fn to_toml_inline(&self) -> String {
        let addrs = self
            .addrs
            .iter()
            .map(|addr| format!("\"{addr}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let mut out = format!(
            "{{ member = \"{}\", name = \"{}\", addrs = [{addrs}]",
            self.member, self.name
        );
        if let Some(relay) = &self.relay {
            out.push_str(&format!(", relay = \"{relay}\""));
        }
        out.push_str(" }");
        out
    }
}
