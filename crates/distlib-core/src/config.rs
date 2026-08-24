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
    /// Members this node will talk to.
    ///
    /// Static in phase 0. From phase 1 the allowlist is derived from the
    /// committed membership log and this field goes away.
    pub allowlist: Vec<MemberId>,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr_v4: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            relay_mode: RelayMode::default(),
            relay_urls: Vec::new(),
            allowlist: Vec::new(),
        }
    }
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
        let allowlist = quoted(
            &self
                .net
                .allowlist
                .iter()
                .map(MemberId::to_string)
                .collect::<Vec<_>>(),
        );

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
             bind_addr_v4 = \"{bind}\"\n\
             \n\
             # \"default\" (n0 relays), \"disabled\", or \"custom\" with relay_urls below.\n\
             relay_mode = \"{relay_mode}\"\n\
             relay_urls = [{relay_urls}]\n\
             \n\
             # Members this node will talk to. Phase 0 only: from phase 1 the\n\
             # allowlist comes from the committed membership log.\n\
             allowlist = [{allowlist}]\n",
            bind = self.net.bind_addr_v4,
            relay_mode = self.net.relay_mode.as_str(),
            relay_urls = quoted(&self.net.relay_urls),
        )
    }
}
