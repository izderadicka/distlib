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
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};

use crate::{
    addr::NodeAddr,
    error::{CoreError, Result},
    group_settings::GroupSettings,
    id::MemberId,
};

/// Prefix for every environment override.
const ENV_PREFIX: &str = "DISTLIB_";
/// Separator for nested keys in environment variables.
const ENV_NESTED_SEPARATOR: &str = "__";
/// The one key resolved before configuration is read, and so forbidden in it.
const DATA_DIR_KEY: &str = "data_dir";
/// The log filter, which shares this prefix but is not configuration.
///
/// `DISTLIB_LOG` is documented as the way to set the log filter, and the
/// binary reads it before any of this runs. Without excluding it here,
/// `deny_unknown_fields` rejects it as an unknown key called `log` — so setting
/// the documented variable stopped the node from starting at all.
const LOG_KEY: &str = "log";

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
    pub library: LibraryConfig,
    pub availability: AvailabilityConfig,
    pub storage: StorageConfig,
    pub transfer: TransferConfig,
    /// The settings of a group this node founds, written into the founding
    /// entry. Read only by `distlib run --found-group`; the defaults when
    /// absent.
    pub group: Option<GroupSettings>,
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
    /// Loopback by *default*, not by constraint: a node on a server or in a
    /// container has to be reachable from somewhere else, and that is a
    /// legitimate thing to configure. What loopback buys is that the default
    /// exposes nothing — the token is then the only thing between a caller and
    /// making this node propose membership changes as itself, and there is no
    /// TLS yet, so a non-loopback address wants a reverse proxy in front of it
    /// until there is.
    pub bind_addr: SocketAddr,

    /// The largest file the web UI may upload in one go, in bytes.
    ///
    /// A guard against a mistake — a wrong file picked, a disk about to fill —
    /// rather than against whoever holds the token, who can add any file on
    /// this machine by its path. Large enough for a long audiobook or a film.
    pub max_upload_bytes: u64,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 11280)),
            max_upload_bytes: 16_000_000_000,
        }
    }
}

/// The library as this node's user handles it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LibraryConfig {
    /// Where a download goes when whoever asked for it named nowhere — the
    /// web UI, which has no working directory to offer the way the CLI does.
    ///
    /// Relative to the data directory; an absolute path is used as it is.
    pub download_dir: PathBuf,
}

impl Default for LibraryConfig {
    fn default() -> Self {
        Self {
            download_dir: PathBuf::from("downloads"),
        }
    }
}

/// How this node tells the group it is online.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AvailabilityConfig {
    /// Seconds between this node's heartbeats in a group of up to fifty;
    /// stretched in proportion above that, and never longer than 20 minutes.
    /// Its peers count it offline after three missed. Never zero, which would
    /// beat without pause.
    pub beat_interval_secs: NonZeroU32,
}

impl Default for AvailabilityConfig {
    fn default() -> Self {
        Self {
            beat_interval_secs: NonZeroU32::new(60).expect("sixty is not zero"),
        }
    }
}

/// The blobs this node keeps on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Seconds between garbage collection passes, which delete every blob
    /// nothing keeps any more. Never zero, which would collect without pause.
    pub gc_interval_secs: NonZeroU32,
    /// The most disk space, in bytes, the blobs this node keeps may take: what
    /// it added, what it keeps for the group's custody and its cache, each
    /// blob counted once. Never shared with the group, and must be above this
    /// node's pledge. No limit when absent.
    pub limit_bytes: Option<NonZeroU64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            gc_interval_secs: NonZeroU32::new(10 * 60).expect("ten minutes is not zero"),
            limit_bytes: None,
        }
    }
}

/// How much this node moves over the network at once.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransferConfig {
    /// How many items custody fetches at once.
    pub max_fetches: NonZeroUsize,
    /// The most this node downloads, in bytes per second, across every
    /// download at once. Never below the group's `min_transfer_rate`: a lower
    /// value is raised to it. No limit when absent.
    pub max_download_rate: Option<NonZeroU64>,
    /// The most this node serves, in bytes per second, to everyone it serves
    /// at once. Never below the group's `min_transfer_rate`, the same as the
    /// download limit. No limit when absent. A sustained rate, not a hard
    /// cap: each request's first chunk, up to 16 KiB, goes out before the
    /// limit applies, so many small requests at once can go over it.
    pub max_upload_rate: Option<NonZeroU64>,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            max_fetches: NonZeroUsize::new(10).expect("ten is not zero"),
            max_download_rate: None,
            max_upload_rate: None,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
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
            .ignore(&[DATA_DIR_KEY, LOG_KEY])
            .split(ENV_NESTED_SEPARATOR)
    }

    /// Renders a starter config file.
    ///
    /// Written out by hand rather than through a serialiser so the file carries
    /// the comments that explain each field — the point of a starter file.
    #[expect(
        clippy::too_many_lines,
        reason = "the starter file as a template: its length is the file's"
    )]
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
             # The core group: the members who vote on the membership log. If this\n\
             # node is one of them it votes; if not, it follows them. Everything\n\
             # else about membership comes from the log itself — this is the one\n\
             # thing that cannot, because reaching the log means connecting to\n\
             # these nodes first.\n\
             #\n\
             # `distlib join <ticket>` fills this in for a node joining a group\n\
             # that already exists.\n\
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
             # the web UI talk to. The token in <data-dir>/api.token is the only\n\
             # thing guarding it, and there is no TLS yet — so if you move this\n\
             # off 127.0.0.1, to reach a node on a server or in a container, put\n\
             # a reverse proxy in front of it.\n\
             enabled = {api_enabled}\n\
             bind_addr = \"{api_bind}\"\n\
             \n\
             # The largest file the web UI may upload at once, in bytes. Files\n\
             # added by path with `distlib add` are not limited.\n\
             max_upload_bytes = {max_upload}\n\
             \n\
             [library]\n\
             # Where a download started from the web UI writes its files, created\n\
             # when first needed. Relative to the data directory, or absolute.\n\
             # `distlib download` writes to the working directory, or --dest.\n\
             download_dir = \"{download_dir}\"\n\
             \n\
             [availability]\n\
             # Seconds between the heartbeats that tell the group this node is\n\
             # online, stretched in groups of more than fifty, to at most 20\n\
             # minutes. Peers count the node offline after three missed.\n\
             beat_interval_secs = {beat_interval}\n\
             \n\
             [storage]\n\
             # Seconds between passes that delete the blobs nothing keeps any\n\
             # more. What you added, and what you downloaded, is kept.\n\
             gc_interval_secs = {gc_interval}\n\
             \n\
             # The most disk space, in bytes, distlib may use on this machine:\n\
             # what you added, what you keep for the group and the cache, each\n\
             # blob counted once. Must be above your pledge. No limit when unset.\n\
             {limit_bytes}\n\
             \n\
             [transfer]\n\
             # How many items this node fetches at once for the group's custody.\n\
             max_fetches = {max_fetches}\n\
             \n\
             # The most this node downloads, in bytes per second, all downloads\n\
             # together. Never below the group's min_transfer_rate, which a lower\n\
             # value is raised to. No limit when unset.\n\
             {max_download_rate}\n\
             \n\
             # The most this node serves to the group, in bytes per second, to\n\
             # everyone at once. Never below min_transfer_rate either. No limit\n\
             # when unset. A sustained rate, not a hard cap: many small requests\n\
             # at once can go over it.\n\
             {max_upload_rate}\n\
             \n\
             {group}",
            bind = self.net.bind_addr_v4,
            relay_mode = self.net.relay_mode.as_str(),
            relay_urls = quoted(&self.net.relay_urls),
            api_enabled = self.api.enabled,
            api_bind = self.api.bind_addr,
            max_upload = self.api.max_upload_bytes,
            download_dir = self.library.download_dir.display(),
            beat_interval = self.availability.beat_interval_secs,
            gc_interval = self.storage.gc_interval_secs,
            limit_bytes =
                optional_setting("limit_bytes", self.storage.limit_bytes, 100_000_000_000),
            max_fetches = self.transfer.max_fetches,
            max_download_rate = optional_setting(
                "max_download_rate",
                self.transfer.max_download_rate,
                10_000_000
            ),
            max_upload_rate =
                optional_setting("max_upload_rate", self.transfer.max_upload_rate, 10_000_000),
            group = group_section(self.group.as_ref()),
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

/// `key = value`, or `example` commented out when there is no value.
fn optional_setting(key: &str, value: Option<NonZeroU64>, example: u64) -> String {
    match value {
        Some(value) => format!("{key} = {value}"),
        None => format!("# {key} = {example}"),
    }
}

/// The `[group]` section: `group`'s values, or the defaults commented out
/// when there is none, so the file shows what a founder may set.
fn group_section(group: Option<&GroupSettings>) -> String {
    let (settings, prefix) = match group {
        Some(settings) => (settings.clone(), ""),
        None => (GroupSettings::default(), "# "),
    };
    let GroupSettings {
        replicas,
        cap_bytes,
        weight_step_bytes,
        min_pledge_bytes,
        pending_expiry,
        settle_secs,
        max_settle_secs,
        emergency_after_secs,
        min_transfer_rate,
        probes,
        probe_every_secs,
        failure_window_secs,
    } = settings;
    let values = [
        format!("replicas = {replicas}"),
        format!("cap_bytes = {cap_bytes}"),
        format!("weight_step_bytes = {weight_step_bytes}"),
        format!("min_pledge_bytes = {min_pledge_bytes}"),
        format!("pending_expiry = {pending_expiry}"),
        format!("settle_secs = {settle_secs}"),
        format!("max_settle_secs = {max_settle_secs}"),
        format!("emergency_after_secs = {emergency_after_secs}"),
        format!("min_transfer_rate = {min_transfer_rate}"),
        format!("probes = {probes}"),
        format!("probe_every_secs = {probe_every_secs}"),
        format!("failure_window_secs = {failure_window_secs}"),
    ];
    let mut section = String::from(
        "# The settings of a group this node founds, written into the founding\n\
         # entry by `distlib run --found-group` and fixed from then on: a node\n\
         # that joins a group, or has founded one already, ignores them.\n",
    );
    section.push_str(&format!("{prefix}[group]\n"));
    for value in values {
        section.push_str(&format!("{prefix}{value}\n"));
    }
    section
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
