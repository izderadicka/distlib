//! The error type shared by everything in `distlib-core`.

use std::path::PathBuf;

use thiserror::Error;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, CoreError>;

/// Errors produced while loading identity, configuration or paths.
#[derive(Debug, Error)]
pub enum CoreError {
    /// A filesystem operation failed. `action` reads as a verb phrase so the
    /// message says what was being attempted, not merely that I/O failed.
    #[error("could not {action} {path}")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A string did not parse as the identifier it claimed to be.
    #[error("invalid {kind}: {value}")]
    InvalidId { kind: &'static str, value: String },

    /// Configuration could not be read or did not match the schema.
    #[error("invalid configuration")]
    Config(#[source] Box<figment::Error>),

    /// The data directory is chosen before the config file is read, so it
    /// cannot also be set inside it.
    #[error(
        "`data_dir` cannot be set in the config file {path}; \
         use --data-dir or the DISTLIB_DATA_DIR environment variable"
    )]
    DataDirInConfig { path: PathBuf },

    /// No platform data directory could be determined and none was given.
    #[error("could not determine a default data directory; pass --data-dir")]
    NoDataDir,

    /// A string that was meant to be a join ticket is not one.
    #[error("that is not a valid join ticket: {reason}")]
    MalformedTicket { reason: String },

    /// The API token file exists but holds nothing.
    #[error("api token {path} is empty; delete it and restart to generate a new one")]
    EmptyToken { path: PathBuf },

    /// The operating system would not provide randomness.
    #[error("could not generate a random token: {message}")]
    Random { message: String },

    /// A file that must be private is readable by other users.
    #[error("{path} is accessible to other users (mode {mode:04o}); expected 0600")]
    NotPrivate { path: PathBuf, mode: u32 },

    /// The secret key file is not a bare 32-byte ed25519 key.
    #[error("secret key {path} is {len} bytes; expected 32")]
    MalformedKey { path: PathBuf, len: usize },

    /// A secret key file already exists and overwriting was not requested.
    #[error("secret key {path} already exists; pass --force to replace it")]
    KeyExists { path: PathBuf },
}

impl CoreError {
    /// Attaches the path and the attempted action to an [`std::io::Error`].
    ///
    /// `std::io::Error` carries no path, so bare `?` propagation would produce
    /// "permission denied" with nothing to act on.
    pub(crate) fn io(
        action: &'static str,
        path: impl Into<PathBuf>,
    ) -> impl FnOnce(std::io::Error) -> Self {
        move |source| Self::Io {
            action,
            path: path.into(),
            source,
        }
    }
}

// `figment::Error` is large enough to bloat every Result in the crate, so it is
// boxed. A manual `From` keeps `?` working at the call sites.
impl From<figment::Error> for CoreError {
    fn from(source: figment::Error) -> Self {
        Self::Config(Box::new(source))
    }
}
