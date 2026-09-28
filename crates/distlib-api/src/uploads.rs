//! Files a browser sends, held until `library.add` takes them (phase 3's D5).
//!
//! A browser cannot name a path on the node's machine — a file picker hands a
//! page the bytes and a name, never where they live — so `library.add`'s
//! `files` are out of its reach. `POST /upload` streams one file's bytes into
//! `<data-dir>/uploads/<id>/<filename>`, and `library.add {uploads: [id]}`
//! then adds it exactly as it adds a path, through the same
//! `Catalogue::add_file`: one deduplication, two doors.
//!
//! **Inside the data directory**, on the blob store's own filesystem and under
//! its permissions, which a system temporary directory is not. **The bytes are
//! written twice**, once here and once into the store — the price D5 named for
//! not taking a second route into the store.
//!
//! **Nothing is left behind by a request that ends early.** An upload that is
//! refused, breaks off, or whose client goes away — which drops the handler
//! mid-stream — takes its directory with it, and `library.add` removes every
//! upload it was given whether it succeeds or fails. Both are drop guards
//! rather than code after an `await`, because a dropped future runs no more
//! of its own code. What can still be left is an upload nobody ever named to
//! `library.add` — a page closed between the two calls, or a call whose params
//! did not parse — and [`Uploads::open`] clears those when the node starts.

use std::{
    fmt,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    str::FromStr,
};

use axum::body::Body;
use data_encoding::HEXLOWER;
use futures_lite::StreamExt as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use tokio::io::AsyncWriteExt as _;

/// Names one upload. Random, and written as 32 lower-case hex digits — so a
/// name that parses as one can only ever be a single, plain path component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadId([u8; 16]);

impl UploadId {
    fn random() -> io::Result<Self> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&HEXLOWER.encode(&self.0))
    }
}

impl FromStr for UploadId {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        HEXLOWER
            .decode(text.as_bytes())
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .map(Self)
            .ok_or_else(|| format!("{text:?} is not an upload id"))
    }
}

impl Serialize for UploadId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for UploadId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// What `POST /upload` answers with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Uploaded {
    pub upload: UploadId,
    pub filename: String,
    pub size: u64,
}

/// Why an upload was not taken.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error(
        "{0:?} is not a filename an item can hold: it has to be a plain name with an extension"
    )]
    Filename(String),
    #[error("larger than the {max} bytes this node takes in one upload (`[api] max_upload_bytes`)")]
    TooLarge { max: u64 },
    #[error("the upload broke off: {0}")]
    Broken(String),
    #[error("no such upload: {0}")]
    Unknown(UploadId),
    #[error("{}: {source}", path.display())]
    Io { path: PathBuf, source: io::Error },
}

/// Where uploads are held, and how large one may be.
#[derive(Debug, Clone)]
pub struct Uploads {
    dir: PathBuf,
    max_bytes: u64,
}

impl Uploads {
    /// Holds uploads in `dir`, first removing any a previous run left there.
    ///
    /// **Only what is named like an upload is removed.** `dir` is a directory
    /// of the node's own, but `[library] download_dir` is configured relative
    /// to the same data directory, and a node told to download into
    /// `uploads` would otherwise lose everything it downloaded at every
    /// start.
    pub fn open(dir: PathBuf, max_bytes: u64) -> io::Result<Self> {
        match std::fs::read_dir(&dir) {
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    let leftover = entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.parse::<UploadId>().is_ok());
                    if leftover {
                        std::fs::remove_dir_all(entry.path())?;
                    }
                }
            }
        }
        Ok(Self { dir, max_bytes })
    }

    /// The largest upload taken, in bytes.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Streams `body` to disk as `filename`, refusing it once it is larger
    /// than [`Self::max_bytes`].
    pub async fn receive(&self, filename: &str, body: Body) -> Result<Uploaded, UploadError> {
        if !is_plain(filename) {
            return Err(UploadError::Filename(filename.to_owned()));
        }
        let upload = UploadId::random().map_err(|source| UploadError::Io {
            path: self.dir.clone(),
            source,
        })?;
        let staged = Staged(self.dir.join(upload.to_string()));
        let path = staged.0.join(filename);
        let io = |source| UploadError::Io {
            path: path.clone(),
            source,
        };

        tokio::fs::create_dir_all(&staged.0).await.map_err(io)?;
        let mut file = tokio::fs::File::create(&path).await.map_err(io)?;
        let mut size: u64 = 0;
        let mut chunks = body.into_data_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|error| UploadError::Broken(error.to_string()))?;
            size += chunk.len() as u64;
            if size > self.max_bytes {
                return Err(UploadError::TooLarge {
                    max: self.max_bytes,
                });
            }
            file.write_all(&chunk).await.map_err(io)?;
        }
        file.flush().await.map_err(io)?;

        staged.keep();
        Ok(Uploaded {
            upload,
            filename: filename.to_owned(),
            size,
        })
    }

    /// The file `upload` holds, and a guard that removes it when dropped —
    /// for a caller about to use it once, successfully or not.
    pub async fn take(&self, upload: UploadId) -> Result<(PathBuf, Staged), UploadError> {
        let staged = Staged(self.dir.join(upload.to_string()));
        // No directory, an unreadable one and an empty one all mean the same
        // to the caller: there is no such upload to take.
        let unknown = || UploadError::Unknown(upload);
        let mut entries = tokio::fs::read_dir(&staged.0)
            .await
            .map_err(|_| unknown())?;
        let file = entries
            .next_entry()
            .await
            .map_err(|_| unknown())?
            .ok_or_else(unknown)?;
        Ok((file.path(), staged))
    }
}

/// An upload's directory, removed when this is dropped unless it is kept.
#[derive(Debug)]
pub struct Staged(PathBuf);

impl Staged {
    fn keep(mut self) {
        self.0 = PathBuf::new();
    }
}

impl Drop for Staged {
    /// **Blocking, and run on an async task** — `Drop` cannot await. Knowingly:
    /// it removes one directory holding one file, which is an unlink whatever
    /// the file's size, and a guard that handed the work to another task
    /// would no longer guarantee it had happened by the time the request
    /// that owned it was gone.
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            // Nothing to be done about a failure here, and the next start
            // clears whatever is left.
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// Whether `name` can be an item's filename: one plain path component, with
/// an extension, and nothing a platform a member may run cannot write.
///
/// **The extension is required** because it is the file's `format` — what
/// `library.add` records for it, and refuses a path without, so both doors
/// agree. **No dot at either end**: a leading one hides a file on Unix, and
/// Windows drops a trailing one, so the file written would not be the one
/// named. A trailing dot is also what `Path::extension` reads as an extension
/// that is there but empty. **No whitespace at either end** either: it cannot
/// be seen in a listing, and Windows drops trailing spaces as it drops dots.
fn is_plain(name: &str) -> bool {
    let path = Path::new(name);
    path.file_name().is_some_and(|plain| plain == name)
        && name.trim() == name
        && !name.starts_with('.')
        && !name.ends_with('.')
        && path.extension().is_some()
        && !name.contains(['\\', ':', '\0'])
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code: a panic on a broken invariant is the point

    use std::time::Duration;

    use axum::body::Bytes;
    use futures_lite::stream;
    use tempfile::TempDir;

    use super::*;

    const ID: &str = "0123456789abcdef0123456789abcdef";

    /// What `dir` holds.
    fn held(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A body that sends one piece and then never another, nor an end.
    fn stalled() -> Body {
        Body::from_stream(
            stream::once(Ok::<_, io::Error>(Bytes::from_static(b"the first part")))
                .chain(stream::pending()),
        )
    }

    #[tokio::test]
    async fn an_upload_whose_client_goes_away_leaves_nothing() {
        let dir = TempDir::new().unwrap();
        let uploads = Uploads::open(dir.path().to_owned(), 1_000).unwrap();

        // Dropped the way axum drops a handler whose client disconnected:
        // mid-stream, with no more of its own code to run.
        let receiving = uploads.receive("mloky.epub", stalled());
        assert!(
            tokio::time::timeout(Duration::from_millis(200), receiving)
                .await
                .is_err()
        );

        assert!(held(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn an_upload_that_breaks_off_leaves_nothing() {
        let dir = TempDir::new().unwrap();
        let uploads = Uploads::open(dir.path().to_owned(), 1_000).unwrap();
        let broken = Body::from_stream(stream::iter([
            Ok(Bytes::from_static(b"the first part")),
            Err(io::Error::other("connection reset")),
        ]));

        let error = uploads.receive("mloky.epub", broken).await.unwrap_err();

        assert!(matches!(error, UploadError::Broken(_)), "{error}");
        assert!(held(dir.path()).is_empty());
    }

    #[tokio::test]
    async fn an_upload_taken_is_gone_once_its_taker_is_done_with_it() {
        let dir = TempDir::new().unwrap();
        let uploads = Uploads::open(dir.path().to_owned(), 1_000).unwrap();
        let uploaded = uploads
            .receive("mloky.epub", Body::from("a book"))
            .await
            .unwrap();

        let (path, staged) = uploads.take(uploaded.upload).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"a book");
        assert!(path.ends_with("mloky.epub"));
        drop(staged);

        assert!(held(dir.path()).is_empty());
        assert!(matches!(
            uploads.take(uploaded.upload).await,
            Err(UploadError::Unknown(_))
        ));
    }

    #[test]
    fn starting_clears_what_a_previous_run_left_and_nothing_else() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join(ID)).unwrap();
        std::fs::write(dir.path().join(ID).join("mloky.epub"), b"left over").unwrap();
        // What a node told to download into this directory would have put
        // there.
        std::fs::write(dir.path().join("downloaded.epub"), b"somebody's").unwrap();
        std::fs::create_dir_all(dir.path().join("a folder")).unwrap();

        Uploads::open(dir.path().to_owned(), 1_000).unwrap();

        let mut left = held(dir.path());
        left.sort();
        assert_eq!(left, ["a folder", "downloaded.epub"]);
    }

    #[test]
    fn a_filename_is_one_plain_name_with_an_extension() {
        for name in [
            "mloky.epub",
            "Válka s mloky.epub",
            "chapter 01.mp3",
            "R.U.R.epub",
        ] {
            assert!(is_plain(name), "{name}");
        }
        for name in [
            "",
            "..",
            ".",
            "mloky",
            ".epub",
            ".hidden.epub",
            "mloky.",
            "mloky.epub.",
            " mloky.epub",
            "mloky.epub ",
            "\tmloky.epub",
            "mloky.epub\n",
            "mloky. ",
            "a/b.epub",
            "../b.epub",
            "a\\b.epub",
            "c:b.epub",
            "b\0.epub",
            "/b.epub",
        ] {
            assert!(!is_plain(name), "{name:?}");
        }
    }

    #[test]
    fn an_upload_id_is_32_lower_case_hex_digits_and_nothing_else() {
        let id = UploadId([0xab; 16]);
        assert_eq!(id.to_string(), "ab".repeat(16));
        assert_eq!(id.to_string().parse::<UploadId>(), Ok(id));
        for text in [
            "AB".repeat(16),
            "ab".repeat(15),
            "ab".repeat(17),
            format!("../{}", "ab".repeat(15)),
            String::new(),
        ] {
            assert!(text.parse::<UploadId>().is_err(), "{text}");
        }
    }
}
