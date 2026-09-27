//! The node's web UI (§7.3): its files, and how they are served.
//!
//! A crate of its own so that the UI can grow — more pages, more static
//! files, its own npm project under `web/` — without `distlib-api` growing
//! with it, and so that a change to the UI rebuilds this crate and not the
//! API's. It knows nothing of JSON-RPC: `distlib-api` serves the API's routes
//! and hands every other request to [`page`], answering in its own envelope
//! whatever this does not take.
//!
//! **Open, where the API is not** (D3). Nothing here is about this node: the
//! HTML and the bundle are the same bytes for every group, and everything a
//! page shows it asks `/rpc` and `/events` for, with the token. A browser
//! cannot set a header on its first navigation, so gating these would mean a
//! cookie or a token in the path — both worse than what they would protect.
//!
//! **A single-page app**: a path that names no file, `/members` say, answers
//! with `index.html`, and the page routes itself. A path that looks like a
//! file — it has an extension — and is not one is not taken, so it is
//! answered as missing: a missing script handed HTML instead is a confusing
//! error in a browser's console, and a missing one reported as missing is
//! not.
//!
//! **Built or not, `cargo build` works** (D7). The files come from
//! `web/dist`, which `npm run build` writes and git ignores; when it is not
//! there, `index.html` is the committed `placeholder.html` saying how to build
//! it. A release build embeds what `web/dist` held when it was compiled; a
//! debug build reads the folder as it runs, so rebuilding the UI needs no
//! rebuild of the node.

use std::borrow::Cow;

use axum::{
    http::{
        HeaderMap, HeaderValue, Method,
        header::{CACHE_CONTROL, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
};
use rust_embed::Embed;

/// The built UI, if there is one.
///
/// `$CARGO_MANIFEST_DIR` rather than a relative path: a debug build resolves
/// a relative folder against wherever the binary is run from, which for a
/// node is anywhere at all.
#[derive(Embed)]
#[folder = "$CARGO_MANIFEST_DIR/web/dist/"]
#[allow_missing = true]
struct Built;

/// What `index.html` is when the UI was not built.
const PLACEHOLDER: &str = include_str!("../placeholder.html");

/// What the page may do, and nothing more.
///
/// The token sits in the page's session storage, so script injected into the
/// page is the way to steal it; this confines script, and everything the page
/// fetches, to this origin. Styles may be inline because Svelte sets them so,
/// and a style cannot read storage. `frame-ancestors 'none'` keeps another
/// site from framing the page to click through it.
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; style-src 'self' 'unsafe-inline'; \
     img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";

/// The UI's answer to a request for `path`: a file of the UI, or the page for
/// a path the page routes itself. `None` for anything else — another method
/// than `GET` or `HEAD`, or a file that is not there — which the caller
/// answers as missing, in whatever form its other refusals take.
pub fn page(method: &Method, path: &str) -> Option<Response> {
    if method != Method::GET && method != Method::HEAD {
        return None;
    }
    // A debug build reads files off disk by this path; rust-embed refuses one
    // that resolves outside the folder, and `distlib-api`'s tests check that
    // it does.
    let path = path.trim_start_matches('/');
    if let Some(file) = Built::get(path).filter(|_| !path.is_empty()) {
        return Some(respond(
            file.metadata.mimetype(),
            cache_for(path),
            file.data,
        ));
    }
    let last = path.rsplit('/').next().unwrap_or_default();
    if last.contains('.') {
        return None;
    }
    let index =
        Built::get("index.html").map_or(Cow::Borrowed(PLACEHOLDER.as_bytes()), |file| file.data);
    Some(respond("text/html; charset=utf-8", NO_CACHE, index))
}

/// `index.html` is asked for fresh every time, so a new build is picked up
/// by a reload; everything Vite writes under `assets/` has its content's hash
/// in its name, so it never changes and can be kept for ever.
fn cache_for(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        NO_CACHE
    }
}

const NO_CACHE: &str = "no-cache";

fn respond(content_type: &str, cache: &'static str, body: Cow<'static, [u8]>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static(cache));
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    // The token is in the fragment, which no browser sends anywhere; this is
    // for the rest of the address.
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    (headers, body.into_owned()).into_response()
}
