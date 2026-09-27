// The API token, as this tab holds it (phase 3's D3).
//
// `distlib ui` prints a link ending `#token=…`. A fragment is never sent to a
// server — not in a request, a log or a `Referer` — so the link carries the
// token to this page and nowhere else. The page moves it into session
// storage, which lasts as long as the tab and is not shared with other tabs,
// and takes it out of the address bar, so it is not in history or in a
// screenshot of the window.

const KEY = "distlib.token";

/** Moves a token in the address bar's fragment into this tab's storage. */
export function adoptToken(): void {
  const token = new URLSearchParams(location.hash.slice(1)).get("token");
  if (token) {
    sessionStorage.setItem(KEY, token);
    history.replaceState(history.state, "", location.pathname + location.search);
  }
}

/** The token this tab holds, if it holds one. */
export function token(): string | null {
  return sessionStorage.getItem(KEY);
}

/** Forgets the token: the node refused it, so it is of no further use. */
export function forgetToken(): void {
  sessionStorage.removeItem(KEY);
}
