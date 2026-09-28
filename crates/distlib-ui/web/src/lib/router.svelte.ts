// Which page the address bar asks for, kept in step with it.
//
// **Real paths, not `#/…`**: the fragment is where `distlib ui`'s link
// carries the token (D3), and the node answers any path without an extension
// with the page (`distlib_ui::page`), so `/node` or `/?q=…` reloaded or
// shared lands where it was. A click on one of the page's own links is
// turned into a `pushState` here rather than a request; back and forward are
// followed through `popstate`.

/** A page, and what in the address says what it shows. */
export type Route =
  /** Browsing, or with `query`, searching; `number` counts from one. */
  | { page: "library"; query: string; number: number }
  | { page: "item"; id: string }
  | { page: "add" }
  | { page: "node" }
  | { page: "missing" };

/** An item id as the node writes one: 32 bytes of lower-case hex. */
const ITEM = /^\/items\/([0-9a-f]{64})$/;

/** Reads the route from an address. Anything unrecognised is `missing`. */
export function parse(url: URL): Route {
  switch (url.pathname) {
    case "/": {
      const number = Number(url.searchParams.get("page"));
      return {
        page: "library",
        query: url.searchParams.get("q")?.trim() ?? "",
        // A page number that is not a whole number from one up is the first.
        number: Number.isInteger(number) && number >= 1 ? number : 1,
      };
    }
    case "/add":
      return { page: "add" };
    case "/node":
      return { page: "node" };
    default: {
      const item = ITEM.exec(url.pathname);
      return item ? { page: "item", id: item[1] } : { page: "missing" };
    }
  }
}

/** The address of a route, leaving out what is its default. */
export function href(route: Route): string {
  switch (route.page) {
    case "library": {
      const search = new URLSearchParams();
      if (route.query) {
        search.set("q", route.query);
      }
      if (route.number > 1) {
        search.set("page", String(route.number));
      }
      const query = search.toString();
      return query ? `/?${query}` : "/";
    }
    case "item":
      return `/items/${route.id}`;
    case "add":
      return "/add";
    case "node":
      return "/node";
    case "missing":
      return location.pathname;
  }
}

const here = () => parse(new URL(location.href));

/** The route the address bar is at. */
export const router = $state({ route: here() });

/** Goes to `to`, a path on this page's origin, as a new history entry. */
export function navigate(to: string): void {
  history.pushState(null, "", to);
  router.route = here();
}

/** Follows the address bar after back or forward. */
export function onPopState(): void {
  router.route = here();
}

/**
 * Takes a click on a link to this page's origin as navigation within the
 * page. A click the browser should have — with a modifier, a button other
 * than the first, on a link to elsewhere, to a new tab or a download — is
 * left alone.
 */
export function onLinkClick(event: MouseEvent): void {
  if (
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey ||
    !(event.target instanceof Element)
  ) {
    return;
  }
  const link = event.target.closest<HTMLAnchorElement>("a[href]");
  if (link === null || link.target || link.hasAttribute("download")) {
    return;
  }
  const url = new URL(link.href);
  if (url.origin !== location.origin) {
    return;
  }
  event.preventDefault();
  navigate(url.pathname + url.search);
}
