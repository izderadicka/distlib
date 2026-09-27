import { describe, expect, it } from "vitest";

import { href, navigate, onLinkClick, onPopState, parse, type Route, router } from "./router.svelte";

const at = (address: string) => parse(new URL(address, "http://node.test"));

describe("reading the address", () => {
  it("reads the library, browsing, from the root", () => {
    expect(at("/")).toEqual({ page: "library", query: "", number: 1 });
  });

  it("reads a search and a page number", () => {
    expect(at("/?q=%C4%8Capek+mloci&page=3")).toEqual({ page: "library", query: "Čapek mloci", number: 3 });
  });

  it("reads a query without the space around it, and one of only space as none", () => {
    expect(at("/?q=+mloci+")).toEqual({ page: "library", query: "mloci", number: 1 });
    expect(at("/?q=+++")).toEqual({ page: "library", query: "", number: 1 });
  });

  it("takes a page number that is not a whole number from one up for the first", () => {
    for (const page of ["0", "-2", "2.5", "two", ""]) {
      expect(at(`/?page=${page}`)).toEqual({ page: "library", query: "", number: 1 });
    }
  });

  it("reads an item", () => {
    const id = "5e70b15a0c92500ec3c5ac1c02014271939be7721f4159d016b98ab63a0a81f0";
    expect(at(`/items/${id}`)).toEqual({ page: "item", id });
  });

  it("takes anything but an item id as the node writes one for no item", () => {
    for (const id of ["5E70".padEnd(64, "0"), "a".repeat(63), "a".repeat(65), "g".repeat(64), ""]) {
      expect(at(`/items/${id}`)).toEqual({ page: "missing" });
    }
    expect(at(`/items/${"a".repeat(64)}/files`)).toEqual({ page: "missing" });
  });

  it("reads the node page", () => {
    expect(at("/node")).toEqual({ page: "node" });
  });

  it("takes anything else for a page that does not exist", () => {
    for (const address of ["/nodes", "/node/", "/items"]) {
      expect(at(address)).toEqual({ page: "missing" });
    }
  });
});

describe("writing the address", () => {
  it("leaves out what is the default", () => {
    expect(href({ page: "library", query: "", number: 1 })).toBe("/");
    expect(href({ page: "node" })).toBe("/node");
    expect(href({ page: "item", id: "a".repeat(64) })).toBe(`/items/${"a".repeat(64)}`);
  });

  it("writes back what it reads, whatever the query holds", () => {
    const routes: Route[] = [
      { page: "library", query: "Čapek & syn?", number: 1 },
      { page: "library", query: "", number: 4 },
      { page: "library", query: 'title:"R.U.R." #2', number: 12 },
    ];
    for (const route of routes) {
      expect(at(href(route))).toEqual(route);
    }
  });
});

describe("moving between pages", () => {
  it("goes to a page as a new history entry", () => {
    const before = history.length;

    navigate("/node");

    expect(location.pathname).toBe("/node");
    expect(history.length).toBe(before + 1);
    expect(router.route).toEqual({ page: "node" });
  });

  it("follows back and forward", () => {
    navigate("/node");
    history.replaceState(null, "", "/?q=mloci");

    onPopState();

    expect(router.route).toEqual({ page: "library", query: "mloci", number: 1 });
  });
});

describe("clicking a link", () => {
  /** Clicks `link`, and says whether the page took the click as its own. */
  function click(link: HTMLAnchorElement, init: MouseEventInit = {}): boolean {
    document.body.append(link);
    const event = new MouseEvent("click", { bubbles: true, cancelable: true, button: 0, ...init });
    link.dispatchEvent(event);
    onLinkClick(event);
    link.remove();
    return event.defaultPrevented;
  }

  function link(address: string, attributes: Record<string, string> = {}): HTMLAnchorElement {
    const element = document.createElement("a");
    element.href = address;
    for (const [name, value] of Object.entries(attributes)) {
      element.setAttribute(name, value);
    }
    return element;
  }

  it("takes a plain click on one of its own links as navigation", () => {
    expect(click(link("/node"))).toBe(true);
    expect(location.pathname).toBe("/node");
    expect(router.route).toEqual({ page: "node" });
  });

  it("follows a click on something inside the link", () => {
    const outer = link("/?q=mloci");
    const inner = document.createElement("span");
    outer.append(inner);
    document.body.append(outer);
    const event = new MouseEvent("click", { bubbles: true, cancelable: true, button: 0 });
    inner.dispatchEvent(event);

    onLinkClick(event);

    outer.remove();
    expect(event.defaultPrevented).toBe(true);
    expect(router.route).toEqual({ page: "library", query: "mloci", number: 1 });
  });

  it("leaves the browser the clicks that are its own", () => {
    const leftAlone: Array<[string, HTMLAnchorElement, MouseEventInit?]> = [
      ["to another site", link("https://example.com/node")],
      ["into a new tab", link("/node", { target: "_blank" })],
      ["as a download", link("/node", { download: "" })],
      ["with ctrl", link("/node"), { ctrlKey: true }],
      ["with meta", link("/node"), { metaKey: true }],
      ["with shift", link("/node"), { shiftKey: true }],
      ["with alt", link("/node"), { altKey: true }],
      ["with the middle button", link("/node"), { button: 1 }],
    ];
    for (const [why, element, init] of leftAlone) {
      expect(click(element, init), why).toBe(false);
      expect(location.pathname, why).toBe("/");
    }
  });

  it("leaves alone a click something else has already handled", () => {
    const element = link("/node");
    element.addEventListener("click", (event) => event.preventDefault());
    document.body.append(element);
    const event = new MouseEvent("click", { bubbles: true, cancelable: true, button: 0 });
    element.dispatchEvent(event);

    onLinkClick(event);

    element.remove();
    expect(location.pathname).toBe("/");
  });
});
