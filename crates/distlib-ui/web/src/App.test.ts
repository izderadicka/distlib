import { render, screen } from "@testing-library/svelte";
import { describe, expect, it, vi } from "vitest";

import App from "./App.svelte";
import { type Watcher, watch } from "./lib/events";

vi.mock("./lib/events", () => ({ watch: vi.fn(() => () => {}) }));
vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(async () => ({ results: [], total: 0 })),
}));

describe("the app", () => {
  it("says how to sign in when the tab holds no token", () => {
    render(App);

    expect(screen.getByText("Not signed in")).toBeTruthy();
    expect(screen.getByText("distlib ui")).toBeTruthy();
    expect(watch).not.toHaveBeenCalled();
  });

  it("shows the node when the tab holds a token", () => {
    sessionStorage.setItem("distlib.token", "secret");

    render(App);

    expect(screen.queryByText("Not signed in")).toBeNull();
    expect(watch).toHaveBeenCalled();
  });

  it("goes back to signing in when the node refuses the token", async () => {
    sessionStorage.setItem("distlib.token", "secret");
    let watcher!: Watcher;
    vi.mocked(watch).mockImplementation((given) => {
      watcher = given;
      return () => {};
    });
    render(App);

    watcher.onUnauthorised();

    await screen.findByText("Not signed in");
  });
});
