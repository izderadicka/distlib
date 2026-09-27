import { describe, expect, it } from "vitest";

import { adoptToken, forgetToken, token } from "./token";

describe("the token handoff", () => {
  it("takes the token out of the address bar and into the tab", () => {
    history.replaceState(null, "", "/members?view=all#token=abc123");

    adoptToken();

    expect(token()).toBe("abc123");
    // Gone from the address — and so from history and screenshots — with
    // the rest of the address left as it was.
    expect(location.hash).toBe("");
    expect(location.pathname).toBe("/members");
    expect(location.search).toBe("?view=all");
  });

  it("leaves the tab alone when the address carries no token", () => {
    sessionStorage.setItem("distlib.token", "held");
    history.replaceState(null, "", "/#section");

    adoptToken();

    expect(token()).toBe("held");
    expect(location.hash).toBe("#section");
  });

  it("forgets a token the node refused", () => {
    history.replaceState(null, "", "/#token=abc123");
    adoptToken();

    forgetToken();

    expect(token()).toBeNull();
  });
});
