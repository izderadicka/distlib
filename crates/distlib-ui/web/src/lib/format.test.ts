import { describe, expect, it } from "vitest";

import { availability, bytes, instant, memberName, ratingSummary } from "./format";

describe("writing a size", () => {
  it("uses the largest decimal unit it reaches, to one place", () => {
    expect(bytes(0)).toBe("0 B");
    expect(bytes(999)).toBe("999 B");
    expect(bytes(1000)).toBe("1 KB");
    expect(bytes(1_500_000_000)).toBe("1.5 GB");
  });

  it("stops at petabytes", () => {
    expect(bytes(2e18)).toBe("2000 PB");
  });
});

describe("reading a moment", () => {
  it("reads microseconds, as the read model keeps them", () => {
    // `last_modified` as a node answered it.
    expect(instant(1790521054348270).toISOString()).toBe("2026-09-27T14:57:34.348Z");
  });
});

describe("saying who has an item", () => {
  it("counts the online holders, and says when this node is one", () => {
    expect(availability({ held: true, providers: 2 })).toBe("held here, 2 online");
    expect(availability({ held: false, providers: 1 })).toBe("1 online");
    expect(availability({ held: false, providers: 0 })).toBe("none online");
  });

  it("does not count while anyone's holdings are unknown: the count could be short", () => {
    expect(availability({ held: true, providers: null })).toBe("held here, online holders unknown");
    expect(availability({ held: false, providers: null })).toBe("online holders unknown");
  });
});

describe("summing up ratings", () => {
  it("says how good, and from how many", () => {
    expect(ratingSummary({ average: 4.5, count: 2, mine: null })).toBe("4.5 from 2 ratings");
    expect(ratingSummary({ average: 4, count: 1, mine: 4 })).toBe("4.0 from 1 rating");
  });

  it("says when nobody has rated it", () => {
    expect(ratingSummary({ average: null, count: 0, mine: null })).toBe("not rated yet");
  });
});

describe("naming a member", () => {
  const names = new Map([["a".repeat(64), "alice"]]);

  it("calls this node's member you, and the others by name", () => {
    expect(memberName("a".repeat(64), names, "a".repeat(64))).toBe("you");
    expect(memberName("a".repeat(64), names, "b".repeat(64))).toBe("alice");
  });

  it("gives the start of the id while the name is not known", () => {
    expect(memberName("c".repeat(64), names, null)).toBe("cccccccc…");
  });
});
