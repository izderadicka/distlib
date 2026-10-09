import { describe, expect, it } from "vitest";

import { changes, draftOf, emptyDraft, fields } from "./metadata";
import type { ItemRecord } from "./rpc";

// As a node answers `library.item`.
const MLOCI: ItemRecord = {
  item_id: "5e70b15a0c92500ec3c5ac1c02014271939be7721f4159d016b98ab63a0a81f0",
  kind: "audiobook",
  title: "Válka s mloky",
  authors: ["Karel Čapek", "Josef Čapek"],
  genres: ["satire", "sci-fi"],
  series: { name: "Mloci", index: 1 },
  year: 1936,
  lang: null,
  description: null,
  replicas: null,
  files: {},
  last_modified: 1790521054348270,
  availability: { held: false, providers: 1 },
};

describe("what a form says about an item", () => {
  it("says only what was filled in", () => {
    expect(fields(emptyDraft())).toEqual({
      kind: "ebook",
      title: undefined,
      authors: undefined,
      genres: undefined,
      series: undefined,
      year: undefined,
      lang: undefined,
      description: undefined,
    });
  });

  it("takes names one to a line, commas and all, and no blank lines", () => {
    const said = fields({
      ...emptyDraft(),
      authors: "  Tolkien, J. R. R.\n\n Christopher Tolkien  \n",
      genres: "fantasy\n  \n",
    });
    expect(said.authors).toEqual(["Tolkien, J. R. R.", "Christopher Tolkien"]);
    expect(said.genres).toEqual(["fantasy"]);
  });

  it("trims what is typed, and keeps a description's own line breaks", () => {
    const said = fields({
      ...emptyDraft(),
      kind: "audiobook",
      title: "  Válka s mloky ",
      lang: " cs ",
      description: "\n Satira.\nO mlocích. \n",
    });
    expect(said).toMatchObject({
      kind: "audiobook",
      title: "Válka s mloky",
      lang: "cs",
      description: "Satira.\nO mlocích.",
    });
  });

  it("gives a series its place only when one is given", () => {
    expect(fields({ ...emptyDraft(), series: " Mloci ", seriesIndex: 2.5 }).series).toEqual({
      name: "Mloci",
      index: 2.5,
    });
    expect(fields({ ...emptyDraft(), series: "Mloci" }).series).toEqual({ name: "Mloci" });
    // A place in no series says nothing.
    expect(fields({ ...emptyDraft(), seriesIndex: 3 }).series).toBeUndefined();
  });

  it("keeps a year, even the year nought", () => {
    expect(fields({ ...emptyDraft(), year: 1936 }).year).toBe(1936);
    expect(fields({ ...emptyDraft(), year: 0 }).year).toBe(0);
  });
});

describe("a draft of what an item already says", () => {
  it("holds every field, lists one to a line", () => {
    expect(draftOf(MLOCI)).toEqual({
      kind: "audiobook",
      title: "Válka s mloky",
      authors: "Karel Čapek\nJosef Čapek",
      genres: "satire\nsci-fi",
      series: "Mloci",
      seriesIndex: 1,
      year: 1936,
      lang: "",
      description: "",
    });
  });

  it("says what the item says, read back", () => {
    const said = fields(draftOf(MLOCI));
    expect(said).toMatchObject({
      kind: MLOCI.kind,
      title: MLOCI.title,
      authors: MLOCI.authors,
      genres: MLOCI.genres,
      series: MLOCI.series,
      year: MLOCI.year,
    });
  });

  it("takes an item nobody gave a type for other", () => {
    expect(draftOf({ ...MLOCI, kind: null }).kind).toBe("other");
  });
});

describe("what an edit changes", () => {
  const before = draftOf(MLOCI);

  it("is nothing when nothing was touched", () => {
    expect(changes(before, { ...before })).toEqual({ changed: {}, emptied: [] });
  });

  it("is only the fields that differ, as they now are", () => {
    const after = { ...before, title: "War with the Newts", genres: "satire\nsci-fi\nclassic" };
    expect(changes(before, after)).toEqual({
      changed: { title: "War with the Newts", genres: ["satire", "sci-fi", "classic"] },
      emptied: [],
    });
  });

  it("is nothing for a change that reads the same once trimmed", () => {
    expect(changes(before, { ...before, title: " Válka s mloky  ", authors: "\nKarel Čapek\n Josef Čapek" })).toEqual({
      changed: {},
      emptied: [],
    });
  });

  it("is the whole series when only its place changes", () => {
    expect(changes(before, { ...before, seriesIndex: 2 }).changed).toEqual({ series: { name: "Mloci", index: 2 } });
    expect(changes(before, { ...before, seriesIndex: null }).changed).toEqual({ series: { name: "Mloci" } });
  });

  it("names the fields emptied, and does not count them as changed", () => {
    const after = { ...before, title: "  ", authors: "", year: null, lang: "cs" };
    expect(changes(before, after)).toEqual({ changed: { lang: "cs" }, emptied: ["title", "authors", "year"] });
  });

  it("counts a type changed from one nobody gave", () => {
    const untyped = draftOf({ ...MLOCI, kind: null });
    expect(changes(untyped, { ...untyped, kind: "ebook" }).changed).toEqual({ kind: "ebook" });
  });
});
