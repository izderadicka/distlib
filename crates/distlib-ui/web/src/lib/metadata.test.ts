import { describe, expect, it } from "vitest";

import { emptyDraft, fields } from "./metadata";

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
