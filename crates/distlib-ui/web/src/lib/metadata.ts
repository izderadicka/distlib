// What a person says about an item, as a form holds it and as the node
// takes it.

import type { ItemSummary, Series } from "./rpc";

export type Kind = NonNullable<ItemSummary["kind"]>;

export const KINDS: readonly Kind[] = ["ebook", "audiobook", "video", "other"];

/**
 * The form's fields as they are typed. Authors and genres are one to a line:
 * a name can have a comma in it ("Tolkien, J. R. R."), a line break cannot.
 */
export interface Draft {
  kind: Kind;
  title: string;
  authors: string;
  genres: string;
  series: string;
  seriesIndex: number | null;
  year: number | null;
  lang: string;
  description: string;
}

/** The metadata fields `library.add` and `library.edit_metadata` take. */
export interface Fields {
  kind?: Kind;
  title?: string;
  authors?: string[];
  genres?: string[];
  series?: Series;
  year?: number;
  lang?: string;
  description?: string;
}

export function emptyDraft(): Draft {
  return {
    kind: "ebook",
    title: "",
    authors: "",
    genres: "",
    series: "",
    seriesIndex: null,
    year: null,
    lang: "",
    description: "",
  };
}

/** Everything filled in, and nothing that was left empty. */
export function fields(draft: Draft): Fields {
  const text = (value: string) => value.trim() || undefined;
  const lines = (value: string) => {
    const each = value
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
    return each.length > 0 ? each : undefined;
  };
  const series = text(draft.series);
  return {
    kind: draft.kind,
    title: text(draft.title),
    authors: lines(draft.authors),
    genres: lines(draft.genres),
    series:
      series === undefined
        ? undefined
        : draft.seriesIndex === null
          ? { name: series }
          : { name: series, index: draft.seriesIndex },
    year: draft.year ?? undefined,
    lang: text(draft.lang),
    description: draft.description.trim() || undefined,
  };
}
