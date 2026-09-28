// What a person says about an item, as a form holds it and as the node
// takes it.

import type { ItemRecord, ItemSummary, Series } from "./rpc";

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

/**
 * A draft holding what `item` already says, for editing it. An item nobody
 * gave a type shows as `other`, which is only sent if it is changed.
 */
export function draftOf(item: ItemRecord): Draft {
  return {
    kind: item.kind ?? "other",
    title: item.title ?? "",
    authors: item.authors?.join("\n") ?? "",
    genres: item.genres?.join("\n") ?? "",
    series: item.series?.name ?? "",
    seriesIndex: item.series?.index ?? null,
    year: item.year,
    lang: item.lang ?? "",
    description: item.description ?? "",
  };
}

/** A field's name, as the node and a person both read it. */
export const LABELS: Record<keyof Fields, string> = {
  kind: "type",
  title: "title",
  authors: "authors",
  genres: "genres",
  series: "series",
  year: "year",
  lang: "language",
  description: "description",
};

/**
 * What an edit from `before` to `after` changes: the fields to write, and
 * the fields emptied — which the node cannot write, since the catalogue has
 * no way yet to say a field is empty rather than unsaid.
 */
export function changes(before: Draft, after: Draft): { changed: Fields; emptied: (keyof Fields)[] } {
  const was = fields(before);
  const now = fields(after);
  const changed: Fields = {};
  const emptied: (keyof Fields)[] = [];
  for (const key of Object.keys(LABELS) as (keyof Fields)[]) {
    if (JSON.stringify(was[key]) === JSON.stringify(now[key])) {
      continue;
    }
    if (now[key] === undefined) {
      emptied.push(key);
    } else {
      Object.assign(changed, { [key]: now[key] });
    }
  }
  return { changed, emptied };
}
