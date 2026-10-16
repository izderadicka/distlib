// How the pages write numbers a person reads.

import type { Availability, Ratings } from "./rpc";

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];

/** A size in decimal units, as a disk is sold: `1.5 GB`. */
export function bytes(count: number): string {
  let value = count;
  let unit = 0;
  while (value >= 1000 && unit < BYTE_UNITS.length - 1) {
    value /= 1000;
    unit += 1;
  }
  return `${Number.isInteger(value) ? value : value.toFixed(1)} ${BYTE_UNITS[unit]}`;
}

const DURATION_UNITS: [seconds: number, one: string, many: string][] = [
  [24 * 60 * 60, "day", "days"],
  [60 * 60, "hour", "hours"],
  [60, "minute", "minutes"],
  [1, "second", "seconds"],
];

/** A span of seconds in the largest unit it is a whole number of: `1 day`, `90 seconds`. */
export function duration(seconds: number): string {
  const [size, one, many] =
    DURATION_UNITS.find(([size]) => seconds >= size && seconds % size === 0) ?? DURATION_UNITS[3];
  const count = seconds / size;
  return `${count} ${count === 1 ? one : many}`;
}

/** A moment the read model records, in microseconds since the epoch. */
export function instant(micros: number): Date {
  return new Date(micros / 1000);
}

/**
 * Who has an item, in the words `distlib search` and `distlib item` use:
 * `held here, 2 online`, `none online` — and `online holders unknown` while
 * some online member's holdings have not arrived, since a count then could
 * be short.
 */
export function availability({ held, providers }: Availability): string {
  const online =
    providers === null ? "online holders unknown"
    : providers === 0 ? "none online"
    : `${providers} online`;
  return held ? `held here, ${online}` : online;
}

/** Current members' ratings in a phrase: `4.5 from 2 ratings`, `not rated yet`. */
export function ratingSummary({ average, count }: Ratings): string {
  if (average === null) {
    return "not rated yet";
  }
  return `${average.toFixed(1)} from ${count} ${count === 1 ? "rating" : "ratings"}`;
}

/**
 * Who a member is, to a person: `you` for this node's own, the name the
 * group knows them by, or the start of their id while their name is not
 * known here.
 */
export function memberName(member: string, names: ReadonlyMap<string, string>, me: string | null): string {
  if (member === me) {
    return "you";
  }
  return names.get(member) || `${member.slice(0, 8)}…`;
}
