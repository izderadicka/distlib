// How the pages write numbers a person reads.

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

/** A moment the read model records, in microseconds since the epoch. */
export function instant(micros: number): Date {
  return new Date(micros / 1000);
}
