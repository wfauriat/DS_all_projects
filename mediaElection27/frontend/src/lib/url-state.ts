/** Helpers for reading/writing dashboard filter state in the URL search params.
 *
 *  We keep filter state in the URL so refreshing preserves it and sharing a
 *  link reproduces the view. Each filter component reads what it needs from
 *  useSearchParams and writes back via setSearchParams. */

import { format, subDays } from "date-fns";

export const DEFAULT_WINDOW_DAYS = 30;

export function parseIntList(value: string | null): number[] {
  if (!value) return [];
  return value
    .split(",")
    .map((v) => Number.parseInt(v, 10))
    .filter((n) => Number.isFinite(n));
}

export function serializeIntList(values: number[]): string {
  return values.join(",");
}

/** Tri-state candidate selection encoded in the URL.
 *
 *  null              — param missing, show ALL (default)
 *  []                — param present but empty, show NONE
 *  [2, 14, ...]      — param present with ids, show those
 */
export type CandidateSelection = number[] | null;

export function parseCandidateSelection(rawValue: string | null): CandidateSelection {
  if (rawValue === null) return null; // missing → "all"
  return parseIntList(rawValue); // present (possibly empty) → explicit list
}

export function writeCandidateSelection(
  params: URLSearchParams,
  selection: CandidateSelection,
): void {
  if (selection === null) {
    params.delete("candidates");
  } else if (selection.length === 0) {
    params.set("candidates", ""); // empty string is the "show none" sentinel
  } else {
    params.set("candidates", serializeIntList(selection));
  }
}

export function parseSourceId(value: string | null): number | null {
  if (!value) return null;
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : null;
}

export function writeSourceId(params: URLSearchParams, id: number | null): void {
  if (id === null) params.delete("source");
  else params.set("source", String(id));
}

export function parseOffset(value: string | null): number {
  if (!value) return 0;
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n) || n < 0) return 0;
  return n;
}

export function writeOffset(params: URLSearchParams, offset: number): void {
  if (offset <= 0) params.delete("offset");
  else params.set("offset", String(offset));
}

export function parseISODate(value: string | null): string | null {
  if (!value) return null;
  // YYYY-MM-DD strict
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  return null;
}

export function defaultFrom(): string {
  return format(subDays(new Date(), DEFAULT_WINDOW_DAYS), "yyyy-MM-dd");
}

export function defaultTo(): string {
  return format(new Date(), "yyyy-MM-dd");
}
