/** Political-lean ordering used to sort candidates left-to-right.
 *
 *  Matches the `lean` values used in `seeds/candidates.yaml`. Anything unknown
 *  (or null) sorts after the known values so it doesn't silently land mid-spectrum. */

const LEAN_ORDER: Record<string, number> = {
  "hard-left": 0,
  left: 1,
  "centre-left": 2,
  centre: 3,
  "centre-right": 4,
  right: 5,
  "hard-right": 6,
};

const UNKNOWN_RANK = 99;

function rank(lean: string | null): number {
  if (lean === null) return UNKNOWN_RANK;
  return LEAN_ORDER[lean] ?? UNKNOWN_RANK;
}

export function compareByLean<T extends { lean: string | null; display_name: string }>(
  a: T,
  b: T,
): number {
  const diff = rank(a.lean) - rank(b.lean);
  if (diff !== 0) return diff;
  return a.display_name.localeCompare(b.display_name, "fr");
}

export function compareSourcesByLean<T extends { lean: string | null; outlet: string }>(
  a: T,
  b: T,
): number {
  const diff = rank(a.lean) - rank(b.lean);
  if (diff !== 0) return diff;
  return a.outlet.localeCompare(b.outlet, "fr");
}
