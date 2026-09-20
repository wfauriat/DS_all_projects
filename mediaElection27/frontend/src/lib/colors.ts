/** Stable color assignment per candidate id.
 *
 *  Picked from a categorical-distinct palette (Tableau 10 + a few extras) so
 *  adjacent candidates stay visually separable on the chart. Mapping is
 *  by id for stability across reloads / filter changes; if a new candidate
 *  joins after id 14, they cycle back into the palette. */

const PALETTE = [
  "#4e79a7",
  "#f28e2b",
  "#e15759",
  "#76b7b2",
  "#59a14f",
  "#edc948",
  "#b07aa1",
  "#ff9da7",
  "#9c755f",
  "#bab0ac",
  "#1f77b4",
  "#ff7f0e",
  "#2ca02c",
  "#d62728",
  "#9467bd",
  "#8c564b",
];

export function colorForCandidate(id: number): string {
  return PALETTE[(id - 1) % PALETTE.length] ?? PALETTE[0];
}
