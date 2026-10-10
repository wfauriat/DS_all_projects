import { useMemo, useState } from "react";

import type { Candidate } from "@/api/candidates";
import type { TimeseriesPoint } from "@/api/timeseries";
import { Sparkline } from "@/components/Sparkline";
import { colorForCandidate } from "@/lib/colors";
import type { CandidateSelection } from "@/lib/url-state";
import { t } from "@/i18n";

interface LeaderboardTableProps {
  points: TimeseriesPoint[];
  candidates: Candidate[];
  /** null = no filter (include every candidate); [] = include none; list = those only. */
  selectedCandidateIds: CandidateSelection;
  fromDate: string;
  toDate: string;
}

type SortKey = "total" | "outlets" | "latest" | "name";
type SortDir = "asc" | "desc";

interface Row {
  candidate: Candidate;
  total: number;
  outletCount: number;
  latestDay: string | null;
  perDay: number[];
}

function enumerateDays(from: string, to: string): string[] {
  const out: string[] = [];
  const start = new Date(`${from}T00:00:00Z`);
  const end = new Date(`${to}T00:00:00Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start > end) return out;
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

function buildRows(
  points: TimeseriesPoint[],
  candidates: Candidate[],
  selection: CandidateSelection,
  fromDate: string,
  toDate: string,
): Row[] {
  if (selection !== null && selection.length === 0) return [];
  const allowed = selection === null ? null : new Set(selection);
  const dayAxis = enumerateDays(fromDate, toDate);
  const dayIndex = new Map(dayAxis.map((d, i) => [d, i]));

  // Bucket: candidate_id → { total, outlets:Set, perDay[], latestDay }
  const acc = new Map<number, { total: number; outlets: Set<number>; perDay: number[]; latest: string | null }>();
  for (const c of candidates) {
    if (allowed && !allowed.has(c.id)) continue;
    acc.set(c.id, { total: 0, outlets: new Set(), perDay: Array(dayAxis.length).fill(0), latest: null });
  }
  for (const p of points) {
    const bucket = acc.get(p.candidate_id);
    if (!bucket) continue;
    bucket.total += p.n_mentions;
    if (p.n_mentions > 0) bucket.outlets.add(p.source_id);
    const idx = dayIndex.get(p.day);
    if (idx !== undefined) bucket.perDay[idx] += p.n_mentions;
    if (p.n_mentions > 0 && (bucket.latest === null || p.day > bucket.latest)) {
      bucket.latest = p.day;
    }
  }
  const rows: Row[] = [];
  for (const c of candidates) {
    const bucket = acc.get(c.id);
    if (!bucket) continue;
    rows.push({
      candidate: c,
      total: bucket.total,
      outletCount: bucket.outlets.size,
      latestDay: bucket.latest,
      perDay: bucket.perDay,
    });
  }
  return rows;
}

function sortRows(rows: Row[], key: SortKey, dir: SortDir): Row[] {
  const mult = dir === "asc" ? 1 : -1;
  const cmp = (a: Row, b: Row): number => {
    switch (key) {
      case "name":
        return a.candidate.display_name.localeCompare(b.candidate.display_name, "fr") * mult;
      case "outlets":
        return (a.outletCount - b.outletCount) * mult;
      case "latest": {
        const av = a.latestDay ?? "";
        const bv = b.latestDay ?? "";
        if (av === bv) return 0;
        return (av < bv ? -1 : 1) * mult;
      }
      case "total":
      default:
        return (a.total - b.total) * mult;
    }
  };
  return [...rows].sort(cmp);
}

export function LeaderboardTable({
  points,
  candidates,
  selectedCandidateIds,
  fromDate,
  toDate,
}: LeaderboardTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("total");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const rows = useMemo(
    () => buildRows(points, candidates, selectedCandidateIds, fromDate, toDate),
    [points, candidates, selectedCandidateIds, fromDate, toDate],
  );
  const sorted = useMemo(() => sortRows(rows, sortKey, sortDir), [rows, sortKey, sortDir]);

  function toggleSort(next: SortKey) {
    if (next === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(next);
      setSortDir(next === "name" ? "asc" : "desc");
    }
  }

  if (sorted.length === 0) {
    return (
      <div className="flex h-40 items-center justify-center rounded-md border border-dashed border-slate-300 bg-white p-8 text-center text-sm text-slate-500">
        {t.chart.noData}
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border border-slate-200 bg-white shadow-sm">
      <table className="w-full text-sm">
        <thead className="border-b border-slate-200 bg-slate-50 text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th scope="col" className="px-3 py-2 text-right">#</th>
            <SortableHeader label={t.leaderboard.col.candidate} active={sortKey === "name"} dir={sortDir} onClick={() => toggleSort("name")} align="left" />
            <SortableHeader label={t.leaderboard.col.totalMentions} active={sortKey === "total"} dir={sortDir} onClick={() => toggleSort("total")} align="right" />
            <SortableHeader label={t.leaderboard.col.outlets} active={sortKey === "outlets"} dir={sortDir} onClick={() => toggleSort("outlets")} align="right" />
            <SortableHeader label={t.leaderboard.col.latest} active={sortKey === "latest"} dir={sortDir} onClick={() => toggleSort("latest")} align="left" />
            <th scope="col" className="px-3 py-2 text-left">{t.leaderboard.col.trend}</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr key={row.candidate.id} className="border-b border-slate-100 last:border-b-0 hover:bg-slate-50">
              <td className="px-3 py-2 text-right tabular-nums text-slate-400">{i + 1}</td>
              <td className="px-3 py-2">
                <div className="flex items-center gap-2">
                  <span
                    aria-hidden
                    className="inline-block h-2.5 w-2.5 rounded-full"
                    style={{ backgroundColor: colorForCandidate(row.candidate.id) }}
                  />
                  <span className={row.candidate.eligible ? "text-slate-900" : "text-slate-500"}>
                    {row.candidate.display_name}
                  </span>
                  {!row.candidate.eligible && (
                    <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-slate-500">
                      {t.leaderboard.ineligibleBadge}
                    </span>
                  )}
                  {row.candidate.party && (
                    <span className="ml-auto text-xs text-slate-400">{row.candidate.party}</span>
                  )}
                </div>
              </td>
              <td className="px-3 py-2 text-right tabular-nums font-medium text-slate-900">
                {row.total.toLocaleString("fr-FR")}
              </td>
              <td className="px-3 py-2 text-right tabular-nums text-slate-700">{row.outletCount}</td>
              <td className="px-3 py-2 tabular-nums text-slate-500">
                {row.latestDay ?? "—"}
              </td>
              <td className="px-3 py-2">
                <Sparkline
                  values={row.perDay}
                  color={colorForCandidate(row.candidate.id)}
                  ariaLabel={`${row.candidate.display_name}: tendance sur ${row.perDay.length} jours`}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

interface SortableHeaderProps {
  label: string;
  active: boolean;
  dir: SortDir;
  align: "left" | "right";
  onClick: () => void;
}

function SortableHeader({ label, active, dir, align, onClick }: SortableHeaderProps) {
  const arrow = active ? (dir === "asc" ? " ▲" : " ▼") : "";
  const alignClass = align === "right" ? "text-right" : "text-left";
  return (
    <th scope="col" className={`px-3 py-2 ${alignClass}`}>
      <button
        type="button"
        onClick={onClick}
        className={`inline-flex items-center gap-1 uppercase tracking-wider hover:text-slate-800 ${
          active ? "text-slate-800" : ""
        }`}
        aria-sort={active ? (dir === "asc" ? "ascending" : "descending") : "none"}
      >
        <span>{label}</span>
        <span aria-hidden className="text-slate-400">{arrow}</span>
      </button>
    </th>
  );
}
