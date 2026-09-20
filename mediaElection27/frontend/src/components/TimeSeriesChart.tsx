import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

import type { Candidate } from "@/api/candidates";
import type { TimeseriesPoint } from "@/api/timeseries";
import { colorForCandidate } from "@/lib/colors";
import type { CandidateSelection } from "@/lib/url-state";
import { t } from "@/i18n";

interface TimeSeriesChartProps {
  points: TimeseriesPoint[];
  candidates: Candidate[];
  /** null = no filter (show every candidate); [] = explicit empty (show none); list = those only. */
  selectedCandidateIds: CandidateSelection;
  /** Optional override for the chart heading. Defaults to the generic "Mentions par jour". */
  title?: string;
}

/** Roll up (day, candidate, source) buckets into (day, candidate) lines —
 *  the chart sums across sources. The frontend stays sources-agnostic for v1;
 *  per-source breakdown is a future drilldown. */
function rollupByCandidate(
  points: TimeseriesPoint[],
  selectedCandidateIds: CandidateSelection,
): Map<number, Map<string, number>> {
  const byCandidate = new Map<number, Map<string, number>>();
  if (selectedCandidateIds !== null && selectedCandidateIds.length === 0) {
    return byCandidate; // explicit empty → render nothing
  }
  const allowed = selectedCandidateIds === null ? null : new Set(selectedCandidateIds);
  for (const p of points) {
    if (allowed && !allowed.has(p.candidate_id)) continue;
    const series = byCandidate.get(p.candidate_id) ?? new Map<string, number>();
    series.set(p.day, (series.get(p.day) ?? 0) + p.n_mentions);
    byCandidate.set(p.candidate_id, series);
  }
  return byCandidate;
}

export function TimeSeriesChart({
  points,
  candidates,
  selectedCandidateIds,
  title,
}: TimeSeriesChartProps) {
  const rolled = useMemo(
    () => rollupByCandidate(points, selectedCandidateIds),
    [points, selectedCandidateIds],
  );

  const option = useMemo<EChartsOption>(() => {
    const candidateById = new Map(candidates.map((c) => [c.id, c]));

    // Build the union of x-axis days across all visible candidates.
    const allDays = new Set<string>();
    for (const series of rolled.values()) for (const day of series.keys()) allDays.add(day);
    const sortedDays = [...allDays].sort();

    const series = [...rolled.entries()].map(([candidateId, daySeries]) => {
      const c = candidateById.get(candidateId);
      const data = sortedDays.map((d) => daySeries.get(d) ?? 0);
      return {
        name: c?.display_name ?? `id ${candidateId}`,
        type: "line" as const,
        smooth: false,
        symbol: "circle",
        symbolSize: 6,
        showSymbol: data.length <= 60, // hide markers when the x-axis is dense
        emphasis: { focus: "series" as const },
        itemStyle: { color: colorForCandidate(candidateId) },
        data,
      };
    });

    return {
      animation: false,
      grid: { left: 48, right: 16, top: 40, bottom: 60 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "line" },
      },
      legend: {
        type: "scroll",
        bottom: 0,
        textStyle: { fontSize: 12 },
      },
      xAxis: {
        type: "category",
        data: sortedDays,
        boundaryGap: false,
        axisLabel: { fontSize: 11 },
      },
      yAxis: {
        type: "value",
        name: t.chart.yAxisLabel,
        nameTextStyle: { fontSize: 11, color: "#64748b" },
        axisLabel: { fontSize: 11 },
        minInterval: 1,
      },
      series,
    };
  }, [rolled, candidates]);

  if (rolled.size === 0) {
    return (
      <div className="flex h-80 items-center justify-center rounded-md border border-dashed border-slate-300 bg-white p-8 text-center text-sm text-slate-500">
        {t.chart.noData}
      </div>
    );
  }

  return (
    <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="mb-2 text-sm font-semibold text-slate-700">{title ?? t.chart.title}</h2>
      <ReactECharts option={option} style={{ height: 420 }} notMerge lazyUpdate />
    </div>
  );
}
