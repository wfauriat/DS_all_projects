import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

import type { Candidate } from "@/api/candidates";
import type { TimeseriesPoint } from "@/api/timeseries";
import { colorForCandidate } from "@/lib/colors";
import { compareByLean } from "@/lib/lean";
import type { CandidateSelection } from "@/lib/url-state";
import { t } from "@/i18n";

interface ShareOfVoiceChartProps {
  points: TimeseriesPoint[];
  candidates: Candidate[];
  /** null = no filter (show every candidate); [] = explicit empty (show none); list = those only. */
  selectedCandidateIds: CandidateSelection;
  fromDate: string;
  toDate: string;
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

interface ShareDatum {
  value: number; // percent 0–100
  rawCount: number;
}

interface ShareTooltipParam {
  axisValue: string;
  seriesName: string;
  marker: string;
  data: ShareDatum;
}

export function ShareOfVoiceChart({
  points,
  candidates,
  selectedCandidateIds,
  fromDate,
  toDate,
}: ShareOfVoiceChartProps) {
  const { option, hasData } = useMemo(() => {
    const emptySelection = selectedCandidateIds !== null && selectedCandidateIds.length === 0;
    if (emptySelection) {
      return { option: {} as EChartsOption, hasData: false };
    }
    const allowed = selectedCandidateIds === null ? null : new Set(selectedCandidateIds);
    const visible = candidates.filter((c) => !allowed || allowed.has(c.id)).sort(compareByLean);

    const days = enumerateDays(fromDate, toDate);
    const dayIndex = new Map(days.map((d, i) => [d, i]));

    const rawByCandidate = new Map<number, number[]>();
    for (const c of visible) rawByCandidate.set(c.id, new Array(days.length).fill(0));
    for (const p of points) {
      const arr = rawByCandidate.get(p.candidate_id);
      if (!arr) continue;
      const i = dayIndex.get(p.day);
      if (i === undefined) continue;
      arr[i] += p.n_mentions;
    }
    const totalPerDay: number[] = new Array(days.length).fill(0);
    for (const arr of rawByCandidate.values()) {
      for (let i = 0; i < days.length; i += 1) totalPerDay[i] += arr[i];
    }
    const anyMention = totalPerDay.some((n) => n > 0);

    const series = visible.map((c) => {
      const arr = rawByCandidate.get(c.id) ?? [];
      const data: ShareDatum[] = arr.map((n, i) => {
        const total = totalPerDay[i];
        if (total === 0) return { value: 0, rawCount: 0 };
        return { value: (n / total) * 100, rawCount: n };
      });
      return {
        name: c.display_name,
        type: "line" as const,
        stack: "share",
        symbol: "none",
        emphasis: { focus: "series" as const },
        lineStyle: { width: 0 },
        areaStyle: { opacity: 0.85 },
        itemStyle: { color: colorForCandidate(c.id) },
        data,
      };
    });

    const builtOption: EChartsOption = {
      animation: false,
      grid: { left: 56, right: 16, top: 40, bottom: 60 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "line" },
        formatter: (paramsAny: unknown) => {
          const params = paramsAny as ShareTooltipParam[];
          if (!params || params.length === 0) return "";
          const day = params[0].axisValue;
          const i = dayIndex.get(day);
          const total = i === undefined ? 0 : totalPerDay[i];
          const header =
            `<div style="margin-bottom:4px;font-weight:600">${day}</div>` +
            `<div style="margin-bottom:6px;color:#64748b;font-size:11px">` +
            `${t.share.tooltipTotal(total)}</div>`;
          const rows = [...params]
            .sort((a, b) => b.data.value - a.data.value)
            .map((p) => {
              if (p.data.value === 0) return "";
              const pct = p.data.value.toFixed(1).replace(".", ",");
              return (
                `<div style="display:flex;align-items:center;gap:6px">` +
                `${p.marker}<span>${p.seriesName}</span>` +
                `<span style="margin-left:auto;font-variant-numeric:tabular-nums">` +
                `${pct}% (${p.data.rawCount})</span></div>`
              );
            })
            .filter((s) => s !== "")
            .join("");
          return header + rows;
        },
      },
      legend: { type: "scroll", bottom: 0, textStyle: { fontSize: 12 } },
      xAxis: {
        type: "category",
        data: days,
        boundaryGap: false,
        axisLabel: { fontSize: 11 },
      },
      yAxis: {
        type: "value",
        min: 0,
        max: 100,
        axisLabel: { fontSize: 11, formatter: "{value} %" },
      },
      series,
    };
    return { option: builtOption, hasData: anyMention };
  }, [points, candidates, selectedCandidateIds, fromDate, toDate]);

  if (!hasData) {
    return (
      <div className="flex h-80 items-center justify-center rounded-md border border-dashed border-slate-300 bg-white p-8 text-center text-sm text-slate-500">
        {t.chart.noData}
      </div>
    );
  }

  return (
    <div className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="mb-2 text-sm font-semibold text-slate-700">{t.share.title}</h2>
      <ReactECharts option={option} style={{ height: 460 }} notMerge lazyUpdate />
    </div>
  );
}
