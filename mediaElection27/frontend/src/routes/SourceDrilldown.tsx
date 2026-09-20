import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useCandidates } from "@/api/candidates";
import { useSources } from "@/api/sources";
import { useTimeseries } from "@/api/timeseries";
import { CandidateFilter } from "@/components/CandidateFilter";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { Layout } from "@/components/Layout";
import { SourceFilter } from "@/components/SourceFilter";
import { Spinner } from "@/components/Spinner";
import { TimeSeriesChart } from "@/components/TimeSeriesChart";
import { compareSourcesByLean } from "@/lib/lean";
import {
  defaultFrom,
  defaultTo,
  parseCandidateSelection,
  parseISODate,
  parseSourceId,
} from "@/lib/url-state";
import { t } from "@/i18n";

export default function SourceDrilldown() {
  const [searchParams] = useSearchParams();

  const candidatesQuery = useCandidates();
  const sourcesQuery = useSources();

  const filters = useMemo(() => {
    const candidateSelection = parseCandidateSelection(searchParams.get("candidates"));
    const from = parseISODate(searchParams.get("from")) ?? defaultFrom();
    const to = parseISODate(searchParams.get("to")) ?? defaultTo();
    const sourceId = parseSourceId(searchParams.get("source"));
    return { candidateSelection, from, to, sourceId };
  }, [searchParams]);

  const orderedSources = useMemo(
    () => (sourcesQuery.data ? [...sourcesQuery.data].sort(compareSourcesByLean) : []),
    [sourcesQuery.data],
  );

  // If the URL has no `source`, default to the first available outlet so the chart
  // has something to render. The URL stays clean (no rewrite); SourceFilter
  // highlights the resolved id.
  const resolvedSourceId =
    filters.sourceId ?? (orderedSources.length > 0 ? orderedSources[0].id : null);

  const timeseriesQuery = useTimeseries({
    candidateIds:
      filters.candidateSelection && filters.candidateSelection.length > 0
        ? filters.candidateSelection
        : undefined,
    sourceIds: resolvedSourceId !== null ? [resolvedSourceId] : undefined,
    from: filters.from,
    to: filters.to,
  });

  const selectedSource =
    resolvedSourceId !== null
      ? orderedSources.find((s) => s.id === resolvedSourceId) ?? null
      : null;

  return (
    <Layout>
      <div className="grid gap-6 md:grid-cols-[260px_1fr]">
        <aside className="space-y-4">
          {sourcesQuery.data ? (
            <SourceFilter sources={sourcesQuery.data} selectedSourceId={resolvedSourceId} />
          ) : sourcesQuery.error ? (
            <ErrorBox message={t.errors.apiUnreachable} />
          ) : (
            <Spinner label={t.chart.loading} />
          )}
          {candidatesQuery.data && <CandidateFilter candidates={candidatesQuery.data} />}
          <DateRangeFilter />
        </aside>

        <div className="space-y-4">
          <header>
            <h2 className="text-lg font-semibold text-slate-900">{t.sources.title}</h2>
            <p className="text-sm text-slate-500">{t.sources.subtitle}</p>
          </header>

          {resolvedSourceId === null && (
            <div className="rounded-md border border-dashed border-slate-300 bg-white p-8 text-center text-sm text-slate-500">
              {t.sources.pickPrompt}
            </div>
          )}

          {timeseriesQuery.isLoading && <Spinner label={t.chart.loading} />}
          {timeseriesQuery.error && <ErrorBox message={t.errors.apiUnreachable} />}

          {timeseriesQuery.data && candidatesQuery.data && selectedSource && (
            <TimeSeriesChart
              points={timeseriesQuery.data.points}
              candidates={candidatesQuery.data}
              selectedCandidateIds={filters.candidateSelection}
              title={t.sources.chartTitle(selectedSource.outlet)}
            />
          )}
        </div>
      </div>
    </Layout>
  );
}

function ErrorBox({ message }: { message: string }) {
  return (
    <div className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {message}
    </div>
  );
}
