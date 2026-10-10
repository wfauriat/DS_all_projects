import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useCandidates } from "@/api/candidates";
import { useSources } from "@/api/sources";
import { useTimeseries } from "@/api/timeseries";
import { CandidateFilter } from "@/components/CandidateFilter";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { Layout } from "@/components/Layout";
import { Spinner } from "@/components/Spinner";
import { Stats } from "@/components/Stats";
import { TimeSeriesChart } from "@/components/TimeSeriesChart";
import { defaultFrom, defaultTo, parseCandidateSelection, parseISODate } from "@/lib/url-state";
import { t } from "@/i18n";

export default function Dashboard() {
  const [searchParams] = useSearchParams();

  const candidatesQuery = useCandidates();
  const sourcesQuery = useSources();

  const filters = useMemo(() => {
    const candidateSelection = parseCandidateSelection(searchParams.get("candidates"));
    const from = parseISODate(searchParams.get("from")) ?? defaultFrom();
    const to = parseISODate(searchParams.get("to")) ?? defaultTo();
    return { candidateSelection, from, to };
  }, [searchParams]);

  const timeseriesQuery = useTimeseries({
    // Only pass an explicit filter when the user picked a non-empty subset; otherwise
    // fetch everything and let the chart filter client-side. (Cheap for v1; per-candidate
    // filter at the API will matter as the corpus grows.)
    candidateIds:
      filters.candidateSelection && filters.candidateSelection.length > 0
        ? filters.candidateSelection
        : undefined,
    from: filters.from,
    to: filters.to,
  });

  return (
    <Layout>
      <div className="grid gap-6 md:grid-cols-[260px_1fr]">
        <aside className="space-y-4">
          {candidatesQuery.data ? (
            <CandidateFilter candidates={candidatesQuery.data} />
          ) : candidatesQuery.error ? (
            <ErrorBox message={t.errors.apiUnreachable} />
          ) : (
            <Spinner label={t.chart.loading} />
          )}
          <DateRangeFilter />
        </aside>

        <div className="space-y-4">
          {timeseriesQuery.data && (
            <Stats
              totalMentions={timeseriesQuery.data.n_total_mentions}
              totalArticles={timeseriesQuery.data.points.reduce((sum, p) => sum + p.n_articles, 0)}
              activeSources={sourcesQuery.data?.length ?? 0}
              fromDate={filters.from}
              toDate={filters.to}
            />
          )}

          {timeseriesQuery.isLoading && <Spinner label={t.chart.loading} />}
          {timeseriesQuery.error && <ErrorBox message={t.errors.apiUnreachable} />}

          {timeseriesQuery.data && candidatesQuery.data && (
            <TimeSeriesChart
              points={timeseriesQuery.data.points}
              candidates={candidatesQuery.data}
              selectedCandidateIds={filters.candidateSelection}
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
