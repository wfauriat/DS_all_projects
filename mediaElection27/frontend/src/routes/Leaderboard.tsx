import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useCandidates } from "@/api/candidates";
import { useTimeseries } from "@/api/timeseries";
import { CandidateFilter } from "@/components/CandidateFilter";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { Layout } from "@/components/Layout";
import { LeaderboardTable } from "@/components/LeaderboardTable";
import { Spinner } from "@/components/Spinner";
import { defaultFrom, defaultTo, parseCandidateSelection, parseISODate } from "@/lib/url-state";
import { t } from "@/i18n";

export default function Leaderboard() {
  const [searchParams] = useSearchParams();

  const candidatesQuery = useCandidates();

  const filters = useMemo(() => {
    const candidateSelection = parseCandidateSelection(searchParams.get("candidates"));
    const from = parseISODate(searchParams.get("from")) ?? defaultFrom();
    const to = parseISODate(searchParams.get("to")) ?? defaultTo();
    return { candidateSelection, from, to };
  }, [searchParams]);

  const timeseriesQuery = useTimeseries({
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
          <header>
            <h2 className="text-lg font-semibold text-slate-900">{t.leaderboard.title}</h2>
            <p className="text-sm text-slate-500">{t.leaderboard.subtitle}</p>
          </header>

          {timeseriesQuery.isLoading && <Spinner label={t.chart.loading} />}
          {timeseriesQuery.error && <ErrorBox message={t.errors.apiUnreachable} />}

          {timeseriesQuery.data && candidatesQuery.data && (
            <LeaderboardTable
              points={timeseriesQuery.data.points}
              candidates={candidatesQuery.data}
              selectedCandidateIds={filters.candidateSelection}
              fromDate={filters.from}
              toDate={filters.to}
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
