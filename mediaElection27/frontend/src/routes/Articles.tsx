import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useArticles } from "@/api/articles";
import type { Candidate } from "@/api/candidates";
import { useCandidates } from "@/api/candidates";
import { useSources } from "@/api/sources";
import { ArticlesTable } from "@/components/ArticlesTable";
import { CandidateFilter } from "@/components/CandidateFilter";
import { DateRangeFilter } from "@/components/DateRangeFilter";
import { Layout } from "@/components/Layout";
import { SourceFilter } from "@/components/SourceFilter";
import { Spinner } from "@/components/Spinner";
import {
  defaultFrom,
  defaultTo,
  parseCandidateSelection,
  parseISODate,
  parseOffset,
  parseSourceId,
  writeOffset,
} from "@/lib/url-state";
import { t } from "@/i18n";

const PAGE_SIZE = 20;

export default function Articles() {
  const [searchParams, setSearchParams] = useSearchParams();

  const candidatesQuery = useCandidates();
  const sourcesQuery = useSources();

  const filters = useMemo(() => {
    const candidateSelection = parseCandidateSelection(searchParams.get("candidates"));
    const from = parseISODate(searchParams.get("from")) ?? defaultFrom();
    const to = parseISODate(searchParams.get("to")) ?? defaultTo();
    const sourceId = parseSourceId(searchParams.get("source"));
    const offset = parseOffset(searchParams.get("offset"));
    return { candidateSelection, from, to, sourceId, offset };
  }, [searchParams]);

  // Tri-state candidate filter → API params:
  //   null            → no filter (all ingested articles, with or without mentions)
  //   []              → has_mention=false (articles with zero mentions)
  //   [id, id, ...]   → candidate_id IN (...)
  const candidateApiParams = useMemo<
    Pick<Parameters<typeof useArticles>[0], "candidateIds" | "hasMention">
  >(() => {
    const sel = filters.candidateSelection;
    if (sel === null) return {};
    if (sel.length === 0) return { hasMention: false };
    return { candidateIds: sel };
  }, [filters.candidateSelection]);

  const articlesQuery = useArticles({
    ...candidateApiParams,
    sourceIds: filters.sourceId !== null ? [filters.sourceId] : undefined,
    from: filters.from,
    to: filters.to,
    limit: PAGE_SIZE,
    offset: filters.offset,
  });

  const candidatesById = useMemo(() => {
    const m = new Map<number, Candidate>();
    for (const c of candidatesQuery.data ?? []) m.set(c.id, c);
    return m;
  }, [candidatesQuery.data]);

  function goToOffset(next: number) {
    const params = new URLSearchParams(searchParams);
    writeOffset(params, next);
    setSearchParams(params, { replace: false });
  }

  const total = articlesQuery.data?.total ?? 0;
  const items = articlesQuery.data?.items ?? [];
  const pageFrom = total === 0 ? 0 : filters.offset + 1;
  const pageTo = Math.min(filters.offset + items.length, total);
  const canPrev = filters.offset > 0;
  const canNext = filters.offset + PAGE_SIZE < total;

  return (
    <Layout>
      <div className="grid gap-6 md:grid-cols-[260px_1fr]">
        <aside className="space-y-4">
          {sourcesQuery.data && (
            <SourceFilter sources={sourcesQuery.data} selectedSourceId={filters.sourceId} allowAll />
          )}
          {candidatesQuery.data && (
            <CandidateFilter candidates={candidatesQuery.data} allArticlesToggle />
          )}
          <DateRangeFilter />
        </aside>

        <div className="space-y-4">
          <header className="flex items-baseline justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">{t.articles.title}</h2>
              <p className="text-sm text-slate-500">{t.articles.subtitle}</p>
            </div>
            {articlesQuery.data && (
              <span className="text-xs text-slate-500">{t.articles.count(total)}</span>
            )}
          </header>

          {articlesQuery.isLoading && <Spinner label={t.chart.loading} />}
          {articlesQuery.error && <ErrorBox message={t.errors.apiUnreachable} />}

          {articlesQuery.data && items.length === 0 && (
            <div className="rounded-md border border-dashed border-slate-300 bg-white p-8 text-center text-sm text-slate-500">
              {t.articles.empty}
            </div>
          )}

          {articlesQuery.data && items.length > 0 && (
            <>
              <ArticlesTable articles={items} candidatesById={candidatesById} />
              <Pagination
                pageFrom={pageFrom}
                pageTo={pageTo}
                total={total}
                canPrev={canPrev}
                canNext={canNext}
                onPrev={() => goToOffset(Math.max(0, filters.offset - PAGE_SIZE))}
                onNext={() => goToOffset(filters.offset + PAGE_SIZE)}
              />
            </>
          )}
        </div>
      </div>
    </Layout>
  );
}

interface PaginationProps {
  pageFrom: number;
  pageTo: number;
  total: number;
  canPrev: boolean;
  canNext: boolean;
  onPrev: () => void;
  onNext: () => void;
}

function Pagination({ pageFrom, pageTo, total, canPrev, canNext, onPrev, onNext }: PaginationProps) {
  return (
    <div className="flex items-center justify-between text-sm text-slate-600">
      <span className="tabular-nums">{t.articles.pageOf(pageFrom, pageTo, total)}</span>
      <div className="flex gap-2">
        <button
          type="button"
          onClick={onPrev}
          disabled={!canPrev}
          className="rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
        >
          ← {t.articles.prev}
        </button>
        <button
          type="button"
          onClick={onNext}
          disabled={!canNext}
          className="rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {t.articles.next} →
        </button>
      </div>
    </div>
  );
}

function ErrorBox({ message }: { message: string }) {
  return (
    <div className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {message}
    </div>
  );
}
