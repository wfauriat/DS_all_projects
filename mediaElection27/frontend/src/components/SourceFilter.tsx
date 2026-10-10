import { useSearchParams } from "react-router-dom";

import type { Source } from "@/api/sources";
import { compareSourcesByLean } from "@/lib/lean";
import { parseSourceId, writeSourceId } from "@/lib/url-state";
import { t } from "@/i18n";

interface SourceFilterProps {
  sources: Source[];
  /** Resolved selected source id (after defaulting to first if URL has no `source`). */
  selectedSourceId: number | null;
  /** When true, renders an "All sources" option at the top and treats no-selection
   *  as a valid state (URL param `source` is absent). Default false (drilldown style). */
  allowAll?: boolean;
}

export function SourceFilter({ sources, selectedSourceId, allowAll = false }: SourceFilterProps) {
  const [searchParams, setSearchParams] = useSearchParams();
  const urlSelection = parseSourceId(searchParams.get("source"));
  const effectiveSelection = allowAll ? urlSelection : (urlSelection ?? selectedSourceId);

  function selectSource(id: number | null) {
    const params = new URLSearchParams(searchParams);
    writeSourceId(params, id);
    setSearchParams(params, { replace: true });
  }

  const ordered = [...sources].sort(compareSourcesByLean);

  return (
    <section className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="mb-3 text-sm font-semibold text-slate-700">{t.sources.filterTitle}</h2>
      <ul className="space-y-1" role="radiogroup" aria-label={t.sources.filterTitle}>
        {allowAll && (
          <li>
            <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 hover:bg-slate-50">
              <input
                type="radio"
                name="source"
                className="h-4 w-4 border-slate-300 text-indigo-600 focus:ring-indigo-500"
                checked={effectiveSelection === null}
                onChange={() => selectSource(null)}
              />
              <span className="text-sm font-medium text-slate-700">{t.sources.allLabel}</span>
            </label>
          </li>
        )}
        {ordered.map((s) => {
          // Treat the URL-derived id as authoritative for the radio "checked" state;
          // the parent's resolved id is what feeds the chart. When the URL is empty
          // we still highlight the resolved default so the UI doesn't look unselected.
          const checked = effectiveSelection === s.id;
          return (
            <li key={s.id}>
              <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 hover:bg-slate-50">
                <input
                  type="radio"
                  name="source"
                  className="h-4 w-4 border-slate-300 text-indigo-600 focus:ring-indigo-500"
                  checked={checked}
                  onChange={() => selectSource(s.id)}
                />
                <span className="text-sm text-slate-800">{s.outlet}</span>
                {s.lean && (
                  <span className="ml-auto text-[10px] uppercase tracking-wider text-slate-400">
                    {s.lean}
                  </span>
                )}
              </label>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
