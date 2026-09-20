import { useSearchParams } from "react-router-dom";

import { defaultFrom, defaultTo, parseISODate } from "@/lib/url-state";
import { t } from "@/i18n";

export function DateRangeFilter() {
  const [searchParams, setSearchParams] = useSearchParams();
  const from = parseISODate(searchParams.get("from")) ?? defaultFrom();
  const to = parseISODate(searchParams.get("to")) ?? defaultTo();

  function update(key: "from" | "to", value: string) {
    const params = new URLSearchParams(searchParams);
    if (value) params.set(key, value);
    else params.delete(key);
    setSearchParams(params, { replace: true });
  }

  return (
    <section className="rounded-md border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="mb-3 text-sm font-semibold text-slate-700">{t.filters.period}</h2>
      <div className="space-y-2">
        <label className="block text-xs text-slate-500">
          {t.filters.from}
          <input
            type="date"
            className="mt-1 block w-full rounded border border-slate-300 px-2 py-1 text-sm focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
            value={from}
            max={to}
            onChange={(e) => update("from", e.target.value)}
          />
        </label>
        <label className="block text-xs text-slate-500">
          {t.filters.to}
          <input
            type="date"
            className="mt-1 block w-full rounded border border-slate-300 px-2 py-1 text-sm focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500"
            value={to}
            min={from}
            onChange={(e) => update("to", e.target.value)}
          />
        </label>
      </div>
    </section>
  );
}
