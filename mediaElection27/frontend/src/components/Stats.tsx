import { t } from "@/i18n";

interface StatsProps {
  totalMentions: number;
  totalArticles: number;
  activeSources: number;
  fromDate: string;
  toDate: string;
}

export function Stats({
  totalMentions,
  totalArticles,
  activeSources,
  fromDate,
  toDate,
}: StatsProps) {
  return (
    <dl className="grid grid-cols-2 gap-3 md:grid-cols-4">
      <Stat label={t.stats.totalMentions} value={totalMentions.toLocaleString("fr-FR")} />
      <Stat label={t.stats.articles} value={totalArticles.toLocaleString("fr-FR")} />
      <Stat label={t.stats.activeSources} value={activeSources.toLocaleString("fr-FR")} />
      <Stat label={t.stats.period} value={`${fromDate} → ${toDate}`} small />
    </dl>
  );
}

function Stat({ label, value, small }: { label: string; value: string; small?: boolean }) {
  return (
    <div className="rounded-md border border-slate-200 bg-white px-4 py-3 shadow-sm">
      <dt className="text-xs font-medium uppercase tracking-wider text-slate-500">{label}</dt>
      <dd className={small ? "mt-1 text-sm text-slate-900" : "mt-1 text-2xl font-semibold text-slate-900"}>
        {value}
      </dd>
    </div>
  );
}
