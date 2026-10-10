import type { Article } from "@/api/articles";
import type { Candidate } from "@/api/candidates";
import { colorForCandidate } from "@/lib/colors";
import { t } from "@/i18n";

interface ArticlesTableProps {
  articles: Article[];
  candidatesById: Map<number, Candidate>;
}

function formatPublished(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  // fr-FR locale, dd/MM HH:mm
  const date = d.toLocaleDateString("fr-FR", { day: "2-digit", month: "2-digit" });
  const time = d.toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" });
  return `${date} ${time}`;
}

export function ArticlesTable({ articles, candidatesById }: ArticlesTableProps) {
  return (
    <div className="overflow-hidden rounded-md border border-slate-200 bg-white shadow-sm">
      <table className="w-full text-sm">
        <thead className="border-b border-slate-200 bg-slate-50 text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th scope="col" className="w-32 px-3 py-2 text-left">{t.articles.col.published}</th>
            <th scope="col" className="px-3 py-2 text-left">{t.articles.col.title}</th>
            <th scope="col" className="w-40 px-3 py-2 text-left">{t.articles.col.outlet}</th>
            <th scope="col" className="px-3 py-2 text-left">{t.articles.col.candidates}</th>
          </tr>
        </thead>
        <tbody>
          {articles.map((a) => (
            <tr key={a.id} className="border-b border-slate-100 last:border-b-0 hover:bg-slate-50">
              <td className="px-3 py-2 tabular-nums text-slate-500">{formatPublished(a.published_at)}</td>
              <td className="px-3 py-2">
                <a
                  href={a.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-slate-900 hover:text-indigo-700 hover:underline"
                  title={t.articles.open}
                >
                  {a.title}
                </a>
              </td>
              <td className="px-3 py-2 text-slate-600">{a.outlet ?? "—"}</td>
              <td className="px-3 py-2">
                <ul className="flex flex-wrap gap-1">
                  {a.candidate_ids.map((cid) => {
                    const c = candidatesById.get(cid);
                    return (
                      <li key={cid}>
                        <span
                          className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-700"
                          style={{ borderLeft: `3px solid ${colorForCandidate(cid)}` }}
                        >
                          {c?.display_name ?? `#${cid}`}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
