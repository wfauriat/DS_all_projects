import type { ReactNode } from "react";
import { NavLink, useLocation } from "react-router-dom";

import { t } from "@/i18n";

const NAV_ITEMS: ReadonlyArray<{ to: string; label: string }> = [
  { to: "/", label: t.nav.dashboard },
  { to: "/leaderboard", label: t.nav.leaderboard },
  { to: "/share", label: t.nav.share },
  { to: "/sources", label: t.nav.sources },
  { to: "/articles", label: t.nav.articles },
];

export function Layout({ children }: { children: ReactNode }) {
  const { search } = useLocation();

  return (
    <div className="flex min-h-full flex-col">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-7xl items-baseline justify-between px-4 py-4 md:px-6">
          <div>
            <h1 className="text-xl font-semibold tracking-tight text-slate-900">
              {t.app.title}
            </h1>
            <p className="text-sm text-slate-500">{t.app.subtitle}</p>
          </div>
          <span className="text-xs text-slate-400">v0.1.0</span>
        </div>
        <nav className="mx-auto max-w-7xl px-4 md:px-6">
          <ul className="flex gap-1 overflow-x-auto">
            {NAV_ITEMS.map((item) => (
              <li key={item.to}>
                <NavLink
                  to={{ pathname: item.to, search }}
                  end={item.to === "/"}
                  className={({ isActive }) =>
                    [
                      "inline-block whitespace-nowrap rounded-t-md border-b-2 px-3 py-2 text-sm transition-colors",
                      isActive
                        ? "border-indigo-600 font-semibold text-indigo-700"
                        : "border-transparent text-slate-600 hover:border-slate-300 hover:text-slate-900",
                    ].join(" ")
                  }
                >
                  {item.label}
                </NavLink>
              </li>
            ))}
          </ul>
        </nav>
      </header>
      <main className="mx-auto w-full max-w-7xl flex-1 px-4 py-6 md:px-6">{children}</main>
      <footer className="border-t border-slate-200 bg-white">
        <div className="mx-auto max-w-7xl px-4 py-4 text-xs text-slate-500 md:px-6">
          {t.footer.methodology}{" "}
          <a
            href="https://github.com/wfauriat/mediaElection27"
            target="_blank"
            rel="noopener noreferrer"
            className="text-indigo-600 hover:underline"
          >
            {t.footer.sourceCode}
          </a>
          .
        </div>
      </footer>
    </div>
  );
}
