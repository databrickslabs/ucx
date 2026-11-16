"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import useSWR from "swr";

import { RunsProvider } from "@/components/runs";
import { Alert } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { fetcher } from "@/lib/api";
import type { Status } from "@/lib/types";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/", label: "Overview", step: "●" },
  { href: "/assessment/", label: "Assessment", step: "1" },
  { href: "/mapping/", label: "Table Mapping", step: "2" },
  { href: "/migration/", label: "Migration", step: "3" },
];

const AUTH_LABELS: Record<string, string> = { obo: "on-behalf-of user", sp: "app service principal" };

/** Shared by the header and the Overview page; SWR de-duplicates the request. */
export const useStatus = () => useSWR<Status>("/api/status", fetcher);

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const { data: status, error } = useStatus();
  const current = NAV.find((item) => item.href === pathname) ?? NAV[0];
  return (
    <RunsProvider>
      <div className="grid min-h-screen grid-cols-[232px_1fr]">
        <aside className="flex flex-col gap-1 bg-sidebar px-3 py-[18px] text-zinc-200">
          <div className="flex items-center gap-2.5 px-2.5 pt-1 pb-[18px] text-base font-bold">
            <span className="grid size-7 place-items-center rounded-[7px] bg-accent text-[13px] text-white">UC</span>
            UCX
          </div>
          <nav className="flex flex-col gap-1">
            {NAV.map((item) => {
              const active = item === current;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  aria-current={active ? "page" : undefined}
                  className={cn(
                    "flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-zinc-300 hover:bg-zinc-800 hover:text-white",
                    active && "bg-zinc-800 text-white",
                  )}
                >
                  <span
                    className={cn(
                      "grid size-5 place-items-center rounded-full border border-zinc-700 text-[11px] text-zinc-400",
                      active && "border-accent text-accent",
                    )}
                  >
                    {item.step}
                  </span>
                  {item.label}
                </Link>
              );
            })}
          </nav>
          <div className="mt-auto p-2.5 text-xs text-zinc-500">
            {status ? `UCX v${status.ucx_version}` : "UCX"}
            <br />
            Unity Catalog migration
          </div>
        </aside>
        <main className="flex min-w-0 flex-col">
          <header className="flex h-14 items-center justify-between border-b border-border bg-panel px-6">
            <div className="font-semibold">{current.label}</div>
            {status && (
              <div className="flex items-center gap-2.5 text-[13px] text-muted">
                <span className="font-mono">{status.host.replace(/^https?:\/\//, "")}</span>
                <span>·</span>
                <span>{status.user}</span>
                <Badge>auth: {AUTH_LABELS[status.auth_mode] ?? status.auth_mode}</Badge>
              </div>
            )}
          </header>
          <div className="w-full max-w-[1180px] p-6 pb-[260px]">
            {error && (
              <Alert tone="error" className="mb-4">
                Can&apos;t load the UCX installation: {error.message}
              </Alert>
            )}
            {children}
          </div>
        </main>
      </div>
    </RunsProvider>
  );
}

export function PageTitle({ title, subtitle }: { title: string; subtitle: ReactNode }) {
  return (
    <>
      <h1 className="mb-1 text-xl font-bold">{title}</h1>
      <p className="mb-5 text-muted">{subtitle}</p>
    </>
  );
}
