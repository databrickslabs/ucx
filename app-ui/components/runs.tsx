"use client";

import { ChevronDown, ChevronUp, ExternalLink } from "lucide-react";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import useSWR from "swr";

import { StateBadge } from "@/components/ui/badge";
import { api, fetcher } from "@/lib/api";
import type { Run } from "@/lib/types";
import { ACTIVE_STATES, cn, timeAgo } from "@/lib/utils";

export const POLL_INTERVAL_MS = 10_000;
/** How long a run that finished stays in the status window. */
export const KEEP_FINISHED_MS = 5 * 60_000;

interface TrackedRun {
  run: Run;
  finishedAt: number | null;
  chained: boolean;
}

interface RunsContextValue {
  /** Show a run that was just started in the status window, before the next poll picks it up. */
  track: (run: { run_id: number; run_url: string; name: string; chained?: boolean }) => void;
  activeCount: number;
}

const RunsContext = createContext<RunsContextValue>({ track: () => {}, activeCount: 0 });

export const useRuns = () => useContext(RunsContext);

export function RunsProvider({ children }: { children: ReactNode }) {
  const [tracked, setTracked] = useState<Map<number, TrackedRun>>(new Map());
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const resolving = useRef(new Set<number>());

  // Loaded when the app opens, then polled: picks up runs started outside the app (CLI, Jobs UI) too.
  const { data, error } = useSWR<{ runs: Run[] }>("/api/runs/active", fetcher, { refreshInterval: POLL_INTERVAL_MS });

  const trackedRef = useRef(tracked);
  trackedRef.current = tracked;

  useEffect(() => {
    if (!data) return;
    setUpdatedAt(Date.now());
    const activeIds = new Set(data.runs.map((run) => run.run_id));
    // Runs we saw before that are no longer active: look up how they ended.
    const vanished = [...trackedRef.current].filter(
      ([runId, entry]) => entry.finishedAt === null && !activeIds.has(runId) && !resolving.current.has(runId),
    );
    setTracked((previous) => {
      const next = new Map(previous);
      for (const run of data.runs) {
        const chained = previous.get(run.run_id)?.chained ?? run.name.includes(" -> ");
        next.set(run.run_id, { run, finishedAt: null, chained });
      }
      return next;
    });
    for (const [runId, entry] of vanished) {
      resolving.current.add(runId);
      api
        .run(runId)
        .then((run) =>
          setTracked((current) => {
            const updated = new Map(current);
            const finished = !ACTIVE_STATES.has(run.state);
            updated.set(runId, { ...entry, run, finishedAt: finished ? Date.now() : null });
            return updated;
          }),
        )
        .catch(() => setTracked((current) => withoutRun(current, runId)))
        .finally(() => resolving.current.delete(runId));
    }
  }, [data]);

  // Tick for relative times, and to drop runs that finished more than KEEP_FINISHED_MS ago.
  useEffect(() => {
    const timer = setInterval(() => {
      const current = Date.now();
      setNow(current);
      setTracked((previous) => {
        const expired = [...previous].filter(([, e]) => e.finishedAt !== null && current - e.finishedAt > KEEP_FINISHED_MS);
        if (expired.length === 0) return previous;
        const next = new Map(previous);
        expired.forEach(([runId]) => next.delete(runId));
        return next;
      });
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const track = useCallback<RunsContextValue["track"]>(({ run_id, run_url, name, chained = false }) => {
    setTracked((previous) => {
      const next = new Map(previous);
      const run: Run = { name, run_id, run_url, state: "PENDING", state_message: "", started_at: new Date().toISOString() };
      next.set(run_id, { run, finishedAt: null, chained });
      return next;
    });
  }, []);

  const entries = useMemo(
    () =>
      [...tracked.values()].sort(
        (a, b) => Number(a.finishedAt !== null) - Number(b.finishedAt !== null) || b.run.run_id - a.run.run_id,
      ),
    [tracked],
  );
  const activeCount = entries.filter((e) => e.finishedAt === null).length;
  const value = useMemo(() => ({ track, activeCount }), [track, activeCount]);

  return (
    <RunsContext.Provider value={value}>
      {children}
      <StatusWindow entries={entries} activeCount={activeCount} updatedAt={updatedAt} now={now} error={error} />
    </RunsContext.Provider>
  );
}

function withoutRun(runs: Map<number, TrackedRun>, runId: number) {
  const next = new Map(runs);
  next.delete(runId);
  return next;
}

function StatusWindow({
  entries,
  activeCount,
  updatedAt,
  now,
  error,
}: {
  entries: TrackedRun[];
  activeCount: number;
  updatedAt: number | null;
  now: number;
  error: unknown;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const updated = updatedAt ? `updated ${timeAgo(new Date(updatedAt).toISOString(), now)}` : "loading…";
  return (
    <section
      aria-label="Active runs"
      className="fixed right-5 bottom-5 z-30 w-[440px] overflow-hidden rounded-xl border border-border bg-panel shadow-[0_12px_32px_rgba(0,0,0,.14)]"
    >
      <button
        type="button"
        onClick={() => setCollapsed(!collapsed)}
        aria-expanded={!collapsed}
        className="flex w-full cursor-pointer items-center justify-between bg-sidebar px-3.5 py-2.5 text-white"
      >
        <span className="flex items-center gap-2 font-semibold">
          <span className="inline-flex items-center gap-1.5 rounded-full bg-blue-900 px-2 py-0.5 text-xs text-blue-100">
            <span className={cn("size-[7px] rounded-full bg-current", activeCount > 0 && "animate-pulse")} />
            {activeCount}
          </span>
          Active runs
        </span>
        <span className="flex items-center gap-2.5 text-xs text-zinc-400">
          {error ? <span className="text-red-300">can't refresh</span> : updated} · every {POLL_INTERVAL_MS / 1000}s
          {collapsed ? <ChevronUp className="size-4" /> : <ChevronDown className="size-4" />}
        </span>
      </button>
      {!collapsed && (
        <div className="max-h-[230px] overflow-auto">
          {entries.length === 0 && <p className="px-3.5 py-4 text-[13px] text-muted">No UCX runs in progress.</p>}
          {entries.map(({ run, finishedAt, chained }) => (
            <div key={run.run_id} className="grid grid-cols-[1fr_auto] gap-x-2.5 gap-y-1 border-b border-zinc-100 px-3.5 py-2.5">
              <div className="font-mono text-[13px] font-semibold break-words">{run.name.replaceAll(" -> ", " → ")}</div>
              <StateBadge state={run.state} />
              <div className="col-span-2 flex justify-between text-xs text-muted">
                <span>
                  {[
                    chained && "chained run",
                    run.state_message,
                    finishedAt !== null ? "finished · kept for 5 min" : `started ${timeAgo(run.started_at, now)}`,
                  ]
                    .filter(Boolean)
                    .join(" · ")}
                </span>
                <a href={run.run_url} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-info">
                  View run <ExternalLink className="size-3" />
                </a>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
