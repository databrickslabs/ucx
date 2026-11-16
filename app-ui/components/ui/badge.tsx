import { cva, type VariantProps } from "class-variance-authority";
import type { ReactNode } from "react";

import { ACTIVE_STATES, cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-semibold whitespace-nowrap",
  {
    variants: {
      tone: {
        ok: "bg-ok-soft text-ok",
        info: "bg-info-soft text-info",
        warn: "bg-warn-soft text-warn",
        bad: "bg-bad-soft text-bad",
        neutral: "bg-zinc-100 text-zinc-600",
      },
    },
    defaultVariants: { tone: "neutral" },
  },
);

export function Badge({ tone, children }: VariantProps<typeof badgeVariants> & { children: ReactNode }) {
  return <span className={badgeVariants({ tone })}>{children}</span>;
}

const OK_STATES = new Set(["SUCCESS", "SUCCESS_WITH_FAILURES"]);
const BAD_STATES = new Set(["FAILED", "TIMEDOUT", "CANCELED", "INTERNAL_ERROR", "UPSTREAM_FAILED", "UPSTREAM_CANCELED"]);

/** Badge for a Databricks job run state, as returned by the API. */
export function StateBadge({ state }: { state: string }) {
  const active = ACTIVE_STATES.has(state);
  const tone = OK_STATES.has(state) ? "ok" : BAD_STATES.has(state) ? "bad" : active ? "info" : "neutral";
  return (
    <Badge tone={tone}>
      {tone !== "neutral" && (
        <span className={cn("size-[7px] rounded-full bg-current", active && "animate-pulse")} aria-hidden />
      )}
      {state}
    </Badge>
  );
}
