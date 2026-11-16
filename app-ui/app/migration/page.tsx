"use client";

import { Play } from "lucide-react";
import { useState } from "react";
import useSWR from "swr";

import { useRuns } from "@/components/runs";
import { PageTitle } from "@/components/shell";
import { Alert } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { api, fetcher } from "@/lib/api";
import type { MigrationPrecheck, RunMigrationRequest } from "@/lib/types";
import { cn } from "@/lib/utils";

type Selection = keyof RunMigrationRequest;

// In run order; the backend runs the selected ones sequentially in this order.
const WORKFLOWS: { key: Selection; name: string; experimental?: boolean; describe: (p: MigrationPrecheck) => string }[] = [
  {
    key: "migrate_tables",
    name: "migrate-tables",
    describe: () => "Migrates managed, external (SYNC) and DBFS-root tables and views using the table mapping.",
  },
  {
    key: "hiveserde_in_place",
    name: "migrate-external-hiveserde-tables-in-place-experimental",
    experimental: true,
    describe: (p) => `Migrates ${p.hiveserde_tables.toLocaleString()} Hive SerDe tables in place.`,
  },
  {
    key: "external_ctas",
    name: "migrate-external-tables-ctas",
    describe: (p) =>
      `Copies ${p.external_ctas_tables.toLocaleString()} external tables that can't be migrated with SYNC, using CREATE TABLE AS SELECT.`,
  },
];

export default function MigrationPage() {
  const { track } = useRuns();
  const { data: precheck, error } = useSWR<MigrationPrecheck>("/api/migration/precheck", fetcher);
  const [selection, setSelection] = useState<RunMigrationRequest>({
    migrate_tables: true,
    hiveserde_in_place: false,
    external_ctas: false,
  });
  const [starting, setStarting] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);

  const selected = WORKFLOWS.filter((w) => selection[w.key]);
  const blocked = !precheck || precheck.prerequisites_error !== null;

  async function run() {
    setStarting(true);
    setRunError(null);
    try {
      const started = await api.runMigration(selection);
      track({ run_id: started.run_id, run_url: started.run_url, name: started.workflows.join(" -> "), chained: true });
    } catch (e) {
      setRunError((e as Error).message);
    } finally {
      setStarting(false);
    }
  }

  return (
    <section>
      <PageTitle
        title="Migration"
        subtitle="Migrates the tables in the mapping to Unity Catalog. Selected workflows run one after another, as a single Databricks run."
      />
      {error && <Alert tone="error" className="mb-3.5">{error.message}</Alert>}
      <div className="mb-4 grid grid-cols-3 gap-4">
        <Stat value={precheck?.total_tables} label="Tables in assessment" />
        <Stat value={precheck?.hiveserde_tables} label="Hive SerDe tables (in-place migration available)" />
        <Stat value={precheck?.external_ctas_tables} label="External tables that can't use SYNC (need CTAS)" />
      </div>
      {precheck?.prerequisites_error && (
        <Alert tone="warn" className="mb-3.5">
          Prerequisite not met: {precheck.prerequisites_error}
        </Alert>
      )}
      {runError && <Alert tone="error" className="mb-3.5">{runError}</Alert>}
      <Card>
        <CardTitle>Choose workflows</CardTitle>
        {WORKFLOWS.map((workflow) => (
          <label
            key={workflow.key}
            className={cn(
              "mb-2.5 flex cursor-pointer items-start gap-3 rounded-lg border border-border p-3.5",
              selection[workflow.key] && "border-accent bg-red-50/30",
            )}
          >
            <Checkbox
              className="mt-0.5"
              checked={selection[workflow.key]}
              onCheckedChange={(checked) => setSelection({ ...selection, [workflow.key]: checked === true })}
            />
            <div>
              <div className="font-semibold">
                {workflow.name} {workflow.experimental && <Badge tone="warn">experimental</Badge>}
              </div>
              <div className="text-[13px] text-muted">{precheck ? workflow.describe(precheck) : "…"}</div>
            </div>
          </label>
        ))}
        <div className="text-[13px] text-muted">Run order:</div>
        <div className="my-3 flex flex-wrap items-center gap-2">
          {selected.length === 0 && <span className="text-[13px] text-muted">Select at least one workflow.</span>}
          {selected.map((workflow, i) => (
            <span key={workflow.key} className="flex items-center gap-2">
              {i > 0 && <span className="text-muted">→</span>}
              <span className="rounded-md bg-zinc-100 px-2.5 py-1.5 font-mono text-[12.5px]">{workflow.name}</span>
            </span>
          ))}
        </div>
        <div className="flex items-center gap-2.5">
          <span className="flex-1 text-[13px] text-muted">If a workflow fails, the ones after it don&apos;t run.</span>
          <Button variant="primary" disabled={blocked || selected.length === 0 || starting} onClick={run}>
            <Play className="size-3.5" />
            {starting ? "Starting…" : `Run ${selected.length} workflow${selected.length === 1 ? "" : "s"}`}
          </Button>
        </div>
      </Card>
    </section>
  );
}

function Stat({ value, label }: { value: number | undefined; label: string }) {
  return (
    <Card>
      <div className="text-[28px] font-bold">{value === undefined ? "…" : value.toLocaleString()}</div>
      <div className="text-[13px] text-muted">{label}</div>
    </Card>
  );
}
