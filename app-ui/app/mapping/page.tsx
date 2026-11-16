"use client";

import { RefreshCw } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import useSWR from "swr";

import { PageTitle } from "@/components/shell";
import { Alert } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { ConfirmDialog } from "@/components/ui/confirm-dialog";
import { Input } from "@/components/ui/input";
import { ApiError, api, fetcher } from "@/lib/api";
import type { MappingRule } from "@/lib/types";
import { cn } from "@/lib/utils";

const PAGE_SIZE = 50;
type EditableField = "catalog_name" | "dst_schema" | "dst_table" | "recon_tolerance_percent" | "compare_rows";

const sourceKey = (r: MappingRule) => `hive_metastore.${r.src_schema}.${r.src_table}`.toLowerCase();
const targetKey = (r: MappingRule) => `${r.catalog_name}.${r.dst_schema}.${r.dst_table}`.toLowerCase();

/** Same rules as the backend validation, so problems show before saving. */
function findDuplicateTargets(rules: MappingRule[]): Set<string> {
  const seen = new Map<string, number>();
  rules.forEach((rule) => seen.set(targetKey(rule), (seen.get(targetKey(rule)) ?? 0) + 1));
  return new Set([...seen].filter(([, count]) => count > 1).map(([key]) => key));
}

export default function MappingPage() {
  const { data, error, isLoading, mutate } = useSWR<{ rules: MappingRule[] }>("/api/mapping", fetcher);
  const noMapping = error instanceof ApiError && error.status === 400;
  const [draft, setDraft] = useState<MappingRule[]>([]);
  const [filter, setFilter] = useState("");
  const [page, setPage] = useState(0);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [confirmCreate, setConfirmCreate] = useState(false);

  useEffect(() => {
    if (data) setDraft(data.rules);
  }, [data]);

  const original = useMemo(() => new Map((data?.rules ?? []).map((r) => [sourceKey(r), r])), [data]);
  const isChanged = (rule: MappingRule, field: EditableField) => original.get(sourceKey(rule))?.[field] !== rule[field];
  const changedRows = draft.filter((rule) => (Object.keys(rule) as (keyof MappingRule)[]).some((f) => original.get(sourceKey(rule))?.[f] !== rule[f]));
  const duplicateTargets = useMemo(() => findDuplicateTargets(draft), [draft]);

  const filtered = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) return draft;
    return draft.filter((r) => `${sourceKey(r)} ${targetKey(r)}`.includes(needle));
  }, [draft, filter]);
  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount - 1);
  const visible = filtered.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

  function update(rule: MappingRule, field: EditableField, value: string | number | boolean) {
    setDraft((rules) => rules.map((r) => (sourceKey(r) === sourceKey(rule) ? { ...r, [field]: value } : r)));
  }

  async function perform(action: () => Promise<{ rules: MappingRule[] }>) {
    setBusy(true);
    setActionError(null);
    try {
      const saved = await action();
      await mutate(saved, { revalidate: false });
    } catch (e) {
      setActionError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  const createButton = (primary: boolean) => (
    <Button
      variant={primary ? "primary" : "secondary"}
      disabled={busy}
      onClick={() => (data ? setConfirmCreate(true) : perform(api.createMapping))}
    >
      <RefreshCw className="size-3.5" /> Create from assessment
    </Button>
  );

  return (
    <section>
      <PageTitle
        title="Table Mapping"
        subtitle={
          <>
            Where each Hive metastore table goes in Unity Catalog. Stored as <span className="font-mono">mapping.csv</span> in
            the UCX install folder.
          </>
        }
      />
      <ConfirmDialog
        open={confirmCreate}
        onOpenChange={setConfirmCreate}
        title="Replace the table mapping?"
        confirmLabel="Replace mapping"
        onConfirm={() => perform(api.createMapping)}
      >
        This creates a new mapping from the assessment and overwrites <span className="font-mono">mapping.csv</span>,
        including any edits you saved before.
      </ConfirmDialog>

      {actionError && <Alert tone="error" className="mb-3.5">{actionError}</Alert>}
      {error && !noMapping && <Alert tone="error">{error.message}</Alert>}
      {isLoading && <p className="text-muted">Loading mapping…</p>}

      {noMapping && (
        <Card>
          <div className="px-5 py-10 text-center text-muted">
            <div className="mb-1.5 text-[15px] font-semibold text-fg">No table mapping yet</div>
            <div className="mb-3.5">Create it from the assessment results, then review the targets.</div>
            {createButton(true)}
          </div>
        </Card>
      )}

      {data && (
        <Card>
          <div className="mb-3 flex flex-wrap items-center gap-2.5">
            {createButton(false)}
            <Input
              type="text"
              placeholder="Filter by schema, table or catalog…"
              className="w-72 flex-none"
              value={filter}
              onChange={(e) => {
                setFilter(e.target.value);
                setPage(0);
              }}
            />
            <span className="flex-1" />
            <div className="flex items-center gap-2.5 whitespace-nowrap">
            <span className="text-[13px] text-muted">
              {draft.length.toLocaleString()} tables
              {changedRows.length > 0 && (
                <>
                  {" · "}
                  <b className="text-info">{changedRows.length} unsaved changes</b>
                </>
              )}
            </span>
            <Button disabled={busy || changedRows.length === 0} onClick={() => setDraft(data.rules)}>
              Discard
            </Button>
            <Button
              variant="primary"
              disabled={busy || changedRows.length === 0 || duplicateTargets.size > 0}
              onClick={() => perform(() => api.saveMapping(draft))}
            >
              {busy ? "Saving…" : "Save mapping"}
            </Button>
            </div>
          </div>
          {duplicateTargets.size > 0 && (
            <Alert tone="error" className="mb-3.5">
              Can&apos;t save: destination table used more than once:{" "}
              <span className="font-mono">{[...duplicateTargets].join(", ")}</span>
            </Alert>
          )}
          <table className="w-full border-collapse text-[13px]">
            <thead>
              <tr className="bg-zinc-50 text-left text-xs tracking-wide text-muted uppercase">
                <th className="w-[24%] border-b border-border px-2.5 py-2 font-semibold">Source (hive_metastore)</th>
                <th className="border-b border-border px-2.5 py-2 font-semibold">Target catalog</th>
                <th className="border-b border-border px-2.5 py-2 font-semibold">Target schema</th>
                <th className="border-b border-border px-2.5 py-2 font-semibold">Target table</th>
                <th className="w-[90px] border-b border-border px-2.5 py-2 font-semibold">Recon tol. %</th>
                <th className="w-[90px] border-b border-border px-2.5 py-2 font-semibold">Compare rows</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((rule) => {
                const duplicate = duplicateTargets.has(targetKey(rule));
                const dirty = changedRows.includes(rule);
                const cell = (field: "catalog_name" | "dst_schema" | "dst_table") => (
                  <Input
                    aria-label={`${field} of ${rule.src_schema}.${rule.src_table}`}
                    className={cn(
                      "px-1.5 py-1 font-mono text-[12.5px]",
                      isChanged(rule, field) && "border-info bg-info-soft",
                      (duplicate || rule[field] === "") && "border-bad bg-bad-soft",
                    )}
                    value={rule[field]}
                    onChange={(e) => update(rule, field, e.target.value)}
                  />
                );
                return (
                  <tr key={sourceKey(rule)} className={cn(duplicate && "bg-red-50/60")}>
                    <td className={cn("border-b border-zinc-100 px-2.5 py-2 font-mono text-[12.5px]", dirty && "shadow-[inset_3px_0_0_var(--color-info)]")}>
                      {rule.src_schema}.{rule.src_table}
                    </td>
                    <td className="border-b border-zinc-100 px-2.5 py-2">{cell("catalog_name")}</td>
                    <td className="border-b border-zinc-100 px-2.5 py-2">{cell("dst_schema")}</td>
                    <td className="border-b border-zinc-100 px-2.5 py-2">{cell("dst_table")}</td>
                    <td className="border-b border-zinc-100 px-2.5 py-2">
                      <Input
                        type="number"
                        min={0}
                        max={100}
                        aria-label={`recon tolerance of ${rule.src_schema}.${rule.src_table}`}
                        className={cn("px-1.5 py-1 font-mono text-[12.5px]", isChanged(rule, "recon_tolerance_percent") && "border-info bg-info-soft")}
                        value={rule.recon_tolerance_percent}
                        onChange={(e) => update(rule, "recon_tolerance_percent", Number(e.target.value))}
                      />
                    </td>
                    <td className="border-b border-zinc-100 px-2.5 py-2 text-center">
                      <Checkbox
                        aria-label={`compare rows of ${rule.src_schema}.${rule.src_table}`}
                        checked={rule.compare_rows}
                        onCheckedChange={(checked) => update(rule, "compare_rows", checked === true)}
                      />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div className="mt-3 flex items-center gap-2.5 text-[13px] text-muted">
            <span className="flex-1">
              {filtered.length === 0
                ? "No tables match the filter"
                : `Showing ${currentPage * PAGE_SIZE + 1}–${Math.min((currentPage + 1) * PAGE_SIZE, filtered.length)} of ${filtered.length.toLocaleString()}`}
            </span>
            <Button size="sm" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>
              ← Prev
            </Button>
            <Button size="sm" disabled={currentPage >= pageCount - 1} onClick={() => setPage(currentPage + 1)}>
              Next →
            </Button>
          </div>
        </Card>
      )}
    </section>
  );
}
