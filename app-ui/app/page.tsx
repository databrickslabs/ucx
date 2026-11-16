"use client";

import Link from "next/link";
import useSWR from "swr";

import { PageTitle, useStatus } from "@/components/shell";
import { StateBadge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardTitle } from "@/components/ui/card";
import { fetcher } from "@/lib/api";
import type { MappingRule, Status } from "@/lib/types";

function nextStep(status: Status, hasMapping: boolean | undefined) {
  const assessment = status.workflows.find((w) => w.step === "assessment");
  if (assessment?.state !== "SUCCESS") {
    return { text: "Run the assessment to find out what needs to move to Unity Catalog.", href: "/assessment/", label: "Go to Assessment" };
  }
  if (hasMapping === false) {
    return { text: "Assessment completed. Create the table mapping and review it before you migrate.", href: "/mapping/", label: "Go to Table Mapping" };
  }
  return { text: "Table mapping ready. Review it once more, then run the migration.", href: "/migration/", label: "Go to Migration" };
}

export default function OverviewPage() {
  const { data: status } = useStatus();
  // The mapping endpoint answers 400 when no mapping.csv exists yet.
  const { data: mapping, error: mappingError } = useSWR<{ rules: MappingRule[] }>("/api/mapping", fetcher);
  const hasMapping = mapping ? mapping.rules.length > 0 : mappingError ? false : undefined;
  const step = status && nextStep(status, hasMapping);

  return (
    <section>
      <PageTitle title="Overview" subtitle="The UCX installation this app works with, and the latest run of each workflow." />
      <div className="grid grid-cols-2 gap-4">
        <Card>
          <CardTitle>Installation</CardTitle>
          <dl className="grid grid-cols-[150px_1fr] gap-x-3 gap-y-2 text-[13px]">
            <dt className="text-muted">Install folder</dt>
            <dd className="font-mono text-[12.5px] break-all">{status?.install_folder ?? "…"}</dd>
            <dt className="text-muted">Inventory database</dt>
            <dd className="font-mono text-[12.5px]">{status ? `hive_metastore.${status.inventory_database}` : "…"}</dd>
            <dt className="text-muted">UCX version</dt>
            <dd className="font-mono text-[12.5px]">{status?.ucx_version ?? "…"}</dd>
            <dt className="text-muted">Auth mode</dt>
            <dd className="font-mono text-[12.5px]">
              {status ? (status.auth_mode === "sp" ? "sp (app service principal)" : "obo (logged-in user)") : "…"}
            </dd>
          </dl>
        </Card>
        <Card>
          <CardTitle>Next step</CardTitle>
          {step && (
            <>
              <p className="mb-3 text-muted">{step.text}</p>
              <Button variant="primary" asChild>
                <Link href={step.href}>{step.label} →</Link>
              </Button>
            </>
          )}
        </Card>
      </div>
      <Card className="mt-4">
        <CardTitle aside={<span className="text-xs font-normal text-muted">latest run per workflow</span>}>Workflows</CardTitle>
        <table className="w-full border-collapse text-[13px]">
          <thead>
            <tr className="bg-zinc-50 text-left text-xs tracking-wide text-muted uppercase">
              <th className="border-b border-border px-2.5 py-2 font-semibold">Workflow</th>
              <th className="border-b border-border px-2.5 py-2 font-semibold">Latest state</th>
              <th className="border-b border-border px-2.5 py-2 font-semibold">Started</th>
            </tr>
          </thead>
          <tbody>
            {status?.workflows.map((workflow) => (
              <tr key={workflow.step}>
                <td className="border-b border-zinc-100 px-2.5 py-2 font-mono text-[12.5px]">{workflow.step}</td>
                <td className="border-b border-zinc-100 px-2.5 py-2">
                  <StateBadge state={workflow.state} />
                </td>
                <td className="border-b border-zinc-100 px-2.5 py-2">{workflow.started}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </section>
  );
}
