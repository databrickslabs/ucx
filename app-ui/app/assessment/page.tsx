"use client";

import { ExternalLink, Play } from "lucide-react";
import { useState } from "react";
import useSWR from "swr";

import { useRuns } from "@/components/runs";
import { PageTitle } from "@/components/shell";
import { Alert } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { api, fetcher } from "@/lib/api";
import type { Dashboard } from "@/lib/types";

export default function AssessmentPage() {
  const { track } = useRuns();
  const [forceRefresh, setForceRefresh] = useState(false);
  const [starting, setStarting] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const { data: dashboard, error: dashboardError } = useSWR<Dashboard>("/api/assessment/dashboard", fetcher);

  async function runAssessment() {
    setStarting(true);
    setRunError(null);
    try {
      const run = await api.runAssessment(forceRefresh);
      track({ run_id: run.run_id, run_url: run.run_url, name: run.workflow });
    } catch (e) {
      setRunError((e as Error).message);
    } finally {
      setStarting(false);
    }
  }

  return (
    <section>
      <PageTitle
        title="Assessment"
        subtitle="Crawls the workspace and the Hive metastore and reports what is compatible with Unity Catalog."
      />
      <Card className="mb-4">
        <CardTitle>Run assessment</CardTitle>
        <div className="mb-3 flex flex-wrap items-center gap-2.5">
          <label className="flex items-center gap-2">
            <Checkbox checked={forceRefresh} onCheckedChange={(checked) => setForceRefresh(checked === true)} />
            Force refresh <span className="text-muted">(re-crawl everything, ignore cached results)</span>
          </label>
          <span className="flex-1" />
          <Button variant="primary" onClick={runAssessment} disabled={starting}>
            <Play className="size-3.5" /> {starting ? "Starting…" : "Run assessment"}
          </Button>
        </div>
        {runError ? (
          <Alert tone="error">{runError}</Alert>
        ) : (
          <Alert>
            The run starts in the background. Progress shows in the <b>Active runs</b> window.
          </Alert>
        )}
      </Card>
      <Card>
        <CardTitle
          aside={
            dashboard && (
              <Button size="sm" asChild>
                <a href={dashboard.url} target="_blank" rel="noreferrer">
                  Open in new tab <ExternalLink className="size-3" />
                </a>
              </Button>
            )
          }
        >
          Assessment dashboard
        </CardTitle>
        {dashboardError && <Alert tone="error">{dashboardError.message}</Alert>}
        {dashboard && (
          <>
            <iframe
              title="UCX assessment dashboard"
              src={dashboard.embed_url}
              className="h-[640px] w-full rounded-lg border border-border bg-zinc-50"
            />
            <Alert tone="warn" className="mt-3">
              If the dashboard doesn&apos;t load here, dashboard embedding may be off for this workspace, or the
              app&apos;s domain isn&apos;t allow-listed. Use <b>Open in new tab</b> instead.
            </Alert>
          </>
        )}
      </Card>
    </section>
  );
}
