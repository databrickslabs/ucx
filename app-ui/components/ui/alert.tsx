import { cva, type VariantProps } from "class-variance-authority";
import { CircleAlert, Info, TriangleAlert } from "lucide-react";
import type { ReactNode } from "react";

import { cn } from "@/lib/utils";

const alertVariants = cva("flex items-start gap-2.5 rounded-lg border px-3.5 py-2.5 text-[13px]", {
  variants: {
    tone: {
      info: "border-blue-200 bg-info-soft text-blue-900",
      warn: "border-amber-200 bg-warn-soft text-amber-900",
      error: "border-red-200 bg-bad-soft text-red-900",
    },
  },
  defaultVariants: { tone: "info" },
});

const icons = { info: Info, warn: TriangleAlert, error: CircleAlert };

export function Alert({
  tone = "info",
  className,
  children,
}: VariantProps<typeof alertVariants> & { className?: string; children: ReactNode }) {
  const Icon = icons[tone ?? "info"];
  return (
    <div role={tone === "error" ? "alert" : "status"} className={cn(alertVariants({ tone }), className)}>
      <Icon className="mt-0.5 size-4 shrink-0" aria-hidden />
      <div>{children}</div>
    </div>
  );
}
