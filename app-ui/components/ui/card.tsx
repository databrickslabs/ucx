import type { ComponentProps, ReactNode } from "react";

import { cn } from "@/lib/utils";

export function Card({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("rounded-[10px] border border-border bg-panel p-[18px]", className)} {...props} />;
}

export function CardTitle({ children, aside }: { children: ReactNode; aside?: ReactNode }) {
  return (
    <h2 className="mb-3 flex items-center justify-between text-[15px] font-semibold">
      <span>{children}</span>
      {aside}
    </h2>
  );
}
