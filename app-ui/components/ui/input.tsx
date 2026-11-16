import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

export function Input({ className, ...props }: ComponentProps<"input">) {
  return (
    <input
      className={cn(
        "w-full rounded-md border border-border bg-white px-2 py-1.5 text-[13px] outline-none " +
          "focus:border-info focus:ring-2 focus:ring-info/20",
        className,
      )}
      {...props}
    />
  );
}
