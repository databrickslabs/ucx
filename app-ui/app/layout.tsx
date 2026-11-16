import type { Metadata } from "next";
import type { ReactNode } from "react";

import { Shell } from "@/components/shell";

import "./globals.css";

export const metadata: Metadata = {
  title: "UCX",
  description: "Unity Catalog migration with UCX",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <Shell>{children}</Shell>
      </body>
    </html>
  );
}
