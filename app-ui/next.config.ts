import type { NextConfig } from "next";

const isDev = process.env.NODE_ENV === "development";

// In development, the UI runs on `next dev` and proxies the API to a local backend:
//   uv run --all-extras uvicorn databricks.labs.ucx.app.main:app --port 8000
const backend = process.env.BACKEND_PROXY_URL || "http://localhost:8000";

const nextConfig: NextConfig = {
  // Production builds are a static export (`out/`), which `make app-ui` copies into the Python package.
  ...(isDev ? {} : { output: "export" }),
  trailingSlash: true,
  images: { unoptimized: true },
  ...(isDev
    ? { rewrites: async () => [{ source: "/api/:path*", destination: `${backend}/api/:path*` }] }
    : {}),
};

export default nextConfig;
