/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  // Always inlined, empty unless a local build sets it, so the development-only /komponen sheet is dead code in
  // every other build and its module is left out (apply-markas-foundation D12).
  env: {
    NEXT_PUBLIC_COMPONENT_SHEET: process.env.NEXT_PUBLIC_COMPONENT_SHEET === "1" ? "1" : "",
  },
  // The file tracer still lists the sheet's source for the dynamic import; keep it out of the standalone output.
  outputFileTracingExcludes: {
    "/komponen": ["./src/app/komponen/**"],
  },
  async redirects() {
    return [
      {
        source: "/dashboard",
        destination: "/",
        permanent: false,
      },
      // Tanya is retired (refine-workspace-layout D10); old links and bookmarks land on Beranda.
      {
        source: "/tanya",
        destination: "/",
        permanent: true,
      },
    ];
  },
  async rewrites() {
    if (process.env.NODE_ENV === "production") return [];
    return [
      {
        source: "/api/:path*",
        destination: `${process.env.BACKEND_URL ?? "http://localhost:8000"}/:path*`,
      },
    ];
  },
};

export default nextConfig;
