import createNextIntlPlugin from "next-intl/plugin";

// Language without i18n routing (add-language-system D1): the request config reads the cookie and Accept-Language,
// and the English catalogue's declaration types every key and ICU argument for `npm run type-check`.
const withNextIntl = createNextIntlPlugin({
  requestConfig: "./src/i18n/request.ts",
  experimental: { createMessagesDeclaration: "./src/messages/en.json" },
});

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
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

export default withNextIntl(nextConfig);
