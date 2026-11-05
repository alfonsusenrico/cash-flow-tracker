import type { Metadata } from "next";
import localFont from "next/font/local";
import "./globals.css";
import { Providers } from "./providers";
import { AppShell } from "@/components/layout/AppShell";
import { PRODUCT_NAME } from "@/components/workspace/navigation";
import { THEME_SCRIPT } from "@/lib/theme";

// Self-hosted (OFL-1.1, Latin subset) so builds never depend on fetching Google Fonts. These are the
// files the Markas+ prototype loaded from Google Fonts: Inter variable for text and amounts, Manrope
// SemiBold (a static 600 instance) for titles and the hero figure. The weight ranges clamp heavier
// requests to 600 instead of synthesising them.
const inter = localFont({
  src: "./fonts/Inter-latin-variable.woff2",
  variable: "--font-inter",
  display: "swap",
  weight: "400 600",
});

const manrope = localFont({
  src: "./fonts/Manrope-latin-600.woff2",
  variable: "--font-manrope",
  display: "swap",
  weight: "600",
});

export const metadata: Metadata = {
  title: PRODUCT_NAME,
  description: "Catatan uang harian: berapa yang keluar, berapa yang aman dipakai, dan apa yang perlu dijawab.",
  icons: {
    icon: "/icon.png",
    apple: "/icon.png",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="id" className={`${inter.variable} ${manrope.variable}`} suppressHydrationWarning>
      <head>
        {/* Applies the stored theme before the first paint (CSP allows inline scripts: 'unsafe-inline'). */}
        <script dangerouslySetInnerHTML={{ __html: THEME_SCRIPT }} />
      </head>
      <body>
        <Providers>
          <AppShell>{children}</AppShell>
        </Providers>
      </body>
    </html>
  );
}
