import type { Metadata } from "next";
import localFont from "next/font/local";
import "./globals.css";
import { Providers } from "./providers";
import { AppShell } from "@/components/layout/AppShell";
import { PRODUCT_NAME } from "@/components/workspace/navigation";
import { THEME_SCRIPT } from "@/lib/theme";

// Self-hosted (OFL-1.1, Latin subset) so builds never depend on fetching Google Fonts.
const jakarta = localFont({
  src: "./fonts/PlusJakartaSans-latin-variable.woff2",
  variable: "--font-jakarta",
  display: "swap",
  weight: "200 800",
});

const geistMono = localFont({
  src: "./fonts/GeistMono-latin-variable.woff2",
  variable: "--font-geist-mono",
  display: "swap",
  weight: "100 900",
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
    <html lang="id" className={`${jakarta.variable} ${geistMono.variable}`} suppressHydrationWarning>
      <head>
        {/* Applies the stored theme before the first paint (CSP allows inline scripts: 'unsafe-inline'). */}
        <script dangerouslySetInnerHTML={{ __html: THEME_SCRIPT }} />
      </head>
      <body className={`${jakarta.className} antialiased`}>
        <Providers>
          <AppShell>{children}</AppShell>
        </Providers>
      </body>
    </html>
  );
}
