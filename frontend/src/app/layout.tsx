import type { Metadata } from "next";
import localFont from "next/font/local";
import "./globals.css";
import { Providers } from "./providers";
import { AppShell } from "@/components/layout/AppShell";

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
  title: "CashFlow — Daily Money & Digital Vault",
  description: "Friction-free daily money tracking, smart daily allowance, and liquid vault",
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
    <html lang="en" className={`${jakarta.variable} ${geistMono.variable}`} suppressHydrationWarning>
      <body className={`${jakarta.className} antialiased`}>
        <Providers>
          <AppShell>{children}</AppShell>
        </Providers>
      </body>
    </html>
  );
}
