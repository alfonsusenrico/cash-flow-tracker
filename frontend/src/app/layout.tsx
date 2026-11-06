import type { Metadata } from "next";
import localFont from "next/font/local";
import { getLocale, getMessages, getTranslations } from "next-intl/server";
import { IntlProvider } from "@/i18n/IntlProvider";
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

/** The title is the product name; the description is in the resolved language (add-language-system D12). */
export async function generateMetadata(): Promise<Metadata> {
  const t = await getTranslations("shell");
  return {
    title: PRODUCT_NAME,
    description: t("metadataDescription"),
    icons: {
      icon: "/icon.png",
      apple: "/icon.png",
    },
  };
}

export default async function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const [locale, messages] = await Promise.all([getLocale(), getMessages()]);
  return (
    <html lang={locale} className={`${inter.variable} ${manrope.variable}`} suppressHydrationWarning>
      <head>
        {/* Applies the stored theme before the first paint (CSP allows inline scripts: 'unsafe-inline'). */}
        <script dangerouslySetInnerHTML={{ __html: THEME_SCRIPT }} />
      </head>
      <body>
        <IntlProvider locale={locale} messages={messages}>
          <Providers>
            <AppShell>{children}</AppShell>
          </Providers>
        </IntlProvider>
      </body>
    </html>
  );
}
