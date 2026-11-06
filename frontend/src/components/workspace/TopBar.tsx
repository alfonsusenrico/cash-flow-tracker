"use client";

import { usePathname, useRouter } from "next/navigation";
import { useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { THEME_CHOICES, type ThemeChoice } from "@/lib/theme";
import { useWorkspace } from "./context";
import { MenuButton } from "./Menu";
import { LEDGER_HREF, LEDGER_SEARCH_HREF } from "./navigation";
import { NotificationBell } from "./Notifications";
import { useShortcut } from "./shortcuts";

/** The id of Transaksi's own search field, which ⌘K focuses while Transaksi is open (D3). */
export const LEDGER_SEARCH_ID = "ledger-search";
export const TOPBAR_SEARCH_ID = "topbar-search";

const THEME_ICONS: Record<ThemeChoice, "sun" | "moon" | "monitor"> = { light: "sun", dark: "moon", system: "monitor" };

/** "Sembunyikan saldo" as a pressed-state icon button (D10); the name stays the same in both states. */
export function HideBalancesButton({ iconSize }: { iconSize: 16 | 20 }) {
  const t = useTranslations("shell");
  const { hideBalances, toggleHideBalances } = useWorkspace();
  return (
    <button type="button" className="ws-icon-btn" aria-pressed={hideBalances} aria-label={t("hideBalances")} onClick={toggleHideBalances}>
      <Icon name={hideBalances ? "eye-off" : "eye"} className={iconSize === 20 ? "h-5 w-5" : "h-4 w-4"} />
    </button>
  );
}

/** The theme entries of the Tema menu, shared by the top bar and Lainnya. */
export function useThemeEntries() {
  const t = useTranslations("common.theme");
  const { themeChoice, setThemeChoice } = useWorkspace();
  return THEME_CHOICES.map((choice) => ({
    key: choice.value,
    label: t(choice.value),
    icon: THEME_ICONS[choice.value],
    checked: choice.value === themeChoice,
    onSelect: () => setThemeChoice(choice.value),
  }));
}

function ThemeMenu() {
  const t = useTranslations("common.theme");
  const { resolvedTheme } = useWorkspace();
  const entries = useThemeEntries();
  return (
    <MenuButton label={t("label")} className="ws-icon-btn" menuLabel={t("label")} heading={t("label")} entries={entries} width={220}>
      <Icon name={resolvedTheme === "dark" ? "moon" : "sun"} className="h-4 w-4" />
    </MenuButton>
  );
}

/** ⌘K / Ctrl+K: Transaksi's own search there, else the top-bar search, else (phone) Transaksi with its search. */
function useSearchShortcut(input: React.RefObject<HTMLInputElement | null>) {
  const router = useRouter();
  const pathname = usePathname();
  useShortcut("mod+k", () => {
    const pageSearch = pathname === LEDGER_HREF ? document.getElementById(LEDGER_SEARCH_ID) : null;
    if (pageSearch) pageSearch.focus();
    else if (input.current && input.current.offsetParent !== null) input.current.focus();
    else router.push(LEDGER_SEARCH_HREF);
  });
}

function SearchBox() {
  const router = useRouter();
  const t = useTranslations("shell");
  const input = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState("");
  useSearchShortcut(input);
  return (
    <form
      role="search"
      className="search"
      onSubmit={(event) => {
        event.preventDefault();
        const text = query.trim();
        router.push(text ? `${LEDGER_HREF}?q=${encodeURIComponent(text)}` : LEDGER_HREF);
      }}
    >
      <span className="ico" aria-hidden="true">
        <Icon name="search" className="h-4 w-4" />
      </span>
      <input
        ref={input}
        id={TOPBAR_SEARCH_ID}
        type="search"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder={t("searchPlaceholder")}
        aria-label={t("searchTransactions")}
        aria-keyshortcuts="Meta+K Control+K"
        autoComplete="off"
      />
      <kbd className="ws-kbd kbd" aria-hidden="true">
        ⌘K
      </kbd>
    </form>
  );
}

/** The sticky 56 px top bar with global actions only (D3): search, hide balances, theme, notifications, + Catat. */
export function TopBar() {
  const t = useTranslations("shell");
  const { openCapture } = useWorkspace();
  return (
    <header className="topbar">
      <SearchBox />
      <span className="spacer" />
      <HideBalancesButton iconSize={16} />
      <ThemeMenu />
      <NotificationBell iconSize={16} id="bell" />
      <button type="button" className="ws-btn ws-btn-accent" onClick={openCapture} aria-keyshortcuts="N">
        <Icon name="plus" className="h-4 w-4" />
        {t("add")}
      </button>
    </header>
  );
}
