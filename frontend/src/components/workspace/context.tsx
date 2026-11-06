"use client";

import { createContext, type ReactNode, useContext, useEffect } from "react";
import { createPortal } from "react-dom";
import { useTranslations } from "next-intl";
import { Icon, type IconName } from "@/components/ui/Icon";
import type { ThemeChoice, ResolvedTheme } from "@/lib/theme";
import { MenuButton } from "./Menu";
import { PHONE_QUERY, useMediaQuery } from "./layers";
import { useInterfaceLang } from "./untranslated";

export interface WorkspaceNotification {
  /** Includes the report end date or the plan date, so a new occurrence is new again (D6). */
  id: string;
  title: string;
  detail: string;
  /** Where the item leads; absent for items that open "Rencana jatuh tempo" instead (D7). */
  href?: string;
  opensDuePlans?: boolean;
}

export interface PageHeader {
  title: string;
  subtitle: string;
}

export interface WorkspaceValue {
  username: string | null;
  /** The account's own name; null until set (Pengaturan · Profil). */
  displayName: string | null;
  reviewAttention: number | null;
  reportBadge: boolean;
  notifications: WorkspaceNotification[];
  /** Ids not yet seen in this browser; every item counts when the seen state is unavailable. */
  unreadIds: ReadonlySet<string>;
  markNotificationsSeen: () => void;
  openDuePlans: () => void;
  themeChoice: ThemeChoice;
  resolvedTheme: ResolvedTheme;
  setThemeChoice: (choice: ThemeChoice) => void;
  hideBalances: boolean;
  toggleHideBalances: () => void;
  collapsed: boolean;
  toggleCollapsed: () => void;
  openCapture: () => void;
  openHelp: () => void;
  logout: () => void;
  header: PageHeader | null;
  setHeader: (header: PageHeader | null) => void;
  actionsSlot: HTMLElement | null;
  setActionsSlot: (element: HTMLElement | null) => void;
}

export const WorkspaceContext = createContext<WorkspaceValue | null>(null);

export function useWorkspace(): WorkspaceValue {
  const value = useContext(WorkspaceContext);
  if (!value) throw new Error("useWorkspace needs the workspace shell");
  return value;
}

/** Lets a page write its own title and subtitle in the title block (Beranda: the date and the period line). */
export function usePageHeader(header: PageHeader | null) {
  const context = useContext(WorkspaceContext);
  const setHeader = context?.setHeader;
  const title = header?.title;
  const subtitle = header?.subtitle;
  useEffect(() => {
    if (!setHeader || title === undefined) return;
    setHeader({ title, subtitle: subtitle ?? "" });
    return () => setHeader(null);
  }, [setHeader, title, subtitle]);
}

export interface PageMoreAction {
  key: string;
  label: string;
  icon: IconName;
  onSelect?: () => void;
  href?: string;
  /** Listed in "…" only below 768 px, where its visible button is hidden (Perbarui harga on Rekening). */
  phoneOnly?: boolean;
}

/**
 * A page's actions in the title block (D4): at most two visible controls as children, the rest in the "…" menu
 * labelled "Tindakan lain". The page keeps every handler; only where the controls render changes. Outside the
 * shell (page tests) they render in place.
 */
export function PageActions({ children, more = [] }: { children?: ReactNode; more?: PageMoreAction[] }) {
  const context = useContext(WorkspaceContext);
  const t = useTranslations("common");
  const lang = useInterfaceLang();
  const phone = useMediaQuery(PHONE_QUERY);
  const listed = more.filter((action) => phone || !action.phoneOnly);
  const content = (
    <>
      {children}
      {listed.length > 0 && (
        <MenuButton
          label={t("moreActions")}
          lang={lang}
          className="ws-icon-btn"
          menuLabel={t("moreActions")}
          width={260}
          entries={listed.map((action) => ({
            key: action.key,
            label: action.label,
            icon: action.icon,
            href: action.href,
            onSelect: action.onSelect,
          }))}
        >
          <Icon name="more" className="h-4 w-4" />
        </MenuButton>
      )}
    </>
  );
  if (!context) return content;
  if (!context.actionsSlot) return null;
  return createPortal(content, context.actionsSlot);
}
