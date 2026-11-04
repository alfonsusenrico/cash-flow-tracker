"use client";

import { createContext, type ReactNode, useContext, useEffect } from "react";
import { createPortal } from "react-dom";
import type { ThemeChoice, ResolvedTheme } from "@/lib/theme";

export interface WorkspaceNotification {
  id: string;
  title: string;
  detail: string;
  href: string;
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
  controlsSlot: HTMLElement | null;
  setControlsSlot: (element: HTMLElement | null) => void;
}

export const WorkspaceContext = createContext<WorkspaceValue | null>(null);

export function useWorkspace(): WorkspaceValue {
  const value = useContext(WorkspaceContext);
  if (!value) throw new Error("useWorkspace needs the workspace shell");
  return value;
}

/** Lets a page write its own top-bar title and subtitle (Beranda: the date and the period line). */
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

/** Renders page controls, such as a period stepper, in the top bar's controls slot (inline outside the shell). */
export function TopBarControls({ children }: { children: ReactNode }) {
  const context = useContext(WorkspaceContext);
  if (!context) return <>{children}</>;
  if (!context.controlsSlot) return null;
  return createPortal(children, context.controlsSlot);
}
