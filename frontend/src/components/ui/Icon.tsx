"use client";

import type { ReactNode, SVGProps } from "react";

export type IconName =
  | "accounts"
  | "allocation"
  | "analysis"
  | "buckets"
  | "categories"
  | "dashboard"
  | "goals"
  | "ledger"
  | "logout"
  | "moon"
  | "move"
  | "netWorth"
  | "obligations"
  | "plus"
  | "settings"
  | "strategy"
  | "sun"
  | "user";

interface IconProps extends SVGProps<SVGSVGElement> {
  name: IconName;
}

const paths: Record<IconName, ReactNode> = {
  accounts: <><path d="M4 10h16" /><path d="M6 10v8" /><path d="M10 10v8" /><path d="M14 10v8" /><path d="M18 10v8" /><path d="M3 18h18" /><path d="M12 3 4 8h16l-8-5Z" /></>,
  allocation: <><path d="M4 5h16" /><path d="M4 12h10" /><path d="M4 19h7" /><path d="m17 14 3 3-3 3" /><path d="M14 17h6" /></>,
  analysis: <><path d="M4 19V5" /><path d="M4 19h16" /><path d="m7 15 3-4 4 2 5-7" /></>,
  buckets: <><path d="M6 8h12l-1 12H7L6 8Z" /><path d="M8 8a4 4 0 0 1 8 0" /></>,
  categories: <><path d="M5 5h6v6H5z" /><path d="M13 5h6v6h-6z" /><path d="M5 13h6v6H5z" /><path d="M13 13h6v6h-6z" /></>,
  dashboard: <><path d="M4 13h7V4H4z" /><path d="M13 20h7V4h-7z" /><path d="M4 20h7v-5H4z" /></>,
  goals: <><circle cx="12" cy="12" r="8" /><circle cx="12" cy="12" r="4" /><path d="M12 8v4l3 2" /></>,
  ledger: <><path d="M6 4h12v16H6z" /><path d="M9 8h6" /><path d="M9 12h6" /><path d="M9 16h4" /></>,
  logout: <><path d="M10 6H6v12h4" /><path d="M14 8l4 4-4 4" /><path d="M8 12h10" /></>,
  moon: <path d="M20 14.5A7.5 7.5 0 0 1 9.5 4 8.5 8.5 0 1 0 20 14.5Z" />,
  move: <><path d="M7 7h10l-3-3" /><path d="m17 7-3 3" /><path d="M17 17H7l3 3" /><path d="m7 17 3-3" /></>,
  netWorth: <><path d="M4 19h16" /><path d="M7 16V9" /><path d="M12 16V5" /><path d="M17 16v-4" /></>,
  obligations: <><path d="M7 4h10v16H7z" /><path d="M10 8h4" /><path d="M10 12h4" /><path d="M10 16h2" /></>,
  plus: <><path d="M12 5v14" /><path d="M5 12h14" /></>,
  settings: <><circle cx="12" cy="12" r="3" /><path d="M19 12a7 7 0 0 0-.1-1l2-1.5-2-3.4-2.4 1a7 7 0 0 0-1.8-1L14.4 3h-4.8l-.3 3.1a7 7 0 0 0-1.8 1l-2.4-1-2 3.4L5.1 11a7 7 0 0 0 0 2l-2 1.5 2 3.4 2.4-1a7 7 0 0 0 1.8 1l.3 3.1h4.8l.3-3.1a7 7 0 0 0 1.8-1l2.4 1 2-3.4-2-1.5c.1-.3.1-.7.1-1Z" /></>,
  strategy: <><path d="M5 19 19 5" /><path d="M7 7h6v6" /><path d="M11 17h6v-6" /></>,
  sun: <><circle cx="12" cy="12" r="4" /><path d="M12 2v2" /><path d="M12 20v2" /><path d="m4.9 4.9 1.4 1.4" /><path d="m17.7 17.7 1.4 1.4" /><path d="M2 12h2" /><path d="M20 12h2" /><path d="m4.9 19.1 1.4-1.4" /><path d="m17.7 6.3 1.4-1.4" /></>,
  user: <><circle cx="12" cy="8" r="4" /><path d="M5 20a7 7 0 0 1 14 0" /></>,
};

export function Icon({ name, className, ...props }: IconProps) {
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className ?? "h-4 w-4"}
      {...props}
    >
      {paths[name]}
    </svg>
  );
}
