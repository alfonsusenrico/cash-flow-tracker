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
  | "info"
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
  | "user"
  | "edit"
  | "trash"
  | "wallet"
  | "credit-card"
  | "creditCard"
  | "smartphone"
  | "tag"
  | "check"
  | "arrow-right"
  | "arrowRight"
  | "calendar"
  | "clock"
  | "download"
  | "home"
  | "inbox"
  | "receipt"
  | "target"
  | "file-text"
  | "help-circle"
  | "sliders"
  | "panel-left"
  | "bell"
  | "shield"
  | "arrow-up-right"
  | "arrow-down-left"
  | "layers"
  | "monitor"
  | "keyboard"
  | "menu"
  | "x"
  | "search"
  | "eye"
  | "eye-off"
  | "repeat"
  | "trending-up"
  | "chevron-left"
  | "chevron-right"
  | "alert-triangle"
  | "arrows-right-left"
  | "sparkles"
  | "chevron-down"
  | "chevron-up"
  | "arrow-up-down"
  | "grip-vertical"
  | "list"
  | "chart"
  | "refresh-cw"
  | "file"
  | "trend"
  | "panel"
  | "more"
  | "help";

interface IconProps extends SVGProps<SVGSVGElement> {
  name: IconName | string;
}

const paths: Record<string, ReactNode> = {
  info: <><circle cx="12" cy="12" r="10" /><path d="M12 11v5" /><path d="M12 8h.01" /></>,
  "alert-triangle": <><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3" /><path d="M12 9v4" /><path d="M12 17h.01" /></>,
  accounts: <><path d="M4 10h16" /><path d="M6 10v8" /><path d="M10 10v8" /><path d="M14 10v8" /><path d="M18 10v8" /><path d="M3 18h18" /><path d="M12 3 4 8h16l-8-5Z" /></>,
  allocation: <><path d="M4 5h16" /><path d="M4 12h10" /><path d="M4 19h7" /><path d="m17 14 3 3-3 3" /><path d="M14 17h6" /></>,
  analysis: <><path d="M4 19V5" /><path d="M4 19h16" /><path d="m7 15 3-4 4 2 5-7" /></>,
  buckets: <><path d="M6 8h12l-1 12H7L6 8Z" /><path d="M8 8a4 4 0 0 1 8 0" /></>,
  categories: <><path d="M5 5h6v6H5z" /><path d="M13 5h6v6h-6z" /><path d="M5 13h6v6H5z" /><path d="M13 13h6v6h-6z" /></>,
  dashboard: <><path d="M4 13h7V4H4z" /><path d="M13 20h7V4h-7z" /><path d="M4 20h7v-5H4z" /></>,
  goals: <><circle cx="12" cy="12" r="8" /><circle cx="12" cy="12" r="4" /><path d="M12 8v4l3 2" /></>,
  ledger: <><path d="M6 4h12v16H6z" /><path d="M9 8h6" /><path d="M9 12h6" /><path d="M9 16h4" /></>,
  move: <><path d="M7 7h10l-3-3" /><path d="m17 7-3 3" /><path d="M17 17H7l3 3" /><path d="m7 17 3-3" /></>,
  netWorth: <><path d="M4 19h16" /><path d="M7 16V9" /><path d="M12 16V5" /><path d="M17 16v-4" /></>,
  obligations: <><path d="M7 4h10v16H7z" /><path d="M10 8h4" /><path d="M10 12h4" /><path d="M10 16h2" /></>,
  strategy: <><path d="M5 19 19 5" /><path d="M7 7h6v6" /><path d="M11 17h6v-6" /></>,
  user: <><circle cx="12" cy="8" r="4" /><path d="M5 20a7 7 0 0 1 14 0" /></>,
  edit: <><path d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5Z" /><path d="m15 5 4 4" /></>,
  trash: <><path d="M3 6h18" /><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6" /><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2" /><line x1="10" x2="10" y1="11" y2="17" /><line x1="14" x2="14" y1="11" y2="17" /></>,
  "credit-card": <><rect width="20" height="14" x="2" y="5" rx="2" /><line x1="2" x2="22" y1="10" y2="10" /></>,
  creditCard: <><rect width="20" height="14" x="2" y="5" rx="2" /><line x1="2" x2="22" y1="10" y2="10" /></>,
  smartphone: <><rect width="14" height="20" x="5" y="2" rx="2" ry="2" /><path d="M12 18h.01" /></>,
  tag: <><path d="M12 2H2v10l9.29 9.29c.94.94 2.48.94 3.42 0l6.58-6.58c.94-.94.94-2.48 0-3.42L12 2Z" /><path d="M7 7h.01" /></>,
  check: <polyline points="20 6 9 17 4 12" />,
  "arrow-right": <><path d="M5 12h14" /><path d="m12 5 7 7-7 7" /></>,
  arrowRight: <><path d="M5 12h14" /><path d="m12 5 7 7-7 7" /></>,
  "chevron-left": <polyline points="15 18 9 12 15 6" />,
  "chevron-down": <polyline points="6 9 12 15 18 9" />,
  "chevron-up": <polyline points="18 15 12 9 6 15" />,
  "arrow-up-down": <><path d="m21 16-4 4-4-4" /><path d="M17 20V4" /><path d="m3 8 4-4 4 4" /><path d="M7 4v16" /></>,
  "grip-vertical": <><circle cx="9" cy="5" r="1" /><circle cx="9" cy="12" r="1" /><circle cx="9" cy="19" r="1" /><circle cx="15" cy="5" r="1" /><circle cx="15" cy="12" r="1" /><circle cx="15" cy="19" r="1" /></>,
  download: <><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" x2="12" y1="15" y2="3" /></>,
  sparkles: <path d="m12 3-1.9 5.8a2 2 0 0 1-1.3 1.3L3 12l5.8 1.9a2 2 0 0 1 1.3 1.3L12 21l1.9-5.8a2 2 0 0 1 1.3-1.3L21 12l-5.8-1.9a2 2 0 0 1-1.3-1.3L12 3Z" />,
  palette: <><circle cx="13.5" cy="6.5" r=".5" fill="currentColor" /><circle cx="17.5" cy="10.5" r=".5" fill="currentColor" /><circle cx="8.5" cy="7.5" r=".5" fill="currentColor" /><circle cx="6.5" cy="12.5" r=".5" fill="currentColor" /><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688 0-.437-.18-.835-.437-1.125-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z" /></>,
  menu: <><line x1="4" x2="20" y1="12" y2="12" /><line x1="4" x2="20" y1="6" y2="6" /><line x1="4" x2="20" y1="18" y2="18" /></>,
  "refresh-cw": <><path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" /><path d="M21 3v5h-5" /><path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" /><path d="M8 16H3v5" /></>,
  "trending-up": <><polyline points="22 7 13.5 15.5 8.5 10.5 2 17" /><polyline points="16 7 22 7 22 13" /></>,
  utensils: <><path d="M18 2v6a3 3 0 0 1-3 3 3 3 0 0 1-3-3V2" /><path d="M15 2v18" /><path d="M7 2v20" /><path d="M7 6H4a2 2 0 0 1-2-2V2" /></>,
  "shopping-cart": <><circle cx="8" cy="21" r="1" /><circle cx="19" cy="21" r="1" /><path d="M2.05 2.05h2l2.66 12.42a2 2 0 0 0 2 1.58h9.78a2 2 0 0 0 1.95-1.57l1.65-7.43H5.12" /></>,
  "shopping-bag": <><path d="M6 2 3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4Z" /><path d="M3 6h18" /><path d="M16 10a4 4 0 0 1-8 0" /></>,
  car: <><path d="M19 17h2c.6 0 1-.4 1-1v-3c0-.9-.7-1.7-1.5-1.9C18.7 10.6 16 10 16 10s-1.3-1.4-2.2-2.3c-.5-.4-1.1-.7-1.8-.7H5c-.6 0-1.1.4-1.4.9l-1.4 2.9A3.7 3.7 0 0 0 2 12v4c0 .6.4 1 1 1h2" /><circle cx="7" cy="17" r="2" /><path d="M9 17h6" /><circle cx="17" cy="17" r="2" /></>,
  zap: <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />,
  heart: <path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z" />,
  film: <><rect width="20" height="20" x="2" y="2" rx="2.18" ry="2.18" /><line x1="7" x2="7" y1="2" y2="22" /><line x1="17" x2="17" y1="2" y2="22" /><line x1="2" x2="22" y1="12" y2="12" /><line x1="2" x2="7" y1="7" y2="7" /><line x1="2" x2="7" y1="17" y2="17" /><line x1="17" x2="22" y1="17" y2="17" /><line x1="17" x2="22" y1="7" y2="7" /></>,
  "dollar-sign": <><line x1="12" x2="12" y1="2" y2="22" /><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" /></>,
  "plus-circle": <><circle cx="12" cy="12" r="10" /><path d="M12 8v8" /><path d="M8 12h8" /></>,
  "minus-circle": <><circle cx="12" cy="12" r="10" /><path d="M8 12h8" /></>,
  coffee: <><path d="M10 2v2" /><path d="M14 2v2" /><path d="M16 8a1 1 0 0 1 1 1v8a4 4 0 0 1-4 4H7a4 4 0 0 1-4-4V9a1 1 0 0 1 1-1h14a4 4 0 1 1 0 8h-1" /><path d="M6 2v2" /></>,
  users: <><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M22 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></>,
  receipt: <><path d="M4 2v20l2-1 2 1 2-1 2 1 2-1 2 1 2-1 2 1V2l-2 1-2-1-2 1-2-1-2 1-2-1-2 1Z" /><path d="M16 8h-6a2 2 0 1 0 0 4h4a2 2 0 1 1 0 4H8" /><path d="M12 17.5v-11" /></>,
  "arrows-right-left": <><path d="m16 3 4 4-4 4" /><path d="M20 7H4" /><path d="m8 21-4-4 4-4" /><path d="M4 17h16" /></>,
  repeat: <><path d="m17 2 4 4-4 4" /><path d="M3 11v-1a4 4 0 0 1 4-4h14" /><path d="m7 22-4-4 4-4" /><path d="M21 13v1a4 4 0 0 1-4 4H3" /></>,
  calendar: <><rect width="18" height="18" x="3" y="4" rx="2" /><line x1="16" x2="16" y1="2" y2="6" /><line x1="8" x2="8" y1="2" y2="6" /><line x1="3" x2="21" y1="10" y2="10" /></>,
  clock: <><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></>,
  shield: <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />,
  "arrow-up-right": <><path d="M7 17 17 7" /><path d="M8 7h9v9" /></>,
  "arrow-down-left": <><path d="M17 7 7 17" /><path d="M16 17H7V8" /></>,
  layers: <><path d="m12 2 9 5-9 5-9-5 9-5z" /><path d="m3 12 9 5 9-5" /><path d="m3 17 9 5 9-5" /></>,
  keyboard: <><rect x="2" y="6" width="20" height="12" rx="2" /><path d="M6 10h.01" /><path d="M10 10h.01" /><path d="M14 10h.01" /><path d="M18 10h.01" /><path d="M7 14h10" /></>,
};

/**
 * The prototype's own 24-grid drawings (option-6 `js/core.js`), used by the shell: navigation, top bar, phone
 * header, tab bar and the shell's menus (apply-markas-shell D12). The app's older names are aliases.
 */
const PROTOTYPE_PATHS: Record<string, string> = {
  home: "M3 10.5 12 3l9 7.5V21h-6v-6H9v6H3z",
  inbox: "M3 13h5l1.5 3h5l1.5-3h5M5.5 5h13L21 13v7H3v-7z",
  list: "M9 6h12M9 12h12M9 18h12M4 6h1M4 12h1M4 18h1",
  chart: "M4 20V11M10 20V5M16 20v-6M21 20H3",
  file: "M14 3H6v18h12V7zM14 3v4h4M9 13h6M9 17h6",
  wallet: "M3 7h16a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2zM3 7V6a2 2 0 0 1 2-2h11v3M16 13.5h2",
  target: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM12 7.5a4.5 4.5 0 1 0 0 9 4.5 4.5 0 0 0 0-9zM12 11.5v1",
  settings: "M4 6h9M17 6h3M4 12h3M11 12h9M4 18h11M19 18h1M15 4v4M9 10v4M17 16v4",
  search: "M11 4a7 7 0 1 0 0 14 7 7 0 0 0 0-14zM20.5 20.5 16 16",
  eye: "M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12zM12 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6z",
  eyeOff: "M3 3l18 18M10.6 5.1A10 10 0 0 1 12 5c6.5 0 10 7 10 7a17 17 0 0 1-3.2 4.1M6.6 6.6C3.8 8.4 2 12 2 12s3.5 7 10 7c1.9 0 3.5-.6 4.9-1.4M9.9 9.9a3 3 0 0 0 4.2 4.2",
  sun: "M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8zM12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4",
  moon: "M20.5 13.5A8.5 8.5 0 1 1 10.5 3.5a6.5 6.5 0 0 0 10 10z",
  monitor: "M3 4h18v12H3zM8 20h8M12 16v4",
  bell: "M6 9a6 6 0 1 1 12 0c0 6 2.5 8 2.5 8h-17S6 15 6 9M10.3 20.5a1.9 1.9 0 0 0 3.4 0",
  plus: "M12 5v14M5 12h14",
  x: "M6 6l12 12M18 6 6 18",
  chevR: "M9 18l6-6-6-6",
  help: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM9.2 9.2a2.9 2.9 0 0 1 5.6 1c0 1.9-2.8 2.6-2.8 2.6M12 16.5v.5",
  logout: "M9 20H5V4h4M15.5 16.5 20 12l-4.5-4.5M20 12H9",
  panel: "M3 4h18v16H3zM9 4v16",
  trend: "M4 17l5-5 4 4 7-8",
};

const PROTOTYPE_ALIASES: Record<string, string> = {
  "file-text": "file",
  sliders: "settings",
  "eye-off": "eyeOff",
  close: "x",
  "chevron-right": "chevR",
  "help-circle": "help",
  "panel-left": "panel",
};

/** The prototype draws its dotted icons as filled 1.6 px dots, not strokes. */
const DOTS: Record<string, [number, number][]> = {
  more: [[5, 12], [12, 12], [19, 12]],
};
const DOT_ALIASES: Record<string, string> = { "more-horizontal": "more" };

function drawing(name: string): ReactNode {
  const prototypeName = PROTOTYPE_ALIASES[name] ?? name;
  const prototypePath = PROTOTYPE_PATHS[prototypeName];
  if (prototypePath) return <path d={prototypePath} />;
  const dots = DOTS[DOT_ALIASES[name] ?? name];
  if (dots) {
    return dots.map(([x, y]) => <circle key={`${x}-${y}`} cx={x} cy={y} r="1.6" fill="currentColor" stroke="none" />);
  }
  return paths[name] || paths.tag;
}

export function Icon({ name, className, ...props }: IconProps) {
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className ?? "h-4 w-4"}
      {...props}
    >
      {drawing(name)}
    </svg>
  );
}
