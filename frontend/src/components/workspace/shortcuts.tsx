"use client";

import {
  createContext,
  type ReactNode,
  type RefObject,
  useCallback,
  useContext,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Icon } from "@/components/ui/Icon";
import { closeTopLayer, syncDialog, useEscapeLayer, useFocusTrap } from "./layers";

/**
 * One registry owns the keyboard shortcuts (design D8). "mod+k" means ⌘K or Ctrl+K and works everywhere.
 * Single-key shortcuts are skipped while typing in a field and can be switched off (WCAG 2.1.4); a
 * scoped shortcut fires only while focus is inside its region, so page keys never leak elsewhere.
 */
export interface ShortcutDefinition {
  /** Lower-case single character such as "n" or "?", or "mod+k". */
  key: string;
  handler: (event: KeyboardEvent) => void;
  /** Region the shortcut belongs to; it fires only while focus is inside. */
  scope?: RefObject<HTMLElement | null>;
}

export const SHORTCUTS_STORAGE_KEY = "shortcuts";

/** One line of the help dialog: the keys and what they do. */
export interface ShortcutHelpEntry {
  keys: string[];
  text: string;
}

/**
 * A page's own keys, listed in the help under the page's name only while the page is mounted. The keys
 * themselves are registered with `useShortcut` and a scope; Enter stays native and is only described.
 */
export interface PageShortcutGroup {
  name: string;
  /** How the switch caption names these keys, for example "tombol satu huruf di Tinjau". */
  caption: string;
  entries: ShortcutHelpEntry[];
}

interface ShortcutContextValue {
  register: (definition: ShortcutDefinition) => () => void;
  registerPageGroup: (group: PageShortcutGroup) => () => void;
  pageGroups: PageShortcutGroup[];
  singleKeysEnabled: boolean;
  setSingleKeysEnabled: (enabled: boolean) => void;
  openHelp: () => void;
}

const ShortcutContext = createContext<ShortcutContextValue | null>(null);

function readSingleKeysEnabled(): boolean {
  try {
    return window.localStorage.getItem(SHORTCUTS_STORAGE_KEY) !== "off";
  } catch {
    return true;
  }
}

export function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || target.isContentEditable;
}

function matches(definition: ShortcutDefinition, event: KeyboardEvent): boolean {
  if (definition.key === "mod+k") return (event.metaKey || event.ctrlKey) && !event.altKey && event.key.toLowerCase() === "k";
  if (event.metaKey || event.ctrlKey || event.altKey) return false;
  return event.key.toLowerCase() === definition.key;
}

export function ShortcutProvider({ children }: { children: ReactNode }) {
  const registry = useRef(new Set<ShortcutDefinition>());
  const [singleKeysEnabled, setEnabledState] = useState(true);
  const [helpOpen, setHelpOpen] = useState(false);
  const [pageGroups, setPageGroups] = useState<PageShortcutGroup[]>([]);
  const enabledRef = useRef(true);

  useEffect(() => {
    const stored = readSingleKeysEnabled();
    enabledRef.current = stored;
    setEnabledState(stored);
  }, []);

  const setSingleKeysEnabled = useCallback((enabled: boolean) => {
    enabledRef.current = enabled;
    setEnabledState(enabled);
    try {
      window.localStorage.setItem(SHORTCUTS_STORAGE_KEY, enabled ? "on" : "off");
    } catch {
      // The switch still applies until the page closes.
    }
  }, []);

  const register = useCallback((definition: ShortcutDefinition) => {
    registry.current.add(definition);
    return () => {
      registry.current.delete(definition);
    };
  }, []);

  const registerPageGroup = useCallback((group: PageShortcutGroup) => {
    setPageGroups((groups) => [...groups, group]);
    return () => setPageGroups((groups) => groups.filter((item) => item !== group));
  }, []);

  const openHelp = useCallback(() => setHelpOpen(true), []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented) return;
      if (event.key === "Escape") {
        if (closeTopLayer()) event.preventDefault();
        return;
      }
      const definitions = [...registry.current].filter((definition) => matches(definition, event));
      if (!definitions.length) return;
      const single = !definitions[0].key.startsWith("mod+");
      if (single && (!enabledRef.current || isTypingTarget(event.target))) return;
      const active = document.activeElement;
      const scoped = definitions.find((definition) => definition.scope?.current?.contains(active));
      const chosen = scoped ?? definitions.find((definition) => !definition.scope);
      if (!chosen) return;
      event.preventDefault();
      chosen.handler(event);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const value = useMemo(
    () => ({ register, registerPageGroup, pageGroups, singleKeysEnabled, setSingleKeysEnabled, openHelp }),
    [register, registerPageGroup, pageGroups, singleKeysEnabled, setSingleKeysEnabled, openHelp],
  );

  return (
    <ShortcutContext.Provider value={value}>
      {children}
      <ShortcutHelpRegistration onOpen={openHelp} />
      <HelpDialog open={helpOpen} onClose={() => setHelpOpen(false)} />
    </ShortcutContext.Provider>
  );
}

export function useShortcuts(): ShortcutContextValue {
  const context = useContext(ShortcutContext);
  if (!context) throw new Error("useShortcuts needs a ShortcutProvider");
  return context;
}

/** Registers a shortcut while the calling component is mounted. */
export function useShortcut(key: string, handler: (event: KeyboardEvent) => void, scope?: RefObject<HTMLElement | null>) {
  const { register } = useShortcuts();
  const handlerRef = useRef(handler);
  useLayoutEffect(() => {
    handlerRef.current = handler;
  }, [handler]);
  useEffect(() => register({ key, scope, handler: (event) => handlerRef.current(event) }), [register, key, scope]);
}

/** Lists a page's keys in the help while the calling component is mounted; pass a stable object. */
export function usePageShortcutGroup(group: PageShortcutGroup) {
  const { registerPageGroup } = useShortcuts();
  useEffect(() => registerPageGroup(group), [registerPageGroup, group]);
}

function ShortcutHelpRegistration({ onOpen }: { onOpen: () => void }) {
  useShortcut("?", onOpen);
  return null;
}

const GLOBAL_KEYS: ShortcutHelpEntry[] = [
  { keys: ["⌘", "K"], text: "Cari transaksi (Ctrl+K di Windows)" },
  { keys: ["N"], text: "Catat transaksi" },
  { keys: ["?"], text: "Buka bantuan ini" },
  { keys: ["Esc"], text: "Tutup penjelasan, menu, atau jendela yang terbuka" },
];

function KeyList({ entries }: { entries: ShortcutHelpEntry[] }) {
  return (
    <dl className="ws-keys">
      {entries.map((entry) => (
        <div key={entry.text} className="contents">
          <dt>
            {entry.keys.map((key) => (
              <kbd key={key} className="ws-kbd mr-1">
                {key}
              </kbd>
            ))}
          </dt>
          <dd>{entry.text}</dd>
        </div>
      ))}
    </dl>
  );
}

function switchCaption(groups: PageShortcutGroup[]): string {
  if (!groups.length) return "N dan ? jalan tanpa tombol lain.";
  return `N, ?, dan ${groups.map((group) => group.caption).join(", ")} jalan tanpa tombol lain.`;
}

export function HelpDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const ref = useRef<HTMLDialogElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  const titleId = useId();
  const context = useContext(ShortcutContext);

  useEffect(() => {
    if (open) returnFocus.current = document.activeElement as HTMLElement | null;
    syncDialog(ref.current, open);
    if (!open) returnFocus.current?.focus?.();
  }, [open]);
  useFocusTrap(ref, open);
  useEscapeLayer(open, onClose);

  return (
    <dialog
      ref={ref}
      className="ws-dialog"
      aria-labelledby={titleId}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
    >
      {open && (
        <>
          <div className="ws-dialog-h">
            <h2 id={titleId}>Pintasan keyboard</h2>
            <button type="button" className="ws-close" aria-label="Tutup bantuan" onClick={onClose}>
              <Icon name="x" className="h-4 w-4" />
            </button>
          </div>
          <KeyList entries={GLOBAL_KEYS} />
          {context?.pageGroups.map((group) => (
            <section key={group.name} aria-label={group.name}>
              <h3 className="ws-keys-h">{group.name}</h3>
              <KeyList entries={group.entries} />
            </section>
          ))}
          {context && (
            <button
              type="button"
              role="switch"
              aria-checked={context.singleKeysEnabled}
              className="ws-switch"
              onClick={() => context.setSingleKeysEnabled(!context.singleKeysEnabled)}
            >
              <span>
                <span className="block font-medium">Pintasan satu tombol</span>
                <span className="block text-[13px] text-[var(--muted)]">
                  {switchCaption(context.pageGroups)} Matikan jika bentrok dengan pembaca layar atau alat bantu lain.
                </span>
              </span>
              <span className="ws-switch-track" aria-hidden="true" />
            </button>
          )}
          <p className="ws-caption mt-3">Angka bergaris bawah titik-titik dapat diklik untuk melihat rincian hitungannya.</p>
        </>
      )}
    </dialog>
  );
}
