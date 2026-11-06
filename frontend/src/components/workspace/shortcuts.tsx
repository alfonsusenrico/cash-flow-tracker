"use client";

import {
  createContext,
  type ReactNode,
  type RefObject,
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { closeTopLayer } from "./layers";
import { ShellDialog } from "./ShellDialog";

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
 * A page's own keys, listed in the help only while the page is mounted, each ending in the group's name
 * ("Di Tinjau"). The keys themselves are registered with `useShortcut` and a scope; Enter stays native and is
 * only described.
 */
export interface PageShortcutGroup {
  name: string;
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

  // Where focus returns when the help closes; recorded before the dialog takes focus.
  const helpOpener = useRef<HTMLElement | null>(null);
  const openHelp = useCallback(() => {
    helpOpener.current = document.activeElement as HTMLElement | null;
    setHelpOpen(true);
  }, []);
  const closeHelp = useCallback(() => {
    setHelpOpen(false);
    helpOpener.current?.focus?.();
    helpOpener.current = null;
  }, []);

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
      <HelpDialog open={helpOpen} onClose={closeHelp} />
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
  { keys: ["⌘K", "Ctrl K"], text: "Cari transaksi" },
  { keys: ["N"], text: "Catat transaksi" },
  { keys: ["?"], text: "Bantuan ini" },
  { keys: ["Esc"], text: "Tutup lapisan teratas" },
];

/** Three facts about how the app works, in the prototype's wording (D8). */
const HELP_FACTS = [
  "HP mengirim notifikasi bank; transaksi tercatat sendiri.",
  "Yang belum pasti masuk ke Tinjau. Jawab sekali, kebiasaan diingat.",
  "Beranda menunjukkan berapa yang aman dipakai sampai gajian.",
];

/** A page key reads "… di Tinjau": the group's name ("Di Tinjau") closes the sentence. */
function pageKeyText(group: PageShortcutGroup, entry: ShortcutHelpEntry): string {
  const where = group.name.charAt(0).toLowerCase() + group.name.slice(1);
  return `${entry.text} ${where}`;
}

/**
 * "Bantuan & pintasan" (D8): the facts, the global keys and the open page's keys in one table, and where the
 * single-key shortcuts are switched off. The switch itself lives in Pengaturan › Tampilan.
 */
export function HelpDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const context = useContext(ShortcutContext);

  const rows = [
    ...GLOBAL_KEYS,
    ...(context?.pageGroups ?? []).flatMap((group) => group.entries.map((entry) => ({ keys: entry.keys, text: pageKeyText(group, entry) }))),
  ];

  return (
    <ShellDialog open={open} onClose={onClose} title="Bantuan & pintasan" width={560} closeLabel="Tutup bantuan">
      <ol className="help-facts">
        {HELP_FACTS.map((fact) => (
          <li key={fact} className="t-body">
            {fact}
          </li>
        ))}
      </ol>
      <table className="help-keys">
        <caption className="sr-only">Pintasan keyboard</caption>
        <thead>
          <tr>
            <th scope="col">Tombol</th>
            <th scope="col">Fungsi</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.text}>
              <td>
                <kbd className="ws-kbd">{row.keys.join(" / ")}</kbd>
              </td>
              <td className="t-body">{row.text}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="t-meta">Pintasan satu tombol bisa dimatikan di Pengaturan › Tampilan.</p>
    </ShellDialog>
  );
}
