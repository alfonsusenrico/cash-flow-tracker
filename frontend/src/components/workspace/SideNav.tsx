"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { Icon } from "@/components/ui/Icon";
import { getDevices, type PairedDevice } from "@/lib/api";
import { deviceProblems, isStale } from "@/lib/deviceHealth";
import { queryKeys } from "@/lib/queryKeys";
import { sameRouteClick } from "@/lib/urlState";
import { useWorkspace } from "./context";
import { NAV_GROUPS, PRODUCT_NAME, SETTINGS_LINK, badgeWords, isActivePath, type NavLink } from "./navigation";

function badgeCount(link: NavLink, reviewAttention: number | null, reportBadge: boolean): number {
  if (link.badge === "review") return reviewAttention ?? 0;
  if (link.badge === "report") return reportBadge ? 1 : 0;
  return 0;
}

export function SideNavLink({ link, count }: { link: NavLink; count: number }) {
  const pathname = usePathname();
  const active = isActivePath(pathname, link.href);
  const words = count && link.badge ? `, ${badgeWords(link.badge, count)}` : "";
  return (
    <Link
      href={link.href}
      onClick={sameRouteClick(link.href, pathname)}
      className="ws-nav-item"
      aria-current={active ? "page" : undefined}
      aria-label={`${link.label}${words}`}
    >
      <Icon name={link.icon} />
      <span className="ws-rail-hide">{link.label}</span>
      {count > 0 && (
        <>
          <span className="ws-count ws-rail-hide" aria-hidden="true">
            {count > 99 ? "99+" : count}
          </span>
          <span className="ws-rail-count" aria-hidden="true">
            {count > 99 ? "99+" : count}
          </span>
        </>
      )}
      <span className="ws-tip" aria-hidden="true">
        {link.label}
      </span>
    </Link>
  );
}

export function SideNav() {
  const { reviewAttention, reportBadge, collapsed, toggleCollapsed, openHelp } = useWorkspace();
  return (
    <aside className="ws-side" aria-label="Navigasi samping">
      <div className="ws-brand">
        <span className="ws-brand-mark" aria-hidden="true">
          <Icon name="layers" className="h-4 w-4" />
        </span>
        <span className="ws-brand-name ws-rail-hide">{PRODUCT_NAME}</span>
        <button
          type="button"
          className="ws-collapse"
          aria-label={collapsed ? "Lebarkan menu samping" : "Ciutkan menu samping"}
          aria-pressed={collapsed}
          title={collapsed ? "Lebarkan menu samping" : "Ciutkan menu samping"}
          onClick={toggleCollapsed}
        >
          <Icon name="panel-left" className="h-[18px] w-[18px]" />
        </button>
      </div>
      <nav className="ws-nav" aria-label="Halaman">
        {NAV_GROUPS.map((group) => (
          <div key={group.label} className="ws-nav-group" role="group" aria-labelledby={`nav-${group.label}`}>
            <div className="ws-nav-h ws-rail-hide" id={`nav-${group.label}`}>
              {group.label}
            </div>
            {group.links.map((link) => (
              <SideNavLink key={link.href} link={link} count={badgeCount(link, reviewAttention, reportBadge)} />
            ))}
          </div>
        ))}
      </nav>
      <div className="ws-side-foot">
        <CompanionStatus />
        <button type="button" className="ws-nav-item" aria-label="Bantuan dan pintasan keyboard" onClick={openHelp}>
          <Icon name="help-circle" />
          <span className="ws-rail-hide">Bantuan</span>
          <span className="ws-tip" aria-hidden="true">
            Bantuan
          </span>
        </button>
        <SideNavLink link={SETTINGS_LINK} count={0} />
      </div>
    </aside>
  );
}

export interface CompanionState {
  tone: "ok" | "warn" | "none";
  title: string;
  detail: string;
}

const PAIRING_HREF = "/settings/perangkat";

function clock(iso: string, now: Date): string {
  const at = new Date(iso);
  const time = at.toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
  const day = (date: Date) => date.toLocaleDateString("id-ID", { timeZone: "Asia/Jakarta" });
  if (day(at) === day(now)) return time;
  const yesterday = new Date(now.getTime() - 86_400_000);
  if (day(at) === day(yesterday)) return `kemarin ${time}`;
  return at.toLocaleDateString("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });
}

/** Ready with the last contact, needs attention with the reason, or no phone paired. */
export function companionState(devices: PairedDevice[], now = new Date()): CompanionState {
  const paired = devices.filter((device) => device.created_via === "pairing");
  if (!paired.length) return { tone: "none", title: "Belum ada HP terhubung", detail: "Sambungkan HP di Pengaturan" };
  const reporting = paired
    .filter((device) => device.health)
    .sort((a, b) => Date.parse(b.health!.reported_at) - Date.parse(a.health!.reported_at));
  const device = reporting[0];
  if (!device?.health) return { tone: "warn", title: "HP perlu dicek", detail: "HP terhubung tapi belum kirim kabar" };
  const health = device.health;
  const lastContact = health.last_capture_at ?? device.last_used_at ?? health.reported_at;
  if (health.ready && !isStale(health, now.getTime())) {
    return { tone: "ok", title: "HP terhubung", detail: `Terakhir menangkap ${clock(lastContact, now)}` };
  }
  const problem = deviceProblems(health, now.getTime())[0] ?? "HP belum siap menangkap notifikasi.";
  return { tone: "warn", title: "HP perlu dicek", detail: problem };
}

export function CompanionStatus() {
  const { data, isError } = useQuery({ queryKey: queryKeys.devices, queryFn: getDevices, refetchInterval: 60_000 });
  if (isError || !data) return null;
  const state = companionState(data.devices ?? []);
  const name = `${state.title}, ${state.detail}`;
  return (
    <Link href={PAIRING_HREF} className="ws-companion" aria-label={name}>
      <span className="ws-dot" data-tone={state.tone} aria-hidden="true" />
      <span className="ws-rail-hide" aria-hidden="true">
        <strong>{state.title}</strong>
        {state.detail}
      </span>
      <span className="ws-tip" aria-hidden="true">
        {name}
      </span>
    </Link>
  );
}
