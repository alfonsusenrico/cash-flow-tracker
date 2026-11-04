"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Icon } from "@/components/ui/Icon";

interface CurrentTestSession {
  id: string;
  status: "open" | "ended";
  planned_end_at: string;
}

const TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
const PATH = "/ingest/test-sessions";

/**
 * Shown on every page while a Sesi uji runs (notification-test-session D8), so a forgotten session is noticed.
 * It reads through `api` directly, like the other workspace-wide queries, and polls once a minute.
 */
export function TestSessionBanner({ enabled }: { enabled: boolean }) {
  const queryClient = useQueryClient();
  const { data } = useQuery<{ ok: boolean; session: CurrentTestSession | null }>({
    queryKey: queryKeys.testSessions.current,
    queryFn: () => api.get(`${PATH}/current`),
    enabled,
    refetchInterval: 60000,
  });
  const end = useMutation({
    mutationFn: (id: string) => api.post(`${PATH}/${id}/end`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.testSessions.all }),
  });
  const session = data?.session;
  if (!session || session.status !== "open") return null;
  return (
    <div className="ws-banner ws-banner-warn ws-plan-notice ws-test-session-banner" role="region" aria-label="Sesi uji">
      <Icon name="clock" className="h-4 w-4 shrink-0" />
      <span className="ws-plan-notice-t tabular-nums">
        Sesi uji berjalan sampai <b>{TIME.format(new Date(session.planned_end_at))}</b> · notifikasi disimpan tanpa dicatat
      </span>
      <Link href="/settings/sesi-uji" className="ws-btn ws-btn-sm">Lihat sesi</Link>
      <button type="button" className="ws-btn ws-btn-sm" disabled={end.isPending} onClick={() => end.mutate(session.id)}>
        {end.isPending ? "Mengakhiri…" : "Akhiri sesi"}
      </button>
    </div>
  );
}
