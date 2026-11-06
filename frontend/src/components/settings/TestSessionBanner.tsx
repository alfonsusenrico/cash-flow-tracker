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
 * One neutral line under the top bar while a Sesi uji runs (owner decision 4, apply-markas-shell D9), because
 * notifications are then stored and not recorded. It reads through `api` directly, like the other
 * workspace-wide queries, and polls once a minute. It scrolls with the page.
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
    <div className="session-line" role="region" aria-label="Sesi uji">
      <Icon name="clock" className="h-4 w-4 session-ico" />
      <span className="session-t">
        Sesi uji berjalan sampai <span className="tabular-nums">{TIME.format(new Date(session.planned_end_at))}</span> · notifikasi
        disimpan, tidak dicatat
      </span>
      <Link href="/settings/sesi-uji" className="ws-btn ws-btn-quiet ws-btn-sm">
        Lihat sesi
      </Link>
      <button type="button" className="ws-btn ws-btn-quiet ws-btn-sm" disabled={end.isPending} onClick={() => end.mutate(session.id)}>
        {end.isPending ? "Mengakhiri…" : "Akhiri sesi"}
      </button>
    </div>
  );
}
