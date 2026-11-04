"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  ApiError, deleteTestSession, endTestSession, getTestSessionEvents, getTestSessions, labelTestSessionEvent,
  startTestSession, testSessionCsvUrl, type TestSession, type TestSessionEvent,
} from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Button } from "@/components/ui/Button";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import {
  SettingsAlert, SettingsCard, SettingsField, SettingsList, SettingsSubsection, settingsControlClass,
} from "@/components/settings/SettingsCard";

/** Durations offered when starting; the server accepts 15 minutes to 6 hours (notification-test-session OD150). */
export const DURATION_OPTIONS = [15, 30, 60, 120, 180, 240, 360] as const;
export const DEFAULT_DURATION = 120;

const TIME = new Intl.DateTimeFormat("id-ID", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Jakarta" });
const DAY = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });
const SECONDS = new Intl.DateTimeFormat("id-ID", {
  hour: "2-digit", minute: "2-digit", second: "2-digit", timeZone: "Asia/Jakarta",
});

export function durationLabel(minutes: number): string {
  if (minutes < 60) return `${minutes} menit`;
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? `${hours} jam ${rest} menit` : `${hours} jam`;
}

function windowText(session: TestSession): string {
  const end = session.ended_at ?? session.planned_end_at;
  const started = new Date(session.started_at);
  return `${DAY.format(started)} ${TIME.format(started)} – ${TIME.format(new Date(end))}`;
}

function countText(session: TestSession): string {
  const labelled = session.labelled_count ? ` · ${session.labelled_count} bernomor kasus` : "";
  return `${session.event_count} notifikasi${labelled}`;
}

function gapText(seconds: number): string {
  if (seconds < 60) return `${Math.max(0, Math.round(seconds))} dtk`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} mnt`;
  return `${Math.round(seconds / 3600)} jam`;
}

function startError(error: unknown): string {
  if (error instanceof ApiError && error.status === 409) return "Sesi lain masih berjalan. Akhiri sesi itu dulu.";
  return "Sesi gagal dimulai. Coba lagi.";
}

export function TestSessionSection() {
  const queryClient = useQueryClient();
  const { data, isError } = useQuery({
    queryKey: queryKeys.testSessions.list,
    queryFn: getTestSessions,
    refetchInterval: 30000,
  });
  const [duration, setDuration] = useState<number>(DEFAULT_DURATION);
  const [error, setError] = useState("");
  const [reportId, setReportId] = useState<string | null>(null);
  const refresh = () => queryClient.invalidateQueries({ queryKey: queryKeys.testSessions.all });

  const start = useMutation({
    mutationFn: () => startTestSession(duration),
    onSuccess: ({ session }) => {
      setError("");
      setReportId(session.id);
      refresh();
    },
    onError: (failure) => setError(startError(failure)),
  });
  const end = useMutation({
    mutationFn: endTestSession,
    onSuccess: () => {
      setError("");
      refresh();
    },
    onError: () => setError("Sesi gagal diakhiri. Coba lagi."),
  });
  const remove = useMutation({
    mutationFn: deleteTestSession,
    onSuccess: (_result, id) => {
      setError("");
      if (reportId === id) setReportId(null);
      refresh();
      document.getElementById("settings-sesi-uji-title")?.focus();
    },
    onError: () => setError("Data sesi gagal dihapus. Coba lagi."),
  });

  const sessions = data?.sessions ?? [];
  const running = sessions.find((session) => session.status === "open") ?? null;
  const past = sessions.filter((session) => session.status === "ended");

  return (
    <SettingsCard section="sesi-uji">
      {error && <SettingsAlert>{error}</SettingsAlert>}
      {isError ? (
        <SettingsAlert>Sesi uji gagal dimuat.</SettingsAlert>
      ) : !data ? (
        <p role="status" className="text-[var(--muted)]">Memuat sesi uji…</p>
      ) : (
        <>
          <SettingsSubsection id="sesi-uji-sekarang" title={running ? "Sesi berjalan" : "Mulai sesi"}>
            {running ? (
              <div className="space-y-3">
                <p className="tabular-nums text-[var(--text)]">
                  Berjalan sampai <b>{TIME.format(new Date(running.planned_end_at))}</b> · {countText(running)}
                </p>
                <p className="text-xs text-[var(--muted)]">Notifikasi selama sesi disimpan tanpa dicatat. HP tetap menampilkan notifikasi seperti biasa.</p>
                <div className="flex flex-wrap gap-2">
                  <Button variant="primary" className="min-h-11" disabled={end.isPending} onClick={() => end.mutate(running.id)}>
                    {end.isPending ? "Mengakhiri…" : "Akhiri sesi"}
                  </Button>
                  <Button
                    className="min-h-11"
                    aria-expanded={reportId === running.id}
                    onClick={() => setReportId(reportId === running.id ? null : running.id)}
                  >
                    {reportId === running.id ? "Tutup laporan" : "Lihat laporan"}
                  </Button>
                </div>
                {reportId === running.id && <SessionReport session={running} />}
              </div>
            ) : (
              <form
                className="flex flex-wrap items-end gap-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  start.mutate();
                }}
              >
                <SettingsField id="sesi-uji-durasi" label="Durasi" className="w-44">
                  {(field) => (
                    <select
                      {...field}
                      value={duration}
                      onChange={(event) => setDuration(Number(event.target.value))}
                      className={settingsControlClass}
                    >
                      {DURATION_OPTIONS.map((minutes) => (
                        <option key={minutes} value={minutes}>{durationLabel(minutes)}</option>
                      ))}
                    </select>
                  )}
                </SettingsField>
                <Button type="submit" variant="primary" className="min-h-11" disabled={start.isPending}>
                  {start.isPending ? "Memulai…" : "Mulai sesi"}
                </Button>
              </form>
            )}
          </SettingsSubsection>

          <SettingsSubsection id="sesi-uji-riwayat" title="Sesi sebelumnya">
            {past.length === 0 ? (
              <p className="text-[var(--muted)]">Belum ada sesi yang selesai.</p>
            ) : (
              <SettingsList label="Sesi uji sebelumnya">
                {past.map((session) => (
                  <li key={session.id} className="space-y-3 px-3 py-2.5">
                    <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
                      <div className="min-w-0 flex-1">
                        <p className="font-semibold tabular-nums text-[var(--text)]">{windowText(session)}</p>
                        <p className="text-xs tabular-nums text-[var(--muted)]">{countText(session)}</p>
                      </div>
                      <div className="flex flex-wrap items-center justify-end gap-2">
                        <Button
                          size="sm"
                          className="min-h-11"
                          aria-expanded={reportId === session.id}
                          aria-label={`${reportId === session.id ? "Tutup laporan" : "Lihat laporan"} sesi ${windowText(session)}`}
                          onClick={() => setReportId(reportId === session.id ? null : session.id)}
                        >
                          {reportId === session.id ? "Tutup laporan" : "Lihat laporan"}
                        </Button>
                        <a
                          href={testSessionCsvUrl(session.id)}
                          download
                          className="inline-flex min-h-11 items-center rounded-[var(--radius-md)] border border-[var(--line)] bg-[var(--surface)] px-3 text-xs font-semibold text-[var(--text)] hover:bg-[var(--window)] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--focus)]"
                          aria-label={`Unduh CSV sesi ${windowText(session)}`}
                        >
                          Unduh CSV
                        </a>
                        <ConfirmRemove
                          subject={`sesi ${windowText(session)}`}
                          triggerLabel={`Hapus data sesi ${windowText(session)}`}
                          verb="Hapus data"
                          confirmLabel="Ya, hapus data"
                          pending={remove.isPending && remove.variables === session.id}
                          disabled={remove.isPending}
                          onConfirm={() => remove.mutate(session.id)}
                        />
                      </div>
                    </div>
                    {reportId === session.id && <SessionReport session={session} />}
                  </li>
                ))}
              </SettingsList>
            )}
          </SettingsSubsection>
        </>
      )}
    </SettingsCard>
  );
}

/** The captured notifications of one session, with the owner's case number and note per notification (D6). */
function SessionReport({ session }: { session: TestSession }) {
  const { data, isError } = useQuery({
    queryKey: queryKeys.testSessions.events(session.id),
    queryFn: () => getTestSessionEvents(session.id),
    refetchInterval: session.status === "open" ? 15000 : false,
  });
  if (isError) return <SettingsAlert>Laporan sesi gagal dimuat.</SettingsAlert>;
  if (!data) return <p role="status" className="text-[var(--muted)]">Memuat laporan…</p>;
  if (data.events.length === 0) {
    return <p className="text-[var(--muted)]">Belum ada notifikasi tersimpan di sesi ini.</p>;
  }
  return (
    <ol aria-label="Notifikasi sesi" className="divide-y divide-[var(--line)] rounded-[var(--radius-md)] border border-[var(--line)]">
      {data.events.map((event) => (
        <ReportRow key={event.event_id} sessionId={session.id} event={event} />
      ))}
    </ol>
  );
}

function ReportRow({ sessionId, event }: { sessionId: string; event: TestSessionEvent }) {
  const { bal } = useAppCtx();
  const queryClient = useQueryClient();
  const [testCase, setTestCase] = useState(event.test_case ?? "");
  const [note, setNote] = useState(event.label_notes ?? "");
  const [saved, setSaved] = useState(false);
  useEffect(() => {
    setTestCase(event.test_case ?? "");
    setNote(event.label_notes ?? "");
  }, [event.test_case, event.label_notes]);
  const dirty = testCase.trim() !== (event.test_case ?? "") || note.trim() !== (event.label_notes ?? "");
  const save = useMutation({
    mutationFn: () => labelTestSessionEvent(sessionId, event.event_id, { test_case: testCase.trim(), label_notes: note.trim() }),
    onSuccess: () => {
      setSaved(true);
      queryClient.invalidateQueries({ queryKey: queryKeys.testSessions.all });
    },
  });
  const direction = event.direction === "income" ? "Uang masuk" : event.direction === "expense" ? "Uang keluar" : "Arah tidak terbaca";
  const amount = event.amount === null ? "Nominal tidak terbaca" : bal(event.amount);
  const prefix = `notif-${event.event_id}`;
  return (
    <li className="space-y-2 px-3 py-2.5">
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <p className="font-semibold text-[var(--text)]">
          {event.app_label} · {direction} <span className="tabular-nums">{amount}</span>
        </p>
        <p className="text-xs tabular-nums text-[var(--muted)]">
          {SECONDS.format(new Date(event.post_time))} · diterima +{gapText(event.gap_seconds)}
        </p>
      </div>
      {event.preview && <p className="break-words text-xs text-[var(--text-2)]">{event.preview}</p>}
      <form
        className="flex flex-wrap items-end gap-2"
        onSubmit={(submit) => {
          submit.preventDefault();
          setSaved(false);
          save.mutate();
        }}
      >
        <SettingsField id={`${prefix}-case`} label="Nomor kasus" className="w-28">
          {(field) => (
            <input
              {...field}
              value={testCase}
              maxLength={8}
              inputMode="numeric"
              onChange={(change) => {
                setSaved(false);
                setTestCase(change.target.value);
              }}
              className={settingsControlClass}
            />
          )}
        </SettingsField>
        <SettingsField id={`${prefix}-note`} label="Catatan" className="min-w-0 flex-1 basis-48">
          {(field) => (
            <input
              {...field}
              value={note}
              maxLength={400}
              onChange={(change) => {
                setSaved(false);
                setNote(change.target.value);
              }}
              className={settingsControlClass}
            />
          )}
        </SettingsField>
        <Button type="submit" size="sm" className="min-h-11" disabled={!dirty || save.isPending}>
          {save.isPending ? "Menyimpan…" : "Simpan"}
        </Button>
        <p role="status" className="min-h-4 w-full text-xs text-[var(--muted)]">
          {save.isError ? "Gagal disimpan. Coba lagi." : saved && !dirty ? "Tersimpan." : ""}
        </p>
      </form>
    </li>
  );
}
