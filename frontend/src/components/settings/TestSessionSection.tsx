"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import {
  ApiError, deleteTestSession, endTestSession, getTestSessionEvents, getTestSessions, labelTestSessionEvent,
  startTestSession, testSessionCsvUrl, type TestSession, type TestSessionEvent,
} from "@/lib/api";
import { useFormat, type Format } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { Field } from "@/components/workspace/Field";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import { sectionHeadingId } from "@/components/settings/sections";
import { SettingsAlert, SettingsSection } from "@/components/settings/SettingsSection";

/** Durations offered when starting; the server accepts 15 minutes to 6 hours (notification-test-session OD150). */
export const DURATION_OPTIONS = [15, 30, 60, 120, 180, 240, 360] as const;
export const DEFAULT_DURATION = 120;

type SessionWords = ReturnType<typeof useTranslations<"settings.testSession">>;

export function durationLabel(minutes: number, t: SessionWords): string {
  if (minutes < 60) return t("minutes", { count: minutes });
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  return rest ? t("hoursMinutes", { hours: String(hours), minutes: String(rest) }) : t("hours", { count: hours });
}

function windowText(session: TestSession, format: Format): string {
  const end = session.ended_at ?? session.planned_end_at;
  return `${format.shortDateAt(session.started_at)} ${format.time(session.started_at)} – ${format.time(end)}`;
}

function countText(session: TestSession, t: SessionWords): string {
  const count = t("rowMeta", { count: session.event_count });
  return session.labelled_count ? `${count} · ${t("labelled", { number: String(session.labelled_count) })}` : count;
}

function gapText(seconds: number, t: SessionWords): string {
  if (seconds < 60) return t("gapSeconds", { value: String(Math.max(0, Math.round(seconds))) });
  if (seconds < 3600) return t("gapMinutes", { value: String(Math.round(seconds / 60)) });
  return t("gapHours", { value: String(Math.round(seconds / 3600)) });
}

/**
 * Sesi uji (apply-markas-pengaturan D15): the duration and "Mulai sesi", or the running line with "Akhiri sesi" and
 * its report; then the previous sessions with their report, CSV and inline "Hapus data". Requests, the 409 message
 * and the polling intervals are unchanged.
 */
export function TestSessionSection() {
  const t = useTranslations("settings.testSession");
  const format = useFormat();
  const queryClient = useQueryClient();
  const { data, isError, refetch } = useQuery({
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
    onError: (failure) => setError(failure instanceof ApiError && failure.status === 409 ? t("startConflict") : t("startFailed")),
  });
  const end = useMutation({
    mutationFn: endTestSession,
    onSuccess: () => {
      setError("");
      refresh();
    },
    onError: () => setError(t("endFailed")),
  });
  const remove = useMutation({
    mutationFn: deleteTestSession,
    onSuccess: (_result, id) => {
      setError("");
      if (reportId === id) setReportId(null);
      refresh();
      document.getElementById(sectionHeadingId("sesi-uji"))?.focus();
    },
    onError: () => setError(t("removeFailed")),
  });

  const sessions = data?.sessions ?? [];
  const running = sessions.find((session) => session.status === "open") ?? null;
  const past = sessions.filter((session) => session.status === "ended");

  return (
    <SettingsSection section="sesi-uji">
      {error && <SettingsAlert>{error}</SettingsAlert>}
      {isError ? (
        <ErrorState message={t("loadFailed")} onRetry={() => void refetch()} />
      ) : !data ? (
        <div aria-busy="true" aria-label={t("loading")}>
          <Skeleton style={{ height: 112 }} />
        </div>
      ) : (
        <>
          {running ? (
            <section aria-label={t("end")} className="form-stack">
              <p className="t-body tabular-nums">
                {t("running", { time: format.time(running.planned_end_at), count: running.event_count })}
                {running.labelled_count ? ` · ${t("labelled", { number: String(running.labelled_count) })}` : ""}
              </p>
              <div className="set-actions">
                <button type="button" className="ws-btn ws-btn-accent" disabled={end.isPending} onClick={() => end.mutate(running.id)}>
                  {end.isPending ? t("ending") : t("end")}
                </button>
                <button type="button" className="ws-btn" aria-expanded={reportId === running.id} onClick={() => setReportId(reportId === running.id ? null : running.id)}>
                  {reportId === running.id ? t("closeReport") : t("report")}
                </button>
              </div>
              {reportId === running.id && <SessionReport session={running} />}
            </section>
          ) : (
            <form
              className="form-stack"
              aria-label={t("start")}
              onSubmit={(event) => {
                event.preventDefault();
                start.mutate();
              }}
            >
              <div className="form-grid">
                <Field id="sesi-uji-durasi" label={t("duration")}>
                  {(field) => (
                    <select {...field} value={duration} onChange={(event) => setDuration(Number(event.target.value))} className="field-control border">
                      {DURATION_OPTIONS.map((minutes) => (
                        <option key={minutes} value={minutes}>
                          {durationLabel(minutes, t)}
                        </option>
                      ))}
                    </select>
                  )}
                </Field>
              </div>
              <div className="set-actions">
                <button type="submit" className="ws-btn ws-btn-accent" disabled={start.isPending}>
                  {start.isPending ? t("starting") : t("start")}
                </button>
              </div>
            </form>
          )}

          <section aria-labelledby="sesi-uji-riwayat">
            <SectionHeader id="sesi-uji-riwayat" label={t("previous")} aside={past.length ? <span className="aside">{past.length}</span> : undefined} />
            {past.length === 0 ? (
              <p className="t-body text-[var(--muted)]">{t("previousEmpty")}</p>
            ) : (
              <ul className="list set-list" aria-labelledby="sesi-uji-riwayat">
                {past.map((session) => {
                  const sessionWindow = windowText(session, format);
                  const open = reportId === session.id;
                  return (
                    <li key={session.id}>
                      <ListRow
                        layout="stacked"
                        title={<span className="tabular-nums">{sessionWindow}</span>}
                        meta={countText(session, t)}
                        actions={
                          <>
                            <span className="ws-badge">{t("done")}</span>
                            <button
                              type="button"
                              className="ws-btn ws-btn-quiet"
                              aria-expanded={open}
                              aria-label={t("reportName", { action: open ? t("closeReport") : t("report"), window: sessionWindow })}
                              onClick={() => setReportId(open ? null : session.id)}
                            >
                              {open ? t("closeReport") : t("report")}
                            </button>
                            <a href={testSessionCsvUrl(session.id)} download className="ws-btn ws-btn-quiet" aria-label={t("csvName", { window: sessionWindow })}>
                              {t("csv")}
                            </a>
                            <ConfirmRemove
                              kind="removeData"
                              subject={t("subject", { window: sessionWindow })}
                              triggerLabel={t("removeDataName", { window: sessionWindow })}
                              pending={remove.isPending && remove.variables === session.id}
                              disabled={remove.isPending}
                              onConfirm={() => remove.mutate(session.id)}
                            />
                          </>
                        }
                      />
                      {open && (
                        <div className="set-inline">
                          <SessionReport session={session} />
                        </div>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </section>
        </>
      )}
    </SettingsSection>
  );
}

/** The captured notifications of one session, with the owner's case number and note per notification (D6). */
function SessionReport({ session }: { session: TestSession }) {
  const t = useTranslations("settings.testSession");
  const { data, isError } = useQuery({
    queryKey: queryKeys.testSessions.events(session.id),
    queryFn: () => getTestSessionEvents(session.id),
    refetchInterval: session.status === "open" ? 15000 : false,
  });
  if (isError) return <SettingsAlert>{t("reportLoadFailed")}</SettingsAlert>;
  if (!data) return <p role="status" className="t-meta">{t("reportLoading")}</p>;
  if (data.events.length === 0) return <p className="t-body text-[var(--muted)]">{t("reportEmpty")}</p>;
  return (
    <ol aria-label={t("events")} className="set-report">
      {data.events.map((event) => (
        <ReportRow key={event.event_id} sessionId={session.id} event={event} />
      ))}
    </ol>
  );
}

function ReportRow({ sessionId, event }: { sessionId: string; event: TestSessionEvent }) {
  const t = useTranslations("settings.testSession");
  const format = useFormat();
  const money = useMoney();
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
  const direction = event.direction === "income" ? t("moneyIn") : event.direction === "expense" ? t("moneyOut") : t("unknownDirection");
  const prefix = `notif-${event.event_id}`;
  return (
    <li>
      <p className="t-strong">
        {event.app_label} · {direction}{" "}
        {event.amount === null ? t("unknownAmount") : <MaskedMoney text={money(event.amount)} className="tabular-nums" />}
      </p>
      <p className="t-meta tabular-nums">{t("received", { time: format.timeSeconds(event.post_time), gap: gapText(event.gap_seconds, t) })}</p>
      {event.preview && <p className="t-meta set-preview">{event.preview}</p>}
      <form
        className="set-label-form"
        onSubmit={(submit) => {
          submit.preventDefault();
          setSaved(false);
          save.mutate();
        }}
      >
        <Field id={`${prefix}-case`} label={t("caseNumber")} className="set-case">
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
              className="field-control border"
            />
          )}
        </Field>
        <Field id={`${prefix}-note`} label={t("note")} className="set-case-note">
          {(field) => (
            <input
              {...field}
              value={note}
              maxLength={400}
              onChange={(change) => {
                setSaved(false);
                setNote(change.target.value);
              }}
              className="field-control border"
            />
          )}
        </Field>
        <button type="submit" className="ws-btn" disabled={!dirty || save.isPending}>
          {save.isPending ? t("saving") : t("save")}
        </button>
        <p role="status" className="t-meta set-label-status">
          {save.isError ? t("saveFailed") : saved && !dirty ? t("saved") : ""}
        </p>
      </form>
    </li>
  );
}
