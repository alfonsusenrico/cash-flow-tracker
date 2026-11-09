"use client";

import { useEffect, useId, useRef, useState } from "react";
import Link from "next/link";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { inSentence } from "@/components/analysis/ComparisonRows";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { type ComparisonFact, type MonthlyReport, type Reflection, refHref } from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";
import { Question } from "./Question";

type Money = (amount: number) => string;

const NOTE_LIMIT = 1000;

/** The facts Laporan lists: the server's, with the top merchant on the Pengeluaran basis (D29). */
export function reportFacts(report: MonthlyReport): ComparisonFact[] {
  const { facts, spending_top_merchant: topMerchant } = report.comparison;
  return facts.flatMap((fact) => (fact.kind === "top_merchant" ? (topMerchant ? [topMerchant] : []) : [fact]));
}

/** Question 4 "Apa yang mau diperbaiki bulan depan?" (apply-markas-laporan D12, D13): the facts, then the note. */
export function ImprovementQuestion({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report");
  return (
    <Question number={4} id="lp-q4" title={t("questions.improve")}>
      <Facts report={report} money={money} />
      <ReflectionNote report={report} />
    </Question>
  );
}

/**
 * "Yang bisa diperbaiki", or "Sejauh ini" while the period runs: one whole-row link per server fact with its
 * figure and "Lihat transaksi"; recurring bills open their list on Analisis.
 */
function Facts({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report.facts");
  const facts = reportFacts(report);
  const title = report.comparison.running ? t("soFar") : t("title");
  return (
    <section aria-labelledby="lp-facts">
      <SectionHeader level={3} id="lp-facts" label={title} />
      {facts.length === 0 ? (
        <p className="t-body muted">{t("empty")}</p>
      ) : (
        <ul className="list" aria-labelledby="lp-facts">
          {facts.map((fact) => {
            const figure = fact.kind === "category_above_average" && fact.difference !== undefined
              ? `+${money(fact.difference)}`
              : fact.kind === "recurring" ? t("perMonth", { amount: money(fact.amount) }) : money(fact.amount);
            const href = fact.ref ? refHref(fact.ref) : fact.kind === "recurring" ? "/insights?tab=ringkasan" : null;
            const content = (
              <>
                <span className="r-main">
                  <span className="r-title lp-fact-title">{fact.label}</span>
                </span>
                <span className="r-amt">
                  <MaskedMoney text={figure} className="t-amt" />
                </span>
                {href && <span className="t-meta c-accent dt-only lp-fact-link">{fact.ref ? t("view") : t("viewList")}</span>}
              </>
            );
            return (
              <li key={`${fact.kind}-${fact.label}`}>
                {href ? <Link href={href} className="row lp-fact">{content}</Link> : <div className="row is-plain lp-fact">{content}</div>}
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

/**
 * The period's note (D13): the saved note read-only with "Ubah", or a quiet "Tulis catatan untuk {period}", and the
 * form in place on demand, never an empty form on load. Requests are `PUT` and `DELETE /reports/monthly/reflection`.
 * The previous period's note is shown only when it exists. The note is the owner's and is never logged.
 */
function ReflectionNote({ report }: { report: MonthlyReport }) {
  const t = useTranslations("report.note");
  const common = useTranslations("common");
  const format = useFormat();
  const queryClient = useQueryClient();
  const fieldId = useId();
  const helpId = useId();
  const periodName = inSentence(report.period.label, report.basis);
  // The note just saved or deleted, shown before the report reloads; undefined follows the report.
  const [shown, setShown] = useState<string | null | undefined>(undefined);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const [status, setStatus] = useState("");
  const [focusTarget, setFocusTarget] = useState<"edit" | "write" | "field" | null>(null);
  const editRef = useRef<HTMLButtonElement>(null);
  const writeRef = useRef<HTMLButtonElement>(null);
  const fieldRef = useRef<HTMLTextAreaElement>(null);
  const saved = shown === undefined ? report.reflection?.note ?? null : shown;

  useEffect(() => {
    setShown(undefined);
    setEditing(false);
    setStatus("");
  }, [report.basis, report.period.start_date]);
  useEffect(() => {
    if (shown !== undefined && (report.reflection?.note ?? null) === shown) setShown(undefined);
  }, [report.reflection, shown]);
  useEffect(() => {
    if (!focusTarget) return;
    const target = focusTarget === "edit" ? editRef.current : focusTarget === "write" ? writeRef.current : fieldRef.current;
    target?.focus();
    setFocusTarget(null);
  }, [focusTarget, editing, saved]);

  const refresh = () => queryClient.invalidateQueries({ queryKey: queryKeys.reports.all });
  const save = useMutation({
    mutationFn: (note: string) =>
      api.put<{ reflection: Reflection }>("/reports/monthly/reflection", { basis: report.basis, start_date: report.period.start_date, note }),
    onSuccess: (result, note) => {
      setShown(result?.reflection?.note ?? note);
      setEditing(false);
      setStatus(t("saved"));
      setFocusTarget("edit");
      refresh();
    },
    onError: (error: Error) => setStatus(error.message || t("saveError")),
  });
  const remove = useMutation({
    mutationFn: () => api.del(`/reports/monthly/reflection?basis=${report.basis}&start_date=${report.period.start_date}`),
    onSuccess: () => {
      setShown(null);
      setEditing(false);
      setStatus(t("deleted"));
      setFocusTarget("write");
      refresh();
    },
    onError: (error: Error) => setStatus(error.message || t("deleteError")),
  });

  const open = () => {
    setDraft(saved ?? "");
    setStatus("");
    setEditing(true);
    setFocusTarget("field");
  };
  const cancel = () => {
    setEditing(false);
    setFocusTarget(saved ? "edit" : "write");
  };
  const unchanged = draft.trim() === (saved ?? "").trim();
  const previous = report.previous_reflection;

  return (
    <div className="lp-note-block">
      {editing ? (
        <form
          className="lp-note-form"
          onSubmit={(event) => {
            event.preventDefault();
            if (draft.trim() && !unchanged) save.mutate(draft);
          }}
        >
          <div className="field">
            <label htmlFor={fieldId}>{t("label", { period: periodName })}</label>
            <textarea
              ref={fieldRef}
              id={fieldId}
              className="field-control lp-textarea"
              value={draft}
              maxLength={NOTE_LIMIT}
              placeholder={t("placeholder")}
              aria-describedby={helpId}
              onChange={(event) => setDraft(event.target.value)}
            />
            <p id={helpId} className="hint">
              {t("help")} <span className="tabular">{format.integer(draft.length)}/{format.integer(NOTE_LIMIT)}</span>
            </p>
          </div>
          <div className="lp-note-actions">
            <button type="submit" className="ws-btn" disabled={!draft.trim() || unchanged || save.isPending}>
              {save.isPending ? t("saving") : t("save")}
            </button>
            {saved && (
              <button type="button" className="ws-btn ws-btn-quiet" disabled={remove.isPending} onClick={() => remove.mutate()}>
                {t("delete")}
              </button>
            )}
            <button type="button" className="ws-btn ws-btn-quiet" onClick={cancel}>
              {common("cancel")}
            </button>
          </div>
        </form>
      ) : saved ? (
        <div className="lp-note">
          <div className="lp-note-h">
            <h3 className="t-strong">{t("label", { period: periodName })}</h3>
            <button ref={editRef} type="button" className="ws-btn ws-btn-quiet ws-btn-sm" onClick={open}>
              {t("edit")}
            </button>
          </div>
          <p className="t-body lp-note-text">{saved}</p>
        </div>
      ) : (
        <button ref={writeRef} type="button" className="ws-btn lp-note-write" onClick={open}>
          {t("write", { period: periodName })}
        </button>
      )}
      <p role="status" className="t-meta lp-note-status">{status}</p>
      {previous && (
        <p className="t-meta lp-note-text">{t("previous", { period: inSentence(previous.label ?? "", report.basis), note: previous.note })}</p>
      )}
    </div>
  );
}
