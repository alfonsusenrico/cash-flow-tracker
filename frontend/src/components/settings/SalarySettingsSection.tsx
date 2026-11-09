"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { DisclosureSummary } from "@/components/ui/DisclosureSummary";
import { KindAmount } from "@/components/transactions/rows";
import { Field } from "@/components/workspace/Field";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { useMoney } from "@/components/workspace/money";
import { Skeleton } from "@/components/workspace/primitives";
import { SettingsAlert, SettingsSection } from "@/components/settings/SettingsSection";

type Income = {
  id: string;
  amount: number;
  date: string;
  merchant: string | null;
  notes: string | null;
  movement_id?: string | null;
  is_reimbursement?: boolean;
};
type Candidate = Income & { confirmed: boolean; needs_review: boolean; confirmation_method: string | null };
type Receipt = Income & { confirmation_method: string };
type SalaryAction = { kind: "source" | "confirm" | "revoke" | "reject"; id: string } | { kind: "detect" };

const PAGE_SIZE = 50;

function request(action: SalaryAction) {
  if (action.kind === "detect") return api.post("/salary/detect");
  if (action.kind === "source") return api.put("/salary/source", { transaction_id: action.id });
  if (action.kind === "confirm") return api.post("/salary/receipts", { transaction_id: action.id });
  if (action.kind === "reject") return api.post("/salary/reject", { transaction_id: action.id });
  return api.del(`/salary/receipts/${action.id}`);
}

/**
 * Gaji & siklus (apply-markas-pengaturan D12): the source box with "Periksa pemasukan dari sumber ini" and the picker
 * behind "Ganti sumber", the receipts to review, and the confirmed salaries with their amounts in the income colour.
 * Salary cycles start only from confirmed receipts; every request is the one the section sent before.
 */
export function SalarySettingsSection() {
  const t = useTranslations("settings.salary");
  const terms = useTranslations("terms");
  const format = useFormat();
  const money = useMoney();
  const qc = useQueryClient();
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState("");
  const [error, setError] = useState("");

  const source = useQuery({
    queryKey: ["salary", "source"],
    queryFn: () => api.get<{ source: { field: string; value: string } | null }>("/salary/source"),
  });
  const candidates = useQuery({
    queryKey: ["salary", "candidates"],
    queryFn: () => api.get<{ candidates: Candidate[] }>("/salary/candidates"),
  });
  const invalidated = useQuery({
    queryKey: ["salary", "invalidated"],
    queryFn: () => api.get<{ detections: Income[] }>("/salary/invalidated-detections"),
  });
  const receipts = useQuery({
    queryKey: ["salary", "receipts"],
    queryFn: () => api.get<{ receipts: Receipt[] }>("/salary/receipts"),
  });
  const incomes = useQuery({
    queryKey: ["salary", "income", offset],
    queryFn: () => api.get<{ transactions: Income[]; total: number }>(`/transactions?type=income&limit=${PAGE_SIZE}&offset=${offset}`),
  });

  const action = useMutation({
    mutationFn: request,
    onSuccess: () => {
      setError("");
      for (const key of ["salary", "dashboard", "transactions", "pulse", "insights"]) {
        void qc.invalidateQueries({ queryKey: [key] });
      }
    },
    onError: (err: Error) => setError(err.message || t("saveFailed")),
  });

  const name = (row: Income) => row.merchant || row.notes || terms("noDescription");
  const describe = (row: Income) => t("row", { date: format.yearDateAt(row.date), amount: money(row.amount), name: name(row) });

  const queries = [source, candidates, invalidated, receipts, incomes];
  const failed = queries.some((query) => query.isError);
  const pending = queries.some((query) => query.isPending);
  const eligible = incomes.data?.transactions.filter((row) => !row.movement_id && !row.is_reimbursement) ?? [];
  const confirmedIds = new Set(receipts.data?.receipts.map((row) => row.id) ?? []);
  const reviewRows = new Map<string, Income>();
  for (const row of candidates.data?.candidates ?? []) {
    if (row.needs_review) reviewRows.set(row.id, row);
  }
  for (const row of invalidated.data?.detections ?? []) reviewRows.set(row.id, row);
  const configured = source.data?.source;
  const busy = action.isPending;
  const changePage = (next: number) => {
    setOffset(next);
    setSelected("");
  };

  return (
    <SettingsSection section="gaji">
      {pending && (
        <div aria-busy="true" aria-label={t("loading")}>
          <Skeleton style={{ height: 112 }} />
        </div>
      )}
      {failed && <SettingsAlert>{t("loadFailed")}</SettingsAlert>}
      {error && <SettingsAlert>{error}</SettingsAlert>}

      {!pending && !failed && (
        <>
          <section aria-labelledby="salary-source-title">
            <SectionHeader id="salary-source-title" label={t("source")} />
            <div className="set-box">
              {configured ? (
                <>
                  <p className="t-strong">{configured.value}</p>
                  <p className="t-meta">{configured.field === "merchant" ? t("bySender") : t("byDescription")}</p>
                  <p className="t-meta">{t("cycleNote")}</p>
                  <div className="set-actions set-box-actions">
                    <button type="button" className="ws-btn" disabled={busy} onClick={() => action.mutate({ kind: "detect" })}>
                      {t("check")}
                    </button>
                  </div>
                </>
              ) : (
                <>
                  <p className="t-body">{t("none")}</p>
                  <p className="t-meta">{t("cycleNote")}</p>
                </>
              )}
              <details open={!configured} className="set-picker">
                <DisclosureSummary>{configured ? t("change") : t("choose")}</DisclosureSummary>
                <div className="form-stack">
                  {eligible.length === 0 ? (
                    <p className="t-meta">{t("noIncome")}</p>
                  ) : (
                    <Field id="salary-income" label={t("pickLabel")} hint={t("pickHint")}>
                      {(field) => (
                        <select {...field} value={selected} onChange={(event) => setSelected(event.target.value)} className="field-control border tabular-nums">
                          <option value="">{t("pickPlaceholder")}</option>
                          {eligible.map((row) => (
                            <option key={row.id} value={row.id}>
                              {describe(row)}
                            </option>
                          ))}
                        </select>
                      )}
                    </Field>
                  )}
                  <div role="group" aria-label={t("pages")} className="set-actions">
                    <button type="button" className="ws-btn" disabled={offset === 0} onClick={() => changePage(Math.max(0, offset - PAGE_SIZE))}>
                      {t("newer")}
                    </button>
                    <button type="button" className="ws-btn" disabled={offset + PAGE_SIZE >= (incomes.data?.total ?? 0)} onClick={() => changePage(offset + PAGE_SIZE)}>
                      {t("older")}
                    </button>
                  </div>
                  <div className="set-actions">
                    <button
                      type="button"
                      className="ws-btn"
                      disabled={!selected || busy || confirmedIds.has(selected)}
                      onClick={() => action.mutate({ kind: "confirm", id: selected })}
                    >
                      {t("markSalary")}
                    </button>
                    <button type="button" className="ws-btn ws-btn-accent" disabled={!selected || busy} onClick={() => action.mutate({ kind: "source", id: selected })}>
                      {t("makeSource")}
                    </button>
                  </div>
                </div>
              </details>
            </div>
          </section>

          <section aria-labelledby="salary-review-title">
            <SectionHeader id="salary-review-title" label={t("review")} aside={reviewRows.size ? <span className="aside">{reviewRows.size}</span> : undefined} />
            {reviewRows.size === 0 ? (
              <p className="t-body text-[var(--muted)]">{t("reviewEmpty")}</p>
            ) : (
              <ul className="list set-list" aria-labelledby="salary-review-title">
                {Array.from(reviewRows.values()).map((row) => (
                  <li key={row.id}>
                    <ListRow
                      layout="stacked"
                      title={name(row)}
                      meta={format.yearDateAt(row.date)}
                      amount={<KindAmount kind="income" amount={row.amount} money={money} />}
                      actions={
                        <>
                          <button type="button" className="ws-btn" disabled={busy} aria-label={t("isSalaryName", { row: describe(row) })} onClick={() => action.mutate({ kind: "confirm", id: row.id })}>
                            {t("isSalary")}
                          </button>
                          <button type="button" className="ws-btn ws-btn-quiet" disabled={busy} aria-label={t("notSalaryName", { row: describe(row) })} onClick={() => action.mutate({ kind: "reject", id: row.id })}>
                            {t("notSalary")}
                          </button>
                        </>
                      }
                    />
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section aria-labelledby="salary-receipts-title">
            <SectionHeader
              id="salary-receipts-title"
              label={t("confirmed")}
              aside={receipts.data?.receipts.length ? <span className="aside">{receipts.data.receipts.length}</span> : undefined}
            />
            {!receipts.data?.receipts.length ? (
              <p className="t-body text-[var(--muted)]">{t("confirmedEmpty")}</p>
            ) : (
              <ul className="list set-list" aria-labelledby="salary-receipts-title">
                {receipts.data.receipts.map((row) => (
                  <li key={row.id}>
                    <ListRow
                      layout="stacked"
                      title={name(row)}
                      meta={`${format.yearDateAt(row.date)} · ${row.confirmation_method === "detected" ? t("detected") : t("byOwner")}`}
                      amount={<KindAmount kind="income" amount={row.amount} money={money} />}
                      actions={
                        <button type="button" className="ws-btn ws-btn-quiet" disabled={busy} aria-label={t("undoName", { row: describe(row) })} onClick={() => action.mutate({ kind: "revoke", id: row.id })}>
                          {t("undo")}
                        </button>
                      }
                    />
                  </li>
                ))}
              </ul>
            )}
          </section>
        </>
      )}
    </SettingsSection>
  );
}
