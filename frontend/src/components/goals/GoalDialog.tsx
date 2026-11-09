"use client";

import { useMemo, useRef, useState, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import type { AccountItem } from "@/components/accounts/types";
import { Field } from "@/components/workspace/Field";
import { ProgressBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { RadioSegments } from "@/components/workspace/primitives";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { cn, formatNumberWithDots, parseNumberFromDots } from "@/lib/utils";
import { goalPercent } from "./GoalsSection";
import { DEFAULT_GOAL_COLOR, DEFAULT_GOAL_ICON, type GoalItem } from "./types";

type Source = "auto" | "manual";

/**
 * The linked balance of a goal's chosen accounts, counting a pocket and its parent once: a pocket whose parent is
 * also chosen is left out, because the parent's balance already holds it (mirrors `dashboard.py`'s reserve rule).
 */
export function linkedBalance(selectedIds: string[], accounts: AccountItem[]): number {
  const byId = new Map<string, AccountItem>();
  for (const account of accounts) {
    byId.set(String(account.id), account);
    for (const pocket of account.children ?? []) byId.set(String(pocket.id), pocket);
  }
  const selected = new Set(selectedIds.map(String));
  let total = 0;
  for (const id of selected) {
    const account = byId.get(id);
    if (!account) continue;
    if (account.parent_id && selected.has(String(account.parent_id))) continue;
    total += Number(account.balance || 0);
  }
  return total;
}

function sameIds(a: string[], b: string[]): boolean {
  return [...a].map(String).sort().join(",") === [...b].map(String).sort().join(",");
}

/**
 * "Ubah target tabungan" and "Buat target tabungan baru" (apply-markas-target D8, D18): a 560 px dialog with the
 * goal's progress, its form and, for an existing goal, "Arsipkan" confirmed in the footer. The rules and requests
 * are today's: a linked goal sends no `current_amount`, changing the progress source asks for a confirmation
 * first, and the stored colour and icon are sent unchanged (the colour field is gone, D8).
 */
export function GoalDialog({
  goal,
  emergency = false,
  accounts,
  onClose,
  onAddProgress,
}: {
  /** The goal being edited; null creates one. */
  goal: GoalItem | null;
  /** A new goal starts marked as the emergency fund ("Buat target dana darurat", D7). */
  emergency?: boolean;
  accounts: AccountItem[];
  onClose: () => void;
  onAddProgress: (goal: GoalItem) => void;
}) {
  const t = useTranslations("goals");
  const tc = useTranslations("common");
  const format = useFormat();
  const { currency } = useAppCtx();
  const qc = useQueryClient();
  const startingIds = useMemo(() => (goal ? goal.account_ids ?? goal.linked_accounts?.map((account) => account.id) ?? [] : []), [goal]);

  const [name, setName] = useState(goal?.name ?? "");
  const [target, setTarget] = useState(goal ? formatNumberWithDots(goal.target_amount) : "");
  const [current, setCurrent] = useState(goal ? formatNumberWithDots(goal.current_amount) : "0");
  const [date, setDate] = useState(goal?.target_date ?? "");
  const [isEmergency, setIsEmergency] = useState(goal ? Boolean(goal.is_emergency) : emergency);
  const [accountIds, setAccountIds] = useState<string[]>(startingIds);
  const [source, setSource] = useState<Source>(startingIds.length ? "auto" : "manual");
  const [backingConfirmed, setBackingConfirmed] = useState(false);
  const [error, setError] = useState("");
  const [confirmArchive, setConfirmArchive] = useState(false);
  const archiveButton = useRef<HTMLButtonElement>(null);

  // A credit account's balance is debt, so it cannot fund a goal's progress (add-credit-accounts D3).
  const topAccounts = useMemo(() => accounts.filter((account) => !account.parent_id && account.type !== "credit"), [accounts]);
  const backingChanged = !sameIds(accountIds, startingIds);
  const storedStandalone = goal !== null && startingIds.length === 0;
  const targetValue = parseNumberFromDots(target);

  const updateIds = (update: (ids: string[]) => string[]) => {
    setBackingConfirmed(false);
    setAccountIds(update);
  };
  const chooseSource = (next: Source) => {
    setSource(next);
    setBackingConfirmed(false);
    // Manual progress has no linked account; "Otomatis" starts from the goal's stored accounts again.
    setAccountIds(next === "manual" ? [] : startingIds);
  };

  const save = useMutation({
    mutationFn: async () => {
      const trimmed = name.trim();
      if (!trimmed) throw new Error(t("goalForm.nameRequired"));
      if (targetValue <= 0) throw new Error(t("goalForm.targetPositive"));
      const accountBacked = accountIds.length > 0;
      const payload = {
        name: trimmed,
        target_amount: targetValue,
        // Account-backed progress is derived from the linked balances; the API refuses any current_amount for it.
        ...(accountBacked ? {} : { current_amount: parseNumberFromDots(current) }),
        target_date: date || (goal ? "" : null),
        color: goal?.color ?? DEFAULT_GOAL_COLOR,
        icon: goal?.icon ?? DEFAULT_GOAL_ICON,
        is_emergency: isEmergency,
        account_ids: accountIds,
      };
      return goal ? api.patch(`/goals/${goal.id}`, payload) : api.post("/goals", payload);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
      onClose();
    },
    onError: (failure: Error) => setError(failure.message || t("goalForm.failed")),
  });
  const archive = useMutation({
    mutationFn: (id: string) => api.del(`/goals/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.goals });
      qc.invalidateQueries({ queryKey: queryKeys.dashboard.all });
    },
  });

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (save.isPending) return;
    if (backingChanged && !backingConfirmed) {
      setBackingConfirmed(true);
      return;
    }
    setError("");
    save.mutate();
  };

  const confirming = backingChanged && backingConfirmed;
  const confirmAction = goal ? t("dialog.confirmSave") : t("dialog.confirmCreate");
  const footer = confirmArchive && goal ? (
    <>
      <p className="grow-text t-body" role="alert">
        {t("dialog.archiveGoalConfirm", { name: goal.name })}
      </p>
      <button
        type="button"
        className="ws-btn"
        autoFocus
        onClick={() => {
          setConfirmArchive(false);
          requestAnimationFrame(() => archiveButton.current?.focus());
        }}
      >
        {t("dialog.cancel")}
      </button>
      <button
        type="button"
        className="ws-btn ws-btn-danger"
        onClick={() => {
          archive.mutate(goal.id);
          onClose();
        }}
      >
        {t("dialog.archiveYes")}
      </button>
    </>
  ) : (
    <>
      {goal && (
        <button ref={archiveButton} type="button" className="ws-btn ws-btn-quiet c-expense" onClick={() => setConfirmArchive(true)}>
          {t("dialog.archive")}
        </button>
      )}
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose}>
        {t("dialog.cancel")}
      </button>
      <button type="submit" form="goal-form" className="ws-btn ws-btn-ink" disabled={save.isPending}>
        {save.isPending ? t("dialog.saving") : confirming ? confirmAction : t("dialog.save")}
      </button>
    </>
  );

  const approx = (value: number) =>
    currency === "USD" && value > 0 ? (
      <span className="t-meta">{t.rich("goalForm.approx", { amount: () => <MaskedMoney text={format.money(value)} /> })}</span>
    ) : undefined;
  const reached = goal ? goal.remaining_amount <= 0 : false;
  const linkedTotal = linkedBalance(accountIds, accounts);

  return (
    <ShellDialog
      open
      onClose={onClose}
      title={goal ? t("dialog.editGoal") : t("dialog.newGoal")}
      meta={goal?.name}
      width={560}
      closeLabel={tc("close")}
      className="tg-dialog"
      footer={footer}
    >
      {goal && (
        <div className="dlg-fig">
          <span className="t-label">{reached ? t("dialog.reached") : t("dialog.saved")}</span>
          <p>
            <MaskedMoney text={format.money(goal.current_amount)} className="t-fig-l" />{" "}
            <span className="t-meta">
              {t.rich("dialog.of", { target: () => <MaskedMoney text={format.money(goal.target_amount)} />, pct: String(goalPercent(goal)) })}
            </span>
          </p>
          <ProgressBar value={goal.percentage_completed} max={100} thick color={reached || goal.is_emergency ? "var(--in-solid)" : "var(--text)"} />
          {!reached && goal.monthly_target_pace != null && goal.target_date && (
            <p className="t-meta">
              {t.rich("dialog.pace", { amount: () => <MaskedMoney text={format.money(goal.monthly_target_pace!)} />, date: format.yearDate(goal.target_date) })}
            </p>
          )}
        </div>
      )}
      <form id="goal-form" className="form-stack" noValidate onSubmit={submit}>
        {error && (
          <p role="alert" className="form-alert">
            {error}
          </p>
        )}
        <div className="form-grid">
          <Field id="goal-name" label={t("goalForm.name")} className="full-row">
            {(aria) => (
              <input {...aria} name="name" type="text" value={name} onChange={(event) => setName(event.target.value)}
                placeholder={t("goalForm.namePlaceholder")} className="field-control" />
            )}
          </Field>
          <Field id="goal-target-amount" label={t("goalForm.target")} hint={approx(targetValue)}>
            {(aria) => (
              <input {...aria} name="target_amount" type="text" inputMode="numeric" value={target}
                onChange={(event) => setTarget(formatNumberWithDots(event.target.value))}
                placeholder={t("goalForm.targetPlaceholder", { amount: format.integer(20_000_000) })} className="field-control tabular-nums" />
            )}
          </Field>
          <Field id="goal-target-date" label={t("goalForm.date")}>
            {(aria) => <input {...aria} name="target_date" type="date" value={date} onChange={(event) => setDate(event.target.value)} className="field-control" />}
          </Field>
        </div>

        <div className="field">
          <span className="flabel" id="goal-source-label">{t("goalForm.source")}</span>
          <RadioSegments<Source>
            labelledBy="goal-source-label"
            options={[{ value: "auto", label: t("goalForm.sourceAuto") }, { value: "manual", label: t("goalForm.sourceManual") }]}
            value={source}
            onChange={chooseSource}
          />
          {source === "auto" && <p className="hint">{t("goalForm.sourceHelp")}</p>}
        </div>

        {source === "auto" ? (
          <div className="field">
            <div className="tg-picker-head">
              <span className="flabel" id="goal-accounts-label">{t("goalForm.accounts")}</span>
              <span className="t-meta">{t("goalForm.selected", { count: accountIds.length })}</span>
            </div>
            <ul className="tg-picker" aria-labelledby="goal-accounts-label">
              {topAccounts.length === 0 && <li className="t-meta">{t("goalForm.noAccounts")}</li>}
              {topAccounts.map((account) => (
                <AccountChoice key={account.id} account={account} ids={accountIds} update={updateIds} />
              ))}
            </ul>
            {accountIds.length > 0 ? (
              <div className="box" aria-live="polite">
                <span className="grow t-body">{t("goalForm.linkedTotal")}</span>
                <span className="tg-linked">
                  <MaskedMoney text={format.money(linkedTotal)} className="t-amt" />
                  {targetValue > 0 && (
                    <span className="t-meta">{t("goalForm.linkedShare", { pct: String(Math.min(100, Math.round((linkedTotal / targetValue) * 100))) })}</span>
                  )}
                </span>
              </div>
            ) : (
              <p className="hint">{t("goalForm.chooseAccount")}</p>
            )}
          </div>
        ) : storedStandalone ? (
          <div className="box">
            <span className="grow t-body">{t("goalForm.manualLine")}</span>
            <button type="button" className="ws-btn ws-btn-quiet" aria-haspopup="dialog" onClick={() => onAddProgress(goal!)}>
              {t("goalForm.addProgress")}
            </button>
          </div>
        ) : (
          <Field id="goal-current-amount" label={t("goalForm.startProgress")} hint={approx(parseNumberFromDots(current)) ?? t("goalForm.startProgressHint")}>
            {(aria) => (
              <input {...aria} name="current_amount" type="text" inputMode="numeric" value={current}
                onChange={(event) => setCurrent(formatNumberWithDots(event.target.value))} placeholder="0" className="field-control tabular-nums" />
            )}
          </Field>
        )}

        <div className="field">
          <label className="field-check">
            <input id="goal-emergency" name="is_emergency" type="checkbox" checked={isEmergency} onChange={(event) => setIsEmergency(event.target.checked)} />
            {t("goalForm.emergency")}
          </label>
          <p className="hint">{accountIds.length > 0 ? t("goalForm.emergencyHelpLinked") : t("goalForm.emergencyHelpManual")}</p>
        </div>

        {confirming && (
          <p role="alert" className="box t-body">
            {accountIds.length > 0 ? t("goalForm.toLinked") : t("goalForm.toManual")} {t("goalForm.confirmHint", { action: confirmAction })}
          </p>
        )}
      </form>
    </ShellDialog>
  );
}

/**
 * One account of the goal picker with its pockets beside the straight group rail. A parent is checked when it is
 * chosen or all its pockets are; unchecking a pocket of a chosen parent keeps its other pockets (today's rules).
 */
function AccountChoice({ account, ids, update }: { account: AccountItem; ids: string[]; update: (change: (ids: string[]) => string[]) => void }) {
  const t = useTranslations("goals.goalForm");
  const format = useFormat();
  const id = String(account.id);
  const pockets = (account.children ?? []).filter((pocket) => !pocket.is_archived);
  const pocketIds = pockets.map((pocket) => String(pocket.id));
  const checked = ids.includes(id) || (pocketIds.length > 0 && pocketIds.every((pocketId) => ids.includes(pocketId)));
  const toggleParent = (on: boolean) =>
    update((current) => {
      const next = new Set(current);
      if (on) [id, ...pocketIds].forEach((value) => next.add(value));
      else [id, ...pocketIds].forEach((value) => next.delete(value));
      return Array.from(next);
    });
  const togglePocket = (pocketId: string, on: boolean) =>
    update((current) => {
      const next = new Set(current);
      if (on) {
        next.add(pocketId);
        if (pocketIds.every((value) => next.has(value))) next.add(id);
      } else {
        if (next.has(id)) {
          next.delete(id);
          pocketIds.forEach((value) => {
            if (value !== pocketId) next.add(value);
          });
        }
        next.delete(pocketId);
      }
      return Array.from(next);
    });
  return (
    <li>
      <label className={cn("tg-choice ws-select-row", checked && "is-on")} data-selected={checked ? "" : undefined}>
        <input type="checkbox" checked={checked} onChange={(event) => toggleParent(event.target.checked)} />
        <span className="grow">
          <span className="t-body">{account.name}</span>
          {pockets.length > 0 && <span className="t-meta"> · {t("parent")}</span>}
        </span>
        <MaskedMoney text={format.money(account.balance)} className="t-amt" />
      </label>
      {pockets.length > 0 && (
        <ul className="ws-group-rail tg-pockets">
          {pockets.map((pocket) => {
            const pocketId = String(pocket.id);
            const on = ids.includes(pocketId) || ids.includes(id);
            return (
              <li key={pocketId}>
                <label className="tg-choice ws-select-row" data-selected={on ? "" : undefined}>
                  <input type="checkbox" checked={on} onChange={(event) => togglePocket(pocketId, event.target.checked)} />
                  <span className="grow">
                    <span className="t-body">{pocket.name}</span>
                    <span className="t-meta"> · {t("pocket")}</span>
                  </span>
                  <MaskedMoney text={format.money(pocket.balance)} className="t-amt" />
                </label>
              </li>
            );
          })}
        </ul>
      )}
    </li>
  );
}
