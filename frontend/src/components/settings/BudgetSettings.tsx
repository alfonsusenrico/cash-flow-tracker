"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { parseRegion } from "@/i18n/language";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { queryKeys } from "@/lib/queryKeys";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Field } from "@/components/workspace/Field";
import { MaskedMoney } from "@/components/workspace/money";
import { RadioSegments } from "@/components/workspace/primitives";
import { SettingsAlert, SettingsSaveStatus, SettingsSection } from "@/components/settings/SettingsSection";

interface BudgetUser {
  currency?: string | null;
  region?: string | null;
  emergency_fund_multiplier?: number | null;
  monthly_spending_budget?: number | null;
}

type Currency = "IDR" | "USD";

/** Every view that shows money in the display currency or against these limits. */
const MONEY_CONSUMERS = [
  queryKeys.auth.all,
  queryKeys.pulse,
  queryKeys.insights,
  queryKeys.dashboard.all,
  queryKeys.goals,
  queryKeys.accounts,
  queryKeys.transactions.all,
  queryKeys.recurring.all,
];

function validate(currency: string, months: number): "monthsRange" | "currencyUnsupported" | null {
  if (!Number.isInteger(months) || months < 1 || months > 36) return "monthsRange";
  if (currency !== "IDR" && currency !== "USD") return "currencyUnsupported";
  return null;
}

/**
 * Mata uang & anggaran (apply-markas-pengaturan D13, D18): region, the IDR/USD segment with today's rate, the
 * emergency-fund months and "Batas pengeluaran bulanan", saved together and nothing else. While balances are hidden
 * the limit is a read-only mask and saving sends the stored limit unchanged, never an empty value.
 */
export function BudgetSettings() {
  const t = useTranslations("settings.budget");
  const words = useTranslations("settings.common");
  const regionText = useTranslations("settings.region");
  const format = useFormat();
  const { hideBalances, bal } = useAppCtx();
  const qc = useQueryClient();
  const { data, isError } = useQuery<{ ok: boolean; user: BudgetUser }>({
    queryKey: queryKeys.auth.me,
    queryFn: () => api.get("/auth/me"),
  });
  const { data: rates } = useQuery<{ ok: boolean; usdidr: number }>({
    queryKey: queryKeys.auth.currencyRates,
    queryFn: () => api.get("/auth/currency/rates"),
  });
  const user = data?.user;
  const loaded = Boolean(user);
  const savedCurrency = (user?.currency || "IDR").toUpperCase();
  const savedRegion = parseRegion(user?.region) ?? "ID";
  const savedMonths = String(user?.emergency_fund_multiplier || 6);
  const storedLimit = user?.monthly_spending_budget ?? null;
  const savedBudget = storedLimit != null ? format.groupDigits(storedLimit) : "";

  const [currency, setCurrency] = useState("IDR");
  const [region, setRegion] = useState<string>("ID");
  const [months, setMonths] = useState("6");
  const [budget, setBudget] = useState("");
  const [error, setError] = useState("");
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    if (!loaded) return;
    setCurrency(savedCurrency);
    setRegion(savedRegion);
    setMonths(savedMonths);
    setBudget(savedBudget);
  }, [loaded, savedCurrency, savedRegion, savedMonths, savedBudget]);

  const save = useMutation({
    mutationFn: (body: { currency: string; region: string; emergency_fund_multiplier: number; monthly_spending_budget: number | null }) =>
      api.patch("/auth/settings", body),
    onSuccess: () => {
      setError("");
      setSaved(true);
      for (const queryKey of MONEY_CONSUMERS) void qc.invalidateQueries({ queryKey });
    },
    onError: (e: Error) => setError(e.message || t("saveFailed")),
  });

  const edit = (setter: (value: string) => void) => (value: string) => {
    setSaved(false);
    setter(value);
  };
  const dirty = loaded && (currency !== savedCurrency || region !== savedRegion || months !== savedMonths || budget !== savedBudget);

  return (
    <SettingsSection section="anggaran">
      <form
        noValidate
        className="form-stack"
        onSubmit={(event) => {
          event.preventDefault();
          if (save.isPending) return;
          const multiplier = Number(months);
          const problem = validate(currency, multiplier);
          setSaved(false);
          if (problem) {
            setError(t(problem));
            return;
          }
          const digits = budget.replace(/[^0-9]/g, "");
          save.mutate({
            currency,
            region,
            emergency_fund_multiplier: multiplier,
            // A hidden limit cannot be edited, so the stored value goes back unchanged.
            monthly_spending_budget: hideBalances ? storedLimit : digits ? Number.parseInt(digits, 10) : null,
          });
        }}
      >
        {isError && <SettingsAlert>{t("loadFailed")}</SettingsAlert>}
        {error && <SettingsAlert>{error}</SettingsAlert>}
        <Field id="settings-region" label={regionText("label")} hint={regionText("hint")}>
          {(field) => (
            <select {...field} name="region" value={region} onChange={(event) => edit(setRegion)(event.target.value)} className="field-control border">
              <option value="ID">{regionText("indonesia")}</option>
              <option value="US">{regionText("unitedStates")}</option>
            </select>
          )}
        </Field>
        <div className="field">
          <span className="flabel" id="settings-currency-label">
            {t("currency")}
          </span>
          <RadioSegments<Currency>
            labelledBy="settings-currency-label"
            options={[
              { value: "IDR", label: "IDR" },
              { value: "USD", label: "USD" },
            ]}
            value={currency === "USD" ? "USD" : "IDR"}
            onChange={(value) => edit(setCurrency)(value)}
          />
          <p className="hint tabular-nums">
            {rates?.usdidr
              ? t.rich("rateHint", { rate: () => <MaskedMoney text={`Rp ${format.integer(rates.usdidr)}`} /> })
              : t("rateMissing")}
          </p>
        </div>
        <div className="form-grid">
          <Field id="settings-emergency-multiplier" label={t("emergencyMonths")}>
            {(field) => (
              <input
                {...field}
                name="emergency_fund_multiplier"
                type="number"
                inputMode="numeric"
                min={1}
                max={36}
                value={months}
                onChange={(event) => edit(setMonths)(event.target.value)}
                className="field-control border tabular-nums"
              />
            )}
          </Field>
          <Field id="settings-monthly-budget" label={t("limit")} hint={hideBalances ? t("limitHidden") : t("limitHint")}>
            {(field) =>
              hideBalances ? (
                <input {...field} name="monthly_spending_budget" type="text" readOnly value={storedLimit != null ? bal(0, "IDR") : ""} className="field-control border tabular-nums" />
              ) : (
                <input
                  {...field}
                  name="monthly_spending_budget"
                  type="text"
                  inputMode="numeric"
                  value={budget}
                  onChange={(event) => edit(setBudget)(format.groupDigits(event.target.value))}
                  className="field-control border tabular-nums"
                />
              )
            }
          </Field>
        </div>
        <div className="set-actions">
          <button type="submit" className="ws-btn ws-btn-accent" disabled={!loaded || save.isPending}>
            {save.isPending ? words("saving") : t("save")}
          </button>
          <SettingsSaveStatus dirty={dirty} saved={saved} />
        </div>
      </form>
    </SettingsSection>
  );
}
