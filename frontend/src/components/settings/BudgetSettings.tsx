"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { useInterfaceLang } from "@/components/workspace/untranslated";
import { parseRegion } from "@/i18n/language";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { formatNumberWithDots } from "@/lib/utils";
import { Button } from "@/components/ui/Button";
import {
  SETTINGS_FIELD_WIDTH, SettingsAlert, SettingsCard, SettingsField, SettingsSaveStatus, settingsControlClass,
} from "@/components/settings/SettingsCard";

interface BudgetUser {
  currency?: string | null;
  region?: string | null;
  emergency_fund_multiplier?: number | null;
  monthly_spending_budget?: number | null;
}

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

function validate(currency: string, months: number) {
  if (!Number.isInteger(months) || months < 1 || months > 36) return "Target dana darurat harus antara 1 dan 36 bulan";
  if (currency !== "IDR" && currency !== "USD") return "Mata uang yang didukung hanya IDR dan USD";
  return "";
}

/** Display currency, emergency-fund target and the monthly operational limit; saved together and nothing else. */
export function BudgetSettings() {
  const qc = useQueryClient();
  const regionText = useTranslations("settings.region");
  const islandLang = useInterfaceLang();
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
  const savedBudget = user?.monthly_spending_budget != null ? formatNumberWithDots(user.monthly_spending_budget) : "";

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
    onError: (e: Error) => setError(e.message || "Pengaturan gagal disimpan. Coba lagi."),
  });

  const edit = (setter: (value: string) => void) => (value: string) => {
    setSaved(false);
    setter(value);
  };
  const dirty = loaded && (currency !== savedCurrency || region !== savedRegion || months !== savedMonths || budget !== savedBudget);
  const monthCount = Number.parseInt(months, 10);

  return (
    <form
      noValidate
      onSubmit={(event) => {
        event.preventDefault();
        if (save.isPending) return;
        const multiplier = Number(months);
        const problem = validate(currency, multiplier);
        setSaved(false);
        if (problem) {
          setError(problem);
          return;
        }
        const digits = budget.replace(/[^0-9]/g, "");
        save.mutate({
          currency,
          region,
          emergency_fund_multiplier: multiplier,
          monthly_spending_budget: digits ? Number.parseInt(digits, 10) : null,
        });
      }}
    >
      <SettingsCard
        section="anggaran"
        footer={
          <>
            <SettingsSaveStatus dirty={dirty} saved={saved} />
            <Button type="submit" variant="primary" size="sm" className="min-h-11" disabled={!loaded || save.isPending}>
              {save.isPending ? "Menyimpan…" : "Simpan anggaran"}
            </Button>
          </>
        }
      >
        {isError && <SettingsAlert>Pengaturan tidak dapat dimuat. Muat ulang halaman untuk mencoba lagi.</SettingsAlert>}
        {error && <SettingsAlert>{error}</SettingsAlert>}
        <div className={`space-y-4 ${SETTINGS_FIELD_WIDTH}`}>
          {/* A translated island ahead of phase 10 (add-language-system D11), marked with its own language. */}
          <div lang={islandLang}>
            <SettingsField id="settings-region" label={regionText("label")} help={regionText("hint")}>
              {(field) => (
                <select {...field} name="region" value={region} onChange={(event) => edit(setRegion)(event.target.value)} className={settingsControlClass}>
                  <option value="ID">{regionText("indonesia")}</option>
                  <option value="US">{regionText("unitedStates")}</option>
                </select>
              )}
            </SettingsField>
          </div>
          <SettingsField
            id="settings-currency"
            label="Mata uang tampilan"
            help={
              <span className="tabular-nums">
                {rates?.usdidr ? `Kurs: 1 USD ≈ Rp ${rates.usdidr.toLocaleString("id-ID")}. ` : "Kurs belum tersedia. "}
                Hanya tampilan; catatan tetap dalam rupiah.
              </span>
            }
          >
            {(field) => (
              <select {...field} name="currency" value={currency} onChange={(event) => edit(setCurrency)(event.target.value)} className={settingsControlClass}>
                <option value="IDR">IDR (Rupiah Indonesia)</option>
                <option value="USD">USD (Dolar Amerika Serikat)</option>
              </select>
            )}
          </SettingsField>

          <SettingsField
            id="settings-emergency-multiplier"
            label="Target dana darurat (bulan)"
            help="Berapa bulan pengeluaran pokok yang ingin dicadangkan, 1–36 bulan (umumnya 6)."
            aside={
              Number.isInteger(monthCount) && monthCount > 0 ? (
                <span className="text-xs font-semibold tabular-nums text-[var(--text-2)]">{monthCount}× pengeluaran pokok</span>
              ) : undefined
            }
          >
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
                className={`${settingsControlClass} tabular-nums`}
              />
            )}
          </SettingsField>

          <SettingsField
            id="settings-monthly-budget"
            label="Batas belanja operasional bulanan (IDR)"
            help="Dipakai untuk sisa batas belanja. Kosongkan untuk memakai total anggaran kategori."
          >
            {(field) => (
              <input
                {...field}
                name="monthly_spending_budget"
                type="text"
                inputMode="numeric"
                value={budget}
                onChange={(event) => edit(setBudget)(formatNumberWithDots(event.target.value))}
                placeholder="Contoh: 10.000.000"
                className={`${settingsControlClass} tabular-nums`}
              />
            )}
          </SettingsField>
        </div>
      </SettingsCard>
    </form>
  );
}
