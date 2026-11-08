"use client";

import { useEffect, useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { RadioSegments } from "@/components/workspace/primitives";
import { useFormat } from "@/lib/format";
import type { KakeiboPillar } from "@/lib/kakeibo";
import { cn } from "@/lib/utils";

export interface ManagedCategory {
  id: string;
  name: string;
  kind: "expense" | "income" | string;
  icon?: string | null;
  color?: string | null;
  kakeibo_type?: string | null;
  monthly_budget?: number | null;
  is_archived?: boolean;
  system_key?: string | null;
}

/** The request body of `POST /categories` and `PATCH /categories/{id}`, exactly as the former form sent it. */
export interface CategoryPayload {
  name: string;
  icon: string;
  color: string;
  kind: "expense" | "income";
  monthly_budget: number | null;
  kakeibo_type: KakeiboPillar | null;
}

export const CATEGORY_ICONS = ["tag", "utensils", "coffee", "shopping-cart", "shopping-bag", "car", "zap", "heart", "film", "wallet", "credit-card", "users"] as const;

export const CATEGORY_COLORS = {
  "#f97316": "orange",
  "#10b981": "emerald",
  "#3b82f6": "blue",
  "#8b5cf6": "purple",
  "#ec4899": "pink",
  "#ef4444": "red",
  "#eab308": "yellow",
  "#14b8a6": "teal",
  "#64748b": "grey",
} as const;

const PILLARS: KakeiboPillar[] = ["need", "want", "culture", "unexpected", "saving"];
const PILLAR_TERM = { need: "needs", want: "wants", culture: "culture", unexpected: "unexpected", saving: "savings" } as const;
type PillarChoice = KakeiboPillar | "none";

export const CATEGORY_FORM_ID = "category-form";

function isPillar(value: string | null | undefined): value is KakeiboPillar {
  return PILLARS.includes(value as KakeiboPillar);
}

/**
 * The category form inside "Kelola kategori" (apply-markas-analisis D9): name, type, pillar, icon and colour as named
 * options, and the monthly budget for spending categories. "Belum diklasifikasi" sends an explicit `null` pillar.
 */
export function CategoryForm({
  category,
  kind: initialKind,
  error,
  onSubmit,
}: {
  category: ManagedCategory | null;
  kind: "expense" | "income";
  error: string;
  onSubmit: (payload: CategoryPayload) => void;
}) {
  const t = useTranslations("analysis.form");
  const terms = useTranslations("terms");
  const format = useFormat();
  const nameRef = useRef<HTMLInputElement>(null);
  const [name, setName] = useState(category?.name ?? "");
  const [kind, setKind] = useState<"expense" | "income">(category ? (category.kind === "income" ? "income" : "expense") : initialKind);
  const [pillar, setPillar] = useState<PillarChoice>(category ? (isPillar(category.kakeibo_type) ? category.kakeibo_type : "none") : "need");
  const [icon, setIcon] = useState(category?.icon || "tag");
  const [color, setColor] = useState(category?.color || "#3b82f6");
  const [budget, setBudget] = useState(category?.monthly_budget ? format.groupDigits(category.monthly_budget) : "");
  const [nameError, setNameError] = useState("");

  useEffect(() => nameRef.current?.focus(), []);

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    if (!name.trim()) {
      setNameError(t("nameRequired"));
      nameRef.current?.focus();
      return;
    }
    const digits = budget.replace(/[^0-9]/g, "");
    onSubmit({
      name: name.trim(),
      icon,
      color,
      kind,
      monthly_budget: kind === "expense" && digits ? Number.parseInt(digits, 10) : null,
      kakeibo_type: kind === "expense" && pillar !== "none" ? pillar : null,
    });
  };

  return (
    <form id={CATEGORY_FORM_ID} className="form-stack" onSubmit={submit} noValidate>
      {error && <p role="alert" className="form-alert">{error}</p>}
      <div className="field">
        <label htmlFor="category-name">{t("name")}</label>
        <input
          ref={nameRef}
          id="category-name"
          className="field-control"
          maxLength={100}
          value={name}
          aria-invalid={nameError ? true : undefined}
          aria-describedby={nameError ? "category-name-error" : undefined}
          onChange={(event) => {
            setName(event.target.value);
            setNameError("");
          }}
        />
        {nameError && <p id="category-name-error" className="err">{nameError}</p>}
      </div>
      <div className="field">
        <span className="flabel" id="category-kind">{t("kind")}</span>
        <RadioSegments
          labelledBy="category-kind"
          options={[{ value: "expense", label: terms("spending") }, { value: "income", label: terms("income") }]}
          value={kind}
          onChange={setKind}
        />
      </div>
      {kind === "expense" && (
        <div className="field">
          <span className="flabel" id="category-pillar">{t("pillar")}</span>
          <RadioSegments
            labelledBy="category-pillar"
            options={[
              ...PILLARS.map((value) => ({ value: value as PillarChoice, label: terms(PILLAR_TERM[value]) })),
              { value: "none" as PillarChoice, label: terms("noPillarYet") },
            ]}
            value={pillar}
            onChange={setPillar}
          />
          <p className="hint">{pillar === "none" ? t("noPillarHint") : t("pillarHint")}</p>
        </div>
      )}
      <div className="field">
        <span className="flabel" id="category-icon">{t("icon")}</span>
        <div role="group" aria-labelledby="category-icon" className="choice-grid">
          {CATEGORY_ICONS.map((option) => (
            <button key={option} type="button" className="choice" aria-pressed={icon === option} onClick={() => setIcon(option)}>
              <Icon name={option} className="h-4 w-4" />
              <span>{t(`icons.${option}`)}</span>
            </button>
          ))}
        </div>
      </div>
      <div className="field">
        <span className="flabel" id="category-colour">{t("colour")}</span>
        <div role="group" aria-labelledby="category-colour" className="choice-grid">
          {Object.entries(CATEGORY_COLORS).map(([value, colourName]) => (
            <button key={value} type="button" className={cn("choice", "is-colour")} aria-pressed={color === value} onClick={() => setColor(value)}>
              <span className="swatch" style={{ background: value }} aria-hidden="true" />
              <span>{t(`colours.${colourName}`)}</span>
            </button>
          ))}
        </div>
      </div>
      {kind === "expense" && (
        <div className="field">
          <label htmlFor="category-budget">{t("budget")}</label>
          <input
            id="category-budget"
            className="field-control tabular-nums"
            inputMode="numeric"
            autoComplete="off"
            value={budget}
            placeholder={t("budgetNone")}
            aria-describedby="category-budget-hint"
            onChange={(event) => setBudget(format.groupDigits(event.target.value))}
          />
          <p id="category-budget-hint" className="hint">{t("budgetHint")}</p>
        </div>
      )}
    </form>
  );
}
