"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import dynamic from "next/dynamic";
import { usePathname, useSearchParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { AccountDetailDialog } from "@/components/accounts/AccountDetailDialog";
import { Icon } from "@/components/ui/Icon";
import { PageActions, type PageMoreAction } from "@/components/workspace/context";
import { replaceSearch } from "@/lib/urlState";
import type { ExpensePlanDraft } from "@/types/recurring";
import { BillsSection } from "./BillsSection";
import { DebtDialog } from "./DebtDialog";
import { DebtPaymentDialog } from "./DebtPaymentDialog";
import { DebtsPanel } from "./DebtsPanel";
import { GoalDialog } from "./GoalDialog";
import { GoalProgressDialog } from "./GoalProgressDialog";
import { GoalsSection } from "./GoalsSection";
import { TargetHero } from "./TargetHero";
import type { GoalItem, ObligationItem } from "./types";
import { useGoalData } from "./useGoalData";

// The plans editor loads when it is opened, so the page's first load does not carry it (gate D18).
const PlansDialog = dynamic(() => import("@/components/recurring/PlansDialog").then((module) => module.PlansDialog), { ssr: false });

type Param = "target" | "utang";

interface PlansState {
  planId?: string;
  expenseDraft?: ExpensePlanDraft;
}

/**
 * Target & Tagihan (apply-markas-target D1, D2, D12): the "Ketahanan Dana" hero, then the reading column with
 * "Tagihan 30 hari ke depan" and "Target tabungan" beside the "Utang & kredit" panel; one column below 1200 px in
 * the phone order hero, Tagihan, Target, Utang. A goal and a debt open their dialogs and are kept in the address
 * as `?target=` and `?utang=`; every dialog returns focus to the control that opened it.
 */
export function GoalsView() {
  const t = useTranslations("goals");
  const pathname = usePathname();
  const search = useSearchParams();
  const data = useGoalData();
  const [plans, setPlans] = useState<PlansState | null>(null);
  const [newGoal, setNewGoal] = useState<{ emergency: boolean } | null>(null);
  const [newDebt, setNewDebt] = useState(false);
  const [progressGoal, setProgressGoal] = useState<GoalItem | null>(null);
  const [payDebt, setPayDebt] = useState<ObligationItem | null>(null);
  const [creditId, setCreditId] = useState<string | null>(null);
  const opener = useRef<HTMLElement | null>(null);
  // A dialog opened from another dialog ("Tambah progres", the payment) returns focus to its button there.
  const innerOpener = useRef<HTMLElement | null>(null);

  const remember = () => {
    const active = document.activeElement;
    opener.current = active instanceof HTMLElement && active !== document.body ? active : null;
  };
  const restore = useCallback(() => {
    const target = opener.current;
    opener.current = null;
    requestAnimationFrame(() => {
      if (target?.isConnected) target.focus();
    });
  }, []);
  const writeParam = useCallback(
    (key: Param, value: string | null) => {
      const next = new URLSearchParams(window.location.search);
      next.delete("target");
      next.delete("utang");
      if (value) next.set(key, value);
      const query = next.toString();
      replaceSearch(query ? `${pathname}?${query}` : pathname);
    },
    [pathname],
  );

  const goalId = search.get("target");
  const debtId = search.get("utang");
  const openGoal = goalId ? data.goals.find((goal) => goal.id === goalId) ?? null : null;
  const openDebt = debtId ? data.debts.find((debt) => debt.id === debtId) ?? null : null;
  // A stale id is cleared silently once its list has loaded (D21).
  useEffect(() => {
    if (goalId && data.goalsQuery.isSuccess && !openGoal) writeParam("target", null);
  }, [goalId, data.goalsQuery.isSuccess, openGoal, writeParam]);
  useEffect(() => {
    if (debtId && data.debtsQuery.isSuccess && !openDebt) writeParam("utang", null);
  }, [debtId, data.debtsQuery.isSuccess, openDebt, writeParam]);

  const showGoal = (goal: GoalItem) => {
    remember();
    writeParam("target", goal.id);
  };
  const showDebt = (debt: ObligationItem) => {
    remember();
    writeParam("utang", debt.id);
  };
  const showPlans = (state: PlansState) => {
    remember();
    setPlans(state);
  };
  const showCredit = (accountId: string) => {
    remember();
    setCreditId(accountId);
  };
  const addGoal = (emergency = false) => {
    remember();
    setNewGoal({ emergency });
  };
  const openInner = (open: () => void) => {
    innerOpener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    open();
  };
  const restoreInner = () => {
    const target = innerOpener.current;
    innerOpener.current = null;
    requestAnimationFrame(() => {
      if (target?.isConnected) target.focus();
    });
  };
  // An archived goal or debt leaves its list, so focus goes to the list's section instead of the gone row.
  const focusSection = (id: string) => () => {
    opener.current = document.querySelector<HTMLElement>(`[aria-labelledby="${id}"]`);
  };
  const addDebt = () => {
    remember();
    setNewDebt(true);
  };

  const emergencyGoal = data.emergencyGoal;
  const emergencyAction: PageMoreAction[] = !data.goalsQuery.isSuccess
    ? []
    : emergencyGoal
      ? [{ key: "dana-darurat", label: t("page.editEmergency"), icon: "edit", onSelect: () => showGoal(emergencyGoal) }]
      : data.flaggedCount === 0
        ? [{ key: "dana-darurat", label: t("page.createEmergency"), icon: "plus", onSelect: () => addGoal(true) }]
        : [];
  const more: PageMoreAction[] = [{ key: "utang", label: t("page.addDebt"), icon: "plus", onSelect: addDebt }, ...emergencyAction];

  return (
    <div className="stack tg-page">
      {/* The title block's actions: two visible, the rest in "…" (D2); on a phone both stay visible. */}
      <PageActions more={more}>
        <button type="button" className="ws-btn" aria-haspopup="dialog" onClick={() => showPlans({})}>
          <Icon name="calendar" className="h-4 w-4" />
          {t("page.plans")}
        </button>
        <button type="button" className="ws-btn" aria-haspopup="dialog" onClick={() => addGoal()}>
          <Icon name="plus" className="h-4 w-4" />
          {t("page.addGoal")}
        </button>
      </PageActions>
      <TargetHero query={data.netWorthQuery} goals={data.goals} />
      <div className="cols tg-cols">
        <div className="stack">
          <BillsSection
            data={data}
            onManage={() => showPlans({})}
            onOpenPlan={(planId) => showPlans({ planId })}
            onOpenDraft={(expenseDraft) => showPlans({ expenseDraft })}
            onOpenCredit={showCredit}
          />
          <GoalsSection data={data} onOpen={showGoal} onAdd={() => addGoal()} />
        </div>
        <DebtsPanel data={data} onOpenDebt={showDebt} onOpenCredit={showCredit} />
      </div>

      {openGoal && (
        <GoalDialog
          key={openGoal.id}
          goal={openGoal}
          accounts={data.accounts}
          onClose={() => {
            writeParam("target", null);
            restore();
          }}
          onAddProgress={(goal) => openInner(() => setProgressGoal(goal))}
          onArchived={focusSection("tg-goals")}
        />
      )}
      {newGoal && (
        <GoalDialog
          goal={null}
          emergency={newGoal.emergency}
          accounts={data.accounts}
          onClose={() => {
            setNewGoal(null);
            restore();
          }}
          onAddProgress={() => undefined}
        />
      )}
      {progressGoal && (
        <GoalProgressDialog
          goal={progressGoal}
          onClose={() => {
            setProgressGoal(null);
            restoreInner();
          }}
        />
      )}
      {openDebt && (
        <DebtDialog
          key={openDebt.id}
          debt={openDebt}
          onClose={() => {
            writeParam("utang", null);
            restore();
          }}
          onPay={(debt) => openInner(() => setPayDebt(debt))}
          onArchived={focusSection("tg-debts")}
        />
      )}
      {newDebt && (
        <DebtDialog
          debt={null}
          onClose={() => {
            setNewDebt(false);
            restore();
          }}
          onPay={() => undefined}
        />
      )}
      {payDebt && (
        <DebtPaymentDialog
          debt={payDebt}
          accounts={data.accounts}
          onClose={() => {
            setPayDebt(null);
            restoreInner();
          }}
        />
      )}
      {plans && (
        <PlansDialog
          open
          planId={plans.planId ?? null}
          expenseDraft={plans.expenseDraft ?? null}
          onClose={() => {
            setPlans(null);
            restore();
          }}
        />
      )}
      <AccountDetailDialog
        accountId={creditId}
        onClose={() => {
          setCreditId(null);
          restore();
        }}
      />
    </div>
  );
}
