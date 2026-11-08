"use client";

import Link from "next/link";
import { useId, useRef, useState, type FormEvent } from "react";
import { useTranslations } from "next-intl";
import { CategoryDialog, type CategoriesState } from "@/components/review/CategoryDialog";
import type { AccountOptionItem } from "@/components/ui/AccountSelectOptions";
import { isCreditAccountChoice, listCreditAccountChoices, listLiquidAccountChoices } from "@/lib/accountOptions";
import type { AmbiguousItem, AwaitingItem, CompletionCandidate, ReviewItem, TopupProductChoice } from "@/lib/review";
import { transaksiHref } from "@/lib/triage";
import { Answer, AnswerList, NestedAnswers } from "../Answers";
import { SignedAmount } from "../ItemBody";
import { dateTime } from "../copy";
import type { ItemContext, ItemParts, SaveResult } from "../types";

/** Account answers numbered 1–9 before the rest move to a select (redesign-review-triage D4). */
const NUMBERED_ACCOUNTS = 9;

export interface TopupOptions {
  /** Amount-tracked mutual-fund products a debit can become a top-up of. */
  products: TopupProductChoice[];
  /** Unit-tracked mutual funds, which must switch to amount tracking first. */
  unitFunds: TopupProductChoice[];
  onConvert: (item: ReviewItem, productId: string, suggestedId?: string) => Promise<SaveResult>;
  onReject: (item: ReviewItem, suggestedId?: string) => Promise<SaveResult>;
}

export interface AwaitingHandlers {
  accounts: AccountOptionItem[];
  categories: CategoriesState;
  /** Rows already on the chosen account that may be the missing leg; set after the server refuses to duplicate. */
  candidates?: CompletionCandidate[];
  onComplete: (item: ReviewItem, accountId: string, createNew?: boolean) => Promise<SaveResult>;
  onLinkExisting: (item: ReviewItem, candidateId: string) => Promise<SaveResult>;
  onSpending: (item: ReviewItem, fields: Record<string, string>) => Promise<SaveResult>;
  /** Present for an outgoing leg from a liquid account: it may have bought a mutual fund. */
  topup?: TopupOptions;
}

interface Counterpart {
  id: string;
  label: string;
  /** A credit account: completing with it records a credit payment or a cash advance (add-credit-accounts D6). */
  credit: boolean;
}

/**
 * Liquid accounts and pockets the leg can pair with, then credit accounts; a parent that holds pockets is not
 * a counterpart. A leg on a credit account gets cash counterparts only: one credit account cannot pay another.
 */
export function counterpartChoices(accounts: AccountOptionItem[], excludeId: string): Counterpart[] {
  const credit = listCreditAccountChoices(accounts) as AccountOptionItem[];
  const legOnCredit = credit.some((account) => String(account.id ?? account.account_id) === excludeId);
  const listed = [...listLiquidAccountChoices(accounts), ...(legOnCredit ? [] : credit)] as AccountOptionItem[];
  const names = new Map(
    accounts
      .flatMap((account) => [account, ...(account.children ?? [])])
      .map((account) => [String(account.id ?? account.account_id), String(account.name ?? account.account_name ?? "")]),
  );
  return listed
    .filter((account) => !account.children?.length && !account.is_parent)
    .map((account) => {
      const id = String(account.id ?? account.account_id);
      const parent = account.parent_id ? names.get(String(account.parent_id)) : null;
      const name = String(account.name ?? account.account_name ?? "");
      return { id, label: parent ? `${parent} · ${name}` : name, credit: isCreditAccountChoice(account) };
    })
    .filter((choice) => choice.id !== excludeId);
}

function EditLink({ item }: { item: ReviewItem }) {
  const t = useTranslations("review.answer");
  return (
    <div className="tj-after">
      <Link href={transaksiHref(item.id, "pindah")} className="ws-btn ws-btn-quiet">
        {t("editInLedger")}
      </Link>
    </div>
  );
}

/** "Ini top up investasi": each amount-tracked product is one press; unit-tracked funds are explained. */
function TopupProducts({ id, item, topup, pending, suggestedId }: {
  id: string;
  item: AwaitingItem;
  topup: TopupOptions;
  pending: boolean;
  suggestedId?: string;
}) {
  const t = useTranslations("review.answer");
  const products = suggestedId ? topup.products.filter((product) => product.id !== suggestedId) : topup.products;
  return (
    <NestedAnswers
      id={id}
      title={t("productsTitle")}
      note={
        <>
          {!products.length && <p className="t-meta tj-line">{t("noProducts")}</p>}
          {topup.unitFunds.length > 0 && (
            <p className="t-meta tj-line">
              {t.rich("unitFunds", {
                funds: topup.unitFunds.map((fund) => fund.label).join(", "),
                link: (chunks) => (
                  <Link href="/accounts" className="ws-linkbtn">
                    {chunks}
                  </Link>
                ),
              })}
            </p>
          )}
          <p className="t-meta tj-line">{t("balanceUnchanged", { account: item.account_name })}</p>
        </>
      }
    >
      {products.map((product, index) => (
        <Answer
          key={product.id}
          n={index + 1}
          disabled={pending}
          label={product.label}
          detail={t("perAmount")}
          onClick={() => void topup.onConvert(item, product.id, suggestedId)}
        />
      ))}
    </NestedAnswers>
  );
}

type Opened = "none" | "other" | "topup";

/**
 * A one-sided own transfer (D9). A learned top-up comes first: 1 "Ya, top up" (E, T), 2 "Pilih lain", 3 "Bukan top
 * up". Otherwise the accounts, "Ini top up investasi" (T) and "Bukan pindah saldo, ini pengeluaran", each one press.
 */
function AwaitingAnswers({ item, ctx, handlers }: { item: AwaitingItem; ctx: ItemContext; handlers: AwaitingHandlers }) {
  const t = useTranslations("review.answer");
  const baseId = useId();
  const { topup, candidates } = handlers;
  const outgoing = item.type === "expense";
  const suggestion = topup && outgoing ? item.topup_suggestion : null;
  const target = suggestion?.target_account_id ?? null;
  const [opened, setOpened] = useState<Opened>("none");
  const [spending, setSpending] = useState(false);
  const [otherAccount, setOtherAccount] = useState("");
  const [missing, setMissing] = useState(false);
  const spendingOpener = useRef<HTMLButtonElement>(null);
  const counterparts = counterpartChoices(handlers.accounts, item.account_id);
  const numbered = counterparts.slice(0, NUMBERED_ACCOUNTS);
  const rest = counterparts.slice(NUMBERED_ACCOUNTS);
  const toggle = (next: Opened) => setOpened((current) => (current === next ? "none" : next));
  const closeSpending = () => {
    setSpending(false);
    spendingOpener.current?.focus();
  };

  const submitOther = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!otherAccount) {
      setMissing(true);
      return;
    }
    setMissing(false);
    await handlers.onComplete(item, otherAccount);
  };

  const pairing = (topupMarked: boolean) => {
    let n = 1;
    return (
      <>
        {numbered.map((account) => (
          <Answer
            key={account.id}
            n={n++}
            disabled={ctx.pending}
            label={account.label}
            detail={account.credit ? (outgoing ? t("creditTo") : t("creditFrom")) : outgoing ? t("toAccount") : t("fromAccount")}
            onClick={() => void handlers.onComplete(item, account.id)}
          />
        ))}
        {topup && outgoing && (
          <Answer
            n={n++}
            topup={topupMarked}
            disabled={ctx.pending}
            label={t("isTopup")}
            detail={t("isTopupDetail")}
            expanded={opened === "topup"}
            controls={opened === "topup" ? `${baseId}-topup` : undefined}
            onClick={() => toggle("topup")}
          />
        )}
        {outgoing && (
          <Answer
            ref={spendingOpener}
            n={n++}
            disabled={ctx.pending}
            label={t("isSpending")}
            detail={t("isSpendingDetail")}
            onClick={() => setSpending(true)}
          />
        )}
      </>
    );
  };

  const otherSelect = rest.length > 0 && (
    <form onSubmit={submitOther} className="tj-other">
      <div className="field">
        <label htmlFor={`${baseId}-other-account`}>{outgoing ? t("otherTarget") : t("otherSource")}</label>
        <select
          id={`${baseId}-other-account`}
          className="border field-control"
          value={otherAccount}
          aria-invalid={missing ? true : undefined}
          aria-describedby={missing ? `${baseId}-other-error` : undefined}
          onChange={(event) => setOtherAccount(event.target.value)}
        >
          <option value="">{t("chooseAccount")}</option>
          {rest.filter((account) => !account.credit).map((account) => (
            <option key={account.id} value={account.id}>
              {account.label}
            </option>
          ))}
          {rest.some((account) => account.credit) && (
            <optgroup label={t("credit")}>
              {rest.filter((account) => account.credit).map((account) => (
                <option key={account.id} value={account.id}>
                  {account.label}
                </option>
              ))}
            </optgroup>
          )}
        </select>
        {missing && (
          <p id={`${baseId}-other-error`} className="err">
            {outgoing ? t("needTarget") : t("needSource")}
          </p>
        )}
      </div>
      <button type="submit" disabled={ctx.pending} className="ws-btn">
        {t("completeTransfer")}
      </button>
    </form>
  );

  let main;
  if (suggestion && topup && target) {
    const product = topup.products.find((entry) => entry.id === target)?.name ?? suggestion.target_label ?? t("thisProduct");
    main = (
      <AnswerList labelledBy={ctx.headingId}>
        <Answer
          n={1}
          primary
          topup
          disabled={ctx.pending}
          label={t("topupYes", { product })}
          detail={suggestion.explanation}
          onClick={() => void topup.onConvert(item, target, target)}
        />
        <Answer
          n={2}
          disabled={ctx.pending}
          label={t("topupOther")}
          detail={t("topupOtherDetail")}
          expanded={opened !== "none"}
          controls={opened !== "none" ? `${baseId}-other` : undefined}
          onClick={() => toggle("other")}
        />
        <Answer n={3} disabled={ctx.pending} label={t("topupNo")} detail={t("topupNoDetail")} onClick={() => void topup.onReject(item, target)} />
      </AnswerList>
    );
  } else if (suggestion && topup) {
    main = (
      <AnswerList labelledBy={ctx.headingId}>
        {suggestion.choices.map((product, index) => (
          <Answer
            key={product.id}
            n={index + 1}
            disabled={ctx.pending}
            label={product.label}
            detail={t("topupHere")}
            onClick={() => void topup.onConvert(item, product.id)}
          />
        ))}
        <Answer
          n={suggestion.choices.length + 1}
          disabled={ctx.pending}
          label={t("topupNo")}
          detail={t("topupNoPair")}
          onClick={() => void topup.onReject(item)}
        />
      </AnswerList>
    );
  } else {
    main = <AnswerList labelledBy={ctx.headingId}>{pairing(true)}</AnswerList>;
  }

  return (
    <>
      {main}
      {target && opened !== "none" && (
        <NestedAnswers id={`${baseId}-other`} title={t("pairTitle")}>
          {pairing(false)}
        </NestedAnswers>
      )}
      {!suggestion && otherSelect}
      {target && opened !== "none" && otherSelect}
      {opened === "topup" && topup && (
        <TopupProducts id={`${baseId}-topup`} item={item} topup={topup} pending={ctx.pending} suggestedId={target ?? undefined} />
      )}
      {candidates && candidates.length > 0 && (
        <NestedAnswers id={`${baseId}-candidates`} title={t("candidatesTitle")} note={<p className="t-meta tj-line">{t("candidatesNote")}</p>}>
          {candidates.map((candidate, index) => (
            <Answer
              key={candidate.id}
              n={index + 1}
              disabled={ctx.pending}
              label={t("link", { amount: ctx.money(candidate.amount) })}
              detail={[dateTime(ctx.words.format, candidate.date), candidate.category_name, candidate.notes].filter(Boolean).join(" · ")}
              onClick={() => void handlers.onLinkExisting(item, candidate.id)}
            />
          ))}
          <Answer
            n={candidates.length + 1}
            disabled={ctx.pending}
            label={t("recordNew")}
            detail={t("recordNewDetail")}
            onClick={() => void handlers.onComplete(item, candidates[0].account_id, true)}
          />
        </NestedAnswers>
      )}
      <EditLink item={item} />
      <CategoryDialog
        open={spending}
        mode="spending"
        onClose={closeSpending}
        label={item.account_name}
        count={1}
        type="expense"
        categories={handlers.categories}
        onSave={async (fields) => {
          const result = await handlers.onSpending(item, fields);
          if (result.saved) setSpending(false);
          return result;
        }}
      />
    </>
  );
}

/** "Pindah saldo belum lengkap" for an awaiting leg (D9). */
export function awaitingParts(item: AwaitingItem, ctx: ItemContext, handlers: AwaitingHandlers): ItemParts {
  const { t, format } = ctx.words;
  const outgoing = item.type === "expense";
  const suggestion = handlers.topup && outgoing ? item.topup_suggestion : null;
  const choosing = Boolean(suggestion && !suggestion.target_account_id);
  const fixed = outgoing ? t("review.question.awaitingOut", { account: item.account_name }) : t("review.question.awaitingIn", { account: item.account_name });
  return {
    // A product the server cannot pick is its own question; it names the amount, so hidden balances keep the fixed one.
    question: choosing && suggestion && !ctx.hidden ? suggestion.prompt : fixed,
    meta: [
      suggestion ? t("review.meta.topup") : t("review.meta.noCounterpart"),
      <SignedAmount key="amount" amount={item.amount} direction="transfer" />,
      dateTime(format, item.date),
      item.account_name,
    ],
    warning: outgoing ? t("review.meta.warning.outgoing") : t("review.meta.warning.incoming"),
    note: choosing && suggestion ? suggestion.explanation : item.notes || undefined,
    reasons: item.reasons,
    answers: <AwaitingAnswers key={`answers:${item.id}`} item={item} ctx={ctx} handlers={handlers} />,
  };
}

/** A leg with several possible counterparts: each candidate is one press, then "Biarkan terpisah" (D9). */
export function ambiguousParts(
  item: AmbiguousItem,
  ctx: ItemContext,
  onLink: (item: AmbiguousItem, counterpartId: string) => Promise<SaveResult>,
  onKeepSeparate: (item: AmbiguousItem) => Promise<SaveResult>,
): ItemParts {
  const { t, format } = ctx.words;
  const outgoing = item.type === "expense";
  return {
    question: outgoing ? t("review.question.ambiguousOut", { account: item.account_name }) : t("review.question.ambiguousIn", { account: item.account_name }),
    meta: [
      t("review.meta.candidates", { count: item.candidates.length }),
      <SignedAmount key="amount" amount={item.amount} direction="transfer" />,
      dateTime(format, item.date),
      item.account_name,
    ],
    note: item.candidates.length === 0 ? t("review.meta.noCandidates") : undefined,
    reasons: item.reasons,
    answers: (
      <>
        <AnswerList labelledBy={ctx.headingId}>
          {item.candidates.map((candidate, index) => (
            <Answer
              key={candidate.id}
              n={index + 1}
              disabled={ctx.pending}
              label={candidate.account_name ?? t("review.answer.account")}
              detail={t("review.answer.mergeDetail", { date: dateTime(format, candidate.date) ?? "", amount: ctx.money(candidate.amount) })}
              onClick={() => void onLink(item, candidate.id)}
            />
          ))}
          <Answer
            n={item.candidates.length + 1}
            disabled={ctx.pending}
            label={t("review.answer.keepSeparate")}
            detail={t("review.answer.keepSeparateDetail")}
            onClick={() => void onKeepSeparate(item)}
          />
        </AnswerList>
        <EditLink item={item} />
      </>
    ),
  };
}
