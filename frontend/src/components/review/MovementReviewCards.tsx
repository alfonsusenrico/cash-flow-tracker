"use client";

import Link from "next/link";
import { transaksiHref } from "@/lib/triage";
import { useId, useState, type FormEvent } from "react";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Choice, Picker } from "@/components/review/triage/Choice";
import { ItemHead, longWhen } from "@/components/review/triage/ItemHead";
import type { AccountOptionItem } from "@/components/ui/AccountSelectOptions";
import type { CategoryOption } from "@/components/review/ReviewGroupCard";
import { listLiquidAccountChoices } from "@/lib/accountOptions";
import { KAKEIBO_PILLARS } from "@/lib/kakeibo";
import type { AmbiguousItem, AwaitingItem, CompletionCandidate, ReviewItem, TopupProductChoice } from "@/lib/review";

/** Account choices numbered 1–9 before the rest move to a select (D4). */
const NUMBERED_ACCOUNTS = 9;

export interface TopupOptions {
  /** Amount-tracked mutual-fund products a debit can become a top-up of. */
  products: TopupProductChoice[];
  /** Unit-tracked mutual funds, which must switch to amount tracking first. */
  unitFunds: TopupProductChoice[];
  onConvert: (item: ReviewItem, productId: string, suggestedId?: string) => Promise<boolean>;
  onReject: (item: ReviewItem, suggestedId?: string) => Promise<boolean>;
}

interface Counterpart {
  id: string;
  label: string;
}

/** Liquid accounts and pockets the leg can pair with; a parent that holds pockets is not a counterpart. */
export function counterpartChoices(accounts: AccountOptionItem[], excludeId: string): Counterpart[] {
  const listed = listLiquidAccountChoices(accounts) as AccountOptionItem[];
  const names = new Map(accounts.flatMap((account) => [account, ...(account.children ?? [])])
    .map((account) => [String(account.id ?? account.account_id), String(account.name ?? account.account_name ?? "")]));
  return listed
    .filter((account) => !(account.children?.length) && !account.is_parent)
    .map((account) => {
      const id = String(account.id ?? account.account_id);
      const parent = account.parent_id ? names.get(String(account.parent_id)) : null;
      const name = String(account.name ?? account.account_name ?? "");
      return { id, label: parent ? `${parent} · ${name}` : name };
    })
    .filter((choice) => choice.id !== excludeId);
}

function EditLink({ item }: { item: ReviewItem }) {
  return (
    <Link href={transaksiHref(item.id, "pindah")} className="ws-btn ws-btn-sm">
      Ubah di Transaksi
    </Link>
  );
}

/** "Ini top up investasi": each amount-tracked product is one press; unit-tracked funds are explained. */
function TopupPicker({ item, topup, pending, suggestedId, id }: {
  item: AwaitingItem;
  topup: TopupOptions;
  pending: boolean;
  suggestedId?: string;
  id: string;
}) {
  const products = suggestedId ? topup.products.filter((product) => product.id !== suggestedId) : topup.products;
  return (
    <Picker id={id} title="Ini top up investasi. Ke produk mana?">
      {products.length > 0 ? (
        <div className="ws-choices">
          {products.map((product, index) => (
            <Choice
              key={product.id}
              n={index + 1}
              disabled={pending}
              label={product.label}
              sub="Dicatat per nominal"
              onClick={() => void topup.onConvert(item, product.id, suggestedId)}
            />
          ))}
        </div>
      ) : (
        <p className="ws-caption">Belum ada produk reksadana yang dicatat berdasarkan nominal.</p>
      )}
      {topup.unitFunds.length > 0 && (
        <p className="ws-caption">
          {topup.unitFunds.map((fund) => fund.label).join(", ")} masih dicatat per unit. Ubah dulu ke pelacakan nominal di{" "}
          <Link href="/accounts" className="ws-linkbtn">Rekening</Link>{" "}
          (menu produk, &quot;Ubah ke pelacakan nominal&quot;).
        </p>
      )}
      <p className="ws-caption">Saldo {item.account_name} tidak berubah lagi karena debit ini sudah tercatat.</p>
    </Picker>
  );
}

/** OD15: the leg was never a transfer. It becomes an owner-classified expense through the bulk recategorise. */
function ExpensePicker({ item, categories, pending, onExpense, id }: {
  item: AwaitingItem;
  categories: CategoryOption[];
  pending: boolean;
  onExpense: (item: ReviewItem, categoryId: string, pillar: string | null) => Promise<boolean>;
  id: string;
}) {
  const [categoryId, setCategoryId] = useState("");
  const [pillar, setPillar] = useState("");
  const [error, setError] = useState("");
  const choices = categories.filter((category) => category.kind === "expense" && !category.is_archived && category.name !== "Internal Movement");
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!categoryId) {
      setError("Pilih kategori pengeluarannya.");
      return;
    }
    setError("");
    await onExpense(item, categoryId, pillar || null);
  };
  return (
    <Picker id={id} title="Bukan pindah saldo, ini pengeluaran">
      <form onSubmit={submit} className="ws-form-row">
        <label className="ws-field">
          Kategori
          <select value={categoryId} onChange={(event) => setCategoryId(event.target.value)} aria-invalid={error ? true : undefined}>
            <option value="">Pilih kategori</option>
            {choices.map((category) => (
              <option key={category.id} value={category.id}>{category.name}</option>
            ))}
          </select>
        </label>
        <label className="ws-field">
          Pilar Kakeibo
          <select value={pillar} onChange={(event) => setPillar(event.target.value)}>
            <option value="">Ikuti kategori</option>
            {KAKEIBO_PILLARS.map((entry) => (
              <option key={entry.value} value={entry.value}>{entry.label}</option>
            ))}
          </select>
        </label>
        <button type="submit" disabled={pending} className="ws-btn ws-btn-ink">Simpan sebagai pengeluaran</button>
      </form>
      <p className="ws-caption">Setelah disimpan, ini dihitung di Keluar dan tidak dipasangkan lagi.</p>
      {error && <p role="alert" className="ws-alert">{error}</p>}
    </Picker>
  );
}

interface AwaitingMovementCardProps {
  item: AwaitingItem;
  headingId: string;
  accounts: AccountOptionItem[];
  categories: CategoryOption[];
  pending: boolean;
  /** Rows already on the chosen account that may be the missing leg; set after the server refuses to duplicate. */
  candidates?: CompletionCandidate[];
  onComplete: (item: ReviewItem, accountId: string, createNew?: boolean) => Promise<boolean>;
  onLinkExisting: (item: ReviewItem, candidateId: string) => Promise<boolean>;
  onExpense: (item: ReviewItem, categoryId: string, pillar: string | null) => Promise<boolean>;
  /** Present for an outgoing leg from a liquid account: it may have bought a mutual fund. */
  topup?: TopupOptions;
}

type AwaitingPanel = "none" | "other" | "topup" | "expense";

/**
 * A one-sided own transfer. A learned top-up is offered first (1 Iya, 2 Pilih lain, 3 Bukan top up);
 * otherwise the counterpart accounts, "Ini top up investasi" and "Bukan pindah saldo, ini pengeluaran".
 */
export function AwaitingMovementCard({
  item, headingId, accounts, categories, pending, candidates, onComplete, onLinkExisting, onExpense, topup,
}: AwaitingMovementCardProps) {
  const { bal } = useAppCtx();
  const baseId = useId();
  const outgoing = item.type === "expense";
  const suggestion = topup && outgoing ? item.topup_suggestion : null;
  const target = suggestion?.target_account_id ?? null;
  const [panel, setPanel] = useState<AwaitingPanel>("none");
  const [otherAccount, setOtherAccount] = useState("");
  const [error, setError] = useState("");
  const counterparts = counterpartChoices(accounts, item.account_id);
  const numbered = counterparts.slice(0, NUMBERED_ACCOUNTS);
  const rest = counterparts.slice(NUMBERED_ACCOUNTS);
  const toggle = (next: AwaitingPanel) => setPanel((current) => (current === next ? "none" : next));

  const submitOther = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!otherAccount) {
      setError(outgoing ? "Pilih rekening tujuan." : "Pilih rekening asal.");
      return;
    }
    setError("");
    await onComplete(item, otherAccount);
  };

  const pairing = (startAt: number) => {
    let n = startAt;
    return (
      <>
        <div className="ws-choices">
          {numbered.map((account) => (
            <Choice
              key={account.id}
              n={n++}
              disabled={pending}
              label={account.label}
              sub={outgoing ? "Pindah saldo ke sini" : "Pindah saldo dari sini"}
              onClick={() => void onComplete(item, account.id)}
            />
          ))}
          {topup && outgoing && (
            <Choice
              n={n++}
              topup
              disabled={pending}
              label="Ini top up investasi"
              sub="Masuk ke produk, dihitung Tabungan"
              expanded={panel === "topup"}
              controls={panel === "topup" ? `${baseId}-topup` : undefined}
              onClick={() => toggle("topup")}
            />
          )}
          {outgoing && (
            <Choice
              n={n++}
              disabled={pending}
              label="Bukan pindah saldo, ini pengeluaran"
              sub="Pilih kategorinya, lalu dihitung di Keluar"
              expanded={panel === "expense"}
              controls={panel === "expense" ? `${baseId}-expense` : undefined}
              onClick={() => toggle("expense")}
            />
          )}
        </div>
        {rest.length > 0 && (
          <form onSubmit={submitOther} className="ws-form-row mt-3">
            <label className="ws-field">
              {outgoing ? "Rekening tujuan lain" : "Rekening asal lain"}
              <select value={otherAccount} onChange={(event) => setOtherAccount(event.target.value)} aria-invalid={error ? true : undefined}>
                <option value="">Pilih rekening atau kantong</option>
                {rest.map((account) => (
                  <option key={account.id} value={account.id}>{account.label}</option>
                ))}
              </select>
            </label>
            <button type="submit" disabled={pending} className="ws-btn">Lengkapi pindah saldo</button>
          </form>
        )}
      </>
    );
  };

  return (
    <>
      <ItemHead
        headingId={headingId}
        kind={{ tone: "tr", icon: "arrows-right-left", text: "Pindah saldo belum lengkap" }}
        title={outgoing ? `Uang keluar dari ${item.account_name}. Masuknya ke mana?` : `Uang masuk ke ${item.account_name}. Dari mana?`}
        account={item.account_name}
        date={item.date}
        source={item.source}
        amount={{ text: `⇄ ${bal(item.amount)}`, className: "ws-amt-tr" }}
        reasons={item.reasons}
      />
      <p className="ws-caption">
        {item.notes ? `${item.notes}. ` : ""}
        {outgoing ? "Selama belum lengkap, ini belum masuk Keluar." : "Selama belum lengkap, ini belum masuk Masuk."}
      </p>

      {suggestion && topup && target && (
        <section aria-labelledby={`${baseId}-suggest`} className="ws-suggest">
          <b id={`${baseId}-suggest`}>{suggestion.prompt}</b>
          <p>{suggestion.target_label ? `${suggestion.target_label}. ` : null}{suggestion.explanation}</p>
          <div className="ws-choices">
            <Choice
              n={1}
              primary
              disabled={pending}
              label={`Iya, top up ${topup.products.find((product) => product.id === target)?.name ?? suggestion.target_label ?? "produk ini"}`}
              sub="Saran"
              onClick={() => void topup.onConvert(item, target, target)}
            />
            <Choice
              n={2}
              disabled={pending}
              label="Pilih lain"
              sub="Produk lain atau rekening pasangan"
              expanded={panel === "other" || panel === "topup" || panel === "expense"}
              controls={panel === "other" ? `${baseId}-other` : undefined}
              onClick={() => toggle("other")}
            />
            <Choice n={3} disabled={pending} label="Bukan top up" sub="Diingat untuk debit seperti ini" onClick={() => void topup.onReject(item, target)} />
          </div>
        </section>
      )}

      {suggestion && topup && !target && (
        <section aria-labelledby={`${baseId}-ask`} className="ws-suggest">
          <b id={`${baseId}-ask`}>{suggestion.prompt}</b>
          <p>{suggestion.explanation}</p>
          <div className="ws-choices">
            {suggestion.choices.map((product, index) => (
              <Choice
                key={product.id}
                n={index + 1}
                disabled={pending}
                label={product.label}
                sub="Top up ke sini"
                onClick={() => void topup.onConvert(item, product.id)}
              />
            ))}
            <Choice
              n={suggestion.choices.length + 1}
              disabled={pending}
              label="Bukan top up"
              sub="Pilih rekening pasangannya"
              onClick={() => void topup.onReject(item)}
            />
          </div>
        </section>
      )}

      {target ? (
        panel !== "none" && (
          <Picker id={`${baseId}-other`} title="Pilih pasangannya">
            {pairing(1)}
          </Picker>
        )
      ) : (
        !suggestion && (
          <div className="mt-3" role="group" aria-label="Pilih pasangannya">
            {pairing(1)}
          </div>
        )
      )}
      {panel === "topup" && topup && (
        <TopupPicker id={`${baseId}-topup`} item={item} topup={topup} pending={pending} suggestedId={target ?? undefined} />
      )}
      {panel === "expense" && (
        <ExpensePicker id={`${baseId}-expense`} item={item} categories={categories} pending={pending} onExpense={onExpense} />
      )}

      {candidates && candidates.length > 0 && (
        <Picker id={`${baseId}-candidates`} title="Sudah ada di rekening ini">
          <p className="ws-caption">Transaksi ini mungkin sisi lain pindah saldo. Hubungkan agar tidak tercatat dua kali.</p>
          <div className="ws-choices">
            {candidates.map((candidate, index) => (
              <Choice
                key={candidate.id}
                n={index + 1}
                disabled={pending}
                label={`Hubungkan: ${bal(candidate.amount)}`}
                sub={[longWhen(candidate.date), candidate.category_name, candidate.notes].filter(Boolean).join(" · ")}
                onClick={() => void onLinkExisting(item, candidate.id)}
              />
            ))}
            <Choice
              n={candidates.length + 1}
              disabled={pending}
              label="Tetap catat baru"
              sub="Rekening itu memang punya transaksi lain"
              onClick={() => void onComplete(item, candidates[0].account_id, true)}
            />
          </div>
        </Picker>
      )}
      {error && <p role="alert" className="ws-alert">{error}</p>}
      <div className="mt-4">
        <EditLink item={item} />
      </div>
    </>
  );
}

interface AmbiguousMovementCardProps {
  item: AmbiguousItem;
  headingId: string;
  pending: boolean;
  onLink: (item: AmbiguousItem, counterpartId: string) => Promise<boolean>;
  onKeepSeparate: (item: AmbiguousItem) => Promise<boolean>;
}

/** A leg with several possible counterparts: each candidate is one press, then "Biarkan terpisah". */
export function AmbiguousMovementCard({ item, headingId, pending, onLink, onKeepSeparate }: AmbiguousMovementCardProps) {
  const { bal } = useAppCtx();
  const outgoing = item.type === "expense";
  return (
    <>
      <ItemHead
        headingId={headingId}
        kind={{ tone: "tr", icon: "arrows-right-left", text: "Pasangan belum pasti" }}
        title={outgoing ? `Keluar dari ${item.account_name}. Pasangannya yang mana?` : `Masuk ke ${item.account_name}. Asalnya yang mana?`}
        account={item.account_name}
        date={item.date}
        source={item.source}
        amount={{ text: `⇄ ${bal(item.amount)}`, className: "ws-amt-tr" }}
        reasons={item.reasons}
      />
      {item.candidates.length === 0 && <p className="ws-caption">Tidak ada pasangan dengan nominal yang sama.</p>}
      <div role="group" aria-label={outgoing ? "Uang ini masuk ke" : "Uang ini berasal dari"} className="ws-choices">
        {item.candidates.map((candidate, index) => (
          <Choice
            key={candidate.id}
            n={index + 1}
            disabled={pending}
            label={candidate.account_name ?? "Rekening"}
            sub={`${longWhen(candidate.date) ?? ""} · ${bal(candidate.amount)} · gabungkan sebagai pindah saldo`}
            onClick={() => void onLink(item, candidate.id)}
          />
        ))}
        <Choice
          n={item.candidates.length + 1}
          disabled={pending}
          label="Biarkan terpisah"
          sub="Bukan pindah saldo, jangan dipasangkan otomatis"
          onClick={() => void onKeepSeparate(item)}
        />
      </div>
      <div className="mt-4">
        <EditLink item={item} />
      </div>
    </>
  );
}
