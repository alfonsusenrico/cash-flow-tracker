"use client";
import { useState, useEffect } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input, Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";
import { toDatetimeLocal, fromDatetimeLocal } from "@/lib/utils";
import { AccountSelectOptions } from "@/components/ui/AccountSelectOptions";
import type { Account, Category, LedgerRow } from "@/types/domain";

interface Props {
  open: boolean;
  onClose: () => void;
  accounts: Account[];
  categories: Category[];
  editing?: LedgerRow | null;
  defaultAccountId?: string;
}

export function TransactionModal({ open, onClose, accounts, categories, editing, defaultAccountId }: Props) {
  const qc = useQueryClient();
  const isEdit = !!editing;

  const [type, setType] = useState<"debit" | "credit">("credit");
  const [accountId, setAccountId] = useState(defaultAccountId ?? accounts[0]?.account_id ?? "");
  const [name, setName] = useState("");
  const [amount, setAmount] = useState(0);
  const [date, setDate] = useState(() => toDatetimeLocal());
  const [categoryId, setCategoryId] = useState("");
  const [notes, setNotes] = useState("");
  const [isTopup, setIsTopup] = useState(false);
  const [err, setErr] = useState("");

  // Stock Investment helper states
  const [isStockMode, setIsStockMode] = useState(false);
  const [stockLots, setStockLots] = useState<number | "">("");
  const [stockPricePerShare, setStockPricePerShare] = useState<number | "">("");
  const [stockTicker, setStockTicker] = useState("");

  function updateStockTotal(lots: number | "", price: number | "", ticker: string) {
    if (typeof lots === "number" && lots > 0 && typeof price === "number" && price > 0) {
      const total = lots * 100 * price;
      setAmount(total);
      const formattedPrice = price.toLocaleString("id-ID");
      const generatedName = ticker.trim()
        ? `${ticker.trim().toUpperCase()} (${lots} lot @ Rp ${formattedPrice})`
        : `Saham (${lots} lot @ Rp ${formattedPrice})`;
      setName(generatedName);
    }
  }

  useEffect(() => {
    if (!open) return;
    setIsStockMode(false);
    setStockLots("");
    setStockPricePerShare("");
    setStockTicker("");
    if (editing) {
      setType(editing.debit > 0 ? "debit" : "credit");
      setAccountId(editing.account_id ?? "");
      setName(editing.transaction_name);
      setAmount(editing.debit > 0 ? editing.debit : editing.credit);
      setDate(toDatetimeLocal(editing.date));
      setIsTopup(editing.is_cycle_topup);
      setCategoryId("");
      setNotes("");
    } else {
      setType("credit");
      const initAccId = defaultAccountId ?? accounts[0]?.account_id ?? "";
      setAccountId(initAccId);
      const initAcc = accounts.find((a) => a.account_id === initAccId);
      if (initAcc?.type === "investment") {
        setIsStockMode(true);
      }
      setName("");
      setAmount(0);
      setDate(toDatetimeLocal());
      setIsTopup(false);
      setCategoryId("");
      setNotes("");
    }
    setErr("");
  }, [open, editing, defaultAccountId, accounts]);

  const mut = useMutation({
    mutationFn: (payload: object) =>
      isEdit
        ? api.put(`/transactions/${editing!.transaction_id}`, payload)
        : api.post("/transactions", payload),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
      onClose();
    },
    onError: (e: Error) => setErr(e.message),
  });

  const deleteMut = useMutation({
    mutationFn: () => api.del(`/transactions/${editing!.transaction_id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
      onClose();
    },
    onError: (e: Error) => setErr(e.message),
  });

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!amount) return setErr("Amount is required");
    mut.mutate({
      account_id: accountId,
      transaction_type: type,
      transaction_name: name,
      amount,
      date: fromDatetimeLocal(date),
      is_cycle_topup: isTopup,
      category_id: categoryId || null,
      notes: notes || null,
    });
  }

  const filteredCategories = categories.filter(
    (c) => !c.is_archived && (type === "debit" ? c.kind === "income" : c.kind === "expense")
  );

  return (
    <Modal open={open} onClose={onClose} title={isEdit ? "Edit Transaction" : "Add Transaction"}>
      <form onSubmit={handleSubmit} className="space-y-4">
        {/* Type toggle */}
        <div className="flex rounded overflow-hidden border border-[var(--border)]">
          {(["credit", "debit"] as const).map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setType(t)}
              className={`flex-1 py-2 text-sm font-medium transition-colors ${
                type === t
                  ? t === "debit" ? "bg-green-600 text-white" : "bg-red-500 text-white"
                  : "bg-[var(--surface)] text-[var(--muted)] hover:bg-[var(--bg)]"
              }`}
            >
              {t === "debit" ? "Cash In" : "Cash Out"}
            </button>
          ))}
        </div>

        <Select
          label="Account"
          value={accountId}
          onChange={(e) => {
            const nextId = e.target.value;
            setAccountId(nextId);
            const acc = accounts.find((a) => a.account_id === nextId);
            if (acc?.type === "investment") {
              setIsStockMode(true);
              const investCat = categories.find((c) => c.name.toLowerCase().includes("invest"));
              if (investCat) setCategoryId(investCat.category_id);
            }
          }}
          required
        >
          <AccountSelectOptions accounts={accounts} allowParentSelection={true} />
        </Select>

        {/* Stock / Lot Calculator */}
        <div className="rounded border border-[var(--border)] bg-[var(--surface)] p-3 space-y-2.5">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold uppercase tracking-wider text-[var(--muted)]">
              Kalkulator Saham (Lot & Harga)
            </span>
            <label className="flex items-center gap-1.5 text-xs text-[var(--muted)] cursor-pointer">
              <input
                type="checkbox"
                checked={isStockMode}
                onChange={(e) => setIsStockMode(e.target.checked)}
                className="rounded"
              />
              Input per Lot
            </label>
          </div>

          {isStockMode && (
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 pt-1">
              <div>
                <label className="block text-xs text-[var(--muted)] mb-1">Ticker</label>
                <input
                  type="text"
                  placeholder="e.g. BBRI"
                  value={stockTicker}
                  onChange={(e) => {
                    const tick = e.target.value.toUpperCase();
                    setStockTicker(tick);
                    updateStockTotal(stockLots, stockPricePerShare, tick);
                  }}
                  className="w-full px-2.5 py-1.5 text-sm bg-[var(--bg)] border border-[var(--border)] rounded text-[var(--text)] uppercase"
                />
              </div>
              <div>
                <label className="block text-xs text-[var(--muted)] mb-1">Lot (1 lot = 100 lembar)</label>
                <input
                  type="number"
                  min="1"
                  step="1"
                  placeholder="e.g. 4"
                  value={stockLots}
                  onChange={(e) => {
                    const val = e.target.value ? Number(e.target.value) : "";
                    setStockLots(val);
                    updateStockTotal(val, stockPricePerShare, stockTicker);
                  }}
                  className="w-full px-2.5 py-1.5 text-sm bg-[var(--bg)] border border-[var(--border)] rounded text-[var(--text)]"
                />
              </div>
              <div>
                <label className="block text-xs text-[var(--muted)] mb-1">Harga / Lembar (IDR)</label>
                <input
                  type="number"
                  min="1"
                  step="1"
                  placeholder="e.g. 3340"
                  value={stockPricePerShare}
                  onChange={(e) => {
                    const val = e.target.value ? Number(e.target.value) : "";
                    setStockPricePerShare(val);
                    updateStockTotal(stockLots, val, stockTicker);
                  }}
                  className="w-full px-2.5 py-1.5 text-sm bg-[var(--bg)] border border-[var(--border)] rounded text-[var(--text)]"
                />
              </div>
            </div>
          )}

          {isStockMode && stockLots !== "" && stockPricePerShare !== "" && Number(stockLots) > 0 && Number(stockPricePerShare) > 0 && (
            <div className="text-xs text-emerald-400 bg-emerald-950/30 border border-emerald-800/40 rounded p-2 flex justify-between items-center">
              <span>
                {stockLots} lot ({Number(stockLots) * 100} lembar) × Rp {Number(stockPricePerShare).toLocaleString("id-ID")}
              </span>
              <span className="font-bold font-mono text-sm">
                = Rp {(Number(stockLots) * 100 * Number(stockPricePerShare)).toLocaleString("id-ID")}
              </span>
            </div>
          )}
        </div>

        <Input
          label="Description"
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
          placeholder="e.g. Lunch, Salary"
        />

        <MoneyInput label="Amount" value={amount} onChange={setAmount} required />

        <Input
          label="Date & Time"
          type="datetime-local"
          value={date}
          onChange={(e) => setDate(e.target.value)}
          required
        />

        {filteredCategories.length > 0 && (
          <Select label="Category (optional)" value={categoryId} onChange={(e) => setCategoryId(e.target.value)}>
            <option value="">— none —</option>
            {filteredCategories.map((c) => (
              <option key={c.category_id} value={c.category_id}>{c.name}</option>
            ))}
          </Select>
        )}

        <Input
          label="Notes (optional)"
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
          placeholder="Optional note"
        />

        {type === "debit" && (
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="checkbox" checked={isTopup} onChange={(e) => setIsTopup(e.target.checked)} className="rounded" />
            Mark as Payroll / Top-up
          </label>
        )}

        {err && <p className="text-sm text-[var(--danger)]">{err}</p>}

        <div className="flex gap-2 pt-1">
          <Button type="submit" variant="primary" className="flex-1" disabled={mut.isPending}>
            {mut.isPending ? "Saving…" : "Save"}
          </Button>
          {isEdit && (
            <Button
              type="button"
              variant="danger"
              disabled={deleteMut.isPending}
              onClick={() => confirm("Delete this transaction?") && deleteMut.mutate()}
            >
              Delete
            </Button>
          )}
          <Button type="button" variant="secondary" onClick={onClose}>Cancel</Button>
        </div>
      </form>
    </Modal>
  );
}
