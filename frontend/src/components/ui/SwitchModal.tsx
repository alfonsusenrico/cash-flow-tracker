"use client";
import { useState, useEffect } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Select } from "@/components/ui/Input";
import { MoneyInput } from "@/components/ui/MoneyInput";
import type { Account } from "@/types/domain";

interface Props {
  open: boolean;
  onClose: () => void;
  accounts: Account[];
}

export function SwitchModal({ open, onClose, accounts }: Props) {
  const qc = useQueryClient();
  const [fromId, setFromId] = useState("");
  const [toId, setToId] = useState("");
  const [amount, setAmount] = useState(0);
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 16));
  const [isTopup, setIsTopup] = useState(false);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (!open) return;
    setFromId(accounts[0]?.account_id ?? "");
    setToId(accounts[1]?.account_id ?? "");
    setAmount(0);
    setDate(new Date().toISOString().slice(0, 16));
    setIsTopup(false);
    setErr("");
  }, [open, accounts]);

  const mut = useMutation({
    mutationFn: () =>
      api.post("/switch", {
        source_account_id: fromId,
        target_account_id: toId,
        amount,
        date,
        is_cycle_topup: isTopup,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["ledger"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
      onClose();
    },
    onError: (e: Error) => setErr(e.message),
  });

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (fromId === toId) return setErr("Source and target must differ");
    if (!amount) return setErr("Amount is required");
    mut.mutate();
  }

  return (
    <Modal open={open} onClose={onClose} title="Switch Balance">
      <form onSubmit={handleSubmit} className="space-y-4">
        <Select label="From Account" value={fromId} onChange={(e) => setFromId(e.target.value)} required>
          {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
        </Select>
        <Select label="To Account" value={toId} onChange={(e) => setToId(e.target.value)} required>
          {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
        </Select>
        <MoneyInput label="Amount" value={amount} onChange={setAmount} required />
        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-[var(--muted)]">Date & Time</label>
          <input
            type="datetime-local"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            required
            className="w-full border border-[var(--border)] rounded px-3 py-2 text-sm bg-[var(--surface)] text-[var(--text)] focus:outline-none focus:ring-2 focus:ring-[var(--primary)]/40"
          />
        </div>
        <label className="flex items-center gap-2 text-sm cursor-pointer">
          <input type="checkbox" checked={isTopup} onChange={(e) => setIsTopup(e.target.checked)} className="rounded" />
          Mark as Payroll / Top-up
        </label>
        {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
        <div className="flex gap-2 pt-1">
          <Button type="submit" variant="primary" className="flex-1" disabled={mut.isPending}>
            {mut.isPending ? "Switching…" : "Switch"}
          </Button>
          <Button type="button" variant="secondary" onClick={onClose}>Cancel</Button>
        </div>
      </form>
    </Modal>
  );
}
