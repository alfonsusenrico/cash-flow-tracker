"use client";
import { useState } from "react";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Select } from "@/components/ui/Input";
import type { Account } from "@/types/domain";

interface Props {
  open: boolean;
  onClose: () => void;
  accounts: Account[];
  paydayDay: number;
}

export function ExportModal({ open, onClose, accounts, paydayDay }: Props) {
  const [scope, setScope] = useState<"all" | "account">("all");
  const [accountId, setAccountId] = useState(accounts[0]?.account_id ?? "");
  const [format, setFormat] = useState<"pdf" | "csv">("pdf");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  async function handleExport(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setErr("");
    try {
      const res = await fetch("/api/export", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          day: paydayDay,
          format,
          scope,
          account_id: scope === "account" ? accountId : undefined,
        }),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(d.detail ?? "Export failed");
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `ledger.${format}`;
      a.click();
      URL.revokeObjectURL(url);
      onClose();
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Export failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Modal open={open} onClose={onClose} title="Export Ledger">
      <form onSubmit={handleExport} className="space-y-4">
        <Select label="Scope" value={scope} onChange={(e) => setScope(e.target.value as "all" | "account")}>
          <option value="all">All Accounts</option>
          <option value="account">Single Account</option>
        </Select>
        {scope === "account" && (
          <Select label="Account" value={accountId} onChange={(e) => setAccountId(e.target.value)}>
            {accounts.map((a) => <option key={a.account_id} value={a.account_id}>{a.account_name}</option>)}
          </Select>
        )}
        <Select label="Format" value={format} onChange={(e) => setFormat(e.target.value as "pdf" | "csv")}>
          <option value="pdf">PDF</option>
          <option value="csv">CSV</option>
        </Select>
        <p className="text-xs text-[var(--muted)]">
          Exports the current payday cycle (payday day: {paydayDay}).
        </p>
        {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
        <div className="flex gap-2 pt-1">
          <Button type="submit" variant="primary" className="flex-1" disabled={loading}>
            {loading ? "Exporting…" : "Export"}
          </Button>
          <Button type="button" variant="secondary" onClick={onClose}>Cancel</Button>
        </div>
      </form>
    </Modal>
  );
}
