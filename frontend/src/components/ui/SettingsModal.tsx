"use client";
import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Modal } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";

interface Props {
  open: boolean;
  onClose: () => void;
  paydayDay: number;
  theme: "light" | "dark";
  onThemeToggle: () => void;
  hideBalances: boolean;
  onHideBalancesToggle: () => void;
}

export function SettingsModal({ open, onClose, paydayDay, theme, onThemeToggle, hideBalances, onHideBalancesToggle }: Props) {
  const qc = useQueryClient();
  const [day, setDay] = useState(String(paydayDay));
  const [apiKey, setApiKey] = useState("");
  const [err, setErr] = useState("");
  const [msg, setMsg] = useState("");

  const paydayMut = useMutation({
    mutationFn: () => api.put("/payday", { day: parseInt(day, 10) }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["summary"] });
      setMsg("Payday saved.");
    },
    onError: (e: Error) => setErr(e.message),
  });

  const apiKeyMut = useMutation({
    mutationFn: () => api.post("/api-key/reset"),
    onSuccess: (res: any) => setApiKey(res.api_key),
    onError: (e: Error) => setErr(e.message),
  });

  return (
    <Modal open={open} onClose={onClose} title="Settings">
      <div className="space-y-6">
        {/* Payday */}
        <section className="space-y-2">
          <h3 className="text-sm font-semibold">Payday</h3>
          <p className="text-xs text-[var(--muted)]">Day of month your pay cycle starts (1–31).</p>
          <div className="flex gap-2">
            <Input
              type="number"
              min={1}
              max={31}
              value={day}
              onChange={(e) => setDay(e.target.value)}
              className="w-24"
            />
            <Button variant="primary" size="sm" onClick={() => paydayMut.mutate()} disabled={paydayMut.isPending}>
              Save
            </Button>
          </div>
          {msg && <p className="text-xs text-green-600">{msg}</p>}
        </section>

        {/* Theme */}
        <section className="space-y-2">
          <h3 className="text-sm font-semibold">Theme</h3>
          <div className="flex items-center gap-3">
            <span className="text-sm text-[var(--muted)]">{theme === "dark" ? "Dark" : "Light"}</span>
            <button
              onClick={onThemeToggle}
              className={`relative w-11 h-6 rounded-full transition-colors ${theme === "dark" ? "bg-[var(--primary)]" : "bg-[var(--border)]"}`}
            >
              <span className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform ${theme === "dark" ? "translate-x-5" : ""}`} />
            </button>
          </div>
        </section>

        {/* Hide balances */}
        <section className="space-y-2">
          <h3 className="text-sm font-semibold">Privacy</h3>
          <label className="flex items-center gap-2 text-sm cursor-pointer">
            <input type="checkbox" checked={hideBalances} onChange={onHideBalancesToggle} className="rounded" />
            Hide balances
          </label>
        </section>

        {/* API Key */}
        <section className="space-y-2">
          <h3 className="text-sm font-semibold">API Key</h3>
          <p className="text-xs text-[var(--muted)]">Reset generates a new key. The old key stops working immediately.</p>
          {apiKey ? (
            <div className="space-y-1">
              <p className="text-xs text-green-600">New key (copy now — shown once):</p>
              <code className="block text-xs bg-[var(--bg)] border border-[var(--border)] rounded px-2 py-1.5 break-all">{apiKey}</code>
              <Button size="sm" variant="secondary" onClick={() => { navigator.clipboard.writeText(apiKey); }}>Copy</Button>
            </div>
          ) : (
            <Button size="sm" variant="danger" onClick={() => apiKeyMut.mutate()} disabled={apiKeyMut.isPending}>
              Reset API Key
            </Button>
          )}
        </section>

        {err && <p className="text-sm text-[var(--danger)]">{err}</p>}
      </div>
    </Modal>
  );
}
