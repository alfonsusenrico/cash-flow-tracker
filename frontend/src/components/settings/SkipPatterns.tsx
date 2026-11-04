"use client";

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/Button";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import {
  SettingsAlert, SettingsCard, SettingsField, SettingsList, SettingsSubsection, settingsControlClass,
} from "@/components/settings/SettingsCard";
import {
  createSkipPattern, getSkipPatterns, stopSkipPattern, type SkipInstitution, type SkipPattern,
} from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";

const DAY = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "Asia/Jakarta" });

type Direction = "income" | "expense";

function directionLabel(direction: Direction): string {
  return direction === "income" ? "uang masuk" : "uang keluar";
}

function patternTitle(pattern: SkipPattern): string {
  return `${pattern.redundant_label}, ${directionLabel(pattern.direction)}`;
}

function patternDetail(pattern: SkipPattern): string {
  const parts = [`Dilewati bila ${pattern.kept_label} sudah melaporkan nominal yang sama`];
  if (pattern.created_at) {
    const origin = pattern.source === "confirmation" ? "dari Tinjau" : "ditambahkan di sini";
    parts.push(`${origin}, ${DAY.format(new Date(pattern.created_at))}`);
  }
  parts.push(pattern.skipped_count ? `${pattern.skipped_count} kali dilewati` : "belum pernah dilewati");
  return parts.join(" · ");
}

/**
 * "Notifikasi ganda": apps whose reports repeat another app's, skipped without a question
 * (skip-redundant-notifications D7). Stopping a pattern only affects later notifications.
 */
export function SkipPatterns() {
  const qc = useQueryClient();
  const { data, isError } = useQuery({ queryKey: queryKeys.skipPatterns, queryFn: getSkipPatterns });
  const [redundant, setRedundant] = useState<SkipInstitution | "">("");
  const [direction, setDirection] = useState<Direction>("income");
  const [kept, setKept] = useState<SkipInstitution | "">("");
  const [error, setError] = useState("");
  const [saved, setSaved] = useState("");

  const refresh = () => qc.invalidateQueries({ queryKey: queryKeys.skipPatterns });

  const add = useMutation({
    mutationFn: createSkipPattern,
    onSuccess: ({ pattern }) => {
      setError("");
      setSaved(`Pola ${patternTitle(pattern)} disimpan.`);
      setRedundant("");
      setKept("");
      void refresh();
    },
    onError: (e: Error) => {
      setSaved("");
      setError(e.message || "Pola gagal disimpan.");
    },
  });

  const stop = useMutation({
    mutationFn: stopSkipPattern,
    onSuccess: () => {
      setError("");
      setSaved("Pola dihentikan. Laporan serupa akan ditanyakan lagi di Tinjau.");
      document.getElementById("settings-notifikasi-ganda-title")?.focus();
      void refresh();
    },
    onError: (e: Error) => setError(e.message || "Pola gagal dihentikan."),
  });

  const institutions = data?.institutions ?? [];
  const keptChoices = institutions.filter((item) => item.institution !== redundant);
  const canSave = Boolean(redundant && kept && redundant !== kept) && !add.isPending;

  return (
    <SettingsCard section="notifikasi-ganda">
      {error && <SettingsAlert>{error}</SettingsAlert>}
      <p role="status" className="text-xs text-[var(--muted)]">{saved}</p>
      {isError ? (
        <SettingsAlert>Daftar pola gagal dimuat.</SettingsAlert>
      ) : !data ? (
        <p role="status" className="text-[var(--muted)]">Memuat…</p>
      ) : data.patterns.length === 0 ? (
        <p className="text-[var(--muted)]">
          Belum ada pola. Pola ditambahkan lewat &ldquo;Benar, satu transaksi&rdquo; di Tinjau, atau di sini.
        </p>
      ) : (
        <SettingsList label="Pola notifikasi ganda">
          {data.patterns.map((pattern) => (
            <li key={pattern.id} className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 px-3 py-2.5">
              <div className="min-w-0 flex-1 break-words">
                <p className="font-semibold text-[var(--text)]">{patternTitle(pattern)}</p>
                <p className="text-xs text-[var(--muted)] tabular-nums">{patternDetail(pattern)}</p>
              </div>
              <ConfirmRemove
                subject={`pola ${patternTitle(pattern)}`}
                triggerLabel={`Berhenti melewati ${patternTitle(pattern)}`}
                verb="Berhenti melewati"
                confirmLabel="Ya, berhenti"
                pendingLabel="Menghentikan…"
                disabled={stop.isPending}
                pending={stop.isPending && stop.variables === pattern.id}
                onConfirm={() => stop.mutate(pattern.id)}
              />
            </li>
          ))}
        </SettingsList>
      )}

      {data && (
        <SettingsSubsection id="skip-pattern-add" title="Tambah pola">
          {institutions.length < 2 ? (
            <p className="text-[var(--muted)]">Pola butuh dua aplikasi yang sudah pernah mengirim notifikasi.</p>
          ) : (
            <form
              className="grid gap-3 sm:grid-cols-3"
              onSubmit={(event) => {
                event.preventDefault();
                if (!redundant || !kept) return;
                add.mutate({ redundant_institution: redundant, direction, kept_institution: kept });
              }}
            >
              <SettingsField id="skip-pattern-redundant" label="Aplikasi yang dilewati">
                {(field) => (
                  <select
                    {...field}
                    value={redundant}
                    onChange={(event) => {
                      const value = event.target.value as SkipInstitution | "";
                      setRedundant(value);
                      if (value && value === kept) setKept("");
                    }}
                    className={settingsControlClass}
                  >
                    <option value="">Pilih aplikasi</option>
                    {institutions.map((item) => <option key={item.institution} value={item.institution}>{item.label}</option>)}
                  </select>
                )}
              </SettingsField>
              <SettingsField id="skip-pattern-direction" label="Jenis">
                {(field) => (
                  <select {...field} value={direction} onChange={(event) => setDirection(event.target.value as Direction)} className={settingsControlClass}>
                    <option value="income">Uang masuk</option>
                    <option value="expense">Uang keluar</option>
                  </select>
                )}
              </SettingsField>
              <SettingsField id="skip-pattern-kept" label="Aplikasi yang dipakai">
                {(field) => (
                  <select {...field} value={kept} onChange={(event) => setKept(event.target.value as SkipInstitution | "")} className={settingsControlClass}>
                    <option value="">Pilih aplikasi</option>
                    {keptChoices.map((item) => <option key={item.institution} value={item.institution}>{item.label}</option>)}
                  </select>
                )}
              </SettingsField>
              <div className="sm:col-span-3">
                <Button type="submit" size="sm" className="min-h-11" disabled={!canSave}>
                  {add.isPending ? "Menyimpan…" : "Simpan pola"}
                </Button>
              </div>
            </form>
          )}
        </SettingsSubsection>
      )}
    </SettingsCard>
  );
}
