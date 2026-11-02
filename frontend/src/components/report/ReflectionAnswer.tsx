"use client";

import { useEffect, useId, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { MonthlyReport, Reflection } from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";
import { Button } from "@/components/ui/Button";
import { QuestionSection } from "./ReportParts";

const MAX_LENGTH = 1000;

export function ReflectionAnswer({ report }: { report: MonthlyReport }) {
  const queryClient = useQueryClient();
  const fieldId = useId();
  const helpId = useId();
  const saved = report.reflection?.note ?? "";
  const [note, setNote] = useState(saved);
  const [status, setStatus] = useState("");
  useEffect(() => {
    setNote(saved);
    setStatus("");
  }, [saved, report.period.start_date]);

  const refresh = () => queryClient.invalidateQueries({ queryKey: queryKeys.reports.all });
  const save = useMutation({
    mutationFn: () =>
      api.put<{ reflection: Reflection }>("/reports/monthly/reflection", {
        basis: report.basis, start_date: report.period.start_date, note,
      }),
    onSuccess: () => {
      setStatus("Catatan tersimpan.");
      refresh();
    },
    onError: (error: Error) => setStatus(error.message || "Catatan belum tersimpan. Coba lagi."),
  });
  const remove = useMutation({
    mutationFn: () =>
      api.del(`/reports/monthly/reflection?basis=${report.basis}&start_date=${report.period.start_date}`),
    onSuccess: () => {
      setNote("");
      setStatus("Catatan dihapus.");
      refresh();
    },
    onError: (error: Error) => setStatus(error.message || "Catatan belum terhapus. Coba lagi."),
  });
  const changed = note.trim() !== saved.trim();
  const previous = report.previous_reflection;

  return (
    <QuestionSection id="report-reflection" number={4} question="Apa yang mau diperbaiki bulan depan?">
      {previous && (
        <figure className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/40 p-3">
          <figcaption className="text-xs font-semibold text-[var(--muted)]">Catatan Anda untuk {previous.label}</figcaption>
          <blockquote className="mt-1 whitespace-pre-line text-sm text-[var(--text)]">{previous.note}</blockquote>
        </figure>
      )}
      <form
        className="space-y-2"
        onSubmit={(event) => {
          event.preventDefault();
          if (note.trim()) save.mutate();
        }}
      >
        <label htmlFor={fieldId} className="block text-sm font-medium text-[var(--text)]">
          Catatan untuk {report.period.label}
        </label>
        <textarea
          id={fieldId}
          value={note}
          maxLength={MAX_LENGTH}
          rows={3}
          aria-describedby={helpId}
          onChange={(event) => setNote(event.target.value)}
          placeholder="Misalnya: masak di rumah tiga kali seminggu"
          className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface)] px-3 py-2 text-sm text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
        />
        <p id={helpId} className="text-xs text-[var(--muted)]">
          Satu atau dua hal saja. Catatan ini muncul lagi di laporan berikutnya. <span className="tabular-nums">{note.length}/{MAX_LENGTH}</span>
        </p>
        <div className="flex flex-wrap items-center gap-2">
          <Button type="submit" variant="primary" className="min-h-11" disabled={!note.trim() || !changed || save.isPending}>
            {save.isPending ? "Menyimpan…" : "Simpan catatan"}
          </Button>
          {report.reflection && (
            <Button type="button" className="min-h-11" onClick={() => remove.mutate()} disabled={remove.isPending}>
              Hapus catatan
            </Button>
          )}
          <p role="status" className="text-xs text-[var(--text-secondary)]">{status}</p>
        </div>
      </form>
    </QuestionSection>
  );
}
