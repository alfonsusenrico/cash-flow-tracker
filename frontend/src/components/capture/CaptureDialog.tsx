"use client";

import { useCallback, useId, useRef, useState } from "react";
import { useTranslations } from "next-intl";
import { ShellDialog } from "@/components/workspace/ShellDialog";
import { PHONE_QUERY, useReturnFocus } from "@/components/workspace/layers";
import { ManualView } from "./ManualView";
import { SentenceView } from "./SentenceView";
import type { CaptureMode, CaptureRequest, FooterState } from "./types";

type View = "sentence" | "manual";

const isPhone = () => typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(PHONE_QUERY).matches;

/**
 * Catat (apply-markas-catat D1, D2): one 560 px dialog, a bottom sheet below 768 px, with two views swapped in
 * place. "Tulis transaksi" records a sentence; "Isi manual" turns the same dialog into "Catat transaksi" with the
 * amount read so far, and "Tulis kalimat" turns it back. Each view keeps its state while the dialog is open. The
 * dialog opens fresh every time its caller mounts it.
 */
export default function CaptureDialog({ request, onClose }: { request: CaptureRequest; onClose: () => void }) {
  const t = useTranslations("capture");
  const common = useTranslations("common");
  const sentenceFormId = useId();
  const manualFormId = useId();
  const [view, setView] = useState<View>(request.view);
  const [manualVisited, setManualVisited] = useState(request.view === "manual");
  const [manualSeed, setManualSeed] = useState<{ mode: CaptureMode; source?: string; target?: string; amount: number | null }>(() =>
    request.view === "manual"
      ? { mode: request.mode, source: request.sourceAccountId, target: request.targetAccountId, amount: request.amount ?? null }
      : { mode: "expense", amount: null },
  );
  const [sentenceVisited, setSentenceVisited] = useState(request.view === "sentence");
  const [sentenceFooter, setSentenceFooter] = useState<FooterState>({ label: t("sentence.record"), disabled: true });
  const [manualFooter, setManualFooter] = useState<FooterState>({ label: t("manual.save"), disabled: true });
  const [meta, setMeta] = useState<React.ReactNode>(null);
  const [announcement, setAnnouncement] = useState("");
  const readAmount = useRef<number | null>(null);
  const [phone] = useState(isPhone);
  useReturnFocus(true);

  const onReading = useCallback((amount: number | null) => {
    readAmount.current = amount;
  }, []);

  const focusView = (next: View) => {
    window.setTimeout(() => {
      const selector = next === "sentence" ? "[data-capture-field]" : "#capture-amount";
      document.querySelector<HTMLElement>(`dialog[open] ${selector}`)?.focus();
    }, 0);
  };

  const toManual = () => {
    // The amount read from the sentence fills the manual amount only on the first visit, while it is still empty.
    if (!manualVisited) setManualSeed({ mode: "expense", amount: readAmount.current });
    setManualVisited(true);
    setView("manual");
    setAnnouncement(t("manual.title"));
    focusView("manual");
  };

  const toSentence = () => {
    setSentenceVisited(true);
    setView("sentence");
    setMeta(null);
    setAnnouncement(t("sentence.title"));
    focusView("sentence");
  };

  const footerState = view === "sentence" ? sentenceFooter : manualFooter;
  const footer = (
    <>
      {/* While a follow-up question waits, the footer offers only Batal (D3). */}
      {!footerState.hidden && (
        <button type="button" className="ws-btn ws-btn-quiet" onClick={view === "sentence" ? toManual : toSentence}>
          {view === "sentence" ? t("sentence.manual") : t("manual.sentence")}
        </button>
      )}
      <span className="grow" />
      <button type="button" className="ws-btn" onClick={onClose}>{common("cancel")}</button>
      {!footerState.hidden && (
        <button type="submit" form={view === "sentence" ? sentenceFormId : manualFormId} className="ws-btn ws-btn-ink" disabled={footerState.disabled}>
          {footerState.label}
        </button>
      )}
    </>
  );

  return (
    <ShellDialog
      open
      onClose={onClose}
      title={view === "sentence" ? t("sentence.title") : t("manual.title")}
      meta={view === "sentence" ? t("sentence.meta") : meta ?? undefined}
      width={560}
      closeLabel={t("close")}
      className="capture-dialog"
      footer={footer}
      focusTitle={phone}
      initialFocus={request.view === "sentence" ? "[data-capture-field]" : "#capture-amount"}
    >
      {sentenceVisited && (
        <SentenceView active={view === "sentence"} formId={sentenceFormId} onFooter={setSentenceFooter} onReading={onReading} onRecorded={onClose} />
      )}
      {manualVisited && (
        <ManualView
          active={view === "manual"}
          formId={manualFormId}
          initialMode={manualSeed.mode}
          initialSourceAccountId={manualSeed.source}
          initialTargetAccountId={manualSeed.target}
          initialAmount={manualSeed.amount}
          onFooter={setManualFooter}
          onMeta={setMeta}
          onSaved={onClose}
        />
      )}
      <p className="sr-only" aria-live="polite">{announcement}</p>
    </ShellDialog>
  );
}
