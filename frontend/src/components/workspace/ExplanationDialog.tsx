"use client";

import Link from "next/link";
import { useId, useState } from "react";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import type { ExplanationLine } from "./explanation";
import { MaskedMoney } from "./money";
import { ShellDialog } from "./ShellDialog";

/** One calculation line of a Rincian; `parts` are the server lines it is made of, opened in place. */
export interface RincianLine {
  key: string;
  label: string;
  /** Already formatted, with its sign. */
  amount: string;
  /** The amount is not money, such as "Belum diketahui": it stays visible while balances are hidden. */
  plain?: boolean;
  parts?: ExplanationLine[];
  /** Short notes under the parts, such as the window of the scheduled amount. */
  partNotes?: string[];
}

export interface RincianData {
  title: string;
  rule: string;
  lines: RincianLine[];
  /** When the period is over: what it is over by, shown before the total. */
  over?: { label: string; amount: string } | null;
  /** With `plain` the total is a count such as "5,4x biaya pokok · 162 hari", visible while balances are hidden. */
  total: { label: string; amount: string; plain?: boolean };
  notes: string[];
  /** The rows still waiting for an answer, listed under the notes. */
  waiting?: { label: string; amount: string; lines: ExplanationLine[]; link: { href: string; label: string } } | null;
  /** The footer link to the rows behind the figure; left out when the figure has none (Rekening without obligations). */
  link?: { href: string; label: string };
}

function Parts({ id, lines, notes = [], link }: { id: string; lines: ExplanationLine[]; notes?: string[]; link?: { href: string; label: string } }) {
  return (
    <div id={id} className="parts">
      <ul>
        {lines.map((line, index) => (
          <li key={`${line.label}-${index}`} className="part">
            <span className="part-label">
              {line.href ? <Link href={line.href}>{line.label}</Link> : line.label}
              {line.note && <span className="part-note">{line.note}</span>}
            </span>
            <MaskedMoney text={line.amount} className="part-amt" />
          </li>
        ))}
      </ul>
      {notes.map((note) => (
        <p key={note} className="t-meta">
          {note}
        </p>
      ))}
      {link && (
        <Link href={link.href} className="ws-linkbtn part-link">
          {link.label}
        </Link>
      )}
    </div>
  );
}

/** A calculation line; with parts it is a disclosure that shows them in place (owner decision 3). */
function LineRow({ line }: { line: RincianLine }) {
  const [open, setOpen] = useState(false);
  const partsId = useId();
  const amount = line.plain ? <span className="t-amt">{line.amount}</span> : <MaskedMoney text={line.amount} className="t-amt" />;
  if (!line.parts?.length) {
    return (
      <li>
        <div className="row is-plain">
          <span className="r-main">
            <span className="r-title">{line.label}</span>
          </span>
          {amount}
        </div>
      </li>
    );
  }
  return (
    <li>
      <button type="button" className="row is-line" aria-expanded={open} aria-controls={open ? partsId : undefined} onClick={() => setOpen((value) => !value)}>
        <span className="r-main">
          <span className="r-title">{line.label}</span>
        </span>
        {amount}
        <Icon name="chevD" className="chev h-4 w-4" />
      </button>
      {open && <Parts id={partsId} lines={line.parts} notes={line.partNotes} />}
    </li>
  );
}

function WaitingDisclosure({ waiting }: { waiting: NonNullable<RincianData["waiting"]> }) {
  const [open, setOpen] = useState(false);
  const partsId = useId();
  return (
    <ul className="list">
      <li>
        <button type="button" className="row is-line" aria-expanded={open} aria-controls={open ? partsId : undefined} onClick={() => setOpen((value) => !value)}>
          <span className="r-main">
            <span className="r-title">
              {waiting.label} · <MaskedMoney text={waiting.amount} />
            </span>
          </span>
          <Icon name="chevD" className="chev h-4 w-4" />
        </button>
        {open && <Parts id={partsId} lines={waiting.lines} link={waiting.link} />}
      </li>
    </ul>
  );
}

/**
 * The Rincian of a hero figure (explainable-figures, apply-markas-beranda D5): a 480 px dialog with the rule, the
 * calculation lines, the total, its notes and a link to the rows behind it; a bottom sheet below 768 px.
 */
export function ExplanationDialog({ open, onClose, data }: { open: boolean; onClose: () => void; data: RincianData | null }) {
  const t = useTranslations("breakdown");
  return (
    <ShellDialog
      open={open && data !== null}
      onClose={onClose}
      title={data?.title ?? ""}
      meta={t("calculation")}
      width={480}
      closeLabel={t("close")}
      className="rincian"
      footer={
        data?.link && (
          <Link href={data.link.href} className="ws-btn">
            {data.link.label}
          </Link>
        )
      }
    >
      {data && (
        <>
          <p className="t-body">{data.rule}</p>
          <ul className="list">
            {data.lines.map((line) => (
              <LineRow key={line.key} line={line} />
            ))}
            {data.over && (
              <li>
                <div className="row is-plain is-over">
                  <span className="r-main">
                    <span className="r-title t-strong">{data.over.label}</span>
                  </span>
                  <MaskedMoney text={data.over.amount} className="t-amt" />
                </div>
              </li>
            )}
            <li>
              <div className={data.over ? "row is-plain is-total after-over" : "row is-plain is-total"}>
                <span className="r-main">
                  <span className="r-title t-strong">{data.total.label}</span>
                </span>
                {data.total.plain ? <span className="t-fig">{data.total.amount}</span> : <MaskedMoney text={data.total.amount} className="t-fig" />}
              </div>
            </li>
          </ul>
          {data.notes.map((note) => (
            <p key={note} className="t-meta">
              {note}
            </p>
          ))}
          {data.waiting && <WaitingDisclosure waiting={data.waiting} />}
        </>
      )}
    </ShellDialog>
  );
}
