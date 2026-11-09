/**
 * "Rincian hitungan": the lines that make up an explainable figure, shown by `ExplanationDialog`. The content comes from
 * the server's lines (explainable-figures spec).
 */
export interface ExplanationLine {
  label: string;
  note?: string | null;
  /** Already formatted, with its sign. */
  amount: string;
  /** Rows behind this line, such as the ledger filtered by period and pillar. */
  href?: string | null;
}

export interface ExplanationData {
  title: string;
  rule: string;
  lines: ExplanationLine[];
  total?: { label: string; amount: string } | null;
  notes?: string[];
  link?: { href: string; label: string } | null;
  /** A figure that cannot be computed: why, and what would make it available. No total is shown. */
  unavailable?: { reason: string; fix: string } | null;
}
