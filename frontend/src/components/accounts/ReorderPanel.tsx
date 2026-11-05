"use client";

import { Card, CardHeader } from "@/components/workspace/primitives";
import { MoveButtons } from "./walletParts";
import type { Section } from "./wallet";
import styles from "./wallet.module.css";

/** The phone's "Ubah urutan" mode: move buttons for rows and for cards within a row (D7; no touch drag). */
export function ReorderPanel({
  sections,
  onMoveSection,
  onMoveCard,
}: {
  sections: Section[];
  onMoveSection: (key: string, destination: number) => void;
  onMoveCard: (section: string, id: string, destination: number) => void;
}) {
  return (
    <Card labelledBy="accounts-reorder">
      <CardHeader id="accounts-reorder" icon="arrow-up-down" title="Ubah urutan" caption="Pakai tombol naik dan turun. Urutan langsung tersimpan." />
      <ul className={styles.reorder} aria-label="Ubah urutan baris">
        {sections.map((section, index) => (
          <li key={section.key} className={styles.reorderItem}>
            <div className={styles.reorderRow}>
              <span className={styles.reorderName}><b>{section.title}</b></span>
              <MoveButtons
                name={`baris ${section.title}`}
                first={index === 0}
                last={index === sections.length - 1}
                onMove={(step) => onMoveSection(section.key, index + step)}
              />
            </div>
            {section.holdings.length > 1 && (
              <ul className={styles.reorderCards} aria-label={`Ubah urutan kartu ${section.title}`}>
                {section.holdings.map((holding, cardIndex) => (
                  <li key={holding.id} className={styles.reorderRow}>
                    <span className={styles.reorderName}>{holding.account.name}</span>
                    <MoveButtons
                      name={holding.account.name}
                      first={cardIndex === 0}
                      last={cardIndex === section.holdings.length - 1}
                      upLabel="Geser ke depan"
                      downLabel="Geser ke belakang"
                      onMove={(step) => onMoveCard(section.key, holding.id, cardIndex + step)}
                    />
                  </li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ul>
    </Card>
  );
}
