import { cn } from "@/lib/utils";
import { formatPercent, type Gain } from "./wallet";
import styles from "./wallet.module.css";

/** "▲ Rp X (Y,Z%)" for a known gain, "Modal belum diisi" when a cost basis is missing; never a partial sum (D6). */
export function GainChip({ gain, money }: { gain: Gain | null; money: (value: number) => string }) {
  if (!gain) return null;
  if (!gain.known) return <span className={styles.gainUnknown}>Modal belum diisi</span>;
  const up = gain.gain >= 0;
  const percent = gain.cost > 0 ? (Math.abs(gain.gain) / gain.cost) * 100 : null;
  return (
    <span className={cn(styles.gain, up ? styles.gainUp : styles.gainDown)}>
      <span aria-hidden="true">{up ? "▲" : "▼"}</span>
      <span>
        {money(Math.abs(gain.gain))}
        {percent != null ? ` (${formatPercent(percent)}%)` : ""}
      </span>
      <span className="sr-only">{up ? " untung" : " rugi"}</span>
    </span>
  );
}
