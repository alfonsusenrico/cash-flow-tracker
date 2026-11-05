import { Icon } from "@/components/ui/Icon";
import { initials } from "./accountFacts";
import { badgeInk } from "./displayColours";
import type { AccountItem } from "./types";
import styles from "./wallet.module.css";

/** The rekening's mark: its initials on its display colour. Decorative; the name is always written beside it. */
export function RekeningLogo({ rekening, colour }: { rekening: AccountItem; colour: string }) {
  return (
    <span className={styles.logo} style={{ background: colour, color: badgeInk(colour) }} aria-hidden="true">
      {initials(rekening.name)}
    </span>
  );
}

/** A colour key beside a written label. */
export function Swatch({ colour }: { colour: string }) {
  return <i className={styles.swatch} style={{ background: colour }} aria-hidden="true" />;
}

export function MoveButtons({
  name,
  first,
  last,
  disabled,
  onMove,
  upLabel = "Naikkan",
  downLabel = "Turunkan",
}: {
  name: string;
  first: boolean;
  last: boolean;
  disabled?: boolean;
  onMove: (step: number) => void;
  upLabel?: string;
  downLabel?: string;
}) {
  return (
    <span className={styles.moveButtons}>
      <button type="button" className="ws-icon-btn" aria-label={`${upLabel} ${name}`} disabled={first || disabled} onClick={() => onMove(-1)}>
        <Icon name="chevron-up" className="h-4 w-4" />
      </button>
      <button type="button" className="ws-icon-btn" aria-label={`${downLabel} ${name}`} disabled={last || disabled} onClick={() => onMove(1)}>
        <Icon name="chevron-down" className="h-4 w-4" />
      </button>
    </span>
  );
}
