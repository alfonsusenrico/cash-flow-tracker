import type { ReactNode } from "react";

/**
 * One numbered Kakeibo question (apply-markas-laporan D4): the number in a 32 px column, the level-2 question and
 * its body 16 px below. Questions sit 48 px apart with a 1 px rule (32 px on the phone).
 */
export function Question({ number, id, title, children }: { number: number; id: string; title: string; children: ReactNode }) {
  return (
    <section className="q" aria-labelledby={id}>
      <span className="q-num" aria-hidden="true">
        {number}
      </span>
      <div className="q-main">
        <h2 className="q-h" id={id}>
          {title}
        </h2>
        <div className="q-body">{children}</div>
      </div>
    </section>
  );
}
