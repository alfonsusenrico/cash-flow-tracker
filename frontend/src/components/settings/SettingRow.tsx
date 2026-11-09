"use client";

import type { ReactNode } from "react";

/**
 * A `.toggle` row (apply-markas-pengaturan D10, D11): the title and its meta line, then a switch, a status word or a
 * button at the end. `titleId` and `metaId` let the end control name and describe itself by the row.
 */
export function SettingRow({
  titleId,
  title,
  meta,
  metaId,
  end,
}: {
  titleId: string;
  title: ReactNode;
  meta?: ReactNode;
  metaId?: string;
  end: ReactNode;
}) {
  return (
    <div className="toggle">
      <div className="tg-main">
        <p className="t-body" id={titleId}>
          {title}
        </p>
        {meta && (
          <p className="t-meta" id={metaId}>
            {meta}
          </p>
        )}
      </div>
      {end}
    </div>
  );
}
