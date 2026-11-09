"use client";

/** Static row placeholders of a section while it loads (SPEC §7, apply-markas-target D11): 56 px, no pulse. */
export function RowsSkeleton({ rows }: { rows: number }) {
  const widths = [46, 38, 52, 42, 36];
  return (
    <div aria-hidden="true">
      {Array.from({ length: rows }, (_, index) => (
        <div key={index} className="skel-row">
          <span className="skel" style={{ width: 40 }} />
          <span className="skel" style={{ width: `${widths[index % widths.length]}%` }} />
          <span className="grow" />
          <span className="skel" style={{ width: 96 }} />
        </div>
      ))}
    </div>
  );
}
