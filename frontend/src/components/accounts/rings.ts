/** Radians of empty space on each side of a ring segment, so neighbouring segments stay apart. */
export const SEGMENT_GAP = 0.012;

function point(centre: number, radius: number, angle: number): [number, number] {
  return [centre + radius * Math.cos(angle - Math.PI / 2), centre + radius * Math.sin(angle - Math.PI / 2)];
}

/** The path of a ring segment between two radii, from angle `start` to `end` (radians, clockwise from the top). */
export function arcPath(centre: number, inner: number, outer: number, start: number, end: number): string {
  // A whole ring would start and end at one point and draw nothing; it stops just short instead.
  const stop = Math.min(end, start + Math.PI * 2 - 0.0001);
  const large = stop - start > Math.PI ? 1 : 0;
  const [x0, y0] = point(centre, outer, start);
  const [x1, y1] = point(centre, outer, stop);
  const [x2, y2] = point(centre, inner, stop);
  const [x3, y3] = point(centre, inner, start);
  return `M${x0},${y0} A${outer},${outer} 0 ${large} 1 ${x1},${y1} L${x2},${y2} A${inner},${inner} 0 ${large} 0 ${x3},${y3} Z`;
}

/** Start and end angles of consecutive segments whose shares (0 to 1) sum to at most one ring, from `offset`. */
export function segmentAngles(shares: number[], offset = 0): { start: number; end: number }[] {
  const angles: { start: number; end: number }[] = [];
  let start = offset;
  for (const share of shares) {
    const end = start + share * Math.PI * 2;
    angles.push({ start, end });
    start = end;
  }
  return angles;
}
