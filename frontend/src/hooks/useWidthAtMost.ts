import { useEffect, useState, type RefObject } from "react";

/**
 * True while the element's content box is at most `maxWidth` CSS pixels wide.
 * Stays false until the first measurement and where ResizeObserver is unavailable,
 * so the wide composition is the default.
 */
export function useWidthAtMost(ref: RefObject<HTMLElement | null>, maxWidth: number): boolean {
  const [atMost, setAtMost] = useState(false);

  useEffect(() => {
    const element = ref.current;
    if (!element || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[entries.length - 1];
      if (entry) setAtMost(entry.contentRect.width <= maxWidth);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref, maxWidth]);

  return atMost;
}
