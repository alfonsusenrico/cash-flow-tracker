"use client";

import { useEffect, useState, useRef } from "react";

/**
 * Smoothly interpolates from a previous numeric value to a targetValue
 * using requestAnimationFrame with a snappy ease-out cubic curve.
 *
 * @param targetValue The target number to count towards.
 * @param durationMs Animation duration in ms (default: 380ms).
 * @returns The currently animated numeric integer value.
 */
export function useAnimatedCounter(
  targetValue: number,
  durationMs: number = 380
): number {
  const [displayValue, setDisplayValue] = useState<number>(() => targetValue || 0);
  const prevValueRef = useRef<number>(targetValue || 0);
  const animFrameRef = useRef<number | null>(null);

  useEffect(() => {
    const startValue = prevValueRef.current;
    const diff = targetValue - startValue;

    if (diff === 0) {
      setDisplayValue(targetValue);
      return;
    }

    // Respect reduced motion accessibility
    const prefersReducedMotion =
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    if (prefersReducedMotion || durationMs <= 0) {
      setDisplayValue(targetValue);
      prevValueRef.current = targetValue;
      return;
    }

    let startTime: number | null = null;

    const step = (timestamp: number) => {
      if (!startTime) startTime = timestamp;
      const elapsed = timestamp - startTime;
      const progress = Math.min(elapsed / durationMs, 1);

      // Ease-out cubic curve: 1 - (1 - progress)^3
      const ease = 1 - Math.pow(1 - progress, 3);
      const current = Math.round(startValue + diff * ease);

      setDisplayValue(current);

      if (progress < 1) {
        animFrameRef.current = requestAnimationFrame(step);
      } else {
        setDisplayValue(targetValue);
        prevValueRef.current = targetValue;
      }
    };

    animFrameRef.current = requestAnimationFrame(step);

    return () => {
      if (animFrameRef.current !== null) {
        cancelAnimationFrame(animFrameRef.current);
      }
    };
  }, [targetValue, durationMs]);

  return displayValue;
}
