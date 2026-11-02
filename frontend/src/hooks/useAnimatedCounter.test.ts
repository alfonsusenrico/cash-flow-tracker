import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useAnimatedCounter } from "./useAnimatedCounter";

function reducedMotion(reduce: boolean) {
  Object.defineProperty(window, "matchMedia", { configurable: true, value: vi.fn().mockReturnValue({ matches: reduce }) });
}

describe("useAnimatedCounter (Beranda KPI figures)", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("counts from the previous to the new value over about 380 ms and ends on the exact value", () => {
    reducedMotion(false);
    const frames: FrameRequestCallback[] = [];
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => frames.push(callback));
    vi.stubGlobal("cancelAnimationFrame", vi.fn());
    const { result, rerender } = renderHook(({ value }) => useAnimatedCounter(value), { initialProps: { value: 100_000 } });
    rerender({ value: 150_000 });
    const seen: number[] = [];
    for (const time of [16, 116, 216, 316, 396]) {
      const frame = frames.shift();
      if (!frame) break;
      act(() => frame(time));
      seen.push(result.current);
    }
    expect(seen.some((value) => value > 100_000 && value < 150_000)).toBe(true);
    expect(result.current).toBe(150_000);
    expect(frames).toHaveLength(0);
  });

  it("shows the new value at once when reduced motion is requested", () => {
    reducedMotion(true);
    const raf = vi.fn();
    vi.stubGlobal("requestAnimationFrame", raf);
    const { result, rerender } = renderHook(({ value }) => useAnimatedCounter(value), { initialProps: { value: 100_000 } });
    rerender({ value: 150_000 });
    expect(result.current).toBe(150_000);
    expect(raf).not.toHaveBeenCalled();
  });
});
