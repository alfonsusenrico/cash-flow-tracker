import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TOAST_MS, ToastProvider, useToast } from "./overlays";

function ToastTrigger({ onUndo }: { onUndo?: () => void }) {
  const { showToast } = useToast();
  return (
    <button type="button" onClick={() => showToast("Transaksi dihapus", onUndo ? { onUndo } : undefined)}>
      Hapus transaksi
    </button>
  );
}

function showToastNow(onUndo?: () => void) {
  render(
    <ToastProvider>
      <ToastTrigger onUndo={onUndo} />
    </ToastProvider>,
  );
  fireEvent.click(screen.getByRole("button", { name: "Hapus transaksi" }));
  return screen.getByText("Transaksi dihapus").closest(".ws-toast") as HTMLElement;
}

describe("Toast", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("speaks through a polite status region and leaves after 6.5 s", () => {
    showToastNow();
    expect(TOAST_MS).toBe(6500);
    const region = screen.getByRole("status");
    expect(region).toHaveAttribute("aria-live", "polite");
    expect(region).toHaveTextContent("Transaksi dihapus");

    act(() => vi.advanceTimersByTime(6499));
    expect(screen.getByText("Transaksi dihapus")).toBeInTheDocument();
    act(() => vi.advanceTimersByTime(1));
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
    expect(screen.getByRole("status")).toBeEmptyDOMElement();
  });

  it("pauses while hovered and resumes with the time that was left", () => {
    const toast = showToastNow();
    act(() => vi.advanceTimersByTime(4000));
    fireEvent.mouseEnter(toast);
    act(() => vi.advanceTimersByTime(20_000));
    expect(screen.getByText("Transaksi dihapus")).toBeInTheDocument();
    fireEvent.mouseLeave(toast);
    act(() => vi.advanceTimersByTime(2499));
    expect(screen.getByText("Transaksi dihapus")).toBeInTheDocument();
    act(() => vi.advanceTimersByTime(1));
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
  });

  it("pauses while focus is inside so Urungkan stays reachable", () => {
    showToastNow(() => {});
    const undo = screen.getByRole("button", { name: "Urungkan" });
    act(() => undo.focus());
    act(() => vi.advanceTimersByTime(20_000));
    expect(undo).toBeInTheDocument();
    act(() => undo.blur());
    act(() => vi.advanceTimersByTime(TOAST_MS));
    expect(screen.queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
  });

  it("calls the undo callback once and closes", () => {
    const onUndo = vi.fn();
    showToastNow(onUndo);
    fireEvent.click(screen.getByRole("button", { name: "Urungkan" }));
    expect(onUndo).toHaveBeenCalledOnce();
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
  });

  it("shows no Urungkan without a callback", () => {
    showToastNow();
    expect(screen.queryByRole("button", { name: "Urungkan" })).not.toBeInTheDocument();
  });

  it("replaces the previous message and restarts the timer", () => {
    showToastNow();
    act(() => vi.advanceTimersByTime(5000));
    fireEvent.click(screen.getByRole("button", { name: "Hapus transaksi" }));
    act(() => vi.advanceTimersByTime(5000));
    expect(screen.getAllByText("Transaksi dihapus")).toHaveLength(1);
    act(() => vi.advanceTimersByTime(1500));
    expect(screen.queryByText("Transaksi dihapus")).not.toBeInTheDocument();
  });
});
