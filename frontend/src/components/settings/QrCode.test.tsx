import { render, screen, waitFor } from "@testing-library/react";
import QRCode from "qrcode";
import { afterEach, describe, expect, it, vi } from "vitest";
import { QrCode } from "./QrCode";

vi.mock("qrcode", () => ({ default: { toCanvas: vi.fn() } }));

/** What the real renderer does: it sizes the buffer and the css to the width it was given. */
function drawLikeTheLibrary(canvas: HTMLCanvasElement, _value: string, options: { width: number }) {
  canvas.width = options.width;
  canvas.height = options.width;
  canvas.style.width = `${options.width}px`;
  canvas.style.height = `${options.width}px`;
  return Promise.resolve();
}

describe("QrCode", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it("keeps its css size on a 2x screen while drawing a sharper buffer", async () => {
    vi.stubGlobal("devicePixelRatio", 2);
    vi.mocked(QRCode.toCanvas).mockImplementation(drawLikeTheLibrary as never);
    render(<QrCode value="https://example.test" size={136} label="Code" onError={() => undefined} />);
    const canvas = screen.getByRole("img", { name: "Code" }) as HTMLCanvasElement;
    await waitFor(() => expect(canvas.width).toBe(272));
    await waitFor(() => expect(canvas.style.width).toBe("136px"));
    expect(canvas.style.height).toBe("136px");
  });
});
