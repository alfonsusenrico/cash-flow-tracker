"use client";

import { useEffect, useRef } from "react";
import QRCode from "qrcode";

/**
 * A QR code drawn at the device pixel ratio inside the white `.qr` box (apply-markas-pengaturan D6, D9). Scanners
 * need dark modules on a light quiet zone, so the code stays black on white in both themes; the box's 8 px white
 * padding adds to the code's own two-module margin.
 */
export function QrCode({
  value,
  size,
  label,
  onError,
}: {
  value: string;
  /** The code's side in css px, without the box's padding. */
  size: number;
  label: string;
  onError: () => void;
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    let cancelled = false;
    const ratio = Math.max(1, Math.round(window.devicePixelRatio || 1));
    QRCode.toCanvas(canvas, value, {
      width: size * ratio,
      margin: 2,
      errorCorrectionLevel: "M",
      color: { dark: "#000000", light: "#ffffff" },
    }).then(() => {
      // The library sets the canvas's css size to its pixel buffer, which draws the code at twice its box on a 2x screen.
      canvas.style.width = `${size}px`;
      canvas.style.height = `${size}px`;
    }).catch(() => {
      if (!cancelled) onError();
    });
    return () => {
      cancelled = true;
    };
    // The error handler may be recreated by its parent; only a new value or size redraws the code.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value, size]);
  return <canvas ref={canvasRef} width={size} height={size} role="img" aria-label={label} style={{ width: size, height: size }} />;
}
