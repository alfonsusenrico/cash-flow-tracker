import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function fmtIDR(n: number): string {
  return `Rp ${n.toLocaleString("id-ID")}`;
}

export function fmtUSD(n: number): string {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD" });
}

export function fmtMoney(n: number, currency: "IDR" | "USD" = "IDR"): string {
  return currency === "USD" ? fmtUSD(n) : fmtIDR(n);
}

export function currentMonthYM(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
}
