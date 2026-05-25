"use client";
import { useState, useEffect } from "react";
import { cn } from "@/lib/utils";

interface MoneyInputProps {
  label?: string;
  value: number;
  onChange: (value: number) => void;
  className?: string;
  required?: boolean;
}

export function MoneyInput({ label, value, onChange, className, required }: MoneyInputProps) {
  const [display, setDisplay] = useState(value > 0 ? value.toLocaleString("id-ID") : "");

  useEffect(() => {
    setDisplay(value > 0 ? value.toLocaleString("id-ID") : "");
  }, [value]);

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const raw = e.target.value.replace(/\./g, "").replace(/[^0-9]/g, "");
    const num = parseInt(raw, 10) || 0;
    setDisplay(num > 0 ? num.toLocaleString("id-ID") : "");
    onChange(num);
  }

  return (
    <div className="flex flex-col gap-1">
      {label && <label className="text-xs font-medium text-[var(--muted)]">{label}</label>}
      <div className="relative">
        <span className="absolute left-3 top-1/2 -translate-y-1/2 text-sm text-[var(--muted)]">Rp</span>
        <input
          type="text"
          inputMode="numeric"
          value={display}
          onChange={handleChange}
          required={required}
          placeholder="0"
          className={cn(
            "w-full border border-[var(--border)] rounded pl-9 pr-3 py-2 text-sm bg-[var(--surface)] text-[var(--text)] focus:outline-none focus:ring-2 focus:ring-[var(--primary)]/40",
            className
          )}
        />
      </div>
    </div>
  );
}
