"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";

const REGISTRATION_PASSWORD_MIN_LENGTH = 8;

export default function LoginPage() {
  const [mode, setMode] = useState<"login" | "register">("login");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  function getSafeRedirectTarget() {
    const params = new URLSearchParams(window.location.search);
    const next = params.get("next");
    if (next && next.startsWith("/") && !next.startsWith("//") && next !== "/dashboard") {
      return next;
    }
    return "/";
  }

  useEffect(() => {
    async function redirectIfAuthenticated() {
      try {
        await api.get("/auth/me");
        window.location.replace(getSafeRedirectTarget());
      } catch {
        // Staying on the login page is correct when there is no valid session.
      }
    }

    redirectIfAuthenticated();
    window.addEventListener("pageshow", redirectIfAuthenticated);
    return () => window.removeEventListener("pageshow", redirectIfAuthenticated);
  }, []);

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError("");
    setLoading(true);
    const fd = new FormData(e.currentTarget);

    try {
      if (mode === "login") {
        await api.post("/auth/login", {
          username: fd.get("username"),
          password: fd.get("password"),
        });
        window.location.replace(getSafeRedirectTarget());
      } else {
        const password = String(fd.get("password") ?? "");
        if (password.length < REGISTRATION_PASSWORD_MIN_LENGTH) {
          throw new Error(`Kata sandi minimal ${REGISTRATION_PASSWORD_MIN_LENGTH} karakter`);
        }
        await api.post("/auth/register", {
          username: fd.get("username"),
          password,
          name: fd.get("full_name") || undefined,
          invite_code: fd.get("invite_code"),
        });
        window.location.replace(getSafeRedirectTarget());
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Terjadi kesalahan, silakan coba lagi");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen flex items-center justify-center bg-[var(--bg)] text-[var(--text)] px-4">
      <div className="w-full max-w-sm rounded-[28px] border border-[var(--border)] bg-[var(--surface)] p-6 shadow-sm sm:p-8">
        <h1 className="text-2xl font-bold mb-1 text-center tracking-tight">CashFlow</h1>
        <p className="text-center text-xs text-[var(--muted)] mb-6">
          {mode === "login" ? "Masuk ke akun Anda" : "Buat akun baru"}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          {mode === "register" && (
            <div>
              <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="full_name">
                Nama Tampilan
              </label>
              <input
                id="full_name"
                name="full_name"
                autoComplete="name"
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
              />
            </div>
          )}
          <div>
            <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="username">
              Nama Pengguna
            </label>
            <input
              id="username"
              name="username"
              autoComplete="username"
              required
              minLength={mode === "register" ? 6 : undefined}
              className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="password">
              Kata Sandi
            </label>
            <input
              id="password"
              name="password"
              type="password"
              autoComplete={mode === "login" ? "current-password" : "new-password"}
              required
              minLength={mode === "register" ? REGISTRATION_PASSWORD_MIN_LENGTH : undefined}
              aria-describedby={mode === "register" ? "password-help" : undefined}
              className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
            />
            {mode === "register" && (
              <p id="password-help" className="mt-1 text-[11px] text-[var(--muted)]">
                Minimal {REGISTRATION_PASSWORD_MIN_LENGTH} karakter.
              </p>
            )}
          </div>
          {mode === "register" && (
            <div>
              <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="invite_code">
                Kode Undangan
              </label>
              <input
                id="invite_code"
                name="invite_code"
                autoComplete="off"
                required
                className="min-h-11 w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none focus-visible:ring-2 focus-visible:ring-[var(--primary)]/30"
              />
            </div>
          )}

          {error && (
            <p role="alert" className="text-xs font-medium text-expense">
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="min-h-11 w-full rounded-xl bg-[#1E201E] px-4 py-2.5 text-sm font-semibold text-white hover:bg-[#313631] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)] disabled:opacity-50"
          >
            {loading ? "Memproses…" : mode === "login" ? "Masuk" : "Daftar Akun"}
          </button>
        </form>

        <p className="text-center text-xs text-[var(--muted)] mt-5">
          {mode === "login" ? (
            <>
              Belum punya akun?{" "}
              <button
                onClick={() => { setMode("register"); setError(""); }}
                className="min-h-11 font-semibold text-[var(--text)] underline underline-offset-4 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
              >
                Daftar sekarang
              </button>
            </>
          ) : (
            <>
              Sudah punya akun?{" "}
              <button
                onClick={() => { setMode("login"); setError(""); }}
                className="min-h-11 font-semibold text-[var(--text)] underline underline-offset-4 focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
              >
                Masuk di sini
              </button>
            </>
          )}
        </p>
      </div>
    </main>
  );
}
