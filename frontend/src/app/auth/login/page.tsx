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
    <main className="min-h-screen flex items-center justify-center bg-[var(--window)] text-[var(--text)] px-4">
      <div className="w-full max-w-sm rounded-xl border border-[var(--line)] bg-[var(--surface)] p-6 sm:p-8">
        <h1 className="text-2xl font-semibold mb-1 text-center tracking-tight">CashFlow</h1>
        <p className="text-center text-xs text-[var(--muted)] mb-6">
          {mode === "login" ? "Masuk ke akun" : "Buat akun baru"}
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
                className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
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
              className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
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
              className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
            />
            {mode === "register" && (
              <p id="password-help" className="mt-1 text-xs text-[var(--muted)]">
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
                className="min-h-11 w-full rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-sm font-medium text-[var(--text)] outline-none"
              />
            </div>
          )}

          {error && (
            <p role="alert" className="text-xs font-medium text-out-fg">
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="ws-btn ws-btn-accent w-full"
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
                className="min-h-11 font-semibold text-[var(--text)] underline underline-offset-4"
              >
                Daftar sekarang
              </button>
            </>
          ) : (
            <>
              Sudah punya akun?{" "}
              <button
                onClick={() => { setMode("login"); setError(""); }}
                className="min-h-11 font-semibold text-[var(--text)] underline underline-offset-4"
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
