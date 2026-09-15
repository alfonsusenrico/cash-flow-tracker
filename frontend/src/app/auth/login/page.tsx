"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";

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
        await api.post("/auth/register", {
          username: fd.get("username"),
          password: fd.get("password"),
          full_name: fd.get("full_name") || undefined,
          invite_code: fd.get("invite_code"),
        });
        setMode("login");
        setError("Pendaftaran berhasil! Silakan masuk ke akun Anda.");
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Terjadi kesalahan, silakan coba lagi");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen flex items-center justify-center bg-[var(--bg)] text-[var(--text)] px-4">
      <div className="w-full max-w-sm p-8 bg-[var(--surface)] border border-[var(--border)] rounded-2xl shadow-sm">
        <h1 className="text-2xl font-bold mb-1 text-center tracking-tight">CashFlow</h1>
        <p className="text-center text-xs text-[var(--muted)] mb-6">
          {mode === "login" ? "Masuk ke akun Anda" : "Buat akun baru"}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          {mode === "register" && (
            <div>
              <label className="block text-xs font-medium text-[var(--muted)] mb-1" htmlFor="full_name">
                Nama Lengkap (opsional)
              </label>
              <input
                id="full_name"
                name="full_name"
                autoComplete="name"
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-income/30 transition-all"
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
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-income/30 transition-all"
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
              className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-income/30 transition-all"
            />
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
                className="w-full rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 py-2 text-xs font-medium text-[var(--text)] outline-none focus:ring-2 focus:ring-income/30 transition-all"
              />
            </div>
          )}

          {error && (
            <p className={`text-xs font-medium ${error.includes("berhasil") ? "text-income" : "text-expense"}`}>
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-xl bg-income hover:bg-income-hover text-white font-semibold py-2.5 text-xs shadow-xs transition-all active:scale-[0.98] disabled:opacity-50"
          >
            {loading ? "Memproses..." : mode === "login" ? "Masuk" : "Daftar Akun"}
          </button>
        </form>

        <p className="text-center text-xs text-[var(--muted)] mt-5">
          {mode === "login" ? (
            <>
              Belum punya akun?{" "}
              <button
                onClick={() => { setMode("register"); setError(""); }}
                className="text-income hover:underline font-semibold"
              >
                Daftar sekarang
              </button>
            </>
          ) : (
            <>
              Sudah punya akun?{" "}
              <button
                onClick={() => { setMode("login"); setError(""); }}
                className="text-income hover:underline font-semibold"
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
