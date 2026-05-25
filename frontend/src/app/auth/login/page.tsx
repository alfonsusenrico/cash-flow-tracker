"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { api } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [mode, setMode] = useState<"login" | "register">("login");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

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
        router.push("/dashboard");
      } else {
        await api.post("/auth/register", {
          username: fd.get("username"),
          password: fd.get("password"),
          full_name: fd.get("full_name") || undefined,
          invite_code: fd.get("invite_code"),
        });
        setMode("login");
        setError("Registered! Please log in.");
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Something went wrong");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900">
      <div className="w-full max-w-sm p-8 bg-white dark:bg-gray-800 rounded-xl shadow">
        <h1 className="text-2xl font-bold mb-2 text-center">Cash Flow Tracker</h1>
        <p className="text-center text-sm text-gray-500 mb-6">
          {mode === "login" ? "Sign in to your account" : "Create a new account"}
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          {mode === "register" && (
            <div>
              <label className="block text-sm font-medium mb-1" htmlFor="full_name">
                Full Name (optional)
              </label>
              <input
                id="full_name"
                name="full_name"
                autoComplete="name"
                className="w-full border rounded px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600"
              />
            </div>
          )}
          <div>
            <label className="block text-sm font-medium mb-1" htmlFor="username">
              Username
            </label>
            <input
              id="username"
              name="username"
              autoComplete="username"
              required
              className="w-full border rounded px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600"
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1" htmlFor="password">
              Password
            </label>
            <input
              id="password"
              name="password"
              type="password"
              autoComplete={mode === "login" ? "current-password" : "new-password"}
              required
              className="w-full border rounded px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600"
            />
          </div>
          {mode === "register" && (
            <div>
              <label className="block text-sm font-medium mb-1" htmlFor="invite_code">
                Invite Code
              </label>
              <input
                id="invite_code"
                name="invite_code"
                autoComplete="off"
                required
                className="w-full border rounded px-3 py-2 text-sm dark:bg-gray-700 dark:border-gray-600"
              />
            </div>
          )}

          {error && (
            <p className={`text-sm ${error.startsWith("Registered") ? "text-green-600" : "text-red-500"}`}>
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-green-600 hover:bg-green-700 text-white font-medium py-2 rounded disabled:opacity-50"
          >
            {loading ? "Please wait…" : mode === "login" ? "Login" : "Register"}
          </button>
        </form>

        <p className="text-center text-sm text-gray-500 mt-4">
          {mode === "login" ? (
            <>
              First time?{" "}
              <button
                onClick={() => { setMode("register"); setError(""); }}
                className="text-green-600 hover:underline font-medium"
              >
                Register
              </button>
            </>
          ) : (
            <>
              Already have an account?{" "}
              <button
                onClick={() => { setMode("login"); setError(""); }}
                className="text-green-600 hover:underline font-medium"
              >
                Login
              </button>
            </>
          )}
        </p>
      </div>
    </main>
  );
}
