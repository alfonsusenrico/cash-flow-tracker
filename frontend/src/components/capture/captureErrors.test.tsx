import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { configureApiMessages } from "@/lib/i18n/runtime";
import { renderWithIntl } from "@/lib/i18n/testing";
import { ToastProvider } from "@/components/workspace/overlays";
import en from "@/messages/en.json";
import id from "@/messages/id.json";
import CaptureDialog from "./CaptureDialog";

vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: false, bal: (amount: number) => `Rp ${amount}` }) }));

const accounts = [
  { id: "jago", name: "Jago", type: "bank", balance: 500_000 },
  { id: "bca", name: "BCA", type: "bank", balance: 200_000 },
];

/** The real request path: `fetch` answers like the API, so the error code is worded by the web catalogue (D13). */
function stubApi(movementBodies: unknown[]) {
  vi.stubGlobal("fetch", vi.fn(async (url: string, init?: RequestInit) => {
    const path = url.replace(/^\/api/, "");
    const reply = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
    if (path === "/accounts") return reply(200, { accounts });
    if (path === "/categories") return reply(200, { categories: [] });
    if (path === "/goals") return reply(200, { goals: [] });
    if (path === "/obligations") return reply(200, { obligations: [] });
    if (path === "/movements" && init?.method === "POST") {
      movementBodies.push(JSON.parse(String(init.body)));
      return reply(400, { detail: { code: "same_account", message: "Source and target accounts must be different", params: {} } });
    }
    return reply(404, { detail: "not found" });
  }));
}

afterEach(() => {
  vi.unstubAllGlobals();
  configureApiMessages("id", id);
});

describe("server refusals in the interface language", () => {
  it.each([
    ["id", id, "Ke rekening", "Pindahkan saldo", "Rekening sumber dan tujuan harus berbeda."],
    ["en", en, "To account", "Move balance", "Source and target accounts must be different."],
  ] as const)("shows same_account under Ke in %s and focuses it", async (language, messages, toLabel, move, text) => {
    configureApiMessages(language, messages);
    const bodies: unknown[] = [];
    stubApi(bodies);
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    renderWithIntl(
      <QueryClientProvider client={queryClient}>
        <ToastProvider>
          <CaptureDialog request={{ view: "manual", mode: "movement" }} onClose={vi.fn()} />
        </ToastProvider>
      </QueryClientProvider>,
      { language },
    );
    const user = userEvent.setup();
    const target = await screen.findByRole("combobox", { name: toLabel });
    await waitFor(() => expect(target).toHaveValue("bca"));
    await user.type(screen.getByRole("textbox", { name: language === "id" ? "Nominal (IDR)" : "Amount (IDR)" }), "10000");
    await user.click(screen.getByRole("button", { name: move }));
    const error = await screen.findByText(text);
    expect(error.id).toBe("capture-target-error");
    expect(target).toHaveAttribute("aria-invalid", "true");
    expect(target.getAttribute("aria-describedby")).toContain("capture-target-error");
    await waitFor(() => expect(target).toHaveFocus());
    expect(bodies).toHaveLength(1);
    expect(within(screen.getByRole("dialog")).queryByText("This page is in Indonesian for now.")).not.toBeInTheDocument();
  });
});
