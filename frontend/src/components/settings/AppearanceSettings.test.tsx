import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "@/lib/api";
import { readLanguageCookie } from "@/lib/i18n/cookie";
import { renderWithIntl } from "@/lib/i18n/testing";
import { queryKeys } from "@/lib/queryKeys";
import { AppearanceSettings } from "./AppearanceSettings";

vi.mock("@/lib/api", () => ({ api: { patch: vi.fn() } }));
const router = vi.hoisted(() => ({ refresh: vi.fn() }));
vi.mock("next/navigation", () => ({ useRouter: () => router }));
vi.mock("@/components/layout/AppLayout", () => ({ useAppCtx: () => ({ hideBalances: false, setHideBalances: vi.fn() }) }));
vi.mock("@/components/workspace/context", () => ({ useWorkspace: () => ({ themeChoice: "system", setThemeChoice: vi.fn(), openHelp: vi.fn() }) }));
vi.mock("@/components/workspace/shortcuts", () => ({ useShortcuts: () => ({ singleKeysEnabled: true, setSingleKeysEnabled: vi.fn() }) }));

function show(language: "en" | "id" = "id") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  client.setQueryData(queryKeys.auth.me, { ok: true, user: { id: "u1", language } });
  const view = renderWithIntl(
    <QueryClientProvider client={client}>
      <AppearanceSettings />
    </QueryClientProvider>,
    { language },
  );
  return { ...view, client, user: userEvent.setup() };
}

beforeEach(() => vi.clearAllMocks());
afterEach(() => {
  document.cookie = "cft_language=; Path=/; Max-Age=0";
});

/** The Language control in Pengaturan › Tampilan (add-language-system D11). */
describe("Language control", () => {
  it("offers both languages, each named and marked in its own language, without flags", async () => {
    const { container } = show();
    const group = screen.getByRole("radiogroup", { name: "Bahasa" });
    const english = within(group).getByRole("radio", { name: "English" });
    const indonesian = within(group).getByRole("radio", { name: "Bahasa Indonesia" });
    expect(english).toHaveAttribute("lang", "en");
    expect(indonesian).toHaveAttribute("lang", "id");
    expect(indonesian).toBeChecked();
    expect(group.querySelector("img, svg")).toBeNull();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("saves the choice, writes the cookie and refreshes in place, keeping focus on the chosen option", async () => {
    vi.mocked(api.patch).mockResolvedValue({ ok: true });
    const { user, client } = show();
    const english = screen.getByRole("radio", { name: "English" });
    await user.click(english);
    await waitFor(() => expect(router.refresh).toHaveBeenCalledTimes(1));
    expect(api.patch).toHaveBeenCalledWith("/auth/settings", { language: "en" });
    expect(readLanguageCookie()).toBe("en");
    expect(english).toHaveFocus();
    expect(english).toBeChecked();
    expect(client.getQueryData<{ user: { language: string } }>(queryKeys.auth.me)?.user.language).toBe("en");
    expect(screen.getByRole("status")).toHaveTextContent("Bahasa disimpan");
  });

  it("keeps the previous language and announces why when the save is refused", async () => {
    vi.mocked(api.patch).mockRejectedValue(new Error("Bahasa ini tidak didukung."));
    const { user } = show();
    await user.click(screen.getByRole("radio", { name: "English" }));
    await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Bahasa ini tidak didukung."));
    expect(screen.getByRole("radio", { name: "Bahasa Indonesia" })).toBeChecked();
    expect(router.refresh).not.toHaveBeenCalled();
    expect(readLanguageCookie()).toBeNull();
  });

  it("reads Language in English and keeps the theme names translated", () => {
    show("en");
    expect(screen.getByRole("radiogroup", { name: "Language" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "English" })).toBeChecked();
    expect(screen.getByRole("radiogroup", { name: "Theme" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "Match system" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Appearance" })).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Single-key shortcuts" })).toBeInTheDocument();
    expect(screen.getByRole("switch", { name: "Hide balances" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Open help" })).toBeInTheDocument();
    expect(document.querySelector("[lang='id']:not([data-value='id'])")).toBeNull();
  });
});
