import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "vitest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import type { Tag } from "@/lib/tags";
import { TagsSection } from "./TagsSection";

vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  api: { get: vi.fn(), patch: vi.fn(), post: vi.fn() },
}));

let tags: Tag[];

function show() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const invalidate = vi.spyOn(client, "invalidateQueries");
  const view = render(<QueryClientProvider client={client}><TagsSection /></QueryClientProvider>);
  return { ...view, invalidate, user: userEvent.setup() };
}

const activeList = () => screen.getByRole("list", { name: "Tag aktif" });

describe("Pengaturan › Tag (transaction-tags D5)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    tags = [
      { id: "tag-coach", name: "fellowship coach", archived: false, transaction_count: 4 },
      { id: "tag-typo", name: "felowship coach", archived: false, transaction_count: 1 },
      { id: "tag-old", name: "Kantor lama", archived: true, transaction_count: 3 },
    ];
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      if (path === "/tags?include_archived=1") return { ok: true, tags };
      throw new Error(`Unexpected GET ${path}`);
    });
    vi.mocked(api.patch).mockResolvedValue({ ok: true, tag: tags[0] });
    vi.mocked(api.post).mockResolvedValue({ ok: true, tag: tags[0] });
  });

  it("lists active tags with their counts and archived ones apart", async () => {
    const { container } = show();
    const list = await screen.findByRole("list", { name: "Tag aktif" });
    expect(within(list).getAllByRole("listitem").map((item) => item.querySelector(".r-title")?.textContent)).toEqual(["fellowship coach", "felowship coach"]);
    expect(within(list).getByText("4 transaksi")).toBeInTheDocument();
    const archived = screen.getByRole("list", { name: "Diarsipkan" });
    expect(within(archived).getByText("Kantor lama")).toBeInTheDocument();
    expect(within(archived).getByRole("button", { name: "Pulihkan tag Kantor lama" })).toBeInTheDocument();
    expect(screen.getByText(/tetap ada di transaksinya dan di filter Transaksi/)).toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("says where tags come from when there are none", async () => {
    tags = [];
    show();
    expect(await screen.findByText("Belum ada tag. Tag dibuat dari Catat atau Ubah transaksi.")).toBeInTheDocument();
  });

  it("renames in the row with Enter, cancels with Escape, and shows a taken name inline", async () => {
    const { user, invalidate } = show();
    const edit = await screen.findByRole("button", { name: "Ubah nama tag felowship coach" });
    await user.click(edit);
    let input = screen.getByRole("textbox", { name: "Nama tag" });
    expect(input).toHaveFocus();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("textbox", { name: "Nama tag" })).not.toBeInTheDocument();
    await waitFor(() => expect(edit).toHaveFocus());
    expect(api.patch).not.toHaveBeenCalled();

    vi.mocked(api.patch).mockRejectedValueOnce(new ApiError(409, { code: "tag_name_taken", message: "Sudah ada tag dengan nama ini." }));
    await user.click(edit);
    input = screen.getByRole("textbox", { name: "Nama tag" });
    await user.clear(input);
    await user.type(input, "fellowship coach{Enter}");
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/tags/tag-typo", { name: "fellowship coach" }));
    expect(await screen.findByText("Sudah ada tag dengan nama ini.")).toBeInTheDocument();
    expect(input).toHaveAttribute("aria-invalid", "true");
    expect(input).toHaveAccessibleDescription(/Sudah ada tag dengan nama ini/);

    await user.clear(input);
    await user.type(input, "  Fellowship   2026 {Enter}");
    await waitFor(() => expect(api.patch).toHaveBeenLastCalledWith("/tags/tag-typo", { name: "Fellowship 2026" }));
    await waitFor(() => expect(screen.queryByRole("textbox", { name: "Nama tag" })).not.toBeInTheDocument());
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.tags.all });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.transactions.all });
  });

  it("refuses an empty name without asking the server", async () => {
    const { user } = show();
    await user.click(await screen.findByRole("button", { name: "Ubah nama tag felowship coach" }));
    const input = screen.getByRole("textbox", { name: "Nama tag" });
    await user.clear(input);
    await user.keyboard("{Enter}");
    expect(await screen.findByText("Isi nama tag sepanjang 1–40 karakter.")).toBeInTheDocument();
    expect(api.patch).not.toHaveBeenCalled();
  });

  it("merges into another tag after a confirmation that states the result", async () => {
    const { user } = show();
    await user.click(await screen.findByRole("button", { name: "Gabungkan tag felowship coach ke tag lain" }));
    const into = screen.getByRole("combobox", { name: "Gabungkan “felowship coach” ke" });
    expect(within(into).getAllByRole("option").map((option) => option.textContent)).toEqual(["Pilih tag", "fellowship coach", "Kantor lama · Diarsipkan"]);
    await user.click(screen.getByRole("button", { name: "Ya, gabungkan" }));
    expect(await screen.findByText("Pilih tag tujuan.")).toBeInTheDocument();
    expect(api.post).not.toHaveBeenCalled();

    await user.selectOptions(into, "tag-coach");
    expect(screen.getByText("1 transaksi dipindah ke “fellowship coach”, lalu tag “felowship coach” dihapus.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Ya, gabungkan" }));
    await waitFor(() => expect(api.post).toHaveBeenCalledWith("/tags/tag-typo/merge", { into_id: "tag-coach" }));
    expect(await screen.findByText("Tag “felowship coach” digabungkan ke “fellowship coach”.")).toBeInTheDocument();
  });

  it("archives an active tag and restores an archived one", async () => {
    const { user } = show();
    await user.click(await within(await screen.findByRole("list", { name: "Tag aktif" })).findByRole("button", { name: "Arsipkan tag fellowship coach" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/tags/tag-coach", { archived: true }));
    await user.click(screen.getByRole("button", { name: "Pulihkan tag Kantor lama" }));
    await waitFor(() => expect(api.patch).toHaveBeenCalledWith("/tags/tag-old", { archived: false }));
    expect(activeList()).toBeInTheDocument();
  });
});
