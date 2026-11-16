import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { api } from "@/lib/api";
import { TAG_LIMIT, normalizeTagName, tagRequest, type Tag, type TagChoice } from "@/lib/tags";
import { TagPicker } from "./TagPicker";

vi.mock("@/lib/api", () => ({ api: { get: vi.fn() } }));

const TAGS: Tag[] = [
  { id: "tag-coach", name: "fellowship coach", archived: false, transaction_count: 4 },
  { id: "tag-malang", name: "Malang trip", archived: false, transaction_count: 2 },
  { id: "tag-old", name: "Kantor lama", archived: true, transaction_count: 7 },
];

function Harness({ initial = [], onValue, allowCreate, includeArchived }: {
  initial?: TagChoice[];
  onValue?: (value: TagChoice[]) => void;
  allowCreate?: boolean;
  includeArchived?: boolean;
}) {
  const [value, setValue] = useState<TagChoice[]>(initial);
  return (
    <form onSubmit={(event) => event.preventDefault()}>
      <TagPicker id="tags" label="Tag (opsional)" value={value} allowCreate={allowCreate} includeArchived={includeArchived}
        onChange={(next) => { setValue(next); onValue?.(next); }} />
    </form>
  );
}

function show(props: Parameters<typeof Harness>[0] = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const view = render(<QueryClientProvider client={client}><Harness {...props} /></QueryClientProvider>);
  return { ...view, user: userEvent.setup() };
}

const field = () => screen.getByRole("combobox", { name: "Tag (opsional)" });
const options = () => within(screen.getByRole("listbox", { hidden: true })).queryAllByRole("option");

describe("TagPicker (transaction-tags D2)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.get).mockImplementation(async (path: string) => {
      const archived = path.endsWith("include_archived=1");
      return { ok: true, tags: TAGS.filter((tag) => archived || !tag.archived) };
    });
  });

  it("is a labelled combobox that filters active tags as the name is typed", async () => {
    const { user, container } = show();
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/tags?include_archived=0"));
    expect(field()).toHaveAttribute("aria-autocomplete", "list");
    expect(field()).toHaveAttribute("aria-expanded", "false");
    expect(field()).toHaveAttribute("aria-controls", "tags-list");

    await user.type(field(), "COACH");
    expect(field()).toHaveAttribute("aria-expanded", "true");
    expect(options().map((option) => option.textContent)).toEqual(["fellowship coach", "Buat tag “COACH”"]);
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("offers Buat tag only when no active tag has the typed name after lower-casing and collapsing spaces", async () => {
    const { user } = show();
    await waitFor(() => expect(api.get).toHaveBeenCalled());
    await user.type(field(), "Fellowship   Coach");
    expect(options().map((option) => option.textContent)).toEqual(["fellowship coach"]);
    await user.clear(field());
    await user.type(field(), "  Kantor   lama ");
    // An archived tag is not offered; creating its name restores it on the server.
    expect(options().map((option) => option.textContent)).toEqual(["Buat tag “Kantor lama”"]);
  });

  it("moves with the arrow keys, picks with Enter, and closes with Escape", async () => {
    const onValue = vi.fn();
    const { user } = show({ onValue });
    await waitFor(() => expect(api.get).toHaveBeenCalled());
    field().focus();
    await user.keyboard("{ArrowDown}");
    expect(field()).toHaveAttribute("aria-expanded", "true");
    expect(field()).toHaveAttribute("aria-activedescendant", "tags-option-0");
    await user.keyboard("{ArrowDown}");
    expect(options()[1]).toHaveAttribute("aria-selected", "true");
    expect(field()).toHaveAttribute("aria-activedescendant", "tags-option-1");
    await user.keyboard("{ArrowUp}{ArrowUp}");
    expect(options()[1]).toHaveAttribute("aria-selected", "true");
    await user.keyboard("{Escape}");
    expect(field()).toHaveAttribute("aria-expanded", "false");

    await user.type(field(), "malang");
    await user.keyboard("{Enter}");
    expect(onValue).toHaveBeenLastCalledWith([{ id: "tag-malang", name: "Malang trip" }]);
    expect(field()).toHaveValue("");
    expect(field()).toHaveFocus();
    expect(within(screen.getByRole("list", { name: "Tag terpilih" })).getByText("Malang trip")).toBeInTheDocument();
    expect(screen.getByText("Tag Malang trip ditambahkan.")).toBeInTheDocument();

    // A picked tag is neither offered again nor created a second time.
    await user.type(field(), "malang trip");
    expect(options()).toHaveLength(0);
  });

  it("creates a new name with Enter and renders a name as text, never as markup", async () => {
    const onValue = vi.fn();
    const { user, container } = show({ onValue });
    await waitFor(() => expect(api.get).toHaveBeenCalled());
    await user.type(field(), "<b>acara</b>");
    await user.keyboard("{Enter}");
    expect(onValue).toHaveBeenLastCalledWith([{ id: null, name: "<b>acara</b>" }]);
    expect(screen.getByText("<b>acara</b>")).toBeInTheDocument();
    expect(container.querySelector("b")).toBeNull();
  });

  it("removes a chip with its labelled button and returns focus to the field", async () => {
    const onValue = vi.fn();
    const { user } = show({ onValue, initial: [{ id: "tag-coach", name: "fellowship coach" }, { id: null, name: "Malang 2" }] });
    await user.click(screen.getByRole("button", { name: "Hapus tag fellowship coach" }));
    expect(onValue).toHaveBeenLastCalledWith([{ id: null, name: "Malang 2" }]);
    expect(field()).toHaveFocus();
    expect(screen.queryByRole("button", { name: "Hapus tag fellowship coach" })).not.toBeInTheDocument();
  });

  it("takes no more tags at the limit and says why", async () => {
    const full = Array.from({ length: TAG_LIMIT }, (_, index) => ({ id: null, name: `tag ${index + 1}` }));
    const onValue = vi.fn();
    const { user } = show({ onValue, initial: full });
    expect(field()).toHaveAttribute("readonly");
    expect(field()).toHaveAccessibleDescription(/Sudah 10 tag, batas untuk satu transaksi/);
    await user.type(field(), "lagi");
    await user.keyboard("{ArrowDown}{Enter}");
    expect(onValue).not.toHaveBeenCalled();
    expect(field()).toHaveAttribute("aria-expanded", "false");
  });

  it("offers archived tags without a create option in the filter", async () => {
    const { user } = show({ allowCreate: false, includeArchived: true });
    await waitFor(() => expect(api.get).toHaveBeenCalledWith("/tags?include_archived=1"));
    await user.type(field(), "kantor");
    expect(options().map((option) => option.textContent)).toEqual(["Kantor lamadiarsipkan"]);
    await user.clear(field());
    await user.type(field(), "tidak ada");
    expect(options()).toHaveLength(0);
    expect(screen.getByText("Tidak ada tag yang cocok.")).toBeInTheDocument();
  });

  it("keeps a typed name when the field is left without picking", async () => {
    const onValue = vi.fn();
    const { user } = show({ onValue });
    await waitFor(() => expect(api.get).toHaveBeenCalled());
    await user.type(field(), "Fellowship  Coach");
    await user.tab();
    expect(onValue).toHaveBeenLastCalledWith([{ id: "tag-coach", name: "fellowship coach" }]);
    await user.type(field(), "Malang 2027");
    await user.tab();
    expect(onValue).toHaveBeenLastCalledWith([{ id: "tag-coach", name: "fellowship coach" }, { id: null, name: "Malang 2027" }]);
    expect(field()).toHaveValue("");
  });

  it("leaves a typed search behind in the filter, where nothing is created", async () => {
    const onValue = vi.fn();
    const { user } = show({ onValue, allowCreate: false });
    await waitFor(() => expect(api.get).toHaveBeenCalled());
    await user.type(field(), "Liburan");
    await user.tab();
    expect(onValue).not.toHaveBeenCalled();
  });

  it("turns its value into existing IDs and new names", () => {
    expect(tagRequest([
      { id: "tag-coach", name: "fellowship coach" },
      { id: null, name: "  Malang   trip " },
    ])).toEqual({ tag_ids: ["tag-coach"], new_tags: ["Malang trip"] });
    expect(tagRequest([])).toEqual({ tag_ids: [], new_tags: [] });
    expect(normalizeTagName(" Fellowship  Coach ")).toBe("fellowship coach");
  });
});
