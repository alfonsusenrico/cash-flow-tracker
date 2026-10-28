import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Sidebar } from "./Sidebar";

vi.mock("next/navigation", () => ({ usePathname: () => "/review" }));
vi.mock("@/lib/api", () => ({ api: { post: vi.fn() } }));
vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({ user: { username: "dina" }, reviewAttention: 7 }),
}));

describe("review navigation entry", () => {
  it("shows the attention count and says it in words", () => {
    render(<Sidebar collapsed={false} onToggleCollapse={vi.fn()} />);
    const link = screen.getByRole("link", { name: /Tinjau/ });
    expect(link).toHaveAttribute("href", "/review");
    expect(link).toHaveTextContent("7");
    expect(link).toHaveAccessibleName(/^Tinjau\s*,\s*7 perlu ditinjau$/);
  });

  it("keeps the count in the name of the collapsed icon link", () => {
    render(<Sidebar collapsed onToggleCollapse={vi.fn()} />);
    expect(screen.getByRole("link", { name: "Tinjau, 7 perlu ditinjau" })).toHaveAttribute("href", "/review");
  });
});
