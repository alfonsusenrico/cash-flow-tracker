import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { HeroWalletCard } from "./HeroWalletCard";

vi.mock("@/components/layout/AppLayout", () => ({
  useAppCtx: () => ({
    hideBalances: false,
    setHideBalances: vi.fn(),
    bal: (amount: number) => `Rp ${amount}`,
    openMovement: vi.fn(),
  }),
}));
vi.mock("@/hooks/useAnimatedCounter", () => ({
  useAnimatedCounter: (amount: number) => amount,
}));

function renderCard(canTransfer: boolean, onOpenMovement = vi.fn()) {
  render(
    <HeroWalletCard
      totalBalance={3000}
      liquidBalance={3000}
      investmentBalance={0}
      accountsCount={2}
      onOpenCapture={vi.fn()}
      onOpenMovement={onOpenMovement}
      canTransfer={canTransfer}
    />,
  );
  return onOpenMovement;
}

describe("desktop Home movement entry point", () => {
  it("opens the canonical movement flow when two liquid accounts exist", async () => {
    const openMovement = renderCard(true);
    await userEvent.setup().click(screen.getByRole("button", { name: "Pindah Saldo" }));
    expect(openMovement).toHaveBeenCalledOnce();
  });

  it("does not offer movement with fewer than two liquid accounts", () => {
    renderCard(false);
    expect(screen.getByRole("button", { name: "Pindah Saldo" })).toBeDisabled();
  });
});
