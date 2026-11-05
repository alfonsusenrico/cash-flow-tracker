import { render, screen } from "@testing-library/react";
import { axe } from "vitest-axe";
import { describe, expect, it } from "vitest";
import { AccountSelectOptions } from "./AccountSelectOptions";

describe("AccountSelectOptions", () => {
  it("limits generic movement choices to liquid accounts when requested", async () => {
    const { container } = render(
      <label>
        Rekening
        <select aria-label="Rekening">
          <AccountSelectOptions
            liquidOnly
            allowParentSelection
            accounts={[
              {
                id: "bank",
                name: "Bank",
                type: "bank",
                children: [
                  { id: "pocket", name: "Kantong", type: "bank", parent_id: "bank" },
                  { id: "position-child", name: "Saham BBRI", type: "investment", instrument_type: "stock", parent_id: "bank" },
                ],
              },
              { id: "position", name: "Saham BBCA", type: "investment", instrument_type: "stock" },
              { id: "rdn", name: "RDN Broker", type: "bank" },
            ]}
          />
        </select>
      </label>,
    );

    expect(screen.getByRole("option", { name: /Bank/ })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: /Kantong/ })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: /RDN Broker/ })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /Saham/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /BBRI|BBCA/ })).not.toBeInTheDocument();
    expect((await axe(container)).violations).toHaveLength(0);
  });

  it("explains when no liquid option can be selected", () => {
    const { container } = render(
      <select aria-label="Rekening">
        <AccountSelectOptions
          liquidOnly
          accounts={[{ id: "position", name: "Saham", type: "investment" }]}
        />
      </select>,
    );

    expect(container.querySelector("option")).toHaveTextContent("Tidak ada rekening likuid yang tersedia");
  });

  it("keeps a liquid RDN pocket under an investment platform selectable", () => {
    render(
      <select aria-label="Rekening">
        <AccountSelectOptions
          liquidOnly
          allowParentSelection
          accounts={[{
            id: "broker",
            name: "Broker",
            type: "investment",
            children: [
              { id: "rdn", name: "RDN", type: "bank", parent_id: "broker" },
              { id: "stock", name: "Saham", type: "investment", parent_id: "broker" },
            ],
          }]}
        />
      </select>,
    );

    expect(screen.getByRole("option", { name: "RDN" })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: /Broker|Saham/ })).not.toBeInTheDocument();
  });

  it("lists credit accounts in their own Kredit group only when a picker opts in", async () => {
    const accounts = [
      { id: "bank", name: "Bank", type: "bank", balance: 500_000 },
      { id: "spaylater", name: "SPayLater", type: "credit", balance: -470_000, credit_owed: 470_000 },
      { id: "kredivo", name: "Kredivo", type: "credit", balance: 0, credit_owed: 0 },
    ];
    const money = (value: number) => `Rp ${value.toLocaleString("id-ID")}`;
    const { container, rerender } = render(
      <select aria-label="Rekening">
        <AccountSelectOptions liquidOnly allowParentSelection includeCredit formatBalance={money} accounts={accounts} />
      </select>,
    );
    const group = screen.getByRole("group", { name: "Kredit" });
    expect([...group.querySelectorAll("option")].map((option) => option.textContent)).toEqual(["SPayLater (utang Rp 470.000)", "Kredivo"]);
    expect(screen.getByRole("option", { name: /^Bank/ }).closest("optgroup")).toBeNull();
    expect((await axe(container)).violations).toHaveLength(0);

    rerender(
      <select aria-label="Rekening">
        <AccountSelectOptions liquidOnly allowParentSelection includeCredit excludeAccountId="spaylater" accounts={accounts} />
      </select>,
    );
    expect(screen.queryByRole("option", { name: /SPayLater/ })).not.toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Kredivo" })).toBeInTheDocument();

    // Without the opt-in a credit account is never offered, liquid-only or not (investment funding, debt payments).
    for (const liquidOnly of [true, false]) {
      rerender(
        <select aria-label="Rekening">
          <AccountSelectOptions liquidOnly={liquidOnly} allowParentSelection accounts={accounts} />
        </select>,
      );
      expect(screen.queryByRole("group", { name: "Kredit" })).not.toBeInTheDocument();
      expect(screen.queryByRole("option", { name: /SPayLater|Kredivo/ })).not.toBeInTheDocument();
    }
  });
});
