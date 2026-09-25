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
});
