import { screen } from "@testing-library/react";
import { useTranslations } from "next-intl";
import { describe, expect, it, vi } from "vitest";
import { renderWithIntl } from "./testing";

function Missing() {
  const t = useTranslations("home") as unknown as (key: string) => string;
  return <p>{t("plantedMissingKey")}</p>;
}

function Present() {
  const t = useTranslations("home");
  return <p>{t("needsAction")}</p>;
}

/** renderWithIntl (add-language-system D16): Indonesian by default, English on request, a missing message throws. */
describe("renderWithIntl", () => {
  it("renders Indonesian by default and English on request", () => {
    const { unmount } = renderWithIntl(<Present />);
    expect(screen.getByText("Perlu tindakan")).toBeInTheDocument();
    unmount();
    renderWithIntl(<Present />, { language: "en" });
    expect(screen.getByText("Needs action")).toBeInTheDocument();
  });

  it("throws on a planted missing message", () => {
    const quiet = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      expect(() => renderWithIntl(<Missing />)).toThrow(/MISSING_MESSAGE/);
    } finally {
      quiet.mockRestore();
    }
  });
});
