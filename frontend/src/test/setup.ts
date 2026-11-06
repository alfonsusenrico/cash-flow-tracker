import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";
import { configureApiMessages } from "@/lib/i18n/runtime";
import id from "@/messages/id.json";

// Every render gets the Indonesian catalogue unless a test asks for another language (add-language-system D16):
// components translate through next-intl, and the existing Indonesian assertions keep their meaning.
vi.mock("@testing-library/react", async (importOriginal) => {
  const library = await importOriginal<typeof import("@testing-library/react")>();
  const { createElement } = await import("react");
  const { IntlTestProvider } = await import("@/lib/i18n/IntlTestProvider");
  type Options = Parameters<typeof library.render>[1];
  const withCatalogue = (options?: Options) => {
    const Inner = options?.wrapper;
    const wrapper = ({ children }: { children: React.ReactNode }) =>
      createElement(IntlTestProvider, null, Inner ? createElement(Inner, null, children) : children);
    return { ...options, wrapper };
  };
  return {
    ...library,
    render: (ui: React.ReactElement, options?: Options) => library.render(ui, withCatalogue(options)),
    renderHook: (callback: (props: unknown) => unknown, options?: Parameters<typeof library.renderHook>[1]) =>
      library.renderHook(callback, withCatalogue(options as Options) as Parameters<typeof library.renderHook>[1]),
  };
});

configureApiMessages("id", id);

afterEach(() => cleanup());

HTMLCanvasElement.prototype.getContext = () => null;
