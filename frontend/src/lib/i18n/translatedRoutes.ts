import { isActivePath } from "@/components/workspace/navigation";

/**
 * Routes whose redesign phase has translated them (add-language-system D10). Every other route shows its Indonesian
 * content inside the translated shell, marked `lang="id"` with the notice line; each phase adds its routes here.
 */
export const TRANSLATED_ROUTES = ["/", "/auth/login", "/review"] as const;

export function isTranslatedRoute(pathname: string): boolean {
  return TRANSLATED_ROUTES.some((route) => (route === "/" ? pathname === "/" : isActivePath(pathname, route)));
}
