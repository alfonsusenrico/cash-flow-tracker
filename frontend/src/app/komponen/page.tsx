import { notFound } from "next/navigation";
import { lazy } from "react";

// The component sheet exists only in local builds with NEXT_PUBLIC_COMPONENT_SHEET=1 (apply-markas-foundation
// D12). next.config.mjs always inlines the variable, so in every other build the import below is dead code and
// the sheet ships nowhere; proxy.ts answers its path with a 404 status there.
const ComponentSheet =
  process.env.NEXT_PUBLIC_COMPONENT_SHEET === "1"
    ? lazy(() => import("./ComponentSheet").then((module) => ({ default: module.ComponentSheet })))
    : null;

export default function KomponenPage() {
  if (!ComponentSheet) notFound();
  return <ComponentSheet />;
}
