import AppLayout from "@/components/layout/AppLayout";
export default function Layout({ children }: { children: React.ReactNode }) {
  return <AppLayout title="Periods">{children}</AppLayout>;
}
