"use client";

import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import { habitStatus, type HabitList } from "@/lib/habits";
import { useAppCtx } from "@/components/layout/AppLayout";
import { ErrorState, Skeleton } from "@/components/workspace/primitives";
import { SettingsCard } from "@/components/settings/SettingsCard";
import styles from "@/components/accounts/money.module.css";

/** Kebiasaan (D8): what the app has learned, with the server's switches as read-only facts (OD51). */
export function HabitsSettings() {
  const { bal } = useAppCtx();
  const query = useQuery<HabitList>({ queryKey: queryKeys.habits.list, queryFn: () => api.get("/habits") });
  const data = query.data;
  const needed = data?.thresholds.min_confirmations ?? 3;

  return (
    <SettingsCard section="kebiasaan">
      {query.isLoading ? (
        <Skeleton style={{ height: 180 }} />
      ) : query.isError || !data ? (
        <ErrorState message="Daftar kebiasaan gagal dimuat." onRetry={() => query.refetch()} />
      ) : (
        <div className={styles.habits}>
          <dl className={styles.drawerFacts}>
            <dt>Pencatatan otomatis dari kebiasaan</dt><dd>{data.auto_record.habits ? "Aktif" : "Nonaktif"}</dd>
            <dt>Top up otomatis</dt><dd>{data.auto_record.topups ? "Aktif" : "Nonaktif"}</dd>
          </dl>
          <p className="ws-caption">
            Diatur oleh pengaturan server. Kebiasaan dipakai setelah {needed} jawaban yang sama, dengan kecocokan minimal{" "}
            {Math.round(data.thresholds.min_agreement * 100)}%.
          </p>
          {data.habits.length === 0 && data.topups.length === 0 ? (
            <div className={styles.emptyBlock}>
              <p><b>Belum ada kebiasaan yang dipelajari.</b></p>
              <p className="ws-caption">Setiap jawaban di Tinjau untuk pembayaran tanpa nama toko atau top up investasi menjadi bahan belajar.</p>
            </div>
          ) : (
            <>
              {data.habits.length > 0 && (
                <table className={`ws-tbl ${styles.table} ${styles.habitTable}`}>
                  <caption className={styles.tableCaption}>Kebiasaan pembayaran</caption>
                  <thead><tr><th scope="col">Saat ditemukan</th><th scope="col">Dicatat sebagai</th><th scope="col" className="r">Dikonfirmasi</th><th scope="col">Status</th></tr></thead>
                  <tbody>
                    {data.habits.map((habit) => (
                      <tr key={habit.key}>
                        <td>{habit.label} · {habit.account.name ?? "Rekening"}</td>
                        <td>{habit.category.name}{habit.kakeibo ? ` · ${kakeiboBucketInfo(habit.kakeibo)?.label ?? habit.kakeibo}` : ""}</td>
                        <td className={`r ${styles.amount}`}>{habit.confirmations}×</td>
                        <td>{habitStatus(habit.state, habit.confirmations, needed, data.auto_record.habits)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {data.topups.length > 0 && (
                <table className={`ws-tbl ${styles.table} ${styles.habitTable}`}>
                  <caption className={styles.tableCaption}>Top up investasi</caption>
                  <thead><tr><th scope="col">Debit</th><th scope="col">Dicatat sebagai</th><th scope="col" className="r">Dikonfirmasi</th><th scope="col">Status</th></tr></thead>
                  <tbody>
                    {data.topups.map((topup) => (
                      <tr key={topup.key}>
                        <td>{bal(topup.amount)} dari {topup.source_account.name}</td>
                        <td>{topup.product.parent_name ? `${topup.product.parent_name} · ` : ""}{topup.product.name}</td>
                        <td className={`r ${styles.amount}`}>{topup.confirmations}×</td>
                        <td>{habitStatus(topup.state, topup.confirmations, needed, data.auto_record.topups)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {data.truncated && <p className="ws-caption">Menampilkan 100 terbaru dari setiap daftar.</p>}
            </>
          )}
        </div>
      )}
    </SettingsCard>
  );
}
