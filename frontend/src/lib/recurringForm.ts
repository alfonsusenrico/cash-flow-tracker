import { z } from "zod";

export const recurringRuleFormSchema = z
  .object({
    name: z.string().trim().min(1, "Nama transaksi terjadwal wajib diisi").max(100),
    type: z.enum(["expense", "income", "transfer"]),
    amount: z.number().int().positive("Nominal harus lebih dari 0"),
    sourceAccountId: z.string().min(1, "Rekening sumber wajib dipilih"),
    targetAccountId: z.string().nullable(),
    categoryId: z.string().nullable(),
    obligationId: z.string().nullable(),
    scheduleType: z.enum(["monthly_day", "payday", "weekly"]),
    scheduleDay: z.number().int().nullable(),
    isPayrollAllocation: z.boolean(),
    autoPost: z.boolean(),
    isActive: z.boolean(),
    notes: z.string().max(500, "Catatan maksimal 500 karakter"),
  })
  .superRefine((rule, context) => {
    if (rule.type === "transfer") {
      if (!rule.targetAccountId) {
        context.addIssue({
          code: "custom",
          path: ["targetAccountId"],
          message: "Rekening tujuan transfer wajib dipilih",
        });
      } else if (rule.targetAccountId === rule.sourceAccountId) {
        context.addIssue({
          code: "custom",
          path: ["targetAccountId"],
          message: "Rekening sumber dan tujuan tidak boleh sama",
        });
      }
    } else if (!rule.categoryId) {
      context.addIssue({
        code: "custom",
        path: ["categoryId"],
        message: "Kategori wajib dipilih",
      });
    }

    if (rule.obligationId && rule.type !== "expense") {
      context.addIssue({
        code: "custom",
        path: ["obligationId"],
        message: "Tagihan hanya dapat ditautkan ke transaksi pengeluaran",
      });
    }
    if (rule.isPayrollAllocation && rule.type !== "transfer") {
      context.addIssue({
        code: "custom",
        path: ["isPayrollAllocation"],
        message: "Alokasi gaji hanya dapat digunakan untuk transfer",
      });
    }

    if (rule.scheduleType === "payday" && rule.scheduleDay !== null) {
      context.addIssue({
        code: "custom",
        path: ["scheduleDay"],
        message: "Jadwal tanggal gajian tidak menggunakan hari atau tanggal khusus",
      });
    } else if (rule.scheduleType === "weekly" && (rule.scheduleDay === null || rule.scheduleDay < 1 || rule.scheduleDay > 7)) {
      context.addIssue({
        code: "custom",
        path: ["scheduleDay"],
        message: "Pilih hari pengulangan mingguan",
      });
    } else if (rule.scheduleType === "monthly_day" && (rule.scheduleDay === null || rule.scheduleDay < 1 || rule.scheduleDay > 31)) {
      context.addIssue({
        code: "custom",
        path: ["scheduleDay"],
        message: "Tanggal pengulangan harus antara 1 dan 31",
      });
    }
  });

export function parseRecurringRuleForm(input: unknown) {
  return recurringRuleFormSchema.safeParse(input);
}
