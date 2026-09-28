import { z } from "zod";

export const movementFormSchema = z
  .object({
    sourceAccountId: z.string().min(1, "Pilih rekening sumber"),
    targetAccountId: z.string().min(1, "Pilih rekening tujuan"),
    amount: z.number().int().positive("Masukkan nominal yang valid"),
    date: z.string().min(1, "Pilih waktu pemindahan"),
    notes: z.string().max(500, "Catatan maksimal 500 karakter"),
  })
  .superRefine((values, context) => {
    if (values.sourceAccountId === values.targetAccountId) {
      context.addIssue({
        code: "custom",
        message: "Rekening sumber dan tujuan tidak boleh sama",
        path: ["targetAccountId"],
      });
    }
  });

export type MovementFormValues = z.infer<typeof movementFormSchema>;
