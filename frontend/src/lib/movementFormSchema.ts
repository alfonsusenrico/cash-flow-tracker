import { z } from "zod";

/**
 * What the browser checks before a movement is sent, shared by Catat and "Ubah pindah saldo". Each issue's message
 * is a code that the caller words from its catalogue (apply-markas-catat D13), so the rules stay in one place.
 */
export type MovementIssueCode =
  | "source_required"
  | "target_required"
  | "amount_invalid"
  | "date_required"
  | "notes_too_long"
  | "same_account";

export const movementFormSchema = z
  .object({
    sourceAccountId: z.string().min(1, "source_required"),
    targetAccountId: z.string().min(1, "target_required"),
    amount: z.number().int().positive("amount_invalid"),
    date: z.string().min(1, "date_required"),
    notes: z.string().max(500, "notes_too_long"),
  })
  .superRefine((values, context) => {
    if (values.sourceAccountId === values.targetAccountId) {
      context.addIssue({
        code: "custom",
        message: "same_account",
        path: ["targetAccountId"],
      });
    }
  });
