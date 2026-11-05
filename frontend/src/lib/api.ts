/**
 * Typed API client.
 * All requests go through /api/* which is proxied to FastAPI.
 * Cookie credentials are included automatically for session auth.
 */

const BASE = "/api";

class ApiError extends Error {
  constructor(
    public status: number,
    public detail: string | ApiErrorDetail,
  ) {
    super(typeof detail === "string" ? detail : detail.message ?? "Request failed");
  }
}

export interface ApiErrorDetail {
  code?: string;
  message?: string;
  available_amount?: number;
  required_amount?: number;
  requested_amount?: number;
  available_units?: number;
  required_units?: number;
  requested_units?: number;
}

function normalizeApiErrorDetail(detail: unknown): string | ApiErrorDetail {
  if (typeof detail !== "object" || detail === null) {
    return String(detail ?? "Request failed");
  }

  const structured = detail as ApiErrorDetail;
  const investmentMessages: Record<string, string> = {
    amount_product_required: "Top up nominal memerlukan produk reksadana tanpa pencatatan unit.",
    amount_product_trade_not_supported: "Produk ini dicatat berdasarkan nominal. Gunakan Top up atau Perbarui nilai.",
    amount_valuation_required: "Gunakan Perbarui nilai untuk memperbarui modal atau nilai produk ini.",
    amount_product_must_remain_leaf: "Produk dengan top up tidak dapat memiliki kantong turunan.",
    invalid_investment_reversal: "Koreksi akan membuat modal atau nilai investasi negatif. Periksa nilai aktual sebelum mencoba lagi.",
    idempotency_mismatch: "Permintaan sebelumnya memakai nominal atau data berbeda. Periksa buku kas sebelum mencatat kembali.",
    investment_topup_lifecycle_required: "Ubah atau hapus melalui detail Top up Investasi.",
    investment_topup_deleted: "Top up ini sudah dihapus. Muat ulang buku kas.",
    archived_topup_account: "Rekening atau produk sudah diarsipkan. Nominal top up tidak dapat ditambah.",
    funding_selection_changed: "Kantong sumber berubah. Pilih kembali rekening sumber.",
    already_amount_tracked: "Produk ini sudah dicatat berdasarkan nominal.",
    unit_mutual_fund_required: "Hanya produk reksadana yang mencatat unit yang dapat diubah ke pelacakan nominal.",
    negative_investment_value: "Nilai produk negatif. Perbarui nilai produk sebelum mengubah pelacakan.",
    transaction_not_convertible: "Transaksi ini tidak dapat dijadikan top up. Hanya pengeluaran tunggal tanpa tagihan, target, atau jadwal yang dapat diubah.",
    liquid_funding_required: "Top up hanya dapat berasal dari rekening bank, e-wallet, atau kas.",
  };
  const deviceMessages: Record<string, string> = {
    device_not_found: "Perangkat sudah dicabut atau tidak ditemukan. Muat ulang daftar perangkat.",
    pairing_configuration_invalid: "Pemasangan ponsel belum dikonfigurasi di server.",
    session_required: "Masuk kembali di browser untuk membuat kode pemasangan.",
  };
  const creditMessages: Record<string, string> = {
    credit_parent_not_allowed: "Rekening kredit tidak dapat memiliki kantong atau menjadi kantong.",
    credit_balance_sign: "Saldo awal tidak sesuai jenis rekening. Utang kredit diisi sebagai angka positif.",
    credit_fields_not_allowed: "Limit dan tanggal bayar hanya untuk rekening kredit.",
    credit_type_change_has_transactions: "Jenis rekening kredit tidak dapat diubah setelah ada transaksi atau kantong.",
    credit_account_not_allowed: "Rekening kredit tidak dapat dipakai di sini. Pilih rekening bank, e-wallet, atau tunai.",
    credit_fee_not_allowed: "Biaya hanya dapat dicatat saat membayar ke rekening kredit dan tidak boleh melebihi nominal.",
    credit_movement_needs_cash: "Salah satu sisi pindah saldo harus rekening bank, e-wallet, atau tunai.",
  };
  const knownMessage = structured.code
    ? investmentMessages[structured.code] ?? deviceMessages[structured.code] ?? creditMessages[structured.code]
    : undefined;
  if (knownMessage) {
    return { ...structured, message: knownMessage };
  }
  if (structured.message) return structured;

  const amount = (value: number | undefined) =>
    typeof value === "number" ? new Intl.NumberFormat("id-ID").format(value) : "—";

  if (structured.code === "insufficient_funds") {
    return {
      ...structured,
      message: `Saldo tidak mencukupi. Tersedia Rp ${amount(structured.available_amount)} dari Rp ${amount(structured.required_amount ?? structured.requested_amount)} yang dibutuhkan.`,
    };
  }
  if (structured.code === "insufficient_units") {
    return {
      ...structured,
      message: `Unit investasi tidak mencukupi. Tersedia ${amount(structured.available_units)} dari ${amount(structured.required_units ?? structured.requested_units)} unit yang dibutuhkan.`,
    };
  }

  return JSON.stringify(detail);
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method,
    credentials: "include",
    headers: body !== undefined ? { "Content-Type": "application/json" } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new ApiError(res.status, normalizeApiErrorDetail(data.detail ?? "Request failed"));
  }
  return data as T;
}

export const api = {
  get: <T>(path: string, signal?: AbortSignal) => request<T>("GET", path, undefined, signal),
  post: <T>(path: string, body?: unknown, signal?: AbortSignal) => request<T>("POST", path, body, signal),
  put: <T>(path: string, body?: unknown) => request<T>("PUT", path, body),
  patch: <T>(path: string, body?: unknown) => request<T>("PATCH", path, body),
  del: <T>(path: string) => request<T>("DELETE", path),
  upload: async <T>(path: string, body: FormData): Promise<T> => {
    const res = await fetch(`${BASE}${path}`, {
      method: "POST",
      credentials: "include",
      body,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new ApiError(res.status, normalizeApiErrorDetail(data.detail ?? "Upload failed"));
    }
    return data as T;
  },
};

export interface CreateMovementPayload {
  source_account_id: string;
  target_account_id: string;
  amount: number;
  notes?: string | null;
  date?: string | null;
  idempotency_key?: string | null;
  /** Interest or a fee charged with a credit bill; only for a movement into a credit account. */
  fee?: number | null;
}

export interface CreateMovementResponse {
  ok: boolean;
  movement_id: string;
  expense_transaction_id: string;
  income_transaction_id: string;
  message?: string;
}

export async function createMovement(payload: CreateMovementPayload): Promise<CreateMovementResponse> {
  return api.post<CreateMovementResponse>("/movements", payload);
}

export async function updateMovement(id: string, payload: CreateMovementPayload): Promise<CreateMovementResponse> {
  return api.patch<CreateMovementResponse>(`/movements/${id}`, payload);
}

export async function deleteMovement(id: string): Promise<{ ok: boolean }> {
  return api.del<{ ok: boolean }>(`/movements/${id}`);
}

export async function splitMovement(id: string): Promise<{ ok: boolean; movement_id: string; transaction_ids: string[] }> {
  return api.post<{ ok: boolean; movement_id: string; transaction_ids: string[] }>(`/movements/${id}/split`);
}

export async function uploadTransactionReceipt(transactionId: string, file: File) {
  const body = new FormData();
  body.append("file", file);
  return api.upload<{ ok: boolean; receipt_path: string }>(
    `/transactions/${transactionId}/receipt`,
    body,
  );
}

export async function updateTransaction(id: string, payload: Record<string, unknown>) {
  return api.patch<{ ok: boolean; transaction?: unknown }>(`/transactions/${id}`, payload);
}

export interface NotificationAlias {
  id: string;
  institution: string;
  name: string;
  account_id: string;
  account: string;
  source: "ai" | "owner";
  created_at: string;
}

export async function getNotificationAliases(): Promise<{ ok: boolean; aliases: NotificationAlias[] }> {
  return api.get<{ ok: boolean; aliases: NotificationAlias[] }>("/ingest/aliases");
}

export async function deleteNotificationAlias(id: string): Promise<{ ok: boolean }> {
  return api.del<{ ok: boolean }>(`/ingest/aliases/${id}`);
}

export interface SenderAlias {
  id: string;
  institution: string;
  mask: string;
  name: string;
  state: "active" | "ambiguous";
  account_id: string;
  account: string;
}

export async function getSenderAliases(): Promise<{ ok: boolean; aliases: SenderAlias[] }> {
  return api.get<{ ok: boolean; aliases: SenderAlias[] }>("/ingest/sender-aliases");
}

export async function updateSenderAlias(id: string, name: string): Promise<{ ok: boolean }> {
  return api.patch<{ ok: boolean }>(`/ingest/sender-aliases/${id}`, { name });
}

export async function deleteSenderAlias(id: string): Promise<{ ok: boolean }> {
  return api.del<{ ok: boolean }>(`/ingest/sender-aliases/${id}`);
}

/** A capture-only "Sesi uji" window (notification-test-session D1); `ended_at` is set once it is over. */
export interface TestSession {
  id: string;
  status: "open" | "ended";
  started_at: string;
  planned_end_at: string;
  ended_at: string | null;
  event_count: number;
  labelled_count: number;
}

/** One notification captured during a session, read by the deterministic parser (D6). */
export interface TestSessionEvent {
  event_id: string;
  test_case: string | null;
  label_notes: string | null;
  post_time: string;
  received_at: string;
  gap_seconds: number;
  package_name: string;
  app_label: string;
  direction: "income" | "expense" | null;
  amount: number | null;
  parse_status: string;
  preview: string;
}

export const TEST_SESSIONS_PATH = "/ingest/test-sessions";

export async function getTestSessions(): Promise<{ ok: boolean; sessions: TestSession[] }> {
  return api.get(TEST_SESSIONS_PATH);
}

export async function getCurrentTestSession(): Promise<{ ok: boolean; session: TestSession | null }> {
  return api.get(`${TEST_SESSIONS_PATH}/current`);
}

export async function startTestSession(durationMinutes: number): Promise<{ ok: boolean; session: TestSession }> {
  return api.post(TEST_SESSIONS_PATH, { duration_minutes: durationMinutes });
}

export async function endTestSession(id: string): Promise<{ ok: boolean; session: TestSession }> {
  return api.post(`${TEST_SESSIONS_PATH}/${id}/end`);
}

export async function getTestSessionEvents(id: string): Promise<{ ok: boolean; events: TestSessionEvent[] }> {
  return api.get(`${TEST_SESSIONS_PATH}/${id}/events`);
}

export async function labelTestSessionEvent(
  id: string, eventId: string, label: { test_case?: string; label_notes?: string },
): Promise<{ ok: boolean; event: TestSessionEvent }> {
  return api.patch(`${TEST_SESSIONS_PATH}/${id}/events/${eventId}`, label);
}

export async function deleteTestSession(id: string): Promise<{ ok: boolean }> {
  return api.del(`${TEST_SESSIONS_PATH}/${id}`);
}

/** The CSV download address; the browser sends the session cookie with the link. */
export function testSessionCsvUrl(id: string): string {
  return `${BASE}${TEST_SESSIONS_PATH}/${id}/export.csv`;
}

/** "This account is also reported by that app" (reconcile-linked-account-reports D1), learned in Tinjau. */
export interface AccountLink {
  id: string;
  account_id: string;
  account_label: string | null;
  institution: string;
  institution_label: string;
  source: "confirmation" | "converted";
  created_at: string | null;
  skipped_count: number;
  last_skipped_at: string | null;
}

export interface ApiKeyMetadata {
  id: string;
  key_prefix: string;
  created_at: string;
  last_used_at: string | null;
}

export async function getApiKeyInfo(): Promise<{ ok: boolean; api_key: ApiKeyMetadata | null }> {
  return api.get<{ ok: boolean; api_key: ApiKeyMetadata | null }>("/auth/api-key");
}

export async function rotateApiKey(): Promise<{ ok: boolean; api_key: string; key_prefix: string; message: string }> {
  return api.post<{ ok: boolean; api_key: string; key_prefix: string; message: string }>("/auth/api-key/reset");
}

export interface PairingCode {
  ok: boolean;
  code: string;
  expires_at: string;
  expires_in_seconds: number;
  qr_payload: string;
}

export type SelfTestStatus = "passed" | "failed" | "not_run";
export type SelfTestFailedStep = "post" | "capture" | "server";

/** The latest health report a phone sent; `ready` is computed by the server. */
export interface DeviceHealth {
  reported_at: string;
  ready: boolean;
  app_version: string;
  listener_connected: boolean;
  notification_access: boolean;
  post_notifications: boolean;
  battery_unrestricted: boolean;
  self_test: { status: SelfTestStatus; failed_step: SelfTestFailedStep | null; at: string | null };
  last_capture_at: string | null;
  queue_pending: number;
}

export interface PairedDevice {
  id: string;
  name: string;
  created_via: "manual" | "pairing";
  key_prefix: string;
  created_at: string;
  last_used_at: string | null;
  /** Null until the phone sends its first health report. */
  health: DeviceHealth | null;
}

export async function createPairingCode(): Promise<PairingCode> {
  return api.post<PairingCode>("/devices/pairing-codes");
}

export async function getDevices(): Promise<{ ok: boolean; devices: PairedDevice[] }> {
  return api.get<{ ok: boolean; devices: PairedDevice[] }>("/devices");
}

export async function revokeDevice(id: string): Promise<{ ok: boolean }> {
  return api.del<{ ok: boolean }>(`/devices/${id}`);
}

export { ApiError };
