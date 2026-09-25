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
  post: <T>(path: string, body?: unknown) => request<T>("POST", path, body),
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

export { ApiError };
