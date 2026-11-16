import { api } from "@/lib/api";

/** The server's limits (transaction-tags D2): tags per transaction, and characters per name after trimming. */
export const TAG_LIMIT = 10;
export const TAG_NAME_MAX_LENGTH = 40;

/** One of the owner's tags as `GET /api/tags` lists it. */
export interface Tag {
  id: string;
  name: string;
  archived: boolean;
  transaction_count: number;
}

/** A tag chosen in a picker: an existing tag by its ID, or a new name (`id` null) created when the row is saved. */
export interface TagChoice {
  id: string | null;
  name: string;
}

/** The display form the server stores: trimmed, with runs of whitespace collapsed to one space. */
export function tagDisplayName(value: string): string {
  return value.split(/\s+/).filter(Boolean).join(" ");
}

/** The form a tag is unique by: the display form in lower case, as the server compares names. */
export function normalizeTagName(value: string): string {
  return tagDisplayName(value).toLowerCase();
}

/** Whether a name is within the server's 1 to 40 characters, counted as code points like the server does. */
export function tagNameValid(value: string): boolean {
  const length = Array.from(tagDisplayName(value)).length;
  return length >= 1 && length <= TAG_NAME_MAX_LENGTH;
}

/** The request fields of a picker's value: existing tags by ID and new ones by name. */
export function tagRequest(value: TagChoice[]): { tag_ids: string[]; new_tags: string[] } {
  return {
    tag_ids: value.flatMap((choice) => (choice.id ? [choice.id] : [])),
    new_tags: value.flatMap((choice) => (choice.id ? [] : [tagDisplayName(choice.name)])),
  };
}

/**
 * The `tag_*` code of a refused request, so a form can show the refusal at its tag field; undefined for any other
 * failure. Read from the error's `detail` rather than by class, as the client's errors carry it.
 */
export function tagErrorCode(failure: unknown): string | undefined {
  const detail = (failure as { detail?: unknown } | null)?.detail;
  const code = typeof detail === "object" && detail !== null ? (detail as { code?: unknown }).code : undefined;
  return typeof code === "string" && code.startsWith("tag_") ? code : undefined;
}

export async function getTags(includeArchived: boolean): Promise<{ ok: boolean; tags: Tag[] }> {
  return api.get<{ ok: boolean; tags: Tag[] }>(`/tags?include_archived=${includeArchived ? 1 : 0}`);
}

export async function updateTag(id: string, patch: { name?: string; archived?: boolean }): Promise<{ ok: boolean; tag: Tag }> {
  return api.patch<{ ok: boolean; tag: Tag }>(`/tags/${id}`, patch);
}

export async function mergeTag(id: string, intoId: string): Promise<{ ok: boolean; tag: Tag }> {
  return api.post<{ ok: boolean; tag: Tag }>(`/tags/${id}/merge`, { into_id: intoId });
}
