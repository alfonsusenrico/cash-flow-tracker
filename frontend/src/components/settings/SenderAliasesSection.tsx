"use client";

import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { deleteSenderAlias, getSenderAliases, updateSenderAlias, type SenderAlias } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Button } from "@/components/ui/Button";
import { ConfirmRemove } from "@/components/settings/ConfirmRemove";
import {
  SETTINGS_FIELD_WIDTH, SettingsAlert, SettingsCard, SettingsField, SettingsList, settingsControlClass,
} from "@/components/settings/SettingsCard";

/** Above this many names the list gets a search field. */
export const SENDER_SEARCH_THRESHOLD = 8;

function fold(value: string) {
  return value.normalize("NFKD").replace(/\p{M}/gu, "").toLocaleLowerCase("id-ID");
}

function matches(alias: SenderAlias, query: string) {
  const needle = fold(query.trim());
  if (!needle) return true;
  return [alias.mask, alias.name, alias.institution, alias.account].some((value) => fold(value).includes(needle));
}

export function SenderAliasesSection() {
  const queryClient = useQueryClient();
  const { data, isError } = useQuery({ queryKey: queryKeys.senderAliases, queryFn: getSenderAliases });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [query, setQuery] = useState("");
  const [error, setError] = useState("");
  const returnFocus = useRef<HTMLButtonElement | null>(null);

  function finishEditing() {
    setEditingId(null);
    setError("");
    requestAnimationFrame(() => returnFocus.current?.focus());
  }

  const save = useMutation({
    mutationFn: ({ id, value }: { id: string; value: string }) => updateSenderAlias(id, value),
    onSuccess: () => {
      setError("");
      finishEditing();
      queryClient.invalidateQueries({ queryKey: queryKeys.senderAliases });
    },
    onError: () => setError("Nama pengirim gagal disimpan. Coba lagi."),
  });
  const remove = useMutation({
    mutationFn: deleteSenderAlias,
    onSuccess: () => {
      setError("");
      queryClient.invalidateQueries({ queryKey: queryKeys.senderAliases });
      document.getElementById("settings-pengirim-title")?.focus();
    },
    onError: () => setError("Nama pengirim gagal dihapus. Coba lagi."),
  });
  const busy = save.isPending || remove.isPending;
  const aliases = data?.aliases ?? [];
  const searchable = aliases.length > SENDER_SEARCH_THRESHOLD;
  const shown = searchable ? aliases.filter((alias) => matches(alias, query)) : aliases;

  return (
    <SettingsCard section="pengirim">
      <p className="text-xs text-[var(--muted)]">Perubahan berlaku untuk transfer berikutnya; transaksi yang sudah tercatat tidak berubah.</p>
      {error && <SettingsAlert><span id="sender-alias-error">{error}</span></SettingsAlert>}
      {isError ? (
        <SettingsAlert>Daftar pengirim gagal dimuat.</SettingsAlert>
      ) : !data ? (
        <p role="status" className="text-[var(--muted)]">Memuat pengirim…</p>
      ) : aliases.length === 0 ? (
        <p className="text-[var(--muted)]">Belum ada nama pengirim tersimpan.</p>
      ) : (
        <>
          {searchable && (
            <div className="space-y-1.5">
              <SettingsField id="sender-search" label="Cari nama pengirim" className={SETTINGS_FIELD_WIDTH}>
                {(field) => (
                  <input
                    {...field}
                    type="search"
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                    placeholder="Nama, samaran, bank atau rekening"
                    className={settingsControlClass}
                  />
                )}
              </SettingsField>
              <p role="status" className="text-xs tabular-nums text-[var(--muted)]">
                {shown.length} dari {aliases.length} nama
              </p>
            </div>
          )}
          {shown.length === 0 ? (
            <p className="text-[var(--muted)]">Tidak ada nama yang cocok.</p>
          ) : (
            <SettingsList label="Nama pengirim tersimpan">
              {shown.map((alias) => (
                <li key={alias.id} className="space-y-3 px-3 py-2.5">
                  <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
                    <div className="min-w-0 flex-1 break-words">
                      <p className="font-semibold text-[var(--text)]">
                        {alias.mask} <span aria-hidden="true">→</span><span className="sr-only">nama pengirim</span> {alias.name}
                      </p>
                      <p className="text-xs text-[var(--muted)]">{alias.institution.toUpperCase()} · {alias.account}</p>
                      {alias.state === "ambiguous" && (
                        <p className="text-xs text-[var(--text)]">Nama berbeda pernah dikonfirmasi. Transfer berikutnya akan ditanyakan lagi.</p>
                      )}
                    </div>
                    <div className="flex flex-wrap items-center justify-end gap-2">
                      <Button
                        size="sm"
                        className="min-h-11"
                        aria-label={`Ubah nama pengirim ${alias.mask}`}
                        disabled={busy || editingId !== null}
                        onClick={(event) => {
                          returnFocus.current = event.currentTarget;
                          setEditingId(alias.id);
                          setName(alias.name);
                          setError("");
                        }}
                      >
                        Ubah
                      </Button>
                      <ConfirmRemove
                        subject={`${alias.mask} → ${alias.name}`}
                        triggerLabel={`Hapus nama pengirim ${alias.mask}`}
                        disabled={busy || editingId !== null}
                        pending={remove.isPending && remove.variables === alias.id}
                        onConfirm={() => remove.mutate(alias.id)}
                      />
                    </div>
                  </div>
                  {editingId === alias.id && (
                    <form
                      className={`space-y-2 ${SETTINGS_FIELD_WIDTH}`}
                      onSubmit={(event) => {
                        event.preventDefault();
                        const value = name.normalize("NFKC").trim();
                        if (!value || Array.from(value).length > 80 || /\p{C}/u.test(value)) {
                          setError("Isi nama pengirim sepanjang 1–80 karakter tanpa baris baru.");
                          return;
                        }
                        save.mutate({ id: alias.id, value });
                      }}
                    >
                      <SettingsField id={`sender-name-${alias.id}`} label="Nama pengirim">
                        {(field) => (
                          <input
                            {...field}
                            autoFocus
                            value={name}
                            maxLength={80}
                            disabled={busy}
                            aria-invalid={Boolean(error)}
                            aria-describedby={error ? "sender-alias-error" : undefined}
                            onChange={(event) => setName(event.target.value)}
                            className={settingsControlClass}
                          />
                        )}
                      </SettingsField>
                      <div className="flex flex-wrap justify-end gap-2">
                        <Button type="button" size="sm" className="min-h-11" disabled={busy} onClick={finishEditing}>Batal</Button>
                        <Button type="submit" variant="primary" size="sm" className="min-h-11" disabled={busy}>Simpan nama</Button>
                      </div>
                    </form>
                  )}
                </li>
              ))}
            </SettingsList>
          )}
        </>
      )}
    </SettingsCard>
  );
}
