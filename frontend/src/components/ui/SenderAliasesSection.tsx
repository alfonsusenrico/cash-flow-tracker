"use client";

import { useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { deleteSenderAlias, getSenderAliases, updateSenderAlias } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Button } from "@/components/ui/Button";
import { Input } from "@/components/ui/Input";

export function SenderAliasesSection({ enabled }: { enabled: boolean }) {
  const queryClient = useQueryClient();
  const { data, isError } = useQuery({
    queryKey: queryKeys.senderAliases,
    queryFn: getSenderAliases,
    enabled,
  });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [error, setError] = useState("");
  const returnFocus = useRef<HTMLButtonElement | null>(null);
  const heading = useRef<HTMLHeadingElement | null>(null);

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
      heading.current?.focus();
    },
    onError: () => setError("Nama pengirim gagal dihapus. Coba lagi."),
  });
  const busy = save.isPending || remove.isPending;

  return (
    <section aria-labelledby="sender-aliases-title" className="space-y-3 border-t border-[var(--border)] pt-3 text-xs">
      <h3 ref={heading} tabIndex={-1} id="sender-aliases-title" className="text-xs font-bold uppercase tracking-wider text-[var(--muted)] focus-visible:outline focus-visible:outline-2">
        Nama pengirim
      </h3>
      <p className="text-[var(--muted)]">Nama yang Anda konfirmasi. Perubahan berlaku untuk transfer berikutnya.</p>
      {error && <p id="sender-alias-error" role="alert" className="text-[var(--danger)]">{error}</p>}
      {isError ? (
        <p role="alert" className="text-[var(--danger)]">Daftar pengirim gagal dimuat.</p>
      ) : !data ? (
        <p role="status" className="text-[var(--muted)]">Memuat pengirim…</p>
      ) : data.aliases.length === 0 ? (
        <p className="text-[var(--muted)]">Belum ada nama pengirim tersimpan.</p>
      ) : (
        <ul className="divide-y divide-[var(--border)] rounded-xl border border-[var(--border)]">
          {data.aliases.map((alias) => (
            <li key={alias.id} className="space-y-2 px-3 py-2">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="min-w-0 flex-1 break-words">
                  <p className="font-semibold text-[var(--text)]">
                    {alias.mask} <span aria-hidden="true">→</span><span className="sr-only">nama pengirim</span> {alias.name}
                  </p>
                  <p className="text-[var(--muted)]">{alias.institution.toUpperCase()} · {alias.account}</p>
                  {alias.state === "ambiguous" && (
                    <p className="text-[var(--text)]">Nama berbeda pernah dikonfirmasi. Transfer berikutnya akan ditanyakan lagi.</p>
                  )}
                </div>
                <div className="flex max-w-full flex-wrap gap-2">
                  <Button
                    variant="secondary"
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
                  >Ubah</Button>
                  <Button
                    variant="secondary"
                    size="sm"
                    className="min-h-11"
                    aria-label={`Hapus nama pengirim ${alias.mask}`}
                    disabled={busy || editingId !== null}
                    onClick={() => remove.mutate(alias.id)}
                  >Hapus</Button>
                </div>
              </div>
              {editingId === alias.id && (
                <form
                  className="space-y-2"
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
                  <Input
                    id={`sender-name-${alias.id}`}
                    label="Nama pengirim"
                    autoFocus
                    value={name}
                    maxLength={80}
                    disabled={busy}
                    aria-invalid={Boolean(error)}
                    aria-describedby={error ? "sender-alias-error" : undefined}
                    onChange={(event) => setName(event.target.value)}
                  />
                  <div className="flex flex-wrap gap-2">
                    <Button type="submit" size="sm" className="min-h-11" disabled={busy}>Simpan nama</Button>
                    <Button type="button" variant="secondary" size="sm" className="min-h-11" disabled={busy} onClick={finishEditing}>Batal</Button>
                  </div>
                </form>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
