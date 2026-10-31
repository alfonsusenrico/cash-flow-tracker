"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Button } from "@/components/ui/Button";
import {
  SETTINGS_FIELD_WIDTH, SettingsAlert, SettingsCard, SettingsField, SettingsSaveStatus, settingsControlClass,
} from "@/components/settings/SettingsCard";

interface ProfileUser {
  username: string;
  name?: string | null;
  name_aliases?: string[] | null;
}

function parseAliases(text: string) {
  return text.split(",").map((alias) => alias.trim()).filter(Boolean);
}

function validate(name: string, aliases: string[]) {
  if (!name.trim()) return "Nama tampilan wajib diisi";
  if (aliases.length > 10) return "Maksimal 10 nama alias";
  if (aliases.some((alias) => alias.length < 3 || alias.length > 150)) return "Setiap nama alias harus 3–150 karakter";
  return "";
}

/** Display name and the names banks print for the owner; saved together and nothing else. */
export function ProfileSettings() {
  const qc = useQueryClient();
  const { data, isError } = useQuery<{ ok: boolean; user: ProfileUser }>({
    queryKey: queryKeys.auth.me,
    queryFn: () => api.get("/auth/me"),
  });
  const user = data?.user;
  const savedName = user ? user.name || user.username : "";
  const savedAliases = (user?.name_aliases ?? []).join(", ");

  const [name, setName] = useState("");
  const [aliases, setAliases] = useState("");
  const [error, setError] = useState("");
  const [saved, setSaved] = useState(false);

  const loaded = Boolean(user);
  useEffect(() => {
    if (!loaded) return;
    setName(savedName);
    setAliases(savedAliases);
  }, [loaded, savedName, savedAliases]);

  const save = useMutation({
    mutationFn: (body: { name: string; name_aliases: string[] }) => api.patch("/auth/settings", body),
    onSuccess: () => {
      setError("");
      setSaved(true);
      void qc.invalidateQueries({ queryKey: queryKeys.auth.all });
    },
    onError: (e: Error) => setError(e.message || "Profil gagal disimpan. Coba lagi."),
  });

  const dirty = loaded && (name !== savedName || aliases !== savedAliases);

  return (
    <form
      noValidate
      onSubmit={(event) => {
        event.preventDefault();
        if (save.isPending) return;
        const nameAliases = parseAliases(aliases);
        const problem = validate(name, nameAliases);
        setSaved(false);
        if (problem) {
          setError(problem);
          return;
        }
        save.mutate({ name: name.trim(), name_aliases: nameAliases });
      }}
    >
      <SettingsCard
        section="profil"
        footer={
          <>
            <SettingsSaveStatus dirty={dirty} saved={saved} />
            <Button type="submit" variant="primary" size="sm" className="min-h-11" disabled={!loaded || save.isPending}>
              {save.isPending ? "Menyimpan…" : "Simpan profil"}
            </Button>
          </>
        }
      >
        {isError && <SettingsAlert>Profil tidak dapat dimuat. Muat ulang halaman untuk mencoba lagi.</SettingsAlert>}
        {error && <SettingsAlert>{error}</SettingsAlert>}
        <div className={`space-y-4 ${SETTINGS_FIELD_WIDTH}`}>
          <p className="text-xs text-[var(--muted)]">
            Nama pengguna <span className="font-semibold text-[var(--text)]">@{user?.username ?? "…"}</span>
          </p>
          <SettingsField id="settings-display-name" label="Nama tampilan" help="Dipakai untuk sapaan di Beranda.">
            {(field) => (
              <input
                {...field}
                name="name"
                type="text"
                value={name}
                maxLength={150}
                onChange={(event) => {
                  setSaved(false);
                  setName(event.target.value);
                }}
                placeholder="Contoh: Raka Purnama"
                className={settingsControlClass}
              />
            )}
          </SettingsField>
          <SettingsField
            id="settings-name-aliases"
            label="Nama di notifikasi bank"
            help={<>Pisahkan dengan koma, maksimal 10. Dipakai mengenali transfer ke rekening sendiri, termasuk nama yang disamarkan bank seperti &quot;RA*A PUR***&quot;. Nama tampilan selalu ikut dipakai.</>}
          >
            {(field) => (
              <input
                {...field}
                name="name_aliases"
                type="text"
                value={aliases}
                onChange={(event) => {
                  setSaved(false);
                  setAliases(event.target.value);
                }}
                placeholder="Contoh: Nama Lengkap Sesuai Rekening"
                className={settingsControlClass}
              />
            )}
          </SettingsField>
        </div>
      </SettingsCard>
    </form>
  );
}
