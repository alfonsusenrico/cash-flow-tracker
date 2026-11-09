"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import { Field } from "@/components/workspace/Field";
import { SettingsAlert, SettingsSaveStatus, SettingsSection } from "@/components/settings/SettingsSection";

interface ProfileUser {
  username: string;
  name?: string | null;
  name_aliases?: string[] | null;
}

type ProfileProblem = "nameRequired" | "tooManyNames" | "nameLength";

function parseAliases(text: string) {
  return text.split(",").map((alias) => alias.trim()).filter(Boolean);
}

function validate(name: string, aliases: string[]): ProfileProblem | null {
  if (!name.trim()) return "nameRequired";
  if (aliases.length > 10) return "tooManyNames";
  if (aliases.some((alias) => alias.length < 3 || alias.length > 150)) return "nameLength";
  return null;
}

/**
 * Profil (apply-markas-pengaturan D12): the username, the display name and the names banks print for the owner in a
 * two-column form; "Simpan profil" sends only `name` and `name_aliases`.
 */
export function ProfileSettings() {
  const t = useTranslations("settings.profile");
  const words = useTranslations("settings.common");
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
    onError: (e: Error) => setError(e.message || t("saveFailed")),
  });

  const dirty = loaded && (name !== savedName || aliases !== savedAliases);

  return (
    <SettingsSection section="profil">
      <form
        noValidate
        className="form-stack"
        onSubmit={(event) => {
          event.preventDefault();
          if (save.isPending) return;
          const nameAliases = parseAliases(aliases);
          const problem = validate(name, nameAliases);
          setSaved(false);
          if (problem) {
            setError(t(problem));
            return;
          }
          save.mutate({ name: name.trim(), name_aliases: nameAliases });
        }}
      >
        {isError && <SettingsAlert>{t("loadFailed")}</SettingsAlert>}
        {error && <SettingsAlert>{error}</SettingsAlert>}
        <div className="form-grid">
          <Field id="settings-username" label={t("username")}>
            {(field) => <input {...field} type="text" readOnly value={user?.username ?? ""} className="field-control border" />}
          </Field>
          <Field id="settings-display-name" label={t("displayName")}>
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
                className="field-control border"
              />
            )}
          </Field>
          <Field id="settings-name-aliases" label={t("bankNames")} hint={t("bankNamesHint")} className="full-row">
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
                className="field-control border"
              />
            )}
          </Field>
        </div>
        <div className="set-actions">
          <button type="submit" className="ws-btn ws-btn-accent" disabled={!loaded || save.isPending}>
            {save.isPending ? words("saving") : t("save")}
          </button>
          <SettingsSaveStatus dirty={dirty} saved={saved} />
        </div>
      </form>
    </SettingsSection>
  );
}
