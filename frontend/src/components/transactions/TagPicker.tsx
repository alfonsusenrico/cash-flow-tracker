"use client";

import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import { TAG_LIMIT, TAG_NAME_MAX_LENGTH, getTags, normalizeTagName, tagDisplayName, type Tag, type TagChoice } from "@/lib/tags";

type TagOption = { kind: "tag"; tag: Tag } | { kind: "create"; name: string };

/** The key a chosen tag is compared by: its ID, or the normalized name of a tag still to be created. */
function choiceKey(choice: TagChoice): string {
  return choice.id ?? `new:${normalizeTagName(choice.name)}`;
}

/**
 * The tag field of Catat, Ubah transaksi and the ledger filter (transaction-tags D2): a combobox over the owner's
 * tags, with "Buat tag “…”" as the last option when no active tag has the typed name after lower-casing and
 * collapsing spaces. Chosen tags are chips with their own remove button; at the limit the field takes no more.
 * Tag names are the owner's data and are only ever rendered as text.
 */
export function TagPicker({
  id,
  label,
  value,
  onChange,
  allowCreate = true,
  includeArchived = false,
  placeholder,
  error,
  disabled = false,
  className,
}: {
  /** The input's ID; the list, options, hint and error derive theirs from it. */
  id: string;
  label: string;
  value: TagChoice[];
  onChange: (next: TagChoice[]) => void;
  /** The ledger filter only picks tags that exist. */
  allowCreate?: boolean;
  /** The ledger filter also offers archived tags, which stay on their rows. */
  includeArchived?: boolean;
  placeholder?: string;
  error?: string;
  disabled?: boolean;
  /** Placement in the surrounding form, such as `full-row` in a two-column grid. */
  className?: string;
}) {
  const t = useTranslations("transactions.tags");
  const tags = useQuery({ queryKey: queryKeys.tags.list(includeArchived), queryFn: () => getTags(includeArchived) }).data?.tags;
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [announcement, setAnnouncement] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const atLimit = value.length >= TAG_LIMIT;
  const needle = normalizeTagName(query);
  const options = useMemo<TagOption[]>(() => {
    const chosen = new Set(value.map(choiceKey));
    const chosenNames = new Set(value.map((choice) => normalizeTagName(choice.name)));
    const all = tags ?? [];
    const matches: TagOption[] = all
      .filter((tag) => !chosen.has(tag.id) && normalizeTagName(tag.name).includes(needle))
      .map((tag) => ({ kind: "tag", tag }));
    const nameExists = chosenNames.has(needle) || all.some((tag) => !tag.archived && normalizeTagName(tag.name) === needle);
    if (allowCreate && needle && !nameExists) matches.push({ kind: "create", name: tagDisplayName(query) });
    return matches;
  }, [allowCreate, needle, query, tags, value]);

  const listOpen = open && !atLimit && !disabled && options.length > 0;
  const activeIndex = listOpen ? Math.min(active, options.length - 1) : -1;
  const optionId = (index: number) => `${id}-option-${index}`;

  useEffect(() => {
    if (activeIndex < 0) return;
    document.getElementById(optionId(activeIndex))?.scrollIntoView?.({ block: "nearest" });
    // The option IDs derive from `id`, which does not change while the field is shown.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeIndex]);

  const pick = (option: TagOption) => {
    const choice: TagChoice = option.kind === "tag" ? { id: option.tag.id, name: option.tag.name } : { id: null, name: option.name };
    onChange([...value, choice]);
    setQuery("");
    setOpen(false);
    setActive(0);
    setAnnouncement(t("added", { name: choice.name }));
  };

  const remove = (index: number) => {
    const removed = value[index];
    onChange(value.filter((_, position) => position !== index));
    setAnnouncement(t("removed", { name: removed.name }));
    inputRef.current?.focus();
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      if (atLimit || options.length === 0) return;
      event.preventDefault();
      const step = event.key === "ArrowDown" ? 1 : -1;
      if (!listOpen) {
        setOpen(true);
        setActive(step > 0 ? 0 : options.length - 1);
        return;
      }
      setActive((activeIndex + step + options.length) % options.length);
      return;
    }
    if (event.key === "Enter") {
      // Enter picks while a name is typed, so it never submits the form with the name left behind; an empty
      // field lets Enter submit as in every other field.
      if (!needle) return;
      event.preventDefault();
      const option = options[activeIndex >= 0 ? activeIndex : 0];
      if (option && !atLimit) pick(option);
      return;
    }
    if (event.key === "Escape") {
      if (!listOpen && !query) return;
      // The list, then the typed text, close before the dialog around the field does.
      event.preventDefault();
      event.stopPropagation();
      if (listOpen) setOpen(false);
      else setQuery("");
    }
  };

  const hintId = `${id}-hint`;
  const errorId = `${id}-error`;
  let hint: string | null = null;
  if (atLimit) hint = t("limit", { limit: String(TAG_LIMIT) });
  else if (allowCreate) hint = t("hint", { limit: String(TAG_LIMIT) });
  const describedBy = [error ? errorId : null, hint ? hintId : null].filter(Boolean).join(" ") || undefined;
  const noMatch = open && !atLimit && needle && options.length === 0;

  return (
    <div className={cn("field tag-picker", className)}>
      <label htmlFor={id}>{label}</label>
      {value.length > 0 && (
        <ul className="tag-chips" aria-label={t("chosen")}>
          {value.map((choice, index) => (
            <li key={choiceKey(choice)} className="chip">
              <span className="lbl">{choice.name}</span>
              <button type="button" aria-label={t("remove", { name: choice.name })} disabled={disabled} onClick={() => remove(index)}>
                <Icon name="x" className="h-3 w-3" />
              </button>
            </li>
          ))}
        </ul>
      )}
      <input
        ref={inputRef}
        id={id}
        type="text"
        role="combobox"
        aria-autocomplete="list"
        aria-expanded={listOpen}
        aria-controls={`${id}-list`}
        aria-activedescendant={activeIndex >= 0 ? optionId(activeIndex) : undefined}
        aria-invalid={error ? true : undefined}
        aria-describedby={describedBy}
        autoComplete="off"
        maxLength={TAG_NAME_MAX_LENGTH}
        readOnly={atLimit}
        disabled={disabled}
        value={query}
        placeholder={placeholder ?? (allowCreate ? t("placeholder") : t("searchPlaceholder"))}
        onChange={(event) => {
          setQuery(event.target.value);
          setOpen(true);
          setActive(0);
        }}
        onClick={() => setOpen(true)}
        onBlur={() => {
          setOpen(false);
          // A typed name left in the field is kept, not lost, when the owner moves on or presses Simpan: the
          // matching tag, else a new one. Options keep focus on press, so picking one never lands here.
          if (!allowCreate || !needle || atLimit) return;
          const exact = options.find((option) => option.kind === "tag" && normalizeTagName(option.tag.name) === needle && !option.tag.archived);
          const create = options.find((option) => option.kind === "create");
          const choice = exact ?? create;
          if (choice) pick(choice);
          else setQuery("");
        }}
        onKeyDown={onKeyDown}
        className="field-control"
      />
      <ul id={`${id}-list`} role="listbox" aria-label={label} hidden={!listOpen} className="tag-options">
        {listOpen && options.map((option, index) => (
          <li
            key={option.kind === "tag" ? option.tag.id : "create"}
            id={optionId(index)}
            role="option"
            aria-selected={index === activeIndex}
            className="tag-option"
            // Pressing an option keeps focus in the field, so picking does not close the list by blur first.
            onMouseDown={(event) => event.preventDefault()}
            onMouseMove={() => setActive(index)}
            onClick={() => pick(option)}
          >
            {option.kind === "tag" ? (
              <>
                <Icon name="tag" className="h-3 w-3" />
                <span className="lbl">{option.tag.name}</span>
                {option.tag.archived && <span className="t-meta">{t("archived")}</span>}
              </>
            ) : (
              <>
                <Icon name="plus" className="h-3 w-3" />
                <span className="lbl">{t("create", { name: option.name })}</span>
              </>
            )}
          </li>
        ))}
      </ul>
      {noMatch && <p className="hint">{t("noMatch")}</p>}
      {error && <p id={errorId} className="err" role="alert">{error}</p>}
      {hint && <p id={hintId} className="hint">{hint}</p>}
      <p className="sr-only" role="status" aria-live="polite">{announcement}</p>
    </div>
  );
}
