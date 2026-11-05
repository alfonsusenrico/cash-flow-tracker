"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";
import type { AccountLayout, LayoutGrouping } from "./types";

const GROUPING_KEY = "rekening-grouping";

function readGrouping(): LayoutGrouping {
  try {
    return window.localStorage.getItem(GROUPING_KEY) === "rekening" ? "rekening" : "use";
  } catch {
    // Storage can be blocked (private windows, cleared site data); the page then opens on the default.
    return "use";
  }
}

/** The grouping choice, remembered per browser as a convenience (OD-C); the order itself lives on the server. */
export function useGrouping(): [LayoutGrouping, (next: LayoutGrouping) => void] {
  const [grouping, setGrouping] = useState<LayoutGrouping>("use");
  useEffect(() => {
    // Read after mounting so the server render and the first client render agree.
    setGrouping(readGrouping());
  }, []);
  const choose = (next: LayoutGrouping) => {
    setGrouping(next);
    try {
      window.localStorage.setItem(GROUPING_KEY, next);
    } catch {
      // Without storage the choice lasts for this visit only.
    }
  };
  return [grouping, choose];
}

function isLayout(value: unknown): value is AccountLayout {
  return typeof value === "object" && value !== null && Array.isArray((value as AccountLayout).sections);
}

/**
 * The saved order of one grouping (D5). A save shows at once; when it fails, the order returns to the last saved
 * one and `failed` is set so the page can say "Urutan gagal disimpan".
 */
export function useWalletLayout(grouping: LayoutGrouping) {
  const qc = useQueryClient();
  const query = useQuery<AccountLayout | null>({
    queryKey: queryKeys.accountLayout(grouping),
    queryFn: async () => {
      const layout = await api.get<unknown>(`/accounts/layout?grouping=${grouping}`);
      return isLayout(layout) ? layout : null;
    },
  });
  const [pending, setPending] = useState<AccountLayout | null>(null);
  const [failed, setFailed] = useState(false);
  const mutation = useMutation({
    mutationFn: (layout: AccountLayout) => api.put<unknown>(`/accounts/layout?grouping=${layout.grouping}`, layout),
    onSuccess: (saved, sent) => {
      qc.setQueryData(queryKeys.accountLayout(sent.grouping), isLayout(saved) ? saved : sent);
      setPending((current) => (current === sent ? null : current));
    },
    onError: (_error, sent) => {
      setPending((current) => (current === sent ? null : current));
      setFailed(true);
    },
  });
  const current = pending?.grouping === grouping ? pending : query.data ?? null;
  const save = (layout: AccountLayout) => {
    setFailed(false);
    setPending(layout);
    mutation.mutate(layout);
  };
  return { layout: current, save, failed, saving: mutation.isPending };
}
