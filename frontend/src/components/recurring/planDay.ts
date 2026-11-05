const DAY = new Intl.DateTimeFormat("id-ID", { day: "numeric", month: "short", timeZone: "UTC" });

/** "3 Nov" for a plan's calendar date (`YYYY-MM-DD`), like the rest of the app. */
export function planDay(date: string): string {
  return DAY.format(new Date(`${date}T00:00:00Z`));
}
