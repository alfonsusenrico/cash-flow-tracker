export const queryKeys = {
  accounts: ["accounts"] as const,
  categories: ["categories"] as const,
  categoryManagement: ["categories", "management"] as const,
  goals: ["goals"] as const,
  obligations: ["obligations"] as const,
  pulse: ["pulse"] as const,
  insights: ["insights"] as const,
  transactions: {
    all: ["transactions"] as const,
    ledger: (params?: string) => ["transactions", "ledger", params ?? ""] as const,
    recent: (from: string) => ["transactions", "recent", from] as const,
    detail: (id: string) => ["transactions", "detail", id] as const,
  },
  dashboard: {
    all: ["dashboard"] as const,
    overview: (timeframe?: string, offset?: number) =>
      ["dashboard", "overview", timeframe ?? "cycle", offset ?? 0] as const,
    analytics: (timeframe?: string, offset?: number) =>
      ["dashboard", "analytics", timeframe ?? "cycle", offset ?? 0] as const,
    netWorth: ["dashboard", "net-worth"] as const,
    cycleSummary: ["dashboard", "cycle-summary"] as const,
    periodComparison: (basis: string, offset: number) => ["dashboard", "period-comparison", basis, offset] as const,
    cashFlowHistory: (basis: string) => ["dashboard", "cash-flow-history", basis] as const,
    recurringCharges: ["dashboard", "recurring-charges"] as const,
    questionRate: (weeks: number) => ["dashboard", "question-rate", weeks] as const,
  },
  recurring: {
    all: ["recurring"] as const,
    payroll: ["recurring", "payroll"] as const,
    pending: ["recurring", "pending"] as const,
  },
  auth: {
    all: ["auth"] as const,
    me: ["auth", "me"] as const,
    apiKey: ["auth", "api-key"] as const,
    currencyRates: ["auth", "currency-rates"] as const,
  },
  review: {
    all: ["review"] as const,
    queue: ["review", "queue"] as const,
    summary: ["review", "summary"] as const,
    suggestions: ["review", "suggestions"] as const,
    suggestionRun: ["review", "suggestions", "run"] as const,
  },
  reports: {
    all: ["reports"] as const,
    monthly: (basis: string, startDate: string | null) => ["reports", "monthly", basis, startDate ?? "latest"] as const,
    periods: (basis: string) => ["reports", "periods", basis] as const,
    latest: ["reports", "latest"] as const,
  },
  classificationGaps: (from: string | null, to: string | null) => ["classification", "gaps", from ?? "", to ?? ""] as const,
  notificationAliases: ["notification-aliases"] as const,
  senderAliases: ["sender-aliases"] as const,
  devices: ["devices"] as const,
  habits: {
    list: ["habits", "list"] as const,
  },
} as const;
