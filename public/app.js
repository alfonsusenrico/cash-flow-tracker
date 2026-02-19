const $ = (id) => document.getElementById(id);
const pad2 = (n) => String(n).padStart(2, "0");

const state = {
  me: null,
  accounts: [],
  primary_account_id: null,
  scope: "all",
  account_id: null,
  from: null,
  to: null,
  rows: [],
  search_rows: [],
  active_tab: "summary",
  overview_accounts: [],
  overview_range: null,
  summary_month: "",
  summary_loading: false,
  summary_stale: true,
  analysis_loading: false,
  analysis_stale: true,
  analysis_data: null,
  analysis_budget_shift: null,
  analysis_budget_mode: "normal",
  total_asset: 0,
  summary_total_asset: 0,
  analysis_total_asset: 0,
  payday_day: null,
  payday_source: "default",
  payday_default: null,
  payday_override: null,
  search_query: "",
  include_switch_all: false,
  hide_balances: false,
  sort_order: "desc",
  page_size: 25,
  default_offset: 0,
  default_has_more: true,
  default_loading: false,
  default_stale: false,
  search_offset: 0,
  search_has_more: true,
  search_loading: false,
  ledger_loaded: false,
  suppress_ledger_refresh: false,
  fx_rate: null,
  fx_updated_at: null,
  fx_loading: false,
  currency: "IDR",
  editing_tx_id: null,
  editing_transfer_id: null,
  budgets_by_account: {},
};

const api = {
  async get(url, options = {}) {
    const r = await fetch(url, { credentials: "include", signal: options.signal });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Request failed");
    return data;
  },
  async post(url, body) {
    const r = await fetch(url, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Request failed");
    return data;
  },
  async put(url, body) {
    const r = await fetch(url, {
      method: "PUT",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Request failed");
    return data;
  },
  async del(url) {
    const r = await fetch(url, { method: "DELETE", credentials: "include" });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Request failed");
    return data;
  },
  async postForm(url, formData) {
    const r = await fetch(url, {
      method: "POST",
      credentials: "include",
      body: formData,
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.detail || "Request failed");
    return data;
  },
};

const ledgerLoadingState = {
  startedAt: 0,
  hideTimer: null,
};

let searchAbortController = null;
let searchRequestToken = 0;

const setLedgerLoading = (isLoading) => {
  const el = $("ledgerLoading");
  if (!el) return;
  if (isLoading) {
    if (ledgerLoadingState.hideTimer) {
      clearTimeout(ledgerLoadingState.hideTimer);
      ledgerLoadingState.hideTimer = null;
    }
    ledgerLoadingState.startedAt = Date.now();
    el.hidden = false;
    el.setAttribute("aria-hidden", "false");
    el.setAttribute("aria-busy", "true");
    return;
  }
  const elapsed = Date.now() - ledgerLoadingState.startedAt;
  const minMs = 350;
  const delay = Math.max(0, minMs - elapsed);
  const finish = () => {
    el.hidden = true;
    el.setAttribute("aria-hidden", "true");
    el.setAttribute("aria-busy", "false");
  };
  if (delay) {
    ledgerLoadingState.hideTimer = setTimeout(finish, delay);
  } else {
    finish();
  }
};

const setLedgerError = (message) => {
  const el = $("ledgerMsg");
  if (!el) return;
  if (message) {
    el.textContent = message;
    el.hidden = false;
  } else {
    el.textContent = "";
    el.hidden = true;
  }
};

const fmtIDR = (n) => (n || 0).toLocaleString("id-ID");
const fmtIDRCurrency = (n) => `Rp ${fmtIDR(n)}`;
const fmtUSD = (n) =>
  `$${Number(n || 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
const fmtBytes = (value) => {
  const size = Number(value || 0);
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(2)} MB`;
};

const fmtMoney = (n) => {
  if (state.currency === "USD") {
    if (!state.fx_rate) return "$-";
    return fmtUSD(Number(n || 0) * state.fx_rate);
  }
  return fmtIDRCurrency(n);
};

const displayMoney = (n) => (state.hide_balances ? "***" : fmtMoney(n));

const isMobile = () => window.matchMedia("(max-width: 980px)").matches;

const getLedgerScrollElement = () => {
  const ledgerPanel = $("tab-ledger");
  const ledgerWrap = $("ledgerTableScroll");
  if (isMobile()) return ledgerPanel || ledgerWrap;
  return ledgerWrap || ledgerPanel;
};

const getAnalysisScrollElement = () => $("tab-analysis") || $("analysisBody");

const syncMobileScrollState = () => {
  if (!isMobile()) {
    document.body.classList.remove("summary-scrolled", "analysis-scrolled", "ledger-scrolled");
    return;
  }
  if (state.active_tab === "summary") {
    const summary = $("tab-summary");
    document.body.classList.toggle("summary-scrolled", summary && summary.scrollTop > 8);
    document.body.classList.remove("ledger-scrolled", "analysis-scrolled");
  } else if (state.active_tab === "analysis") {
    const analysis = getAnalysisScrollElement();
    document.body.classList.toggle("analysis-scrolled", analysis && analysis.scrollTop > 8);
    document.body.classList.remove("summary-scrolled", "ledger-scrolled");
  } else {
    const ledger = $("tab-ledger");
    document.body.classList.toggle("ledger-scrolled", ledger && ledger.scrollTop > 8);
    document.body.classList.remove("summary-scrolled", "analysis-scrolled");
  }
};

const bindMobileScrollState = () => {
  const summary = $("tab-summary");
  const analysis = getAnalysisScrollElement();
  const ledger = $("tab-ledger");
  const handler = () => syncMobileScrollState();
  if (summary) summary.addEventListener("scroll", handler);
  if (analysis) analysis.addEventListener("scroll", handler);
  if (ledger) ledger.addEventListener("scroll", handler);
  window.addEventListener("resize", handler);
  syncMobileScrollState();
};

const parseAmount = (value) => {
  const cleaned = String(value || "").replace(/[^\d]/g, "");
  return cleaned ? Number(cleaned) : 0;
};

const formatMoneyInput = (input) => {
  if (!input) return;
  const digits = String(input.value || "").replace(/[^\d]/g, "");
  if (!digits) {
    input.value = "";
    input.dataset.rawValue = "";
    return;
  }
  const normalized = digits.replace(/^0+(?=\d)/, "");
  const formatted = normalized.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  input.value = formatted;
  input.dataset.rawValue = normalized;
  if (document.activeElement === input) {
    const end = input.value.length;
    input.setSelectionRange(end, end);
  }
};

const bindMoneyInput = (input) => {
  if (!input) return;
  const handler = () => formatMoneyInput(input);
  input.addEventListener("input", handler);
  input.addEventListener("blur", handler);
};

const bindMoneyInputs = () => {
  document.querySelectorAll(".money-input").forEach((input) => bindMoneyInput(input));
};

const bindHorizontalWheelScroll = (el) => {
  if (!el) return;
  el.addEventListener(
    "wheel",
    (e) => {
      if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) return;
      if (el.scrollWidth <= el.clientWidth) return;
      el.scrollLeft += e.deltaY;
      e.preventDefault();
    },
    { passive: false }
  );
};

const bindHorizontalDragScroll = (el) => {
  if (!el || !("PointerEvent" in window)) return;
  let isDragging = false;
  let startX = 0;
  let startScroll = 0;
  let pointerId = null;
  let lastX = 0;
  let lastTime = 0;
  let velocity = 0;
  let rafId = null;
  let lastFrameTime = 0;
  const stopMomentum = () => {
    if (rafId) cancelAnimationFrame(rafId);
    rafId = null;
    lastFrameTime = 0;
  };
  const stepMomentum = (t) => {
    if (!lastFrameTime) lastFrameTime = t;
    const dt = t - lastFrameTime;
    lastFrameTime = t;
    el.scrollLeft -= velocity * dt;
    velocity *= 0.95;
    if (Math.abs(velocity) > 0.02) {
      rafId = requestAnimationFrame(stepMomentum);
    } else {
      stopMomentum();
    }
  };
  const onPointerDown = (e) => {
    if (e.pointerType === "touch") return;
    if (typeof e.button === "number" && e.button !== 0) return;
    stopMomentum();
    isDragging = true;
    startX = e.clientX;
    startScroll = el.scrollLeft;
    lastX = e.clientX;
    lastTime = performance.now();
    velocity = 0;
    pointerId = e.pointerId;
    el.classList.add("is-dragging");
    el.setPointerCapture(pointerId);
  };
  const onPointerMove = (e) => {
    if (!isDragging) return;
    const dx = e.clientX - startX;
    el.scrollLeft = startScroll - dx;
    const now = performance.now();
    const dt = Math.max(1, now - lastTime);
    const nextVelocity = (e.clientX - lastX) / dt;
    velocity = velocity * 0.7 + nextVelocity * 0.3;
    lastX = e.clientX;
    lastTime = now;
    e.preventDefault();
  };
  const endDrag = () => {
    if (!isDragging) return;
    isDragging = false;
    el.classList.remove("is-dragging");
    if (pointerId !== null) {
      try {
        el.releasePointerCapture(pointerId);
      } catch {
        // ignore
      }
    }
    pointerId = null;
    if (Math.abs(velocity) > 0.02) {
      rafId = requestAnimationFrame(stepMomentum);
    }
  };
  el.addEventListener("pointerdown", onPointerDown);
  el.addEventListener("pointermove", onPointerMove);
  el.addEventListener("pointerup", endDrag);
  el.addEventListener("pointerleave", endDrag);
  el.addEventListener("pointercancel", endDrag);
};

const isoToLocalDisplay = (isoZ) => {
  // isoZ like 2025-12-29T10:00:00Z
  const d = new Date(isoZ);
  if (isNaN(d)) return isoZ;
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
};

const todayYMD = () => new Date().toISOString().slice(0, 10);

const formatDisplayDate = (d) => {
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}`;
};

const ymdToDate = (ymd) => {
  if (!ymd) return null;
  const [y, m, d] = String(ymd).split("-").map(Number);
  if (!y || !m || !d) return null;
  return new Date(y, m - 1, d);
};
const formatLedgerDayLabel = (ymd) => {
  const d = ymdToDate(ymd);
  if (!d) return ymd || "";
  const nowYear = new Date().getFullYear();
  const options = { weekday: "short", day: "2-digit", month: "short" };
  if (d.getFullYear() !== nowYear) {
    options.year = "numeric";
  }
  return d.toLocaleDateString("en-US", options);
};

const formatDayParts = (ymd) => {
  const d = ymdToDate(ymd);
  if (!d) return { label: ymd || "", weekday: "" };
  const day = String(d.getDate()).padStart(2, "0");
  const month = d.toLocaleDateString("en-US", { month: "short" });
  const weekday = d.toLocaleDateString("en-US", { weekday: "short" });
  return { label: `${day} ${month}`, weekday };
};
const dateToYMD = (d) => {
  if (!d) return "";
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
};

const isoToLocalYMD = (isoZ) => {
  const d = new Date(isoZ);
  if (isNaN(d)) return "";
  return dateToYMD(d);
};

const isoToLocalTimeWithSeconds = (isoZ) => {
  const d = new Date(isoZ);
  if (isNaN(d)) return "";
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
};

const isoToLocalTime = (isoZ) => {
  const withSeconds = isoToLocalTimeWithSeconds(isoZ);
  return withSeconds ? withSeconds.slice(0, 5) : "";
};

const nowTimeWithSeconds = () => {
  const d = new Date();
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
};

const ymdTimeToIso = (ymd, timeWithSeconds) => {
  if (!ymd) return "";
  const [y, m, d] = String(ymd).split("-").map(Number);
  if (!y || !m || !d) return "";
  const [hh, mm, ss] = String(timeWithSeconds || "00:00:00").split(":").map(Number);
  const local = new Date(y, m - 1, d, hh || 0, mm || 0, ss || 0);
  return local.toISOString();
};

const formatYmdDisplay = (ymd) => {
  const d = ymdToDate(ymd);
  return d ? formatDisplayDate(d) : "";
};

const buildDateList = (from, to) => {
  const start = ymdToDate(from);
  const end = ymdToDate(to);
  if (!start || !end) return [];
  const dates = [];
  const cursor = new Date(start);
  while (cursor <= end) {
    dates.push(dateToYMD(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }
  return dates;
};

const clampYmdToToday = (ymd) => {
  const today = dateToYMD(new Date());
  if (!ymd) return today;
  return ymd > today ? today : ymd;
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");

const minusDaysYMD = (days) => {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
};

const formatRangeText = (from, to) => {
  const fmtDate = (ymd) => (ymd ? ymd.split("-").reverse().join("/") : "");
  return `${fmtDate(from)} - ${fmtDate(to)}`;
};

const formatRangeLabel = (from, to) => {
  const fmtDate = (ymd) => (ymd ? ymd.split("-").reverse().join("/") : "");
  return `
    <span class="range-date" data-date-target="from" role="button" tabindex="0" aria-label="Edit from date">${escapeHtml(fmtDate(from))}</span>
    <span class="range-separator">-</span>
    <span class="range-date" data-date-target="to" role="button" tabindex="0" aria-label="Edit to date">${escapeHtml(fmtDate(to))}</span>
  `;
};

function renderRangeDisplay() {
  const rangeEl = $("ledgerRangeDisplay");
  const rangeTextEl = $("ledgerRangeText");
  if (rangeTextEl) rangeTextEl.innerHTML = formatRangeLabel(state.from, state.to);
  if (!rangeEl) return;
  if (isMobile()) {
    rangeEl.innerHTML = formatRangeLabel(state.from, state.to);
    return;
  }
  rangeEl.textContent = formatRangeText(state.from, state.to);
}

const clearLedgerSearch = () => {
  if (searchAbortController) {
    searchAbortController.abort();
    searchAbortController = null;
  }
  state.search_query = "";
  state.search_rows = [];
  state.search_offset = 0;
  state.search_has_more = true;
  state.search_loading = false;
  const ledgerSearch = $("ledgerSearch");
  if (ledgerSearch) ledgerSearch.value = "";
};

const applyLedgerRange = (from, to) => {
  if (!from || !to) return;
  state.from = from;
  state.to = to;
  const fromInput = $("fromDate");
  const toInput = $("toDate");
  if (fromInput) fromInput.value = state.from;
  if (toInput) toInput.value = state.to;
  renderRangeDisplay();
};

const setLedgerScopeAll = () => {
  state.scope = "all";
  state.account_id = null;
  const mobileSelect = $("mobileAccountSelect");
  if (mobileSelect) mobileSelect.value = "all";
  const titleSelect = $("ledgerTitleSelect");
  if (titleSelect) titleSelect.value = "all";
};

const navigateToLedgerDay = (ymd) => {
  if (!ymd) return;
  const day = clampYmdToToday(ymd);
  if (!day) return;
  cancelLedgerReload();
  clearLedgerSearch();
  setLedgerScopeAll();
  applyLedgerRange(day, day);
  if (state.active_tab === "ledger") {
    reloadLedgerWithDefaultStale().catch(console.error);
  } else {
    setActiveTab("ledger");
  }
};

const FX_STORAGE_KEY = "fx_rate_idr_usd";

function loadFxCache() {
  try {
    const raw = localStorage.getItem(FX_STORAGE_KEY);
    if (!raw) return;
    const data = JSON.parse(raw);
    if (!data?.rate || !data?.date) return;
    const today = new Date().toISOString().slice(0, 10);
    if (data.date === today) {
      state.fx_rate = Number(data.rate);
      state.fx_updated_at = data.date;
    }
  } catch {
    // ignore cache errors
  }
}

function saveFxCache(rate) {
  const date = new Date().toISOString().slice(0, 10);
  state.fx_rate = Number(rate);
  state.fx_updated_at = date;
  localStorage.setItem(FX_STORAGE_KEY, JSON.stringify({ rate: state.fx_rate, date }));
}

function isFxStale() {
  if (!state.fx_rate || !state.fx_updated_at) return true;
  return state.fx_updated_at !== todayYMD();
}

async function fetchFxRate() {
  const res = await fetch("https://open.er-api.com/v6/latest/IDR");
  if (!res.ok) throw new Error("Rate unavailable");
  const data = await res.json();
  const rate = Number(data?.rates?.USD || 0);
  if (!rate) throw new Error("Rate missing");
  saveFxCache(rate);
}

function updateFxUI() {
  document.querySelectorAll(".seg-btn").forEach((btn) => {
    if (btn.dataset.currency) {
      btn.classList.toggle("active", btn.dataset.currency === state.currency);
    }
  });
}

function updateBudgetShiftModeUI() {
  document.querySelectorAll("#budgetShiftModeToggle .seg-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.budgetMode === state.analysis_budget_mode);
  });
}

async function loadFxOnStartup() {
  if (!isFxStale()) {
    updateFxUI();
    return;
  }
  state.fx_loading = true;
  updateFxUI();
  try {
    await fetchFxRate();
  } catch (err) {
    console.error(err);
  } finally {
    state.fx_loading = false;
    updateFxUI();
    if (state.currency === "USD") {
      renderLedger(activeRows());
      renderSummary();
      renderAnalysis();
    }
  }
}

function renderAccounts() {
  const opts = state.accounts
    .map((a) => `<option value="${a.account_id}">${escapeHtml(a.account_name)}</option>`)
    .join("");

  $("txAccountSelect").innerHTML = opts;

  $("accountsBody").innerHTML = state.accounts
    .map((a) => {
      const accountName = escapeHtml(a.account_name);
      const deleteBtn = `<button class="btn small danger" data-action="delete" data-id="${a.account_id}">Delete</button>`;
      return `<tr>
        <td>${accountName}</td>
        <td class="num">
          <div class="actions">
            <button class="btn small" data-action="edit" data-id="${a.account_id}">Edit</button>
            ${deleteBtn}
          </div>
        </td>
      </tr>`;
    })
    .join("");

  const filterAccounts = state.accounts.slice();

  const mobileSelect = $("mobileAccountSelect");
  if (mobileSelect) {
    const mobileOptions = [
      `<option value="all">All</option>`,
      ...filterAccounts.map((a) => `<option value="${a.account_id}">${escapeHtml(a.account_name)}</option>`),
    ];
    mobileSelect.innerHTML = mobileOptions.join("");
    mobileSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
  }

  const titleSelect = $("ledgerTitleSelect");
  if (titleSelect) {
    const titleOptions = [
      `<option value="all">All</option>`,
      ...filterAccounts.map((a) => `<option value="${a.account_id}">${escapeHtml(a.account_name)}</option>`),
    ];
    titleSelect.innerHTML = titleOptions.join("");
    titleSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
  }

  updateSwitchTargets();
  updateExportAccounts();
}

const getBudgetMonth = () => state.summary_month || currentMonthYM();

function syncAccountBudgetInput(accountId) {
  const input = $("accountBudget");
  if (!input) return;
  if (!accountId) {
    input.value = "";
    input.dataset.rawValue = "";
    return;
  }
  const budget = state.budgets_by_account[accountId];
  if (!budget) {
    input.value = "";
    input.dataset.rawValue = "";
    return;
  }
  input.value = fmtNumber(budget.amount);
  input.dataset.rawValue = String(budget.amount);
}

function updateTotals(summary) {
  state.total_asset = Number(summary?.total_asset || 0);
  state.summary_accounts = summary?.accounts || [];
}

const updateSummaryTotalAsset = (value) => {
  state.summary_total_asset = Number(value || 0);
  const el = $("summaryTotalAsset");
  if (el) el.textContent = displayMoney(state.summary_total_asset || 0);
};

const updateAnalysisTotalAsset = (value) => {
  state.analysis_total_asset = Number(value || 0);
  const el = $("analysisTotalAsset");
  if (el) el.textContent = displayMoney(state.analysis_total_asset || 0);
};

const currentMonthYM = () => {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  return `${now.getFullYear()}-${month}`;
};

const fmtNumber = (n) => Number(n || 0).toLocaleString("id-ID");

const clampMonthYM = (ym) => {
  const max = currentMonthYM();
  if (!ym) return max;
  return ym > max ? max : ym;
};

const parseYM = (ym) => {
  if (!ym) return null;
  const [year, month] = String(ym).split("-").map((v) => Number(v));
  if (!year || !month) return null;
  return { year, month };
};

const formatMonthLabel = (ym) => {
  if (!ym) return "";
  const [year, month] = ym.split("-").map((v) => Number(v));
  if (!year || !month) return ym;
  const date = new Date(year, month - 1, 1);
  return date.toLocaleString("en-US", { month: "long", year: "numeric" });
};


const updateSummaryMonthText = () => {
  const value = state.summary_month || currentMonthYM();
  const labels = [$("summaryMonthText"), $("analysisMonthText")];
  labels.forEach((el) => {
    if (el) el.textContent = formatMonthLabel(value);
  });
};

const updatePaydayText = () => {
  const day = state.payday_day;
  const suffix = state.payday_source === "override" ? " (custom)" : "";
  const label = day ? `Payday: ${day}${suffix}` : "Payday";
  const buttons = [$("paydayBtn"), $("analysisPaydayBtn")];
  buttons.forEach((btn) => {
    if (btn) btn.textContent = label;
  });
};

const applyPaydayInfo = (payday) => {
  if (!payday) return;
  const dayVal = Number(payday.day || 0);
  state.payday_day = Number.isFinite(dayVal) && dayVal > 0 ? dayVal : null;
  state.payday_source = payday.source || "default";
  state.payday_default = payday.default_day ?? null;
  state.payday_override = payday.override_day ?? null;
  updatePaydayText();
};

const renderSummary = () => {
  const cards = $("summaryCards");
  const rangeText = $("summaryRangeText");
  const msg = $("summaryMsg");
  if (msg) msg.textContent = "";
  updateSummaryTotalAsset(state.summary_total_asset);
  if (rangeText) {
    const fromDate = ymdToDate(state.overview_range?.from);
    const toDate = ymdToDate(state.overview_range?.to);
    rangeText.textContent = fromDate && toDate
      ? `${formatDisplayDate(fromDate)} - ${formatDisplayDate(toDate)}`
      : "Range unavailable.";
  }
  if (!cards) return;
  if (!state.overview_accounts.length) {
    cards.innerHTML = `<div class="summary-empty">No accounts available for this period.</div>`;
    return;
  }
  cards.innerHTML = state.overview_accounts
    .map((acc) => {
      const totalIn = Number(acc.total_in || 0);
      const totalOut = Number(acc.total_out || 0);
      const max = Math.max(totalIn, totalOut, 1);
      const inPct = Math.min(100, (totalIn / max) * 100);
      const outPct = Math.min(100, (totalOut / max) * 100);
      const budgetClass = acc.budget_status ? ` budget-${acc.budget_status}` : "";
      const cardTarget = acc.account_id;
      const cardLabel = `View ledger for ${acc.account_name}`;
      const hasBudget = acc.budget != null;
      const budgetValue = hasBudget ? displayMoney(acc.budget) : "-";
      const balanceRow = `<div class="summary-balance-row">
          <div>
            <div class="summary-balance-label">Last Month Balance</div>
            <div class="summary-balance">${displayMoney(acc.starting_balance || 0)}</div>
          </div>
          <div>
            <div class="summary-balance-label">Current Balance</div>
            <div class="summary-balance">${displayMoney(acc.current_balance || 0)}</div>
          </div>
          <div>
            <div class="summary-balance-label">Limit</div>
            <div class="summary-balance">${budgetValue}</div>
          </div>
        </div>`;
      const editBtn = `<button class="card-edit" data-action="edit" data-id="${acc.account_id}" aria-label="Edit account ${escapeHtml(acc.account_name)}" title="Edit account">
            <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
              <path d="M4 20h4l10-10-4-4L4 16v4z" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/>
              <path d="M13.5 6.5l4 4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>
          </button>`;
      return `<div class="summary-card${budgetClass}" data-account-id="${cardTarget}" role="button" tabindex="0" aria-label="${escapeHtml(cardLabel)}" style="--in:${inPct}%; --out:${outPct}%;">
        <div class="summary-card-head">
          <div class="summary-card-title">${escapeHtml(acc.account_name)}</div>
          <div class="summary-card-actions">
            ${editBtn}
          </div>
        </div>
        ${balanceRow}
        <div class="summary-io">
          <div class="io-item">
            <span class="io-label">Transaction In</span>
            <span class="io-value" style="color:#10b981;">${displayMoney(totalIn)}</span>
          </div>
          <div class="io-item">
            <span class="io-label">Transaction Out</span>
            <span class="io-value" style="color:#ef4444;">${displayMoney(totalOut)}</span>
          </div>
        </div>
        <div class="summary-bars">
          <span class="in"></span>
          <span class="out"></span>
        </div>
      </div>`;
    })
    .join("");
};

const loadSummary = async ({ force = false } = {}) => {
  if (state.summary_loading) return;
  if (!force && !state.summary_stale && state.overview_accounts.length) {
    renderSummary();
    return;
  }
  const msg = $("summaryMsg");
  if (msg) msg.textContent = "Loading summary...";
  state.summary_loading = true;
  try {
    const month = state.summary_month || currentMonthYM();
    if (!state.summary_month) state.summary_month = month;
    const res = await api.get(`/api/summary?month=${encodeURIComponent(month)}`);
    applyPaydayInfo(res.payday);
    updateSummaryTotalAsset(res.total_asset || 0);
    state.overview_accounts = res.accounts || [];
    state.budgets_by_account = {};
    state.overview_accounts.forEach((acc) => {
      if (acc && acc.account_id && acc.budget != null) {
        state.budgets_by_account[acc.account_id] = {
          amount: acc.budget,
          budget_id: acc.budget_id || null,
        };
      }
    });
    state.overview_range = res.range || null;
    state.summary_stale = false;
    renderSummary();
    const currentAccountId = $("accountId")?.value;
    if (currentAccountId) {
      syncAccountBudgetInput(currentAccountId);
    }
  } catch (err) {
    if (msg) msg.textContent = err.message || "Failed to load summary";
  } finally {
    state.summary_loading = false;
  }
};

const renderAnalysis = () => {
  const data = state.analysis_data || {};
  const totals = data.totals || { total_in: 0, total_out: 0, net: 0 };
  const rangeText = $("analysisRangeText");
  const msg = $("analysisMsg");
  if (msg) msg.textContent = "";
  updateAnalysisTotalAsset(state.analysis_total_asset);
  if (rangeText) {
    const fromDate = ymdToDate(data.range?.from);
    const toDate = ymdToDate(data.range?.to);
    rangeText.textContent = fromDate && toDate
      ? `${formatDisplayDate(fromDate)} - ${formatDisplayDate(toDate)}`
      : "Range unavailable.";
  }

  const totalsEl = $("analysisTotals");
  if (totalsEl) {
    const net = Number(totals.net || 0);
    const netClass = net < 0 ? "neg" : "pos";
    totalsEl.innerHTML = `
      <div class="analysis-card">
        <div class="analysis-card-label">Transaction In</div>
        <div class="analysis-card-value in">${displayMoney(totals.total_in || 0)}</div>
      </div>
      <div class="analysis-card">
        <div class="analysis-card-label">Transaction Out</div>
        <div class="analysis-card-value out">${displayMoney(totals.total_out || 0)}</div>
      </div>
      <div class="analysis-card">
        <div class="analysis-card-label">Transaction Net</div>
        <div class="analysis-card-value ${netClass}">${displayMoney(net)}</div>
      </div>
    `;
  }

  const renderDailyWeeks = (rows, weeklyRows, el) => {
    if (!el) return;
    if (!rows.length) {
      el.innerHTML = `<div class="analysis-empty">No activity in this period.</div>`;
      return;
    }
    const weeks = [];
    for (let i = 0; i < rows.length; i += 7) {
      weeks.push(rows.slice(i, i + 7));
    }
    el.innerHTML = weeks
      .map((week, idx) => {
        const summary = Array.isArray(weeklyRows) ? weeklyRows[idx] : null;
        const fallbackTotals = week.reduce(
          (acc, day) => {
            acc.totalIn += Number(day.total_in || 0);
            acc.totalOut += Number(day.total_out || 0);
            return acc;
          },
          { totalIn: 0, totalOut: 0 }
        );
        const totalIn = Number(summary?.total_in ?? fallbackTotals.totalIn);
        const totalOut = Number(summary?.total_out ?? fallbackTotals.totalOut);
        const net = totalIn - totalOut;
        const netClass = net < 0 ? "neg" : "pos";
        const cards = week
          .map((r) => {
            const totalIn = Number(r.total_in || 0);
            const totalOut = Number(r.total_out || 0);
            const net = totalIn - totalOut;
            const netClass = net < 0 ? "neg" : "pos";
            const parts = formatDayParts(r.date);
            const weekday = escapeHtml(parts.weekday);
            const label = escapeHtml(parts.label);
            const ariaLabel = escapeHtml(`View transactions for ${formatYmdDisplay(r.date)}`);
            return `
              <div class="analysis-day-card" role="button" tabindex="0" data-date="${escapeHtml(r.date)}" aria-label="${ariaLabel}">
                <div class="analysis-day-top">
                  <span class="analysis-day-weekday">${weekday}</span>
                  <span class="analysis-day-date">${label}</span>
                </div>
                <div class="analysis-day-net ${netClass}">${displayMoney(net)}</div>
                <div class="analysis-day-sub">
                  <span class="analysis-day-in">In ${displayMoney(totalIn)}</span>
                  <span class="analysis-day-out">Out ${displayMoney(totalOut)}</span>
                </div>
              </div>
            `;
          })
          .join("");
        return `
          <div class="analysis-week-block">
            <div class="analysis-week-summary">
              <div class="analysis-week-title">Week ${idx + 1}</div>
              <div class="analysis-week-metrics">
                <span class="analysis-week-net ${netClass}">Net ${displayMoney(net)}</span>
                <span class="analysis-week-in">In ${displayMoney(totalIn)}</span>
                <span class="analysis-week-out">Out ${displayMoney(totalOut)}</span>
              </div>
            </div>
            <div class="analysis-week-row">${cards}</div>
          </div>
        `;
      })
      .join("");
  };

  const renderFlowCards = (rows, el, { labelFn, metaFn } = {}) => {
    if (!el) return;
    if (!rows.length) {
      el.innerHTML = `<div class="analysis-empty">No activity in this period.</div>`;
      return;
    }
    const maxVal = Math.max(
      1,
      ...rows.map((r) => Math.max(Number(r.total_in || 0), Number(r.total_out || 0)))
    );
    el.innerHTML = rows
      .map((r, idx) => {
        const totalIn = Number(r.total_in || 0);
        const totalOut = Number(r.total_out || 0);
        const net = totalIn - totalOut;
        const inPct = Math.min(100, Math.round((totalIn / maxVal) * 100));
        const outPct = Math.min(100, Math.round((totalOut / maxVal) * 100));
        const label = labelFn ? escapeHtml(labelFn(r, idx)) : "";
        const meta = metaFn ? escapeHtml(metaFn(r, idx)) : "";
        const metaLine = meta ? `<div class="analysis-chip-meta">${meta}</div>` : "";
        const netClass = net < 0 ? "neg" : "pos";
        return `
          <div class="analysis-chip" style="--in:${inPct}%; --out:${outPct}%;">
            <div class="analysis-chip-label">${label}</div>
            ${metaLine}
            <div class="analysis-chip-value ${netClass}">${displayMoney(net)}</div>
            <div class="analysis-chip-sub">
              <span class="analysis-in">In ${displayMoney(totalIn)}</span>
              <span class="analysis-out">Out ${displayMoney(totalOut)}</span>
            </div>
            <div class="analysis-bars">
              <span class="in"></span>
              <span class="out"></span>
            </div>
          </div>
        `;
      })
      .join("");
  };

  const dailyRows = Array.isArray(data.daily) ? data.daily : [];
  const weeklyRows = Array.isArray(data.weekly) ? data.weekly : [];
  renderDailyWeeks(dailyRows, weeklyRows, $("analysisDaily"));

  const categoriesEl = $("analysisCategories");
  const categories = Array.isArray(data.categories) ? data.categories : [];
  if (categoriesEl) {
    if (!categories.length) {
      categoriesEl.innerHTML = `<div class="analysis-empty">No category activity yet.</div>`;
    } else {
      const maxOut = Math.max(1, ...categories.map((c) => Number(c.total_out || 0)));
      categoriesEl.innerHTML = categories
        .map((c) => {
          const totalOutCat = Number(c.total_out || 0);
          const topupBase = Number(c.topup_base || 0);
          const usagePct = c.usage_pct == null ? null : Number(c.usage_pct);
          const usageText = topupBase > 0
            ? `Used ${usagePct == null ? Math.round((totalOutCat / topupBase) * 100) : usagePct}% of top-up ${displayMoney(topupBase)}`
            : "No top-up/payroll base in this cycle";
          const fill = Math.min(100, Math.round((totalOutCat / maxOut) * 100));
          const name = escapeHtml(c.account_name || "Unknown");
          return `
            <div class="analysis-row analysis-row-compact" style="--fill:${fill}%;">
              <div class="analysis-row-head">
                <span class="analysis-label">${name}</span>
                <span class="analysis-spend">${displayMoney(totalOutCat)}</span>
              </div>
              <div class="analysis-values">
                <span class="analysis-sub">${escapeHtml(usageText)}</span>
              </div>
              <div class="analysis-bar">
                <span></span>
              </div>
            </div>
          `;
        })
        .join("");
    }
  }

  const shift = state.analysis_budget_shift || {};
  if (shift.strategy) {
    state.analysis_budget_mode = String(shift.strategy);
  }
  updateBudgetShiftModeUI();
  const shiftTotals = shift.totals || {};
  const shiftAccounts = Array.isArray(shift.accounts) ? shift.accounts : [];
  const shiftEdges = Array.isArray(shift.switch_edges) ? shift.switch_edges : [];

  const shiftTotalsEl = $("analysisBudgetShiftTotals");
  if (shiftTotalsEl) {
    const gap = Number(shiftTotals.budget_gap || 0);
    const gapClass = gap > 0 ? "neg" : "pos";
    shiftTotalsEl.innerHTML = `
      <div class="analysis-card">
        <div class="analysis-card-label">Planned Budget</div>
        <div class="analysis-card-value">${displayMoney(shiftTotals.planned_budget || 0)}</div>
      </div>
      <div class="analysis-card">
        <div class="analysis-card-label">Actual Spend</div>
        <div class="analysis-card-value out">${displayMoney(shiftTotals.actual_spend || 0)}</div>
      </div>
      <div class="analysis-card">
        <div class="analysis-card-label">Budget Gap</div>
        <div class="analysis-card-value ${gapClass}">${displayMoney(gap)}</div>
      </div>
      <div class="analysis-card">
        <div class="analysis-card-label">Net Switching</div>
        <div class="analysis-card-value">${displayMoney(shiftTotals.net_switch || 0)}</div>
      </div>
    `;
  }

  const shiftBody = $("analysisBudgetShiftBody");
  if (shiftBody) {
    if (!shiftAccounts.length) {
      shiftBody.innerHTML = `<tr><td colspan="9" class="muted">No budget shift data for this month.</td></tr>`;
    } else {
      shiftBody.innerHTML = shiftAccounts
        .map((row) => {
          const budget = row.planned_budget == null ? "—" : displayMoney(row.planned_budget);
          const gap = row.budget_gap == null ? null : Number(row.budget_gap || 0);
          const gapClass = gap == null ? "" : gap > 0 ? "neg" : "pos";
          const statusLabel = String(row.status || "").replaceAll("_", " ");
          return `
            <tr>
              <td>${escapeHtml(row.account_name || "Unknown")}</td>
              <td class="num">${budget}</td>
              <td class="num">${displayMoney(row.actual_spend || 0)}</td>
              <td class="num">${displayMoney(row.switch_in || 0)}</td>
              <td class="num">${displayMoney(row.switch_out || 0)}</td>
              <td class="num ${gapClass}">${gap == null ? "—" : displayMoney(gap)}</td>
              <td class="num">${displayMoney(row.suggested_budget || 0)}</td>
              <td>${escapeHtml(String(row.profile_type || "dynamic_spending").replaceAll("_", " "))}</td>
              <td><span class="analysis-status ${escapeHtml(String(row.status || "balanced"))}">${escapeHtml(statusLabel || "balanced")}</span></td>
            </tr>
          `;
        })
        .join("");
    }
  }

  const flowEl = $("analysisSwitchFlow");
  if (flowEl) {
    if (!shiftEdges.length) {
      flowEl.innerHTML = `<div class="analysis-empty">No switching flow in this period.</div>`;
    } else {
      flowEl.innerHTML = shiftEdges
        .map((edge) => {
          const amount = Number(edge.amount || 0);
          return `
            <div class="analysis-row analysis-row-compact">
              <div class="analysis-row-head">
                <span class="analysis-label">${escapeHtml(edge.source_account_name || "Unknown")} → ${escapeHtml(edge.target_account_name || "Unknown")}</span>
                <span class="analysis-spend">${displayMoney(amount)}</span>
              </div>
              <div class="analysis-values">
                <span class="analysis-sub">Transfer pressure signal</span>
              </div>
            </div>
          `;
        })
        .join("");
    }
  }
};

const loadAnalysis = async ({ force = false } = {}) => {
  if (state.analysis_loading) return;
  if (!force && !state.analysis_stale && state.analysis_data) {
    renderAnalysis();
    return;
  }
  const msg = $("analysisMsg");
  if (msg) msg.textContent = "Loading analysis...";
  state.analysis_loading = true;
  try {
    const month = state.summary_month || currentMonthYM();
    if (!state.summary_month) state.summary_month = month;
    const mode = encodeURIComponent(state.analysis_budget_mode || "normal");
    const [res, shiftRes] = await Promise.all([
      api.get(`/api/analysis?month=${encodeURIComponent(month)}`),
      api.get(`/api/analysis/budget-shift?month=${encodeURIComponent(month)}&mode=${mode}`),
    ]);
    state.analysis_data = res || null;
    state.analysis_budget_shift = shiftRes || null;
    applyPaydayInfo(res?.payday);
    updateAnalysisTotalAsset(res?.total_asset || 0);
    state.analysis_stale = false;
    renderAnalysis();
  } catch (err) {
    if (msg) msg.textContent = err.message || "Failed to load analysis";
  } finally {
    state.analysis_loading = false;
  }
};

const markSummaryStale = () => {
  state.summary_stale = true;
  state.analysis_stale = true;
  if (state.active_tab === "summary") {
    loadSummary({ force: true }).catch(console.error);
  }
  if (state.active_tab === "analysis") {
    loadAnalysis({ force: true }).catch(console.error);
  }
};


function updateSwitchTargets(sourceOverride) {
  const switchFromSelect = $("switchFromSelect");
  const switchToSelect = $("switchToSelect");
  if (!switchToSelect) return;
  const sourceId = sourceOverride || (switchFromSelect ? switchFromSelect.value : null) || state.account_id;
  const allOptions = state.accounts.map(
    (a) => `<option value="${a.account_id}">${escapeHtml(a.account_name)}</option>`
  );
  if (switchFromSelect) {
    switchFromSelect.innerHTML = [
      `<option value="">Select account</option>`,
      ...allOptions,
    ].join("");
    if (sourceId) switchFromSelect.value = sourceId;
  }
  const targetOptions = [
    `<option value="">Select account</option>`,
    ...state.accounts
      .filter((a) => (!sourceId || a.account_id !== sourceId))
      .map((a) => `<option value="${a.account_id}">${escapeHtml(a.account_name)}</option>`),
  ];
  switchToSelect.innerHTML = targetOptions.join("");
}

function updateExportAccounts() {
  const exportAccountSelect = $("exportAccountSelect");
  if (!exportAccountSelect) return;
  const list = state.accounts
    .filter((a) => a)
    .sort((a, b) => String(a.account_name || "").localeCompare(String(b.account_name || "")));
  const options = [
    `<option value="all">All</option>`,
    ...list.map((a) => `<option value="${a.account_id}">${escapeHtml(a.account_name)}</option>`),
  ];
  exportAccountSelect.innerHTML = options.join("");
}

const getLedgerViewState = () => {
  const isAll = state.scope === "all";
  const canTransact = state.scope === "account" && state.account_id;
  const hasAccounts = Array.isArray(state.accounts) && state.accounts.length > 0;
  const showSwitch = canTransact;
  const showAdd = hasAccounts;
  const showAssetSummary = isAll;
  const showInternalToggle = isAll;
  return { isAll, showSwitch, showAdd, showAssetSummary, showInternalToggle };
};

const canTransactOnAccount = () => state.scope === "account" && !!state.account_id;
const hasAnyAccount = () => Array.isArray(state.accounts) && state.accounts.length > 0;

const requireAccountScope = (message) => {
  if (canTransactOnAccount()) return true;
  alert(message || "Select an account to manage transactions.");
  return false;
};

const requireAnyAccount = (message) => {
  if (hasAnyAccount()) return true;
  alert(message || "Create an account first.");
  return false;
};

const TREND_ICON =
  '<svg class="amount-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
  '<polyline points="23 6 13.5 15.5 8.5 10.5 1 18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>' +
  '<polyline points="17 6 23 6 23 12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>' +
  "</svg>";

const buildLedgerRow = (r, idx, isAll) => {
  const dayKey = isoToLocalYMD(r.date);
  const accCell = `<td class="account-cell">${isAll ? escapeHtml(r.account_name) : ""}</td>`;
  const dayLabel = dayKey ? formatDayParts(dayKey).label : "";
  const timeValue = isoToLocalTime(r.date);
  const timeLine = dayLabel && timeValue ? `${dayLabel} - ${timeValue}` : (dayLabel || timeValue);
  const timeLabel = escapeHtml(timeLine);
  const dateLabel = escapeHtml(isoToLocalDisplay(r.date));
  const nameLabel = escapeHtml(r.transaction_name);
  const inValue = r.debit ? `${TREND_ICON}${displayMoney(r.debit)}` : "";
  const outValue = r.credit ? `${TREND_ICON}${displayMoney(r.credit)}` : "";
  return `<tr class="ledger-row" data-tx-id="${r.transaction_id}" data-day="${dayKey || ""}" style="cursor:pointer">
    <td>${r.no ?? idx + 1}</td>
    ${accCell}
    <td class="tx-date" title="${dateLabel}">
      <span class="tx-time">${timeLabel}</span>
      <span class="tx-date-full">${dateLabel}</span>
    </td>
    <td class="tx-name" title="${nameLabel}">${nameLabel}</td>
    <td class="num amount-in">${inValue}</td>
    <td class="num amount-out">${outValue}</td>
    <td class="num"><b>${displayMoney(r.balance)}</b></td>
  </tr>`;
};

const renderLedgerChrome = () => {
  const table = $("ledgerTable");
  const { isAll, showSwitch, showAdd, showAssetSummary, showInternalToggle } = getLedgerViewState();
  const switchBtn = $("switchBtn");
  const fabSwitchOption = $("fabSwitchOption");
  const addBtn = $("addTxBtn");
  const switchToggle = $("ledgerSwitchToggle");
  const fabAddOption = document.querySelector('#fabSheet button[data-action="add"]');
  if (switchBtn) switchBtn.hidden = !showSwitch;
  if (fabSwitchOption) fabSwitchOption.hidden = !showSwitch;
  if (addBtn) addBtn.hidden = !showAdd;
  if (fabAddOption) fabAddOption.hidden = !showAdd;
  if (switchToggle) {
    switchToggle.hidden = !showInternalToggle;
    switchToggle.textContent = state.include_switch_all ? "Internal: On" : "Internal: Off";
    switchToggle.setAttribute("aria-pressed", state.include_switch_all ? "true" : "false");
    switchToggle.classList.toggle("active", !!state.include_switch_all);
  }
  updateSwitchTargets();

  const totalLabel = $("ledgerTotalLabel");
  const accountBalance = state.summary_accounts?.find((a) => a.account_id === state.account_id)?.balance ?? 0;
  if (showAssetSummary) {
    if (totalLabel) totalLabel.textContent = "Total Asset";
    $("ledgerTotal").textContent = displayMoney(state.total_asset || 0);
  } else {
    if (totalLabel) totalLabel.textContent = "Total Balance";
    $("ledgerTotal").textContent = displayMoney(accountBalance || 0);
  }

  if (isAll) {
    $("thAccount").hidden = false;
  } else {
    $("thAccount").hidden = true;
  }
  const titleSelect = $("ledgerTitleSelect");
  if (titleSelect) {
    const targetValue = isAll ? "all" : (state.account_id || "all");
    const hasOption = titleSelect.querySelector(`option[value="${targetValue}"]`);
    titleSelect.value = hasOption ? targetValue : "all";
  }
  if (table) {
    table.classList.toggle("is-account", !isAll);
  }

  renderRangeDisplay();
  const dateHeader = $("ledgerDateHeader");
  if (dateHeader) {
    dateHeader.setAttribute("aria-sort", state.sort_order === "asc" ? "ascending" : "descending");
  }
};

const renderLedgerBody = (rows) => {
  const body = $("ledgerBody");
  if (!body) return;
  const { isAll } = getLedgerViewState();
  if (!rows.length) {
    const colSpan = isAll ? 7 : 6;
    const message = state.search_query
      ? "No transactions match your search."
      : "No transactions yet for this range.";
    body.innerHTML = `<tr><td colspan="${colSpan}" class="empty-row">${message}</td></tr>`;
    return;
  }
  const html = [];
  rows.forEach((r, idx) => {
    html.push(buildLedgerRow(r, idx, isAll));
  });
  body.innerHTML = html.join("");
};

const appendLedgerRows = (rows) => {
  if (!rows.length) return;
  const body = $("ledgerBody");
  if (!body) return;
  const { isAll } = getLedgerViewState();
  if (body.querySelector(".empty-row")) {
    body.innerHTML = "";
  }
  const existingCount = body.querySelectorAll("tr[data-tx-id]").length;
  const html = [];
  rows.forEach((r, idx) => {
    html.push(buildLedgerRow(r, existingCount + idx, isAll));
  });
  body.insertAdjacentHTML("beforeend", html.join(""));
};

function renderLedger(rows) {
  renderLedgerChrome();
  renderLedgerBody(rows);
}

async function loadMe() {
  try {
    state.me = await api.get("/api/me");
    $("me").textContent = `${state.me.full_name} (${state.me.username})`;
    const mobileMe = $("mobileMe");
    if (mobileMe) mobileMe.textContent = `${state.me.full_name} (${state.me.username})`;
  } catch {
    location.href = "./login.html";
  }
}

function updateEmptyAccountsBanner() {
  const banner = $("emptyAccountsBanner");
  if (!banner) return;
  banner.hidden = state.accounts.length > 0;
}

async function loadAccounts() {
  const res = await api.get("/api/accounts");
  state.accounts = res.accounts || [];
  const primary = state.accounts[0] || null;
  state.primary_account_id = primary?.account_id || null;

  renderAccounts();
  updateEmptyAccountsBanner();

  if (state.scope === "account") {
    const exists = state.accounts.find((a) => a.account_id === state.account_id);
    if (!exists) {
      state.scope = "all";
      state.account_id = null;
    }
  }
}

const activeRows = () => (state.search_query ? state.search_rows : state.rows);

const activePaging = () => {
  if (state.search_query) {
    return {
      offsetKey: "search_offset",
      hasMoreKey: "search_has_more",
      loadingKey: "search_loading",
      rowsKey: "search_rows",
    };
  }
  return {
    offsetKey: "default_offset",
    hasMoreKey: "default_has_more",
    loadingKey: "default_loading",
    rowsKey: "rows",
  };
};

const markDefaultStale = () => {
  if (state.search_query) state.default_stale = true;
};

async function loadLedgerPage({ reset = false } = {}) {
  const { offsetKey, hasMoreKey, loadingKey, rowsKey } = activePaging();
  const isSearch = !!state.search_query;
  if (state[loadingKey] && !(reset && isSearch)) return;
  if (!state[hasMoreKey] && !reset) return;

  if (reset) {
    state[offsetKey] = 0;
    state[hasMoreKey] = true;
    state[rowsKey] = [];
  }

  if (reset && isSearch) {
    searchRequestToken += 1;
    if (searchAbortController) searchAbortController.abort();
    searchAbortController = new AbortController();
  }

  const requestToken = isSearch ? searchRequestToken : null;
  const signal = isSearch && searchAbortController ? searchAbortController.signal : undefined;

  state[loadingKey] = true;
  const showSpinner = !reset && state[rowsKey].length > 0;
  if (showSpinner) setLedgerLoading(true);
  if (reset) setLedgerError("");
  const scrollEl = getLedgerScrollElement();
  const prevScrollTop = !reset && scrollEl ? scrollEl.scrollTop : 0;
  const scope = state.scope;
  const acc = scope === "account" ? `&account_id=${encodeURIComponent(state.account_id || "")}` : "";
  const q = state.search_query ? `&q=${encodeURIComponent(state.search_query)}` : "";
  const includeSummary = reset;
  const summaryParam = includeSummary ? "" : "&include_summary=false";
  const includeSwitchParam = scope === "all" ? `&include_switch=${state.include_switch_all ? "true" : "false"}` : "";
  const url =
    `/api/ledger?scope=${encodeURIComponent(scope)}` +
    acc +
    `&from_date=${encodeURIComponent(state.from)}` +
    `&to_date=${encodeURIComponent(state.to)}` +
    `&limit=${encodeURIComponent(state.page_size)}` +
    `&offset=${encodeURIComponent(state[offsetKey])}` +
    `&order=${encodeURIComponent(state.sort_order)}` +
    q +
    includeSwitchParam +
    summaryParam;

  try {
    const res = await api.get(url, { signal });
    if (isSearch && requestToken !== searchRequestToken) return;
    const nextRows = res.rows || [];
    if (res.summary) updateTotals(res.summary);
    state[rowsKey] = reset ? nextRows : state[rowsKey].concat(nextRows);
    const paging = res.paging || {};
    state[offsetKey] = paging.next_offset ?? state[offsetKey] + nextRows.length;
    state[hasMoreKey] = paging.has_more ?? nextRows.length === state.page_size;
    if (!state.search_query) {
      state.default_stale = false;
    }
    renderLedgerChrome();
    if (reset) {
      state.ledger_loaded = true;
      renderLedgerBody(activeRows());
    } else {
      appendLedgerRows(nextRows);
      if (scrollEl) {
        requestAnimationFrame(() => {
          scrollEl.scrollTop = prevScrollTop;
        });
      }
    }
    setLedgerError("");
  } catch (err) {
    if (err && err.name === "AbortError") return;
    setLedgerError(err?.message || "Ledger load failed.");
  } finally {
    state[loadingKey] = false;
    if (showSpinner) setLedgerLoading(false);
  }
}

async function reloadLedger() {
  await loadLedgerPage({ reset: true });
}

async function reloadLedgerToCount(targetCount) {
  await reloadLedger();
  if (!Number.isFinite(targetCount)) return;
  while (state.rows.length < targetCount && state.default_has_more) {
    await loadLedgerPage();
  }
}

const reloadLedgerWithDefaultStale = () => {
  markDefaultStale();
  return reloadLedger();
};

let ledgerDebounceTimer = null;
const scheduleLedgerReload = (runner, delay = 400) => {
  if (ledgerDebounceTimer) {
    clearTimeout(ledgerDebounceTimer);
  }
  setLedgerLoading(true);
  ledgerDebounceTimer = setTimeout(() => {
    ledgerDebounceTimer = null;
    Promise.resolve()
      .then(() => runner())
      .catch(console.error)
      .finally(() => setLedgerLoading(false));
  }, delay);
};
const cancelLedgerReload = () => {
  if (ledgerDebounceTimer) {
    clearTimeout(ledgerDebounceTimer);
    ledgerDebounceTimer = null;
  }
  setLedgerLoading(false);
};

const setActiveTab = (tab) => {
  const ledger = $("tab-ledger");
  const summary = $("tab-summary");
  const analysis = $("tab-analysis");
  const fabBtn = $("mobileAddBtn");
  state.active_tab = tab;
  if (ledger) ledger.hidden = tab !== "ledger";
  if (summary) summary.hidden = tab !== "summary";
  if (analysis) analysis.hidden = tab !== "analysis";
  if (fabBtn) fabBtn.hidden = tab !== "ledger";
  if (tab !== "ledger") {
    document.body.classList.remove("fab-open");
  }
  document.querySelectorAll(".tab-btn").forEach((btn) => {
    const isActive = btn.dataset.tab === tab;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  });
  if (tab === "summary") {
    loadSummary().catch(console.error);
  } else if (tab === "analysis") {
    loadAnalysis().catch(console.error);
  } else if (tab === "ledger") {
    if (!state.suppress_ledger_refresh && !state.default_loading) {
      reloadLedgerWithDefaultStale().catch(console.error);
    }
  }
  syncMobileScrollState();
};

function bindEvents() {
  bindMoneyInputs();
  bindMobileScrollState();

  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => setActiveTab(btn.dataset.tab));
  });

  document.querySelectorAll("#budgetShiftModeToggle .seg-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const nextMode = btn.dataset.budgetMode || "normal";
      if (nextMode === state.analysis_budget_mode) return;
      state.analysis_budget_mode = nextMode;
      updateBudgetShiftModeUI();
      state.analysis_stale = true;
      if (state.active_tab === "analysis") {
        loadAnalysis({ force: true }).catch(console.error);
      }
    });
  });
  updateBudgetShiftModeUI();

  const summaryMonthBtn = $("summaryMonthBtn");
  const analysisMonthBtn = $("analysisMonthBtn");
  const summaryMonthPicker = $("summaryMonthPicker");
  const summaryMonthBackdrop = $("summaryMonthPickerBackdrop");
  const summaryMonthGrid = $("summaryMonthGrid");
  const summaryMonthYear = $("summaryMonthYear");
  const summaryMonthPrev = $("summaryMonthPrev");
  const summaryMonthNext = $("summaryMonthNext");
  const summaryMonthApply = $("summaryMonthApply");
  const summaryMonthCancel = $("summaryMonthCancel");
  let monthPickerYear = null;
  let monthPickerValue = null;

  const formatMonthShort = (year, month) =>
    new Date(year, month - 1, 1).toLocaleString("en-US", { month: "short" });

  const positionMonthPicker = (anchorEl) => {
    if (!summaryMonthPicker) return;
    const rect = anchorEl?.getBoundingClientRect?.();
    let pickerRect = summaryMonthPicker.getBoundingClientRect();
    let left = 16;
    let top = 16;
    if (isMobile() || !rect) {
      const width = Math.min(window.innerWidth - 32, pickerRect.width);
      left = Math.round((window.innerWidth - width) / 2);
      top = Math.round(Math.max(24, window.innerHeight * 0.2));
      summaryMonthPicker.style.width = `${width}px`;
      pickerRect = summaryMonthPicker.getBoundingClientRect();
    } else {
      summaryMonthPicker.style.width = "";
      pickerRect = summaryMonthPicker.getBoundingClientRect();
      left = rect.left;
      top = rect.bottom + 8;
      const maxLeft = window.innerWidth - pickerRect.width - 8;
      const maxTop = window.innerHeight - pickerRect.height - 8;
      left = Math.min(Math.max(8, left), Math.max(8, maxLeft));
      top = Math.min(Math.max(8, top), Math.max(8, maxTop));
      if (rect.bottom + pickerRect.height + 8 > window.innerHeight) {
        top = Math.max(8, rect.top - pickerRect.height - 8);
      }
    }
    summaryMonthPicker.style.left = `${Math.round(left)}px`;
    summaryMonthPicker.style.top = `${Math.round(top)}px`;
  };

  const renderMonthPicker = () => {
    if (!summaryMonthGrid || !summaryMonthYear || !monthPickerYear) return;
    const max = parseYM(currentMonthYM());
    summaryMonthYear.textContent = String(monthPickerYear);
    summaryMonthGrid.innerHTML = "";
    for (let m = 1; m <= 12; m += 1) {
      const ym = `${monthPickerYear}-${String(m).padStart(2, "0")}`;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "month-cell";
      btn.dataset.ym = ym;
      btn.textContent = formatMonthShort(monthPickerYear, m);
      const isDisabled = max && (monthPickerYear > max.year || (monthPickerYear === max.year && m > max.month));
      if (isDisabled) {
        btn.classList.add("is-disabled");
        btn.disabled = true;
      }
      if (ym === currentMonthYM()) btn.classList.add("is-current");
      if (ym === monthPickerValue) btn.classList.add("is-selected");
      summaryMonthGrid.appendChild(btn);
    }
    if (summaryMonthNext) {
      summaryMonthNext.disabled = max && monthPickerYear >= max.year;
    }
  };

  const openMonthPicker = (anchorEl) => {
    if (!summaryMonthPicker || !summaryMonthBackdrop) return;
    const safeValue = clampMonthYM(state.summary_month || currentMonthYM());
    const parsed = parseYM(safeValue);
    monthPickerYear = parsed ? parsed.year : new Date().getFullYear();
    monthPickerValue = safeValue;
    summaryMonthPicker.removeAttribute("hidden");
    summaryMonthPicker.setAttribute("aria-hidden", "false");
    summaryMonthBackdrop.removeAttribute("hidden");
    renderMonthPicker();
    requestAnimationFrame(() => {
      summaryMonthPicker.classList.add("open");
      summaryMonthBackdrop.classList.add("open");
      positionMonthPicker(anchorEl || summaryMonthBtn);
    });
  };

  const closeMonthPicker = () => {
    if (!summaryMonthPicker || !summaryMonthBackdrop) return;
    summaryMonthPicker.classList.remove("open");
    summaryMonthBackdrop.classList.remove("open");
    setTimeout(() => {
      summaryMonthPicker.setAttribute("hidden", "");
      summaryMonthPicker.setAttribute("aria-hidden", "true");
      summaryMonthBackdrop.setAttribute("hidden", "");
    }, 160);
  };

  if (summaryMonthBtn) {
    summaryMonthBtn.addEventListener("click", () => openMonthPicker(summaryMonthBtn));
  }
  if (analysisMonthBtn) {
    analysisMonthBtn.addEventListener("click", () => openMonthPicker(analysisMonthBtn));
  }
  if (summaryMonthGrid) {
    summaryMonthGrid.addEventListener("click", (e) => {
      const cell = e.target.closest(".month-cell");
      if (!cell || cell.disabled) return;
      monthPickerValue = cell.dataset.ym;
      renderMonthPicker();
    });
  }
  if (summaryMonthPrev) {
    summaryMonthPrev.addEventListener("click", () => {
      if (!monthPickerYear) return;
      monthPickerYear -= 1;
      renderMonthPicker();
    });
  }
  if (summaryMonthNext) {
    summaryMonthNext.addEventListener("click", () => {
      if (!monthPickerYear) return;
      monthPickerYear += 1;
      renderMonthPicker();
    });
  }
  if (summaryMonthApply) {
    summaryMonthApply.addEventListener("click", () => {
      if (!monthPickerValue) return closeMonthPicker();
      state.summary_month = clampMonthYM(monthPickerValue);
      updateSummaryMonthText();
      markSummaryStale();
      closeMonthPicker();
    });
  }
  if (summaryMonthCancel) summaryMonthCancel.addEventListener("click", closeMonthPicker);
  if (summaryMonthBackdrop) summaryMonthBackdrop.addEventListener("click", closeMonthPicker);
  updateSummaryMonthText();
  updatePaydayText();

  const paydayBtn = $("paydayBtn");
  const analysisPaydayBtn = $("analysisPaydayBtn");
  const paydayModal = $("paydayModal");
  const paydayForm = $("paydayForm");
  const paydayDayInput = $("paydayDayInput");
  const paydayMsg = $("paydayMsg");
  const paydayMonthLabel = $("paydayMonthLabel");
  const paydayDefaultHint = $("paydayDefaultHint");
  const closePaydayBtn = $("closePaydayModal");
  const setPaydayDefaultBtn = $("setPaydayDefaultBtn");
  const clearPaydayOverrideBtn = $("clearPaydayOverrideBtn");

  const openPaydayModal = () => {
    if (!paydayModal) return;
    const month = state.summary_month || currentMonthYM();
    if (paydayMonthLabel) paydayMonthLabel.value = formatMonthLabel(month);
    if (paydayDayInput) {
      const fallback = state.payday_day || state.payday_default || "";
      paydayDayInput.value = fallback ? String(fallback) : "";
    }
    if (paydayDefaultHint) {
      const defaultLabel = state.payday_default ? `Default: ${state.payday_default}` : "Default: -";
      const sourceLabel = state.payday_source === "override" ? " (custom for this month)" : "";
      paydayDefaultHint.textContent = `${defaultLabel}${sourceLabel}`;
    }
    if (paydayMsg) paydayMsg.textContent = "";
    paydayModal.hidden = false;
  };

  const closePaydayModal = () => {
    if (paydayModal) paydayModal.hidden = true;
  };

  if (paydayBtn) paydayBtn.addEventListener("click", openPaydayModal);
  if (analysisPaydayBtn) analysisPaydayBtn.addEventListener("click", openPaydayModal);
  if (closePaydayBtn) closePaydayBtn.addEventListener("click", closePaydayModal);
  if (paydayModal) {
    paydayModal.addEventListener("click", (e) => e.target === paydayModal && closePaydayModal());
  }

  if (clearPaydayOverrideBtn) {
    clearPaydayOverrideBtn.addEventListener("click", async () => {
      const month = state.summary_month || currentMonthYM();
      if (paydayMsg) paydayMsg.textContent = "";
      try {
        await api.put("/api/payday", { month, clear_override: true });
        closePaydayModal();
        markSummaryStale();
      } catch (err) {
        if (paydayMsg) paydayMsg.textContent = err.message || "Failed to reset payday";
      }
    });
  }

  if (setPaydayDefaultBtn) {
    setPaydayDefaultBtn.addEventListener("click", async () => {
      const day = Number(paydayDayInput?.value || 0);
      if (!day || day < 1 || day > 31) {
        if (paydayMsg) paydayMsg.textContent = "Enter a day between 1 and 31.";
        return;
      }
      if (paydayMsg) paydayMsg.textContent = "";
      try {
        await api.put("/api/payday", { day });
        closePaydayModal();
        markSummaryStale();
      } catch (err) {
        if (paydayMsg) paydayMsg.textContent = err.message || "Failed to save default payday";
      }
    });
  }

  if (paydayForm) {
    paydayForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const month = state.summary_month || currentMonthYM();
      const day = Number(paydayDayInput?.value || 0);
      if (!day || day < 1 || day > 31) {
        if (paydayMsg) paydayMsg.textContent = "Enter a day between 1 and 31.";
        return;
      }
      if (paydayMsg) paydayMsg.textContent = "";
      try {
        await api.put("/api/payday", { month, day });
        closePaydayModal();
        markSummaryStale();
      } catch (err) {
        if (paydayMsg) paydayMsg.textContent = err.message || "Failed to save payday";
      }
    });
  }

  document.querySelectorAll(".analysis-scroll").forEach((el) => {
    bindHorizontalWheelScroll(el);
    bindHorizontalDragScroll(el);
  });

  const summaryCards = $("summaryCards");
  if (summaryCards) {
    const openSummaryLedger = (card) => {
      const target = card?.dataset?.accountId;
      if (!target) return;
      const summaryRange = state.overview_range;
      if (summaryRange?.from && summaryRange?.to) {
        applyLedgerRange(summaryRange.from, summaryRange.to);
      }
      const titleSelect = $("ledgerTitleSelect");
      if (titleSelect) {
        state.suppress_ledger_refresh = true;
        setActiveTab("ledger");
        titleSelect.value = target;
        titleSelect.dispatchEvent(new Event("change", { bubbles: true }));
        state.suppress_ledger_refresh = false;
        return;
      }
      setActiveTab("ledger");
      state.scope = "account";
      state.account_id = target;
      if (summaryRange?.from && summaryRange?.to) {
        applyLedgerRange(summaryRange.from, summaryRange.to);
      }
      reloadLedgerWithDefaultStale().catch(console.error);
    };
    summaryCards.addEventListener("click", (e) => {
      const editBtn = e.target.closest("button[data-action='edit']");
      if (editBtn) {
        e.stopPropagation();
        const acc = state.accounts.find((a) => a.account_id === editBtn.dataset.id);
        if (acc) {
          openAccountsModal("create");
          startAccountEdit(acc);
        }
        return;
      }
      const card = e.target.closest(".summary-card");
      if (!card) return;
      openSummaryLedger(card);
    });
    summaryCards.addEventListener("keydown", (e) => {
      if (e.key !== "Enter" && e.key !== " ") return;
      if (e.target.closest("button[data-action='edit']")) return;
      const card = e.target.closest(".summary-card");
      if (!card) return;
      e.preventDefault();
      openSummaryLedger(card);
    });
  }

  const mobileAccountSelect = $("mobileAccountSelect");
  if (mobileAccountSelect) {
    mobileAccountSelect.addEventListener("change", (e) => {
      const value = e.target.value;
      if (value === "all") {
        state.scope = "all";
        state.account_id = null;
      } else {
        state.scope = "account";
        state.account_id = value;
      }

      const titleSelect = $("ledgerTitleSelect");
      if (titleSelect) {
        titleSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
      }

      reloadLedgerWithDefaultStale().catch(console.error);
    });
  }

  const ledgerTitleSelect = $("ledgerTitleSelect");
  if (ledgerTitleSelect) {
    ledgerTitleSelect.addEventListener("change", (e) => {
      const value = e.target.value;
      if (value === "all") {
        state.scope = "all";
        state.account_id = null;
      } else {
        state.scope = "account";
        state.account_id = value;
      }

      const mobileSelect = $("mobileAccountSelect");
      if (mobileSelect) {
        mobileSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
      }

      reloadLedgerWithDefaultStale().catch(console.error);
    });
  }

  const ledgerSwitchToggle = $("ledgerSwitchToggle");
  if (ledgerSwitchToggle) {
    ledgerSwitchToggle.addEventListener("click", () => {
      if (state.scope !== "all") return;
      state.include_switch_all = !state.include_switch_all;
      renderLedgerChrome();
      reloadLedgerWithDefaultStale().catch(console.error);
    });
  }

  const dateHeader = $("ledgerDateHeader");
  if (dateHeader) {
    dateHeader.addEventListener("click", () => {
      state.sort_order = state.sort_order === "asc" ? "desc" : "asc";
      dateHeader.setAttribute("aria-sort", state.sort_order === "asc" ? "ascending" : "descending");
      cancelLedgerReload();
      reloadLedgerWithDefaultStale().catch(console.error);
    });
  }

  const ledgerSearch = $("ledgerSearch");
  if (ledgerSearch) {
    ledgerSearch.value = state.search_query || "";
    ledgerSearch.addEventListener("input", (e) => {
      const value = (e.target.value || "").trim();
      state.search_query = value;
      if (!value) {
        cancelLedgerReload();
        setLedgerError("");
        if (state.default_stale) {
          const targetCount = state.rows.length;
          reloadLedgerToCount(targetCount).catch(console.error);
        } else {
          renderLedger(activeRows());
        }
        return;
      }
      scheduleLedgerReload(reloadLedger, 450);
    });
  }

  const ledgerSentinel = $("ledgerSentinel");
  const scrollEl = getLedgerScrollElement();
  if (scrollEl && ledgerSentinel && "IntersectionObserver" in window) {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          loadLedgerPage().catch(console.error);
        }
      },
      { root: scrollEl, rootMargin: "120px 0px", threshold: 0.1 }
    );
    observer.observe(ledgerSentinel);
  } else if (scrollEl) {
    scrollEl.addEventListener("scroll", () => {
      const threshold = 20;
      if (scrollEl.scrollTop + scrollEl.clientHeight >= scrollEl.scrollHeight - threshold) {
        loadLedgerPage().catch(console.error);
      }
    });
  }

  $("fromDate").addEventListener("change", () => {
    state.from = $("fromDate").value;
    renderRangeDisplay();
    reloadLedgerWithDefaultStale().catch(console.error);
  });
  $("toDate").addEventListener("change", () => {
    state.to = $("toDate").value;
    renderRangeDisplay();
    reloadLedgerWithDefaultStale().catch(console.error);
  });

  const datePicker = $("ledgerDatePicker");
  const datePickerBackdrop = $("ledgerDatePickerBackdrop");
  const datePickerGrid = $("ledgerDatePickerGrid");
  const datePickerMonth = $("ledgerDatePickerMonth");
  const datePickerLabel = $("ledgerDatePickerLabel");
  const datePickerPrev = $("ledgerDatePrev");
  const datePickerNext = $("ledgerDateNext");
  const datePickerApply = $("ledgerDateApply");
  const datePickerCancel = $("ledgerDateCancel");
  const datePickerClear = $("ledgerDateClear");
  let pickerMode = "range";
  let pickerTarget = "from";
  let pickerMonth = null;
  let pickerFrom = null;
  let pickerTo = null;
  let pickerSingle = null;
  let pickerSingleApply = null;
  let pickerAnchor = null;

  const formatPickerMonth = (date) =>
    date.toLocaleString("en-US", { month: "long", year: "numeric" });

  const clampPickerPosition = (left, top, rect) => {
    const maxLeft = window.innerWidth - rect.width - 8;
    const maxTop = window.innerHeight - rect.height - 8;
    const nextLeft = Math.min(Math.max(8, left), Math.max(8, maxLeft));
    const nextTop = Math.min(Math.max(8, top), Math.max(8, maxTop));
    return { left: nextLeft, top: nextTop };
  };

  const positionDatePicker = (anchorEl) => {
    if (!datePicker) return;
    pickerAnchor = anchorEl || pickerAnchor;
    const rect = pickerAnchor?.getBoundingClientRect?.();
    let pickerRect = datePicker.getBoundingClientRect();
    let left = 16;
    let top = 16;
    if (isMobile() || !rect) {
      const width = Math.min(window.innerWidth - 32, pickerRect.width);
      left = Math.round((window.innerWidth - width) / 2);
      top = Math.round(Math.max(24, window.innerHeight * 0.18));
      datePicker.style.width = `${width}px`;
      pickerRect = datePicker.getBoundingClientRect();
    } else {
      datePicker.style.width = "";
      pickerRect = datePicker.getBoundingClientRect();
      left = rect.left;
      top = rect.bottom + 8;
      const adjusted = clampPickerPosition(left, top, pickerRect);
      left = adjusted.left;
      top = adjusted.top;
      if (top === adjusted.top && rect.bottom + pickerRect.height + 8 > window.innerHeight) {
        top = Math.max(8, rect.top - pickerRect.height - 8);
      }
    }
    datePicker.style.left = `${Math.round(left)}px`;
    datePicker.style.top = `${Math.round(top)}px`;
  };

  const renderDatePicker = () => {
    if (!datePickerGrid || !pickerMonth) return;
    const isSingle = pickerMode === "single";
    const maxYmd = dateToYMD(new Date());
    if (datePickerMonth) datePickerMonth.textContent = formatPickerMonth(pickerMonth);
    if (datePickerLabel) {
      datePickerLabel.textContent = isSingle ? "Date" : (pickerTarget === "to" ? "To" : "From");
    }
    const y = pickerMonth.getFullYear();
    const m = pickerMonth.getMonth();
    const firstDay = new Date(y, m, 1);
    const weekday = (firstDay.getDay() + 6) % 7; // Monday start
    const daysInMonth = new Date(y, m + 1, 0).getDate();
    const daysInPrev = new Date(y, m, 0).getDate();
    const fromDate = !isSingle && pickerFrom ? ymdToDate(pickerFrom) : null;
    const toDate = !isSingle && pickerTo ? ymdToDate(pickerTo) : null;
    const fromTs = fromDate ? fromDate.getTime() : null;
    const toTs = toDate ? toDate.getTime() : null;
    const rangeStart = !isSingle && fromTs && toTs ? Math.min(fromTs, toTs) : null;
    const rangeEnd = !isSingle && fromTs && toTs ? Math.max(fromTs, toTs) : null;
    const today = todayYMD();
    datePickerGrid.innerHTML = "";
    for (let i = 0; i < 42; i += 1) {
      const dayNum = i - weekday + 1;
      let cellDate = null;
      let isMuted = false;
      if (dayNum <= 0) {
        cellDate = new Date(y, m - 1, daysInPrev + dayNum);
        isMuted = true;
      } else if (dayNum > daysInMonth) {
        cellDate = new Date(y, m + 1, dayNum - daysInMonth);
        isMuted = true;
      } else {
        cellDate = new Date(y, m, dayNum);
      }
      const ymd = dateToYMD(cellDate);
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "date-cell";
      btn.dataset.ymd = ymd;
      btn.textContent = String(cellDate.getDate());
      if (isSingle && ymd > maxYmd) {
        btn.disabled = true;
        btn.classList.add("is-muted");
      }
      if (isMuted) btn.classList.add("is-muted");
      if (ymd === today) btn.classList.add("is-today");
      if (isSingle && pickerSingle && ymd === pickerSingle) btn.classList.add("is-selected");
      if (!isSingle && pickerFrom && ymd === pickerFrom) btn.classList.add("is-selected");
      if (!isSingle && pickerTo && ymd === pickerTo) btn.classList.add("is-selected");
      const cellTs = cellDate.getTime();
      if (rangeStart && rangeEnd && cellTs >= rangeStart && cellTs <= rangeEnd) {
        btn.classList.add("is-range");
      }
      datePickerGrid.appendChild(btn);
    }
  };

  const closeDatePicker = () => {
    if (!datePicker || !datePickerBackdrop) return;
    datePicker.classList.remove("open");
    datePickerBackdrop.classList.remove("open");
    setTimeout(() => {
      datePicker.setAttribute("hidden", "");
      datePicker.setAttribute("aria-hidden", "true");
      datePickerBackdrop.setAttribute("hidden", "");
    }, 160);
  };

  const openDatePicker = (target, anchorEl) => {
    if (!datePicker || !datePickerBackdrop) return;
    pickerMode = "range";
    pickerSingle = null;
    pickerSingleApply = null;
    pickerTarget = target;
    pickerFrom = state.from;
    pickerTo = state.to;
    pickerAnchor = anchorEl || pickerAnchor;
    const base =
      ymdToDate(target === "to" ? pickerTo : pickerFrom) ||
      ymdToDate(pickerFrom) ||
      ymdToDate(pickerTo) ||
      new Date();
    pickerMonth = new Date(base.getFullYear(), base.getMonth(), 1);
    renderDatePicker();
    datePicker.removeAttribute("hidden");
    datePicker.setAttribute("aria-hidden", "false");
    datePickerBackdrop.removeAttribute("hidden");
    requestAnimationFrame(() => {
      datePicker.classList.add("open");
      datePickerBackdrop.classList.add("open");
      positionDatePicker(pickerAnchor);
    });
  };

  const openDatePickerSingle = (value, anchorEl, onApply) => {
    if (!datePicker || !datePickerBackdrop) return;
    pickerMode = "single";
    pickerSingle = clampYmdToToday(value);
    pickerSingleApply = typeof onApply === "function" ? onApply : null;
    pickerFrom = null;
    pickerTo = null;
    pickerAnchor = anchorEl || pickerAnchor;
    const base = ymdToDate(pickerSingle) || new Date();
    pickerMonth = new Date(base.getFullYear(), base.getMonth(), 1);
    renderDatePicker();
    datePicker.removeAttribute("hidden");
    datePicker.setAttribute("aria-hidden", "false");
    datePickerBackdrop.removeAttribute("hidden");
    requestAnimationFrame(() => {
      datePicker.classList.add("open");
      datePickerBackdrop.classList.add("open");
      positionDatePicker(pickerAnchor);
    });
  };

  if (datePickerGrid) {
    datePickerGrid.addEventListener("click", (e) => {
      const cell = e.target.closest(".date-cell");
      if (!cell) return;
      if (cell.disabled) return;
      const ymd = cell.dataset.ymd;
      if (!ymd) return;
      if (pickerMode === "single") {
        pickerSingle = ymd;
        renderDatePicker();
        return;
      }
      if (pickerTarget === "from") {
        pickerFrom = ymd;
        if (pickerTo && pickerFrom > pickerTo) pickerTo = ymd;
      } else {
        pickerTo = ymd;
        if (pickerFrom && pickerTo < pickerFrom) pickerFrom = ymd;
      }
      const clickedDate = ymdToDate(ymd);
      if (clickedDate) {
        pickerMonth = new Date(clickedDate.getFullYear(), clickedDate.getMonth(), 1);
      }
      renderDatePicker();
    });
  }

  if (datePickerPrev) {
    datePickerPrev.addEventListener("click", () => {
      if (!pickerMonth) return;
      pickerMonth = new Date(pickerMonth.getFullYear(), pickerMonth.getMonth() - 1, 1);
      renderDatePicker();
    });
  }
  if (datePickerNext) {
    datePickerNext.addEventListener("click", () => {
      if (!pickerMonth) return;
      pickerMonth = new Date(pickerMonth.getFullYear(), pickerMonth.getMonth() + 1, 1);
      renderDatePicker();
    });
  }
  if (datePickerApply) {
    datePickerApply.addEventListener("click", () => {
      if (pickerMode === "single") {
        const nextDate = pickerSingle || dateToYMD(new Date());
        if (pickerSingleApply) pickerSingleApply(nextDate);
        closeDatePicker();
        return;
      }
      const nextFrom = pickerFrom || state.from;
      const nextTo = pickerTo || state.to;
      if (!nextFrom || !nextTo) return closeDatePicker();
      state.from = nextFrom;
      state.to = nextTo;
      const fromInput = $("fromDate");
      const toInput = $("toDate");
      if (fromInput) fromInput.value = state.from;
      if (toInput) toInput.value = state.to;
      renderRangeDisplay();
      closeDatePicker();
      scheduleLedgerReload(reloadLedgerWithDefaultStale, 350);
    });
  }
  if (datePickerCancel) datePickerCancel.addEventListener("click", closeDatePicker);
  if (datePickerClear) {
    datePickerClear.addEventListener("click", () => {
      if (pickerMode === "single") {
        pickerSingle = dateToYMD(new Date());
        renderDatePicker();
        return;
      }
      state.from = minusDaysYMD(30);
      state.to = todayYMD();
      const fromInput = $("fromDate");
      const toInput = $("toDate");
      if (fromInput) fromInput.value = state.from;
      if (toInput) toInput.value = state.to;
      renderRangeDisplay();
      closeDatePicker();
      scheduleLedgerReload(reloadLedgerWithDefaultStale, 350);
    });
  }
  if (datePickerBackdrop) datePickerBackdrop.addEventListener("click", closeDatePicker);
  document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    if (datePicker?.classList.contains("open")) closeDatePicker();
    if (summaryMonthPicker?.classList.contains("open")) closeMonthPicker();
  });
  window.addEventListener("resize", () => {
    if (datePicker?.classList.contains("open")) positionDatePicker(pickerAnchor);
  });

  const bindRangePicker = (el) => {
    if (!el) return;
    el.setAttribute("title", "Edit date range");
    el.addEventListener("click", (e) => {
      const target = e.target.closest(".range-date[data-date-target]");
      if (!target || !el.contains(target)) return;
      if (target.dataset?.dateTarget === "to") {
        openDatePicker("to", target);
        return;
      }
      openDatePicker("from", target);
    });
    el.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        const target = e.target.closest(".range-date[data-date-target]");
        if (!target || !el.contains(target)) return;
        e.preventDefault();
        if (target.dataset?.dateTarget === "to") {
          openDatePicker("to", target);
          return;
        }
        openDatePicker("from", target);
      }
    });
  };

  bindRangePicker($("ledgerRangeDisplay"));
  bindRangePicker($("ledgerRangeText"));

  const analysisDaily = $("analysisDaily");
  if (analysisDaily) {
    analysisDaily.addEventListener("click", (e) => {
      const card = e.target.closest(".analysis-day-card[data-date]");
      if (!card) return;
      const day = card.dataset.date;
      navigateToLedgerDay(day);
    });
    analysisDaily.addEventListener("keydown", (e) => {
      if (e.key !== "Enter" && e.key !== " ") return;
      const card = e.target.closest(".analysis-day-card[data-date]");
      if (!card) return;
      e.preventDefault();
      const day = card.dataset.date;
      navigateToLedgerDay(day);
    });
  }

  // Theme Toggle
  const themeBtn = $("themeToggleBtn");
  const themeCheckbox = $("mobileThemeToggleCheckbox");

  const setTheme = (isDark) => {
    document.body.classList.toggle("dark", isDark);
    if (themeCheckbox) themeCheckbox.checked = isDark;
    localStorage.setItem("theme", isDark ? "dark" : "light");
  };

  // Init theme
  const savedTheme = localStorage.getItem("theme");
  if (savedTheme === "dark") setTheme(true);

  const sunIcon = `<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false"><circle cx="12" cy="12" r="4" fill="none" stroke="currentColor" stroke-width="2"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>`;
  const moonIcon = `<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false"><path d="M21 15.5A9 9 0 1 1 8.5 3a7 7 0 0 0 12.5 12.5z" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
  const updateThemeIcon = () => {
    if (!themeBtn) return;
    const isDark = document.body.classList.contains("dark");
    themeBtn.innerHTML = isDark ? sunIcon : moonIcon;
    themeBtn.setAttribute("aria-label", isDark ? "Switch to light theme" : "Switch to dark theme");
    themeBtn.title = isDark ? "Switch to light theme" : "Switch to dark theme";
  };

  updateThemeIcon();
  themeBtn.addEventListener("click", () => {
    const isDark = !document.body.classList.contains("dark");
    setTheme(isDark);
    updateThemeIcon();
  });

  if (themeCheckbox) {
    themeCheckbox.addEventListener("change", (e) => {
      setTheme(e.target.checked);
      updateThemeIcon();
    });
  }


  $("logoutBtn").addEventListener("click", async () => {
    try {
      await api.post("/api/auth/logout", {});
    } finally {
      location.href = "./login.html";
    }
  });

  const backdrop = $("sidebarBackdrop");
  if (backdrop) backdrop.addEventListener("click", () => {
    document.body.classList.remove("menu-open");
  });

  const mobileMenuBtn = $("mobileMenuBtn");
  if (mobileMenuBtn) {
    mobileMenuBtn.addEventListener("click", () => {
      document.body.classList.remove("fab-open");
      setTimeout(() => document.body.classList.add("menu-open"), 10);
    });
  }

  $("closeMobileMenuBtn").addEventListener("click", () => document.body.classList.remove("menu-open"));

  // Fix for Mobile Sheet visibility (remove hidden attr if present to allow CSS transition)
  const mobileSheet = $("mobileMenuSheet");
  const syncMobileSheet = () => {
    if (!mobileSheet) return;
    if (isMobile()) {
      mobileSheet.removeAttribute("hidden");
    } else {
      mobileSheet.setAttribute("hidden", "");
    }
  };
  syncMobileSheet();

  const fabBackdrop = $("fabBackdrop");
  const fabSheet = $("fabSheet");
  const syncFabSheet = () => {
    if (!fabBackdrop || !fabSheet) return;
    if (isMobile()) {
      fabBackdrop.removeAttribute("hidden");
      fabSheet.removeAttribute("hidden");
    } else {
      fabBackdrop.setAttribute("hidden", "");
      fabSheet.setAttribute("hidden", "");
      document.body.classList.remove("fab-open");
    }
  };
  syncFabSheet();

  window.addEventListener("resize", () => {
    if (!isMobile()) {
      document.body.classList.remove("menu-open");
      document.body.classList.remove("fab-open");
    }
    syncMobileSheet();
    syncFabSheet();
  });

  // Mobile Menu Actions
  // Theme toggle handled by 'change' event on checkbox above

  $("mobileLogoutBtn").addEventListener("click", async () => {
    try { await api.post("/api/auth/logout", {}); }
    finally { location.href = "./login.html"; }
  });

  // Unified Currency Toggle Logic for Desktop and Mobile
  const handleCurrencyChange = (target) => {
    const next = target.dataset.currency;
    if (!next || next === state.currency) return;
    state.currency = next;

    // Update UI for all toggle buttons (sync desktop and mobile)
    document.querySelectorAll("#currencyToggle .seg-btn, #mobileCurrencyToggle .seg-btn").forEach(b =>
      b.classList.toggle("active", b.dataset.currency === state.currency)
    );

    updateFxUI();
    renderLedger(activeRows());
    renderSummary();
    renderAnalysis();
  };

  document.querySelectorAll("#currencyToggle .seg-btn, #mobileCurrencyToggle .seg-btn").forEach(btn => {
    btn.addEventListener("click", (e) => handleCurrencyChange(e.target));
  });

  const txForm = $("txForm");
  const txIdInput = document.querySelector('input[name="transaction_id"]');
  const txDateInput = $("txDateInput");
  const txDateDisplay = $("txDateDisplay");
  const txTopupField = $("txTopupField");
  const txTopupFlag = $("txTopupFlag");
  const txReceiptFile = $("txReceiptFile");
  const txReceiptCategory = $("txReceiptCategory");
  const txReceiptSection = $("txReceiptSection");
  const txReceiptOpen = $("txReceiptOpen");
  const txReceiptMeta = $("txReceiptMeta");
  const txReceiptPreview = $("txReceiptPreview");
  const deleteReceiptBtn = $("deleteReceiptBtn");

  const receiptViewUrl = (txId) =>
    `/api/transactions/${encodeURIComponent(txId)}/receipt/view?v=${Date.now()}`;

  const resetTxReceiptUI = () => {
    if (txReceiptSection) txReceiptSection.hidden = true;
    if (txReceiptOpen) txReceiptOpen.href = "#";
    if (txReceiptMeta) txReceiptMeta.textContent = "";
    if (txReceiptPreview) txReceiptPreview.innerHTML = "";
  };

  const renderTxReceipt = (txId, receipt) => {
    if (!txId || !receipt) {
      resetTxReceiptUI();
      return;
    }
    if (txReceiptSection) txReceiptSection.hidden = false;
    const viewUrl = receiptViewUrl(txId);
    if (txReceiptOpen) txReceiptOpen.href = viewUrl;
    const sourceLabel = receipt.original_filename || receipt.original_mime || "receipt";
    const metaText = `${sourceLabel} | ${receipt.category} | ${fmtBytes(receipt.stored_size)} stored`;
    if (txReceiptMeta) txReceiptMeta.textContent = metaText;
    if (!txReceiptPreview) return;
    if (receipt.stored_mime && String(receipt.stored_mime).startsWith("image/")) {
      txReceiptPreview.innerHTML = `<img src="${viewUrl}" alt="Receipt preview" loading="lazy" />`;
      return;
    }
    txReceiptPreview.innerHTML = `<iframe src="${viewUrl}" title="Receipt preview"></iframe>`;
  };

  const loadTxReceipt = async (txId) => {
    if (!txId) {
      resetTxReceiptUI();
      return;
    }
    try {
      const res = await api.get(`/api/transactions/${encodeURIComponent(txId)}/receipt`);
      renderTxReceipt(txId, res?.receipt || null);
      if (txReceiptCategory && res?.receipt?.category) {
        txReceiptCategory.value = res.receipt.category;
      }
    } catch (err) {
      if (String(err.message || "").includes("Receipt not found")) {
        resetTxReceiptUI();
        return;
      }
      $("txMsg").textContent = err.message || "Failed to load receipt";
      resetTxReceiptUI();
    }
  };
  const syncTxTopupState = () => {
    if (!txForm || !txTopupFlag) return;
    const selectedType = txForm.querySelector('input[name="transaction_type"]:checked')?.value;
    const allowTopup = selectedType === "debit";
    if (!allowTopup) txTopupFlag.checked = false;
    if (txTopupField) txTopupField.hidden = !allowTopup;
  };
  let txDateInitial = "";
  let txTimeInitial = "";

  const setTxDate = (ymd, { setInitial = false } = {}) => {
    const safeDate = clampYmdToToday(ymd);
    if (!safeDate) return;
    if (txDateInput) txDateInput.value = safeDate;
    if (txDateDisplay) txDateDisplay.value = formatYmdDisplay(safeDate);
    if (setInitial) txDateInitial = safeDate;
  };

  const openTxDatePicker = () => {
    const current = clampYmdToToday(txDateInput?.value);
    openDatePickerSingle(current, txDateDisplay, (next) => setTxDate(next));
  };
  if (txDateDisplay) {
    txDateDisplay.addEventListener("click", openTxDatePicker);
    txDateDisplay.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openTxDatePicker();
      }
    });
  }

  const closeModal = () => {
    const modal = $("modal");
    if (modal) modal.hidden = true;
    $("txMsg").textContent = "";
    $("deleteTxBtn").hidden = true;
    $("deleteTxBtn").dataset.txId = "";
    state.editing_tx_id = null;
    if (txForm) txForm.reset();
    if (txIdInput) txIdInput.value = "";
    if (txDateInput) txDateInput.value = "";
    if (txDateDisplay) txDateDisplay.value = "";
    if (txTopupFlag) txTopupFlag.checked = false;
    if (txReceiptFile) txReceiptFile.value = "";
    if (txReceiptCategory) txReceiptCategory.value = "general";
    resetTxReceiptUI();
    syncTxTopupState();
    txDateInitial = "";
    txTimeInitial = "";
  };

  const openModal = (tx) => {
    const modal = $("modal");
    if (!modal) return;
    $("txMsg").textContent = "";
    $("deleteTxBtn").hidden = true;
    $("txModalTitle").textContent = "Add Transaction";
    if (txForm) txForm.reset();
    if (txIdInput) txIdInput.value = "";
    if (txReceiptCategory) txReceiptCategory.value = "general";
    if (txReceiptFile) txReceiptFile.value = "";
    resetTxReceiptUI();
    state.editing_tx_id = null;
    const fallbackDate = clampYmdToToday();
    txTimeInitial = nowTimeWithSeconds();
    setTxDate(fallbackDate, { setInitial: true });

    const defaultAccount = state.scope === "account" && state.account_id
      ? state.account_id
      : (state.primary_account_id || state.accounts[0]?.account_id);
    if (defaultAccount) {
      $("txAccountSelect").value = defaultAccount;
    }

    if (tx) {
      $("txModalTitle").textContent = "Edit Transaction";
      if (txIdInput) txIdInput.value = tx.transaction_id;
      if (txForm) {
        txForm.transaction_name.value = tx.transaction_name || "";
        txForm.amount.value = fmtIDR(tx.debit || tx.credit || 0);
      }
      if (tx.account_id) $("txAccountSelect").value = tx.account_id;
      const txDate = isoToLocalYMD(tx.date);
      if (txDate) setTxDate(txDate, { setInitial: true });
      txTimeInitial = isoToLocalTimeWithSeconds(tx.date) || txTimeInitial;
      const type = tx.debit ? "debit" : "credit";
      const typeInput = document.querySelector(`input[name="transaction_type"][value="${type}"]`);
      if (typeInput) typeInput.checked = true;
      if (txTopupFlag) txTopupFlag.checked = !!tx.is_cycle_topup;
      $("deleteTxBtn").hidden = false;
      $("deleteTxBtn").dataset.txId = tx.transaction_id;
      state.editing_tx_id = tx.transaction_id;
      if (txReceiptCategory) txReceiptCategory.value = "general";
      loadTxReceipt(tx.transaction_id);
    }

    syncTxTopupState();
    modal.hidden = false;
  };

  const addBtn = $("addTxBtn");
  if (addBtn) {
    addBtn.addEventListener("click", () => {
      if (!requireAnyAccount("Create an account first to add transactions.")) return;
      openModal();
    });
  }
  const mobileAddBtn = $("mobileAddBtn");
  const closeFabMenu = () => document.body.classList.remove("fab-open");
  const openFabMenu = () => {
    if (!isMobile()) {
      if (!requireAnyAccount("Create an account first to add transactions.")) return;
      openModal();
      return;
    }
    document.body.classList.remove("menu-open");
    document.body.classList.add("fab-open");
  };
  if (mobileAddBtn) mobileAddBtn.addEventListener("click", openFabMenu);
  if (fabBackdrop) fabBackdrop.addEventListener("click", closeFabMenu);
  if (fabSheet) {
    fabSheet.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-action]");
      if (!btn) return;
      const action = btn.dataset.action;
      closeFabMenu();
      if (action === "add") {
        if (!requireAnyAccount("Create an account first to add transactions.")) return;
        return openModal();
      }
      if (action === "switch") {
        if (!requireAccountScope("Select an account to switch balances.")) return;
        return openSwitchModal();
      }
      if (action === "export") return openExportModal();
    });
  }
  const closeBtn = $("closeModal");
  if (closeBtn) closeBtn.addEventListener("click", closeModal);
  const modal = $("modal");
  if (modal) modal.addEventListener("click", (e) => e.target === modal && closeModal());
  if (txForm) {
    txForm.querySelectorAll('input[name="transaction_type"]').forEach((el) => {
      el.addEventListener("change", syncTxTopupState);
    });
    syncTxTopupState();
  }
  if (deleteReceiptBtn) {
    deleteReceiptBtn.addEventListener("click", async () => {
      const txId = (txIdInput && txIdInput.value) || state.editing_tx_id;
      if (!txId) return;
      if (!confirm("Delete receipt from this transaction?")) return;
      try {
        await api.del(`/api/transactions/${encodeURIComponent(txId)}/receipt`);
        resetTxReceiptUI();
      } catch (err) {
        $("txMsg").textContent = err.message || "Failed to delete receipt";
      }
    });
  }

  const switchModal = $("switchModal");
  const switchForm = $("switchForm");
  const switchFromSelect = $("switchFromSelect");
  const switchToSelect = $("switchToSelect");
  const switchAmountInput = $("switchAmount");
  const switchDateInput = $("switchDateInput");
  const switchDateDisplay = $("switchDateDisplay");
  const switchTopupField = $("switchTopupField");
  const switchTopupFlag = $("switchTopupFlag");
  const switchTitle = $("switchModalTitle");
  const deleteSwitchBtn = $("deleteSwitchBtn");
  let switchDateInitial = "";
  let switchTimeInitial = "";

  const setSwitchDate = (ymd, { setInitial = false } = {}) => {
    const safeDate = clampYmdToToday(ymd);
    if (!safeDate) return;
    if (switchDateInput) switchDateInput.value = safeDate;
    if (switchDateDisplay) switchDateDisplay.value = formatYmdDisplay(safeDate);
    if (setInitial) switchDateInitial = safeDate;
  };

  const openSwitchDatePicker = () => {
    const current = clampYmdToToday(switchDateInput?.value);
    openDatePickerSingle(current, switchDateDisplay, (next) => setSwitchDate(next));
  };

  if (switchDateDisplay) {
    switchDateDisplay.addEventListener("click", openSwitchDatePicker);
    switchDateDisplay.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openSwitchDatePicker();
      }
    });
  }

  const openSwitchModal = ({ sourceId, targetId, amount, date, transferId, isCycleTopup } = {}) => {
    if (!switchModal) return;
    if (switchForm) switchForm.reset();
    const isEditMode = !!transferId;
    if (switchTitle) switchTitle.textContent = transferId ? "Edit Switch" : "Switch Balance";
    const msg = $("switchMsg");
    if (msg) msg.textContent = "";
    state.editing_transfer_id = transferId || null;
    if (deleteSwitchBtn) {
      deleteSwitchBtn.hidden = !transferId;
      deleteSwitchBtn.dataset.transferId = transferId || "";
    }

    const fallbackSource =
      sourceId || state.account_id || state.primary_account_id || state.accounts[0]?.account_id;
    updateSwitchTargets(fallbackSource || undefined);
    if (switchFromSelect && fallbackSource) switchFromSelect.value = fallbackSource;
    if (switchToSelect && targetId) switchToSelect.value = targetId;

    if (switchAmountInput) {
      switchAmountInput.value = amount ? fmtIDR(amount) : "";
    }
    if (switchTopupField) switchTopupField.hidden = isEditMode;
    if (switchTopupFlag) switchTopupFlag.checked = isEditMode ? false : !!isCycleTopup;

    const ymd = date ? isoToLocalYMD(date) : clampYmdToToday();
    switchTimeInitial = date ? (isoToLocalTimeWithSeconds(date) || nowTimeWithSeconds()) : nowTimeWithSeconds();
    setSwitchDate(ymd, { setInitial: true });

    switchModal.hidden = false;
  };

  const openSwitchEdit = async (tx) => {
    if (!tx?.transfer_id) return;
    try {
      const detail = await api.get(`/api/switch/${tx.transfer_id}`);
      openSwitchModal({
        sourceId: detail.source_account_id,
        targetId: detail.target_account_id,
        amount: detail.amount,
        date: detail.date,
        transferId: detail.transfer_id,
        isCycleTopup: detail.is_cycle_topup,
      });
    } catch (err) {
      alert(err.message || "Failed to load switch details");
    }
  };

  const closeSwitchModal = () => {
    if (switchModal) switchModal.hidden = true;
    state.editing_transfer_id = null;
    if (deleteSwitchBtn) {
      deleteSwitchBtn.hidden = true;
      deleteSwitchBtn.dataset.transferId = "";
    }
  };

  const switchBtn = $("switchBtn");
  if (switchBtn) {
    switchBtn.addEventListener("click", () => {
      if (!requireAccountScope("Select an account to switch balances.")) return;
      openSwitchModal();
    });
  }
  const closeSwitchBtn = $("closeSwitchModal");
  if (closeSwitchBtn) closeSwitchBtn.addEventListener("click", closeSwitchModal);
  if (switchModal) {
    switchModal.addEventListener("click", (e) => e.target === switchModal && closeSwitchModal());
  }

  if (switchFromSelect) {
    switchFromSelect.addEventListener("change", (e) => {
      const sourceId = e.target.value || null;
      updateSwitchTargets(sourceId || undefined);
    });
  }

  if (switchForm) {
    switchForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const source = switchFromSelect?.value || state.account_id;
      const target = switchToSelect?.value || "";
      const amount = parseAmount(switchAmountInput?.value || "");
      const msg = $("switchMsg");
      if (msg) msg.textContent = "";
      if (!source || !target || amount <= 0) {
        if (msg) msg.textContent = "Select source, target, and amount.";
        return;
      }
      if (source === target) {
        if (msg) msg.textContent = "Source and target must differ.";
        return;
      }

      const selectedDate = clampYmdToToday(switchDateInput?.value);
      let datePayload = undefined;
      if (selectedDate) {
        if (state.editing_transfer_id) {
          if (selectedDate !== switchDateInitial) {
            datePayload = ymdTimeToIso(selectedDate, switchTimeInitial || nowTimeWithSeconds());
          }
        } else {
          datePayload = ymdTimeToIso(selectedDate, switchTimeInitial || nowTimeWithSeconds());
        }
      }

      try {
        if (state.editing_transfer_id) {
          await api.put(`/api/switch/${state.editing_transfer_id}`, {
            source_account_id: source,
            target_account_id: target,
            amount,
            date: datePayload,
          });
        } else {
          const isCycleTopup = !!switchTopupFlag?.checked;
          await api.post("/api/switch", {
            source_account_id: source,
            target_account_id: target,
            amount,
            date: datePayload,
            is_cycle_topup: isCycleTopup,
          });
        }
        closeSwitchModal();
        markSummaryStale();
        await reloadLedgerWithDefaultStale();
      } catch (err) {
        if (msg) msg.textContent = err.message || "Switch failed";
      }
    });
  }

  if (deleteSwitchBtn) {
    deleteSwitchBtn.addEventListener("click", async () => {
      const id = deleteSwitchBtn.dataset.transferId || state.editing_transfer_id;
      if (!id) return;
      if (!confirm("Delete this switch?")) return;
      try {
        await api.del(`/api/switch/${id}`);
        closeSwitchModal();
        markSummaryStale();
        await reloadLedgerWithDefaultStale();
      } catch (err) {
        const msg = $("switchMsg");
        if (msg) msg.textContent = err.message || "Delete failed";
      }
    });
  }

  const exportModal = $("exportModal");
  const exportAccountSelect = $("exportAccountSelect");
  const exportDay = $("exportDay");
  const exportFormat = $("exportFormat");
  const exportPreview = $("exportRangePreview");
  const exportMsg = $("exportMsg");

  const exportCount = $("exportCountPreview");
  const exportIn = $("exportInPreview");
  const exportOut = $("exportOutPreview");
  const exportNet = $("exportNetPreview");
  const setPreviewValue = (el, value) => {
    if (el) el.textContent = value;
  };

  const updateExportPreview = async () => {
    if (!exportDay || !exportPreview) return;
    const day = Number(exportDay.value);
    if (!day || day < 1 || day > 31) {
      exportPreview.textContent = "Enter a day between 1 and 31.";
      setPreviewValue(exportCount, "-");
      setPreviewValue(exportIn, "-");
      setPreviewValue(exportOut, "-");
      setPreviewValue(exportNet, "-");
      return;
    }

    const selectedAccount = exportAccountSelect?.value || "all";
    const scope = selectedAccount === "all" ? "all" : "account";
    const acc = scope === "account" ? `&account_id=${encodeURIComponent(selectedAccount)}` : "";
    exportPreview.textContent = "Loading preview...";
    try {
      const res = await api.get(
        `/api/export/preview?day=${encodeURIComponent(day)}&scope=${encodeURIComponent(scope)}${acc}`
      );
      const fromDate = ymdToDate(res?.range?.from);
      const toDate = ymdToDate(res?.range?.to);
      if (fromDate && toDate) {
        exportPreview.textContent = `Range: ${formatDisplayDate(fromDate)} - ${formatDisplayDate(toDate)}`;
      } else {
        exportPreview.textContent = "Range unavailable.";
      }
      const summary = res?.summary || {};
      setPreviewValue(exportCount, String(summary.count ?? 0));
      setPreviewValue(exportIn, displayMoney(summary.total_in ?? 0));
      setPreviewValue(exportOut, displayMoney(summary.total_out ?? 0));
      setPreviewValue(exportNet, displayMoney(summary.net ?? 0));
    } catch (err) {
      exportPreview.textContent = err.message || "Preview unavailable";
      setPreviewValue(exportCount, "-");
      setPreviewValue(exportIn, "-");
      setPreviewValue(exportOut, "-");
      setPreviewValue(exportNet, "-");
    }
  };

  const hideBtn = $("hideBalancesBtn");
  const mobileHideToggle = $("mobileHideBalancesToggle");
  const hideIcon = `<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><circle cx="12" cy="12" r="3" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
  const showIcon = `<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false"><path d="M17.94 17.94A10.94 10.94 0 0 1 12 20c-7 0-11-8-11-8a21.77 21.77 0 0 1 5.06-6.94" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M9.9 4.24A10.94 10.94 0 0 1 12 4c7 0 11 8 11 8a21.77 21.77 0 0 1-3.24 4.24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M14.12 14.12a3 3 0 0 1-4.24-4.24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/><path d="M1 1l22 22" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
  const updateHideBtn = () => {
    const isHidden = state.hide_balances;
    const apply = (btn) => {
      if (!btn) return;
      btn.innerHTML = isHidden ? showIcon : hideIcon;
      btn.title = isHidden ? "Show balances" : "Hide balances";
      btn.setAttribute("aria-label", btn.title);
      btn.setAttribute("aria-pressed", isHidden ? "true" : "false");
    };
    apply(hideBtn);
    if (mobileHideToggle) {
      mobileHideToggle.checked = isHidden;
    }
  };
  const savedHide = localStorage.getItem("hide_balances") === "true";
  state.hide_balances = savedHide;
  updateHideBtn();
  const handleHideToggle = (nextValue) => {
    state.hide_balances = typeof nextValue === "boolean" ? nextValue : !state.hide_balances;
    localStorage.setItem("hide_balances", String(state.hide_balances));
    updateHideBtn();
    renderLedger(activeRows());
    renderSummary();
    renderAnalysis();
    if (exportModal && !exportModal.hidden) {
      updateExportPreview();
    }
  };
  if (hideBtn) hideBtn.addEventListener("click", () => handleHideToggle());
  if (mobileHideToggle) {
    mobileHideToggle.addEventListener("change", (e) => handleHideToggle(e.target.checked));
  }

  const openExportModal = () => {
    if (!exportModal || !exportDay || !exportFormat || !exportAccountSelect) return;
    updateExportAccounts();
    const savedDay = localStorage.getItem("export_day");
    const savedFormat = localStorage.getItem("export_format");
    const savedAccount = localStorage.getItem("export_account");
    const fallbackAccount =
      state.scope === "account" && state.account_id ? state.account_id : "all";
    exportDay.value = savedDay || "25";
    exportFormat.value = savedFormat || "pdf";
    const targetAccount = savedAccount || fallbackAccount;
    const hasOption = exportAccountSelect.querySelector(`option[value="${targetAccount}"]`);
    exportAccountSelect.value = hasOption ? targetAccount : "all";
    if (exportMsg) exportMsg.textContent = "";
    updateExportPreview();
    exportModal.hidden = false;
  };

  const closeExportModal = () => {
    if (exportModal) exportModal.hidden = true;
  };

  const exportBtn = $("exportBtn");
  if (exportBtn) exportBtn.addEventListener("click", openExportModal);
  const closeExportBtn = $("closeExportModal");
  if (closeExportBtn) closeExportBtn.addEventListener("click", closeExportModal);
  if (exportModal) {
    exportModal.addEventListener("click", (e) => e.target === exportModal && closeExportModal());
  }
  if (exportDay) exportDay.addEventListener("input", updateExportPreview);
  if (exportFormat) exportFormat.addEventListener("change", updateExportPreview);
  if (exportAccountSelect) exportAccountSelect.addEventListener("change", updateExportPreview);

  const exportForm = $("exportForm");
  if (exportForm) {
    exportForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      if (!exportDay || !exportFormat || !exportAccountSelect) return;
      const day = Number(exportDay.value);
      if (!day || day < 1 || day > 31) {
        if (exportMsg) exportMsg.textContent = "Enter a valid day of month.";
        return;
      }
      if (state.currency === "USD" && !state.fx_rate) {
        if (exportMsg) exportMsg.textContent = "USD rate unavailable. Switch to IDR or update rate.";
        return;
      }

      if (exportMsg) exportMsg.textContent = "";
      const selectedAccount = exportAccountSelect.value || "all";
      const scope = selectedAccount === "all" ? "all" : "account";
      const params = new URLSearchParams({
        day: String(day),
        format: exportFormat.value || "pdf",
        scope,
        currency: state.currency,
      });
      if (scope === "account" && selectedAccount) {
        params.set("account_id", selectedAccount);
      }
      if (state.currency === "USD" && state.fx_rate) {
        params.set("fx_rate", String(state.fx_rate));
      }

      const link = document.createElement("a");
      link.href = `/api/export?${params.toString()}`;
      link.download = `ledger_export.${exportFormat.value || "pdf"}`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      closeExportModal();
      localStorage.setItem("export_day", String(day));
      localStorage.setItem("export_format", exportFormat.value || "pdf");
      localStorage.setItem("export_account", selectedAccount);
    });
  }

  const ledgerBody = $("ledgerBody");
  if (ledgerBody) {
    ledgerBody.addEventListener("click", (e) => {
      const row = e.target.closest("tr[data-tx-id]");
      if (!row) return;
      const tx = activeRows().find((r) => r.transaction_id === row.dataset.txId);
      if (!tx) return;
      if (tx.is_transfer && tx.transfer_id) {
        openSwitchEdit(tx);
      } else {
        openModal(tx);
      }
    });
  }

  $("txForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    $("txMsg").textContent = "";
    const formData = new FormData(e.target);
    const receiptFile = formData.get("receipt_file");
    const receiptCategoryValue = String(formData.get("receipt_category") || "").trim() || "general";
    formData.delete("receipt_file");
    formData.delete("receipt_category");

    const body = Object.fromEntries(formData.entries());
    body.is_cycle_topup = !!txTopupFlag?.checked;
    if (!body.transaction_id && state.editing_tx_id) {
      body.transaction_id = state.editing_tx_id;
    }
    body.amount = parseAmount(body.amount);

    const isEdit = !!body.transaction_id;
    if (txDateInput) {
      const selectedDate = clampYmdToToday(txDateInput.value);
      if (isEdit) {
        if (selectedDate && selectedDate !== txDateInitial) {
          body.date = ymdTimeToIso(selectedDate, txTimeInitial || nowTimeWithSeconds());
        } else {
          delete body.date;
        }
      } else {
        body.date = ymdTimeToIso(selectedDate, txTimeInitial || nowTimeWithSeconds());
      }
    } else if (!isEdit) {
      body.date = new Date().toISOString();
    }

    try {
      let transactionId = body.transaction_id || null;
      if (isEdit) {
        await api.put(`/api/transactions/${body.transaction_id}`, body);
      } else {
        const created = await api.post("/api/transactions", body);
        transactionId = created?.transaction_id || null;
      }

      if (receiptFile instanceof File && receiptFile.size > 0) {
        if (!transactionId) {
          throw new Error("Transaction created but missing transaction id");
        }
        const receiptForm = new FormData();
        receiptForm.append("file", receiptFile);
        receiptForm.append("category", receiptCategoryValue);
        await api.postForm(`/api/transactions/${encodeURIComponent(transactionId)}/receipt`, receiptForm);
      }

      closeModal();
      markSummaryStale();
      await reloadLedgerWithDefaultStale();
    } catch (err) {
      $("txMsg").textContent = err.message || "Failed";
    }
  });

  // Delete Transaction
  $("deleteTxBtn").addEventListener("click", async () => {
    const id =
      (txIdInput && txIdInput.value) ||
      state.editing_tx_id ||
      $("deleteTxBtn").dataset.txId ||
      "";
    if (!id) return;
    if (!confirm("Delete this transaction?")) return;

    try {
      await api.del(`/api/transactions/${id}`);
      closeModal();
      markSummaryStale();
      await reloadLedgerWithDefaultStale();
    } catch (err) {
      $("txMsg").textContent = err.message || "Delete failed";
    }
  });

  // API key modal
  const apiKeyModal = $("apiKeyModal");
  const apiKeyMaskedInput = $("apiKeyMasked");
  const apiKeyPlainWrap = $("apiKeyPlainWrap");
  const apiKeyPlainInput = $("apiKeyPlain");
  const apiKeyMsg = $("apiKeyMsg");
  const copyApiKeyBtn = $("copyApiKeyBtn");
  const resetApiKeyBtn = $("resetApiKeyBtn");

  const resetApiKeyView = () => {
    if (apiKeyPlainInput) apiKeyPlainInput.value = "";
    if (apiKeyPlainWrap) apiKeyPlainWrap.hidden = true;
    if (copyApiKeyBtn) copyApiKeyBtn.hidden = true;
  };

  const closeApiKeyModal = () => {
    if (apiKeyModal) apiKeyModal.hidden = true;
    if (apiKeyMsg) apiKeyMsg.textContent = "";
    resetApiKeyView();
  };

  const loadApiKey = async () => {
    if (apiKeyMaskedInput) apiKeyMaskedInput.value = "Loading...";
    if (apiKeyMsg) apiKeyMsg.textContent = "";
    resetApiKeyView();
    try {
      const res = await api.get("/api/api-key");
      const key = res?.api_key || {};
      if (apiKeyMaskedInput) apiKeyMaskedInput.value = key.key_masked || "Unavailable";
      if (apiKeyMsg) {
        apiKeyMsg.textContent = key.last_used_at
          ? `Last used: ${isoToLocalDisplay(key.last_used_at)}`
          : "API key is active and ready.";
      }
    } catch (err) {
      const msg = err?.message || "Failed to load API key";
      if (apiKeyMaskedInput) {
        apiKeyMaskedInput.value = msg.includes("API key not found") ? "No active API key" : "Unavailable";
      }
      if (apiKeyMsg) {
        apiKeyMsg.textContent = msg.includes("API key not found")
          ? "No API key yet. Click Reset API Key to generate one."
          : msg;
      }
    }
  };

  const openApiKeyModal = async () => {
    if (!apiKeyModal) return;
    apiKeyModal.hidden = false;
    await loadApiKey();
  };

  const copyText = async (text) => {
    if (!text) return false;
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
        return true;
      }
    } catch {
      // fallback below
    }
    const el = document.createElement("textarea");
    el.value = text;
    el.style.position = "fixed";
    el.style.left = "-9999px";
    document.body.appendChild(el);
    el.focus();
    el.select();
    let ok = false;
    try {
      ok = document.execCommand("copy");
    } catch {
      ok = false;
    }
    document.body.removeChild(el);
    return ok;
  };

  if (resetApiKeyBtn) {
    resetApiKeyBtn.addEventListener("click", async () => {
      if (!confirm("Reset API key? Current key will stop working immediately.")) return;
      if (apiKeyMsg) apiKeyMsg.textContent = "";
      resetApiKeyBtn.disabled = true;
      try {
        const res = await api.post("/api/api-key/reset", {});
        if (apiKeyMaskedInput) apiKeyMaskedInput.value = res.masked || "Updated";
        if (apiKeyPlainInput) apiKeyPlainInput.value = res.api_key || "";
        if (apiKeyPlainWrap) apiKeyPlainWrap.hidden = false;
        if (copyApiKeyBtn) copyApiKeyBtn.hidden = !(res.api_key || "");
        if (apiKeyMsg) apiKeyMsg.textContent = "API key reset. Update your integrations now.";
      } catch (err) {
        if (apiKeyMsg) apiKeyMsg.textContent = err.message || "Failed to reset API key";
      } finally {
        resetApiKeyBtn.disabled = false;
      }
    });
  }

  if (copyApiKeyBtn) {
    copyApiKeyBtn.addEventListener("click", async () => {
      const text = apiKeyPlainInput?.value || "";
      const ok = await copyText(text);
      if (apiKeyMsg) apiKeyMsg.textContent = ok ? "New API key copied." : "Copy failed.";
    });
  }

  const apiKeyBtn = $("apiKeyBtn");
  if (apiKeyBtn) apiKeyBtn.addEventListener("click", openApiKeyModal);
  const mobileApiKeyBtn = $("mobileApiKeyBtn");
  if (mobileApiKeyBtn) {
    mobileApiKeyBtn.addEventListener("click", () => {
      document.body.classList.remove("menu-open");
      openApiKeyModal();
    });
  }
  const closeApiKeyBtn = $("closeApiKeyModal");
  if (closeApiKeyBtn) closeApiKeyBtn.addEventListener("click", closeApiKeyModal);
  if (apiKeyModal) {
    apiKeyModal.addEventListener("click", (e) => e.target === apiKeyModal && closeApiKeyModal());
  }

  // Accounts modal
  const setAccountFormTabLabel = (label) => {
    const tabBtn = $("accountFormTabBtn");
    if (tabBtn) tabBtn.textContent = label;
  };

  const resetAccountForm = () => {
    const budgetInput = $("accountBudget");
    const budgetField = $("accountBudgetField");
    const initialField = $("accountInitialField");
    const initialInput = $("accountInitialBalance");
    $("accountId").value = "";
    $("newAccountName").value = "";
    if (initialInput) {
      initialInput.value = "";
      initialInput.dataset.rawValue = "";
    }
    if (initialField) initialField.hidden = false;
    if (budgetInput) {
      budgetInput.value = "";
      budgetInput.dataset.rawValue = "";
      budgetInput.disabled = false;
      budgetInput.placeholder = "0";
    }
    if (budgetField) budgetField.hidden = false;
    $("accountFormTitle").textContent = "New Account Details";
    $("cancelEditBtn").hidden = true;
    $("accountMsg").textContent = "";
    setAccountFormTabLabel("New Account");
  };

  const switchAccountsTab = (tab) => {
    document.querySelectorAll(".nav-btn").forEach((b) => b.classList.toggle("active", b.dataset.accTab === tab));
    ["list", "create"].forEach((t) => {
      $("acc-tab-" + t).classList.toggle("hidden", t !== tab);
    });
  };

  const openAccountsModal = (tab = "list") => {
    switchAccountsTab(tab);
    $("accountsModal").hidden = false;
  };

  const closeAccountsModal = () => {
    $("accountsModal").hidden = true;
    resetAccountForm();
  };

  const startAccountEdit = (acc) => {
    if (!acc) return;
    const budgetField = $("accountBudgetField");
    const initialField = $("accountInitialField");
    const initialInput = $("accountInitialBalance");
    $("accountId").value = acc.account_id;
    $("newAccountName").value = acc.account_name;
    if (initialInput) {
      initialInput.value = "";
      initialInput.dataset.rawValue = "";
    }
    if (initialField) initialField.hidden = true;
    $("accountFormTitle").textContent = "Edit Account Details";
    $("cancelEditBtn").hidden = false;
    setAccountFormTabLabel("Edit Account");
    const budgetInput = $("accountBudget");
    if (budgetInput) {
      budgetInput.disabled = false;
      budgetInput.placeholder = "0";
      if (budgetField) budgetField.hidden = false;
      syncAccountBudgetInput(acc.account_id);
    }
    switchAccountsTab("create");
  };

  resetAccountForm();

  const manageBtn = $("manageAccountsBtn");
  if (manageBtn) manageBtn.addEventListener("click", () => openAccountsModal("list"));
  const mobileManageBtn = $("mobileManageAccountsBtn");
  if (mobileManageBtn) {
    mobileManageBtn.addEventListener("click", () => {
      document.body.classList.remove("menu-open");
      openAccountsModal("list");
    });
  }
  const emptyAccountsCreateBtn = $("emptyAccountsCreateBtn");
  if (emptyAccountsCreateBtn) {
    emptyAccountsCreateBtn.addEventListener("click", () => openAccountsModal("create"));
  }
  $("closeAccountsModal").addEventListener("click", closeAccountsModal);
  $("accountsModal").addEventListener("click", (e) => e.target === $("accountsModal") && closeAccountsModal());

  document.querySelectorAll(".nav-btn").forEach((b) =>
    b.addEventListener("click", () => {
      if (b.dataset.accTab === "list") {
        resetAccountForm();
      }
      switchAccountsTab(b.dataset.accTab);
    })
  );

  const accountsBody = $("accountsBody");
  accountsBody.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const id = btn.dataset.id;
    const action = btn.dataset.action;

    if (action === "edit") {
      const acc = state.accounts.find((a) => a.account_id === id);
      if (!acc) return;
      startAccountEdit(acc);
    }

    if (action === "delete") {
      if (!confirm("Delete this account and its transactions?")) return;
      try {
        await api.del(`/api/accounts/${id}`);
        await loadAccounts();
        markSummaryStale();
        await reloadLedgerWithDefaultStale();
      } catch (err) {
        alert(err.message || "Delete failed");
      }
    }
  });

  $("cancelEditBtn").addEventListener("click", () => {
    resetAccountForm();
    switchAccountsTab("list");
  });

  $("accountForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    $("accountMsg").textContent = "";
    const account_name = ($("newAccountName").value || "").trim();
    const account_id = $("accountId").value || null;
    const budgetInput = $("accountBudget");
    const budgetRaw = budgetInput && !budgetInput.disabled ? (budgetInput.value || "").trim() : "";
    const budgetAmount = budgetRaw ? parseAmount(budgetRaw) : null;
    const budgetMonth = getBudgetMonth();
    const initialInput = $("accountInitialBalance");
    const initialRaw = initialInput ? (initialInput.value || "").trim() : "";
    const initialAmount = initialRaw ? parseAmount(initialRaw) : 0;

    if (!account_name) return;

    const payload = { account_name };
    if (!account_id && initialAmount > 0) {
      payload.initial_balance = initialAmount;
    }

    try {
      let nextAccountId = account_id;
      if (account_id) {
        await api.put(`/api/accounts/${account_id}`, payload);
      } else {
        const res = await api.post("/api/accounts", payload);
        nextAccountId = res.account_id || null;
      }
      if (nextAccountId) {
        const prevBudget = state.budgets_by_account[nextAccountId]?.amount ?? null;
        const prevBudgetId = state.budgets_by_account[nextAccountId]?.budget_id ?? null;
        if (budgetAmount == null) {
          if (prevBudgetId) {
            await api.del(`/api/budgets/${prevBudgetId}`);
          }
          delete state.budgets_by_account[nextAccountId];
        } else if (budgetAmount !== prevBudget || !prevBudgetId) {
          const res = await api.post("/api/budgets", {
            account_id: nextAccountId,
            month: budgetMonth,
            amount: budgetAmount,
          });
          state.budgets_by_account[nextAccountId] = {
            amount: budgetAmount,
            budget_id: res.budget_id || prevBudgetId || null,
          };
        }
      }
      resetAccountForm();
      await loadAccounts();
      markSummaryStale();
      await reloadLedgerWithDefaultStale();
      switchAccountsTab("list");
    } catch (err) {
      $("accountMsg").textContent = err.message || "Save failed";
    }
  });

  if (!state.accounts.length) {
    openAccountsModal("create");
  }

}

(async function main() {
  await loadMe();
  loadFxCache();
  await loadAccounts();

  state.summary_month = currentMonthYM();

  // default date range 30 days
  state.from = minusDaysYMD(30);
  state.to = todayYMD();
  $("fromDate").value = state.from;
  $("toDate").value = state.to;

  bindEvents();
  setActiveTab("summary");
  loadFxOnStartup();
})();
