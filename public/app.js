const $ = (id) => document.getElementById(id);

const state = {
  me: null,
  accounts: [],
  main_account_id: null,
  scope: "all",
  account_id: null,
  from: null,
  to: null,
  rows: [],
  total_asset: 0,
  unallocated_balance: 0,
  search_query: "",
  hide_balances: false,
  sort_order: "desc",
  fx_rate: null,
  fx_updated_at: null,
  currency: "IDR",
  editing_tx_id: null,
};

const api = {
  async get(url) {
    const r = await fetch(url, { credentials: "include" });
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
};

const fmtIDR = (n) => (n || 0).toLocaleString("id-ID");
const fmtIDRCurrency = (n) => `Rp ${fmtIDR(n)}`;
const fmtUSD = (n) =>
  `$${Number(n || 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

const fmtMoney = (n) => {
  if (state.currency === "USD") {
    if (!state.fx_rate) return "$-";
    return fmtUSD(Number(n || 0) * state.fx_rate);
  }
  return fmtIDRCurrency(n);
};

const displayMoney = (n) => (state.hide_balances ? "***" : fmtMoney(n));

const isMobile = () => window.matchMedia("(max-width: 980px)").matches;

const setFxRateText = (text) => {
  const el = $("fxRate");
  if (el) el.textContent = text;
};

const parseAmount = (value) => {
  const cleaned = String(value || "").replace(/[^\d]/g, "");
  return cleaned ? Number(cleaned) : 0;
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

const normalizeSearch = (value) => String(value || "").toLowerCase().replace(/[^a-z0-9]/g, "");

const fuzzyMatch = (query, text) => {
  if (!query) return true;
  let qi = 0;
  for (let i = 0; i < text.length && qi < query.length; i += 1) {
    if (text[i] === query[qi]) qi += 1;
  }
  return qi === query.length;
};

const minusDaysYMD = (days) => {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
};

const defaultTxDatetimeLocal = () => {
  const d = new Date();
  d.setMinutes(d.getMinutes() - d.getTimezoneOffset());
  return d.toISOString().slice(0, 16); // YYYY-MM-DDTHH:MM
};

const formatRangeText = (from, to) => {
  const fmtDate = (ymd) => (ymd ? ymd.split("-").reverse().join("/") : "");
  return `${fmtDate(from)} - ${fmtDate(to)}`;
};

const formatRangeLabel = (from, to) => {
  const fmtDate = (ymd) => (ymd ? ymd.split("-").reverse().join("/") : "");
  return `
    <span class="range-date" data-date-target="from">${fmtDate(from)}</span>
    <span class="range-separator">-</span>
    <span class="range-date" data-date-target="to">${fmtDate(to)}</span>
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
    btn.classList.toggle("active", btn.dataset.currency === state.currency);
  });

  const rateEl = $("fxRate");
  if (!rateEl) return;
  if (!state.fx_rate) {
    rateEl.textContent = state.currency === "USD" ? "Rate: loading..." : "Rate: -";
    return;
  }
  const idrPerUsd = Math.round(1 / state.fx_rate);
  const date = state.fx_updated_at ? ` (${state.fx_updated_at})` : "";
  rateEl.textContent = `Rate: 1 USD = ${fmtIDRCurrency(idrPerUsd)}${date}`;
}

function renderAccounts() {
  const opts = state.accounts
    .map((a) => `<option value="${a.account_id}">${a.account_name}</option>`)
    .join("");

  $("txAccountSelect").innerHTML = opts;

  $("accountsBody").innerHTML = state.accounts
    .map((a) => {
      const isMain = a.account_id === state.main_account_id;
      const tag = isMain ? `<span class="tag">Main</span>` : "";
      const deleteBtn = isMain
        ? ""
        : `<button class="btn small danger" data-action="delete" data-id="${a.account_id}">Delete</button>`;
      return `<tr>
        <td>${a.account_name}${tag}</td>
        <td class="num">
          <div class="actions">
            <button class="btn small" data-action="edit" data-id="${a.account_id}">Edit</button>
            ${deleteBtn}
          </div>
        </td>
      </tr>`;
    })
    .join("");

  const filterAccounts = state.accounts.filter((a) => a.account_id !== state.main_account_id);
  const accountList = $("accountList");
  if (accountList) {
    const listItems = [
      `<label class="account-option">
        <input type="radio" name="accountFilter" value="all" ${state.scope === "all" ? "checked" : ""} />
        <span>Main Account</span>
        <span class="tag">Main</span>
      </label>`,
      ...filterAccounts.map((a) => {
        const checked = state.scope === "account" && state.account_id === a.account_id ? "checked" : "";
        return `<label class="account-option">
          <input type="radio" name="accountFilter" value="${a.account_id}" ${checked} />
          <span>${a.account_name}</span>
        </label>`;
      }),
    ];
    accountList.innerHTML = listItems.join("");
  }

  const mobileSelect = $("mobileAccountSelect");
  if (mobileSelect) {
    const mobileOptions = [
      `<option value="all">Main Account</option>`,
      ...filterAccounts.map((a) => `<option value="${a.account_id}">${a.account_name}</option>`),
    ];
    mobileSelect.innerHTML = mobileOptions.join("");
    mobileSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
  }

  const titleSelect = $("ledgerTitleSelect");
  if (titleSelect) {
    const titleOptions = [
      `<option value="all">Main Account</option>`,
      ...filterAccounts.map((a) => `<option value="${a.account_id}">${a.account_name}</option>`),
    ];
    titleSelect.innerHTML = titleOptions.join("");
    titleSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
  }

  const allocateSelect = $("allocateAccountSelect");
  if (allocateSelect) {
    const allocateOptions = [
      `<option value="">Select account</option>`,
      ...filterAccounts.map((a) => `<option value="${a.account_id}">${a.account_name}</option>`),
    ];
    allocateSelect.innerHTML = allocateOptions.join("");
  }

  updateSwitchTargets();
  updateExportAccounts();
}

function updateTotals(summary) {
  state.total_asset = Number(summary?.total_asset || 0);
  state.unallocated_balance = Number(summary?.unallocated || 0);
  state.summary_accounts = summary?.accounts || [];
}

function updateSwitchTargets() {
  const switchToSelect = $("switchToSelect");
  if (!switchToSelect) return;
  const sourceId = state.account_id;
  const targetOptions = [
    `<option value="">Select account</option>`,
    ...state.accounts
      .filter(
        (a) => a.account_id !== state.main_account_id && (!sourceId || a.account_id !== sourceId)
      )
      .map((a) => `<option value="${a.account_id}">${a.account_name}</option>`),
  ];
  switchToSelect.innerHTML = targetOptions.join("");
}

function updateExportAccounts() {
  const exportAccountSelect = $("exportAccountSelect");
  if (!exportAccountSelect) return;
  const nonMain = state.accounts
    .filter((a) => a.account_id !== state.main_account_id)
    .sort((a, b) => a.account_name.localeCompare(b.account_name));
  const options = [
    `<option value="all">Main Account</option>`,
    ...nonMain.map((a) => `<option value="${a.account_id}">${a.account_name}</option>`),
  ];
  exportAccountSelect.innerHTML = options.join("");
}

function filterRowsBySearch(rows, query) {
  const cleaned = normalizeSearch(query);
  if (!cleaned) return rows;
  return rows.filter((r) => {
    const haystack = normalizeSearch(
      [
        r.transaction_name,
        r.account_name,
        isoToLocalDisplay(r.date),
        r.debit,
        r.credit,
        r.balance,
      ].join(" ")
    );
    return fuzzyMatch(cleaned, haystack);
  });
}

function renderLedger(rows) {
  const body = $("ledgerBody");
  const table = $("ledgerTable");
  const isAll = state.scope === "all";
  const isMainAccount = state.scope === "account" && state.account_id === state.main_account_id;
  const showAllocate = isAll || isMainAccount;
  const showSwitch = state.scope === "account" && state.account_id && state.account_id !== state.main_account_id;
  const showAssetSummary = isAll || isMainAccount;
  const allocateBtn = $("allocateBtn");
  const switchBtn = $("switchBtn");
  const fabAllocateOption = $("fabAllocateOption");
  const fabSwitchOption = $("fabSwitchOption");
  if (allocateBtn) allocateBtn.hidden = !showAllocate;
  if (switchBtn) switchBtn.hidden = !showSwitch;
  if (fabAllocateOption) fabAllocateOption.hidden = !showAllocate;
  if (fabSwitchOption) fabSwitchOption.hidden = !showSwitch;
  updateSwitchTargets();

  const totalLabel = $("ledgerTotalLabel");
  const diffLabel = $("ledgerDiffLabel");
  const diffBlock = $("ledgerDiffBlock");
  const accountBalance = state.summary_accounts?.find((a) => a.account_id === state.account_id)?.balance ?? 0;
  if (showAssetSummary) {
    if (totalLabel) totalLabel.textContent = "Total Asset";
    if (diffLabel) diffLabel.textContent = "Unallocated Balance";
    $("ledgerTotal").textContent = displayMoney(state.total_asset || 0);
    $("ledgerDiff").textContent = displayMoney(state.unallocated_balance || 0);
    if (diffBlock) diffBlock.hidden = false;
  } else {
    if (totalLabel) totalLabel.textContent = "Total Balance";
    $("ledgerTotal").textContent = displayMoney(accountBalance || 0);
    if (diffBlock) diffBlock.hidden = true;
  }
  
  // Update Title
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

  // Update Range Display
  renderRangeDisplay();
  const dateHeader = $("ledgerDateHeader");
  if (dateHeader) {
    dateHeader.setAttribute("aria-sort", state.sort_order === "asc" ? "ascending" : "descending");
  }

  const filteredRows = filterRowsBySearch(rows, state.search_query);
  const sortedRows = [...filteredRows].sort((a, b) => {
    const da = new Date(a.date).getTime();
    const db = new Date(b.date).getTime();
    return state.sort_order === "asc" ? da - db : db - da;
  });

  if (!sortedRows.length) {
    const colSpan = isAll ? 7 : 6;
    const message = state.search_query
      ? "No transactions match your search."
      : "No transactions yet for this range.";
    body.innerHTML = `<tr><td colspan="${colSpan}" class="empty-row">${message}</td></tr>`;
    return;
  }

  body.innerHTML = sortedRows
    .map(
      (r, idx) => {
        const accCell = `<td class="account-cell">${isAll ? r.account_name : ""}</td>`;
        return `<tr data-tx-id="${r.transaction_id}" style="cursor:pointer">
        <td>${idx + 1}</td>
        ${accCell}
        <td>${isoToLocalDisplay(r.date)}</td>
        <td>${r.transaction_name}</td>
        <td class="num amount-in">${r.debit ? displayMoney(r.debit) : ""}</td>
        <td class="num amount-out">${r.credit ? displayMoney(r.credit) : ""}</td>
        <td class="num"><b>${displayMoney(r.balance)}</b></td>
      </tr>`;
      }
    )
    .join("");

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

async function loadAccounts() {
  const res = await api.get("/api/accounts");
  state.accounts = res.accounts || [];
  const main = state.accounts.find((a) => !a.parent_account_id);
  state.main_account_id = main?.account_id || null;

  renderAccounts();

  if (state.scope === "account") {
    const exists = state.accounts.find((a) => a.account_id === state.account_id);
    if (!exists || state.account_id === state.main_account_id) {
      state.scope = "all";
      state.account_id = null;
    }
  }
}

async function loadLedger() {
  const scope = state.scope;
  const acc = scope === "account" ? `&account_id=${encodeURIComponent(state.account_id || "")}` : "";
  const url =
    `/api/ledger?scope=${encodeURIComponent(scope)}` +
    acc +
    `&from_date=${encodeURIComponent(state.from)}` +
    `&to_date=${encodeURIComponent(state.to)}`;
  const res = await api.get(url);
  state.rows = res.rows || [];
  updateTotals(res.summary);
  renderLedger(state.rows);
}

function bindEvents() {
  const accountList = $("accountList");
  if (accountList) {
    accountList.addEventListener("change", (e) => {
      const input = e.target;
      if (!input || input.name !== "accountFilter") return;
      if (input.value === "all") {
        state.scope = "all";
        state.account_id = null;
      } else {
        state.scope = "account";
        state.account_id = input.value;
      }
      const mobileSelect = $("mobileAccountSelect");
      if (mobileSelect) {
        mobileSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
      }
      loadLedger().catch(console.error);
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

      document.querySelectorAll('input[name="accountFilter"]').forEach((input) => {
        input.checked = input.value === (state.scope === "account" ? state.account_id : "all");
      });

      const titleSelect = $("ledgerTitleSelect");
      if (titleSelect) {
        titleSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
      }

      loadLedger().catch(console.error);
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

      document.querySelectorAll('input[name="accountFilter"]').forEach((input) => {
        input.checked = input.value === (state.scope === "account" ? state.account_id : "all");
      });

      const mobileSelect = $("mobileAccountSelect");
      if (mobileSelect) {
        mobileSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
      }

      loadLedger().catch(console.error);
    });
  }

  const dateHeader = $("ledgerDateHeader");
  if (dateHeader) {
    dateHeader.addEventListener("click", () => {
      state.sort_order = state.sort_order === "asc" ? "desc" : "asc";
      dateHeader.setAttribute("aria-sort", state.sort_order === "asc" ? "ascending" : "descending");
      renderLedger(state.rows);
    });
  }

  const ledgerSearch = $("ledgerSearch");
  if (ledgerSearch) {
    ledgerSearch.value = state.search_query || "";
    ledgerSearch.addEventListener("input", (e) => {
      state.search_query = e.target.value || "";
      renderLedger(state.rows);
    });
  }

  $("fromDate").addEventListener("change", () => {
    state.from = $("fromDate").value;
    renderRangeDisplay();
    loadLedger().catch(console.error);
  });
  $("toDate").addEventListener("change", () => {
    state.to = $("toDate").value;
    renderRangeDisplay();
    loadLedger().catch(console.error);
  });
  
  // Open picker on click
  const showPicker = (e) => { try { e.target.showPicker(); } catch(err) {} };
  $("fromDate").addEventListener("click", showPicker);
  $("toDate").addEventListener("click", showPicker);

  const openFromDatePicker = () => {
    const from = $("fromDate");
    if (!from) return;
    try { from.showPicker(); } catch (err) { from.focus(); }
  };
  const openToDatePicker = () => {
    const to = $("toDate");
    if (!to) return;
    try { to.showPicker(); } catch (err) { to.focus(); }
  };

  const bindRangePicker = (el) => {
    if (!el) return;
    el.setAttribute("role", "button");
    el.setAttribute("tabindex", "0");
    el.setAttribute("title", "Edit date range");
    el.addEventListener("click", (e) => {
      const target = e.target.closest("[data-date-target]");
      if (!target) return openFromDatePicker();
      if (target.dataset.dateTarget === "to") {
        openToDatePicker();
        return;
      }
      openFromDatePicker();
    });
    el.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openFromDatePicker();
      }
    });
  };

  bindRangePicker($("ledgerRangeDisplay"));
  bindRangePicker($("ledgerRangeText"));

  const reloadBtn = $("reloadBtn");
  if (reloadBtn) reloadBtn.addEventListener("click", () => loadLedger().catch(console.error));

  // Theme Toggle
  const themeBtn = $("themeToggleBtn");
  const themeCheckbox = $("mobileThemeToggleCheckbox");
  
  const setTheme = (isDark) => {
    document.body.classList.toggle("dark", isDark);
    themeBtn.textContent = isDark ? "☀️" : "🌙";
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
  const handleCurrencyChange = async (target) => {
      const next = target.dataset.currency;
      if (!next || next === state.currency) return;
      state.currency = next;
      
      // Update UI for all toggle buttons (sync desktop and mobile)
      document.querySelectorAll(".seg-btn").forEach(b => 
          b.classList.toggle("active", b.dataset.currency === state.currency)
      );
      
      if (state.currency === "USD" && isFxStale()) {
        try { await fetchFxRate(); } catch (err) { state.currency = "IDR"; }
      }
      updateFxUI();
      renderLedger(state.rows);
  };

  document.querySelectorAll(".seg-btn").forEach(btn => {
      btn.addEventListener("click", (e) => handleCurrencyChange(e.target));
  });

  const refreshFx = async () => {
    try {
      await fetchFxRate();
      updateFxUI();
      renderLedger(state.rows);
    } catch (err) {
      console.error(err);
    }
  };

  const fxBtn = $("fxRefreshBtn");
  if (fxBtn) fxBtn.addEventListener("click", refreshFx);
  const mobileFxBtn = $("mobileFxRefreshBtn");
  if (mobileFxBtn) mobileFxBtn.addEventListener("click", refreshFx);

  const txForm = $("txForm");
  const txIdInput = document.querySelector('input[name="transaction_id"]');

  const closeModal = () => {
    const modal = $("modal");
    if (modal) modal.hidden = true;
    $("txMsg").textContent = "";
    $("deleteTxBtn").hidden = true;
    $("deleteTxBtn").dataset.txId = "";
    state.editing_tx_id = null;
    if (txForm) txForm.reset();
    if (txIdInput) txIdInput.value = "";
  };

  const openModal = (tx) => {
    const modal = $("modal");
    if (!modal) return;
    $("txMsg").textContent = "";
    $("deleteTxBtn").hidden = true;
    $("txModalTitle").textContent = "Add Transaction";
    if (txForm) txForm.reset();
    if (txIdInput) txIdInput.value = "";
    state.editing_tx_id = null;

    const defaultAccount = state.scope === "account" && state.account_id ? state.account_id : state.main_account_id;
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
      const type = tx.debit ? "debit" : "credit";
      const typeInput = document.querySelector(`input[name="transaction_type"][value="${type}"]`);
      if (typeInput) typeInput.checked = true;
      $("deleteTxBtn").hidden = false;
      $("deleteTxBtn").dataset.txId = tx.transaction_id;
      state.editing_tx_id = tx.transaction_id;
    }

    modal.hidden = false;
  };

  const addBtn = $("addTxBtn");
  if (addBtn) addBtn.addEventListener("click", () => openModal());
  const mobileAddBtn = $("mobileAddBtn");
  const closeFabMenu = () => document.body.classList.remove("fab-open");
  const openFabMenu = () => {
    if (!isMobile()) {
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
      if (action === "add") return openModal();
      if (action === "allocate") return openAllocateModal();
      if (action === "switch") return openSwitchModal();
      if (action === "export") return openExportModal();
    });
  }
  const closeBtn = $("closeModal");
  if (closeBtn) closeBtn.addEventListener("click", closeModal);
  const modal = $("modal");
  if (modal) modal.addEventListener("click", (e) => e.target === modal && closeModal());

  const allocateModal = $("allocateModal");
  const openAllocateModal = () => {
    if (!allocateModal) return;
    const form = $("allocateForm");
    if (form) form.reset();
    const msg = $("allocateMsg");
    if (msg) msg.textContent = "";
    allocateModal.hidden = false;
  };
  const closeAllocateModal = () => {
    if (allocateModal) allocateModal.hidden = true;
  };
  const allocateBtn = $("allocateBtn");
  if (allocateBtn) allocateBtn.addEventListener("click", openAllocateModal);
  const closeAllocateBtn = $("closeAllocateModal");
  if (closeAllocateBtn) closeAllocateBtn.addEventListener("click", closeAllocateModal);
  if (allocateModal) {
    allocateModal.addEventListener("click", (e) => e.target === allocateModal && closeAllocateModal());
  }

  const allocateForm = $("allocateForm");
  if (allocateForm) {
    allocateForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const target = $("allocateAccountSelect").value;
      const amount = parseAmount($("allocateAmount").value);
      const msg = $("allocateMsg");
      if (msg) msg.textContent = "";
      if (!target || amount <= 0) {
        if (msg) msg.textContent = "Select account and amount.";
        return;
      }
      try {
        await api.post("/api/allocate", { target_account_id: target, amount });
        closeAllocateModal();
        await loadLedger();
      } catch (err) {
        if (msg) msg.textContent = err.message || "Allocate failed";
      }
    });
  }

  const switchModal = $("switchModal");
  const openSwitchModal = () => {
    if (!switchModal) return;
    if (!state.account_id || state.account_id === state.main_account_id) return;
    const form = $("switchForm");
    if (form) form.reset();
    const msg = $("switchMsg");
    if (msg) msg.textContent = "";
    updateSwitchTargets();
    switchModal.hidden = false;
  };
  const closeSwitchModal = () => {
    if (switchModal) switchModal.hidden = true;
  };
  const switchBtn = $("switchBtn");
  if (switchBtn) switchBtn.addEventListener("click", openSwitchModal);
  const closeSwitchBtn = $("closeSwitchModal");
  if (closeSwitchBtn) closeSwitchBtn.addEventListener("click", closeSwitchModal);
  if (switchModal) {
    switchModal.addEventListener("click", (e) => e.target === switchModal && closeSwitchModal());
  }

  const switchForm = $("switchForm");
  if (switchForm) {
    switchForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const source = state.account_id;
      const target = $("switchToSelect").value;
      const amount = parseAmount($("switchAmount").value);
      const msg = $("switchMsg");
      if (msg) msg.textContent = "";
      if (!source || !target || amount <= 0) {
        if (msg) msg.textContent = "Select target account and amount.";
        return;
      }
      const sourceBalance =
        state.summary_accounts?.find((a) => a.account_id === source)?.balance ?? null;
      if (sourceBalance !== null && amount > Number(sourceBalance)) {
        if (msg) msg.textContent = "Amount exceeds the source account balance.";
        return;
      }
      try {
        await api.post("/api/switch", { source_account_id: source, target_account_id: target, amount });
        closeSwitchModal();
        await loadLedger();
      } catch (err) {
        if (msg) msg.textContent = err.message || "Switch failed";
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
    renderLedger(state.rows);
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
      const tx = state.rows.find((r) => r.transaction_id === row.dataset.txId);
      if (!tx) return;
      openModal(tx);
    });
  }

  const bottomNav = $("bottomNav");
  if (bottomNav) {
    bottomNav.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-action]");
      if (!btn) return;
      
      // Update Active State
      bottomNav.querySelectorAll(".dock-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");

      const action = btn.dataset.action;
      
      // Close other panels
      document.body.classList.remove("menu-open");

      if (action === "ledger") {
        // Just scroll top or reset view if needed
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }
      if (action === "add") {
        openModal();
      }
      if (action === "accounts") {
        openAccountsModal("list");
      }
      if (action === "menu") {
        setTimeout(() => document.body.classList.add("menu-open"), 10);
      }
    });
  }

  $("txForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    $("txMsg").textContent = "";
    const body = Object.fromEntries(new FormData(e.target).entries());
    if (!body.transaction_id && state.editing_tx_id) {
      body.transaction_id = state.editing_tx_id;
    }
    body.amount = parseAmount(body.amount);
    
    const isEdit = !!body.transaction_id;
    if (!isEdit) {
       body.date = new Date().toISOString(); 
    }

    try {
      if (isEdit) {
         await api.put(`/api/transactions/${body.transaction_id}`, body);
      } else {
         await api.post("/api/transactions", body);
      }
      closeModal();
      await loadLedger();
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
          await loadLedger();
      } catch (err) {
          $("txMsg").textContent = err.message || "Delete failed";
      }
  });

  // Accounts modal
  const setAccountFormTabLabel = (label) => {
    const tabBtn = $("accountFormTabBtn");
    if (tabBtn) tabBtn.textContent = label;
  };

  const resetAccountForm = () => {
    $("accountId").value = "";
    $("newAccountName").value = "";
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

  $("accountsBody").addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const id = btn.dataset.id;
    const action = btn.dataset.action;

    if (action === "edit") {
      const acc = state.accounts.find((a) => a.account_id === id);
      if (!acc) return;
      $("accountId").value = acc.account_id;
      $("newAccountName").value = acc.account_name;
      $("accountFormTitle").textContent = "Edit Account Details";
      $("cancelEditBtn").hidden = false;
      setAccountFormTabLabel("Edit Account");
      switchAccountsTab("create");
    }

    if (action === "delete") {
      if (!confirm("Delete this account and its transactions?")) return;
      try {
        await api.del(`/api/accounts/${id}`);
        await loadAccounts();
        await loadLedger();
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

    if (!account_name) return;
    
    const payload = { account_name };

    try {
      if (account_id) {
        await api.put(`/api/accounts/${account_id}`, payload);
      } else {
        await api.post("/api/accounts", payload);
      }
      resetAccountForm();
      await loadAccounts();
      await loadLedger();
      switchAccountsTab("list");
    } catch (err) {
      $("accountMsg").textContent = err.message || "Save failed";
    }
  });

}

(async function main() {
  await loadMe();
  loadFxCache();
  await loadAccounts();

  // default date range 30 days
  state.from = minusDaysYMD(30);
  state.to = todayYMD();
  $("fromDate").value = state.from;
  $("toDate").value = state.to;

  bindEvents();
  await loadLedger();
  if (isFxStale()) {
    fetchFxRate().then(updateFxUI).catch(() => updateFxUI());
  } else {
    updateFxUI();
  }
})();
