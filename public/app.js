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
  total_balance: 0,
  balance_diff: 0,
  fx_rate: null,
  fx_updated_at: null,
  currency: "IDR",
  editing_account: null, // { id, original_balance, is_main }
  editing_tx_id: null,
  pending_payload: null, // payload waiting for password
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
        <span>All</span>
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
      `<option value="all">All</option>`,
      ...filterAccounts.map((a) => `<option value="${a.account_id}">${a.account_name}</option>`),
    ];
    mobileSelect.innerHTML = mobileOptions.join("");
    mobileSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
  }

  const titleSelect = $("ledgerTitleSelect");
  if (titleSelect) {
    const titleOptions = [
      `<option value="all">All Accounts</option>`,
      ...filterAccounts.map((a) => `<option value="${a.account_id}">${a.account_name}</option>`),
    ];
    titleSelect.innerHTML = titleOptions.join("");
    titleSelect.value = state.scope === "account" && state.account_id ? state.account_id : "all";
  }
}

function updateTotals(summary) {
  const accounts = summary?.accounts || [];
  if (!accounts.length) {
    state.total_balance = 0;
    state.balance_diff = 0;
    return;
  }
  if (state.scope === "all") {
    const main = accounts.find((a) => a.account_id === state.main_account_id);
    state.total_balance = main ? Number(main.balance || 0) : Number(accounts[0].balance || 0);
    const childSum = accounts
      .filter((a) => a.account_id !== state.main_account_id)
      .reduce((sum, a) => sum + Number(a.balance || 0), 0);
    state.balance_diff = state.total_balance - childSum;
    return;
  }
  const current = accounts.find((a) => a.account_id === state.account_id) || accounts[0];
  state.total_balance = Number(current?.balance || 0);
  state.balance_diff = 0;
}

function renderLedger(rows) {
  const body = $("ledgerBody");
  const table = $("ledgerTable");
  const isAll = state.scope === "all";
  
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

  if (!rows.length) {
    const colSpan = isAll ? 7 : 6;
    body.innerHTML = `<tr><td colspan="${colSpan}" class="empty-row">No transactions yet for this range.</td></tr>`;
    $("ledgerTotal").textContent = fmtMoney(state.total_balance || 0);
    $("ledgerDiff").textContent = fmtMoney(state.balance_diff || 0);
    $("ledgerDiffBlock").hidden = !isAll;
    return;
  }

  body.innerHTML = rows
    .map(
      (r) => {
        const accCell = `<td class="account-cell">${isAll ? r.account_name : ""}</td>`;
        return `<tr data-tx-id="${r.transaction_id}" style="cursor:pointer">
        <td>${r.no}</td>
        ${accCell}
        <td>${isoToLocalDisplay(r.date)}</td>
        <td>${r.transaction_name}</td>
        <td class="num amount-in">${r.debit ? fmtMoney(r.debit) : ""}</td>
        <td class="num amount-out">${r.credit ? fmtMoney(r.credit) : ""}</td>
        <td class="num"><b>${fmtMoney(r.balance)}</b></td>
      </tr>`;
      }
    )
    .join("");

  $("ledgerTotal").textContent = fmtMoney(state.total_balance || 0);
  $("ledgerDiff").textContent = fmtMoney(state.balance_diff || 0);
  $("ledgerDiffBlock").hidden = !isAll;
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

  themeBtn.addEventListener("click", () => {
    const isDark = !document.body.classList.contains("dark");
    setTheme(isDark);
  });
  
  if (themeCheckbox) {
      themeCheckbox.addEventListener("change", (e) => {
          setTheme(e.target.checked);
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

  window.addEventListener("resize", () => {
    if (!isMobile()) {
      document.body.classList.remove("menu-open");
    }
    syncMobileSheet();
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
  if (mobileAddBtn) mobileAddBtn.addEventListener("click", () => openModal());
  const closeBtn = $("closeModal");
  if (closeBtn) closeBtn.addEventListener("click", closeModal);
  const modal = $("modal");
  if (modal) modal.addEventListener("click", (e) => e.target === modal && closeModal());

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
    state.editing_account = null;
    state.pending_payload = null;
    $("accountId").value = "";
    $("newAccountName").value = "";
    $("openingBalance").value = "";
    $("openingBalance").placeholder = "0";
    $("openingBalance").disabled = false;
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
      const isMain = acc.account_id === state.main_account_id;
      const opening = Number(acc.opening_balance || 0) + Number(acc.opening_adjust || 0);
      
      state.editing_account = {
        id: acc.account_id,
        original_balance: opening,
        is_main: isMain
      };

      $("accountId").value = acc.account_id;
      $("newAccountName").value = acc.account_name;
      $("openingBalance").value = opening === 0 ? "" : fmtIDR(opening);
      $("openingBalance").placeholder = "0";
      $("openingBalance").disabled = false; // Always allow editing now
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
    const opening_balance = parseAmount($("openingBalance").value);

    if (!account_name) return;
    
    // Prepare payload
    const payload = { account_name, opening_balance };
    
    // Check if we need password confirmation
    if (account_id && state.editing_account && state.editing_account.id === account_id) {
       const ctx = state.editing_account;
       // If Main Account AND Balance Changed AND Original was not 0 (already set)
       // actually user said "Lock... but open to re-edit". 
       // If it was 0, maybe we don't need password? 
       // But to be safe and consistent with "lock", let's require password if it's changing from a non-zero value.
       if (ctx.is_main && ctx.original_balance !== 0 && opening_balance !== ctx.original_balance) {
           // Require Password
           state.pending_payload = payload;
           $("confirmPasswordInput").value = "";
           $("passwordModal").hidden = false;
           return;
       }
    }

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

  // Password Modal Logic
  $("cancelPasswordBtn").addEventListener("click", () => {
      $("passwordModal").hidden = true;
      state.pending_payload = null;
  });

  $("confirmPasswordBtn").addEventListener("click", async () => {
      const pwd = $("confirmPasswordInput").value;
      if (!pwd) return;
      
      const payload = { ...state.pending_payload, password: pwd };
      const account_id = state.editing_account.id;
      
      try {
          await api.put(`/api/accounts/${account_id}`, payload);
          $("passwordModal").hidden = true;
          state.pending_payload = null;
          
          resetAccountForm();
          await loadAccounts();
          await loadLedger();
          switchAccountsTab("list");
      } catch (err) {
          alert(err.message || "Authentication failed");
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
