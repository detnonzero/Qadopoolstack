const page = document.body?.dataset?.page || "login";

const flashKeys = {
  message: "qado_flash_message",
  isError: "qado_flash_is_error",
  username: "qado_flash_username",
};

const storageKeys = {
  sessionToken: "qado_session_token",
  minerToken: "qado_miner_token",
  minerTokens: "qado_miner_tokens",
  sessionUsername: "qado_session_username",
};

const qadoAtomicScale = 1_000_000_000n;
const instantSubmitSendingMs = 220;
const instantSubmitSuccessMs = 900;
const minerStatsRefreshMs = 12_000;

function normalizeUsername(value) {
  return `${value ?? ""}`.trim().toLowerCase();
}

function loadStoredMinerTokens(sessionUsername) {
  const raw = localStorage.getItem(storageKeys.minerTokens);
  const tokens = {};

  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        for (const [username, token] of Object.entries(parsed)) {
          const normalizedUsername = normalizeUsername(username);
          const normalizedToken = `${token ?? ""}`.trim();
          if (normalizedUsername && normalizedToken) {
            tokens[normalizedUsername] = normalizedToken;
          }
        }
      }
    } catch {
    }
  }

  const legacyToken = `${localStorage.getItem(storageKeys.minerToken) || ""}`.trim();
  if (sessionUsername && legacyToken && !tokens[sessionUsername]) {
    tokens[sessionUsername] = legacyToken;
    localStorage.setItem(storageKeys.minerTokens, JSON.stringify(tokens));
  }

  if (legacyToken) {
    localStorage.removeItem(storageKeys.minerToken);
  }

  return tokens;
}

const initialSessionUsername = normalizeUsername(localStorage.getItem(storageKeys.sessionUsername) || "");
const initialMinerTokens = loadStoredMinerTokens(initialSessionUsername);

const state = {
  sessionToken: localStorage.getItem(storageKeys.sessionToken) || "",
  minerTokens: initialMinerTokens,
  minerToken: initialSessionUsername ? initialMinerTokens[initialSessionUsername] || "" : "",
  sessionUsername: initialSessionUsername,
  publicConfig: {
    accountRegistrationEnabled: true,
  },
  me: null,
  qadoPaySummary: null,
};

const minerStatsAutoRefresh = {
  intervalId: 0,
  inFlight: false,
  visibilityBound: false,
};

function persistStoredMinerTokens() {
  if (Object.keys(state.minerTokens).length === 0) {
    localStorage.removeItem(storageKeys.minerTokens);
    return;
  }

  localStorage.setItem(storageKeys.minerTokens, JSON.stringify(state.minerTokens));
}

function getStoredMinerToken(username) {
  const normalizedUsername = normalizeUsername(username);
  return normalizedUsername ? state.minerTokens[normalizedUsername] || "" : "";
}

function storeMinerTokenForUser(username, token) {
  const normalizedUsername = normalizeUsername(username);
  const normalizedToken = `${token ?? ""}`.trim();
  if (!normalizedUsername || !normalizedToken) {
    return;
  }

  state.minerTokens[normalizedUsername] = normalizedToken;
  state.minerToken = normalizedToken;
  persistStoredMinerTokens();
}

function clearStoredMinerToken(username) {
  const normalizedUsername = normalizeUsername(username);
  if (normalizedUsername && state.minerTokens[normalizedUsername]) {
    delete state.minerTokens[normalizedUsername];
    persistStoredMinerTokens();
  }

  if (!normalizedUsername || state.sessionUsername === normalizedUsername) {
    state.minerToken = "";
  }
}

const el = (id) => document.getElementById(id);

function setText(id, value) {
  const node = el(id);
  if (node) {
    node.textContent = String(value);
  }
}

function parseFiniteNumber(value) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }

    const parsed = Number.parseFloat(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function formatMinerDifficulty(value) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) {
    return "-";
  }

  return Math.trunc(parsed).toString();
}

function formatHashrate(value) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null || parsed <= 0) {
    return "-";
  }

  const units = [
    { label: "H/s", scale: 1 },
    { label: "kH/s", scale: 1_000 },
    { label: "MH/s", scale: 1_000_000 },
    { label: "GH/s", scale: 1_000_000_000 },
    { label: "TH/s", scale: 1_000_000_000_000 },
    { label: "PH/s", scale: 1_000_000_000_000_000 },
  ];

  let unit = units[0];
  for (const candidate of units) {
    unit = candidate;
    if (parsed < candidate.scale * 1_000 || candidate === units[units.length - 1]) {
      break;
    }
  }

  const scaled = parsed / unit.scale;
  const fractionDigits = scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2;
  return `${scaled.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  })} ${unit.label}`;
}

function formatTimeAgo(value) {
  if (!value) {
    return "-";
  }

  const timestamp = new Date(value).getTime();
  if (Number.isNaN(timestamp)) {
    return "-";
  }

  const elapsedSeconds = Math.max(0, Math.trunc((Date.now() - timestamp) / 1000));
  if (elapsedSeconds < 5) {
    return "just now";
  }

  if (elapsedSeconds < 60) {
    return `${elapsedSeconds}s ago`;
  }

  const elapsedMinutes = Math.trunc(elapsedSeconds / 60);
  if (elapsedMinutes < 60) {
    return `${elapsedMinutes}m ago`;
  }

  const elapsedHours = Math.trunc(elapsedMinutes / 60);
  if (elapsedHours < 24) {
    return `${elapsedHours}h ago`;
  }

  const elapsedDays = Math.trunc(elapsedHours / 24);
  return `${elapsedDays}d ago`;
}

function formatTimestamp(value) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatSignedAmount(value) {
  const text = `${value ?? ""}`.trim();
  if (!text) {
    return "0.0";
  }

  const parsed = parseFiniteNumber(text);
  if (parsed !== null && parsed > 0 && !text.startsWith("+")) {
    return `+${text}`;
  }

  return text;
}

function parseQadoAtomic(value) {
  if (typeof value === "bigint") {
    return value;
  }

  const raw = `${value ?? ""}`.trim();
  if (!raw) {
    return null;
  }

  const normalized = normalizeDecimalInput(raw);
  const match = normalized.match(/^([+-]?)(\d+)(?:\.(\d+))?$/);
  if (!match) {
    return null;
  }

  const sign = match[1] === "-" ? -1n : 1n;
  const whole = BigInt(match[2]);
  const fraction = (match[3] || "").slice(0, 9).padEnd(9, "0");
  return sign * ((whole * qadoAtomicScale) + BigInt(fraction));
}

function formatQadoAtomic(value) {
  const atomic = typeof value === "bigint" ? value : BigInt(value ?? 0);
  const negative = atomic < 0n;
  const absolute = negative ? -atomic : atomic;
  const whole = absolute / qadoAtomicScale;
  const fractionRaw = (absolute % qadoAtomicScale).toString().padStart(9, "0");
  const fraction = fractionRaw.replace(/0+$/, "");
  return `${negative ? "-" : ""}${whole.toString()}.${fraction || "0"}`;
}

function abbreviateMiddle(value, head = 16, tail = 12) {
  const text = `${value ?? ""}`;
  if (text.length <= head + tail + 1) {
    return text;
  }

  return `${text.slice(0, head)}...${text.slice(-tail)}`;
}

function normalizeDecimalInput(value) {
  return `${value ?? ""}`.trim().replaceAll(" ", "").replace(",", ".");
}

function cloneQadoPaySummary(summary) {
  if (!summary) {
    return null;
  }

  return {
    addressBook: Array.isArray(summary.addressBook)
      ? summary.addressBook.map((item) => ({ ...item }))
      : [],
    payments: Array.isArray(summary.payments)
      ? summary.payments.map((item) => ({ ...item }))
      : [],
    overview: summary.overview
      ? {
          sentToday: summary.overview.sentToday || "0.0",
          receivedToday: summary.overview.receivedToday || "0.0",
          netToday: summary.overview.netToday || "0.0",
          lastPayment: summary.overview.lastPayment ? { ...summary.overview.lastPayment } : null,
        }
      : null,
  };
}

function setTransferButtonVisualState(button, stateName, defaultLabel) {
  if (!button) {
    return;
  }

  button.classList.toggle("instant-submit-sending", stateName === "sending");
  button.classList.toggle("instant-submit-sent", stateName === "sent");
  button.textContent = stateName === "sending"
    ? "Sending..."
    : stateName === "sent"
      ? "Sent\u2714"
      : defaultLabel;
}

function bindSubmit(id, handler) {
  const form = el(id);
  if (!form) {
    return;
  }

  form.addEventListener("submit", (event) => handler(event).catch((error) => setNotice(error.message, true)));
}

function bindClick(id, handler) {
  const button = el(id);
  if (!button) {
    return;
  }

  button.addEventListener("click", handler);
}

function setNotice(message = "", isError = false) {
  const node = el("notice");
  if (!node) {
    return;
  }

  node.textContent = message;
  node.classList.toggle("hidden", !message);
  node.classList.toggle("error", Boolean(message) && isError);
  node.classList.toggle("success", Boolean(message) && !isError);
}

async function loadPublicConfig() {
  const response = await fetch("/public/config", { cache: "no-store" });
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(data.error || `Request failed (${response.status})`);
  }

  state.publicConfig = {
    accountRegistrationEnabled: data.accountRegistrationEnabled !== false,
  };

  return state.publicConfig;
}

function applyAccountRegistrationAvailability() {
  const registrationEnabled = state.publicConfig.accountRegistrationEnabled !== false;
  const createAccountLink = el("createAccountLink");
  if (createAccountLink) {
    createAccountLink.classList.toggle("disabled", !registrationEnabled);
    createAccountLink.textContent = registrationEnabled ? "Create account" : "Create account (inactive)";

    if (registrationEnabled) {
      createAccountLink.setAttribute("href", "/register");
    } else {
      createAccountLink.removeAttribute("href");
    }
  }

  const registerForm = el("registerForm");
  if (registerForm) {
    registerForm.classList.toggle("auth-form-disabled", !registrationEnabled);
    registerForm.querySelectorAll("input, button").forEach((node) => {
      node.disabled = !registrationEnabled;
    });
  }

  const registerInactiveHint = el("registerInactiveHint");
  if (registerInactiveHint) {
    registerInactiveHint.classList.toggle("hidden", registrationEnabled);
    registerInactiveHint.textContent = registrationEnabled
      ? ""
      : "Account creation is currently inactive.";
  }
}

function setFlash(message, isError = false) {
  sessionStorage.setItem(flashKeys.message, message);
  sessionStorage.setItem(flashKeys.isError, isError ? "1" : "0");
}

function consumeFlash() {
  const message = sessionStorage.getItem(flashKeys.message);
  const isError = sessionStorage.getItem(flashKeys.isError) === "1";
  sessionStorage.removeItem(flashKeys.message);
  sessionStorage.removeItem(flashKeys.isError);

  return { message, isError };
}

function storePrefillUsername(username) {
  sessionStorage.setItem(flashKeys.username, username);
}

function consumePrefillUsername() {
  const username = sessionStorage.getItem(flashKeys.username);
  sessionStorage.removeItem(flashKeys.username);
  return username;
}

function navigate(path) {
  window.location.replace(path);
}

function clearSessionState() {
  state.sessionToken = "";
  state.sessionUsername = "";
  state.me = null;
  localStorage.removeItem(storageKeys.sessionToken);
  localStorage.removeItem(storageKeys.sessionUsername);
}

function clearMinerState() {
  clearStoredMinerToken(state.sessionUsername);
}

function syncSessionIdentity(username) {
  const normalizedUsername = normalizeUsername(username);
  if (!normalizedUsername) {
    return;
  }

  state.sessionUsername = normalizedUsername;
  state.minerToken = getStoredMinerToken(normalizedUsername);
  localStorage.setItem(storageKeys.sessionUsername, normalizedUsername);
}

function syncShellUser(username) {
  const normalizedUsername = normalizeUsername(username);
  const glyph = normalizedUsername ? normalizedUsername[0].toUpperCase() : "U";
  setText("menuUsername", normalizedUsername || "-");
  setText("profileGlyph", glyph);
}

function applyActiveNavigation() {
  const activeByPage = {
    dashboard: "navDashboard",
    wallet: "navWallet",
    "qado-pay": "navQadoPay",
  };

  Object.values(activeByPage).forEach((id) => {
    const node = el(id);
    if (node) {
      node.classList.remove("active");
    }
  });

  const activeId = activeByPage[page];
  const activeNode = activeId ? el(activeId) : null;
  if (activeNode) {
    activeNode.classList.add("active");
  }
}

function closeProfileMenu() {
  const menu = el("profileMenu");
  const toggle = el("menuToggleButton");
  if (!menu || !toggle) {
    return;
  }

  menu.classList.add("hidden");
  toggle.setAttribute("aria-expanded", "false");
}

function bindProfileMenu() {
  const toggle = el("menuToggleButton");
  const menu = el("profileMenu");
  if (!toggle || !menu || toggle.dataset.bound === "1") {
    applyActiveNavigation();
    return;
  }

  toggle.dataset.bound = "1";
  applyActiveNavigation();

  toggle.addEventListener("click", (event) => {
    event.stopPropagation();
    const willOpen = menu.classList.contains("hidden");
    menu.classList.toggle("hidden", !willOpen);
    toggle.setAttribute("aria-expanded", willOpen ? "true" : "false");
  });

  document.addEventListener("click", (event) => {
    if (menu.classList.contains("hidden")) {
      return;
    }

    if (!menu.contains(event.target) && !toggle.contains(event.target)) {
      closeProfileMenu();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeProfileMenu();
    }
  });

  bindClick("profileSignOutButton", () => {
    closeProfileMenu();
    onSignOut();
  });
}

function resetDashboard() {
  setText("meUsername", "-");
  setText("balAvailable", "0.0");
  setText("balPendingWithdrawals", "0.0");
  setText("balPendingDeposits", "0.0");
  setText("balMined", "0.0");
  setText("balImmatureMining", "0.0");
  setText("balDeposited", "0.0");
  setText("balWithdrawn", "0.0");
  setText("poolFeePercent", "0.00%");
  setText("minerApiToken", "-");
  setText("depositAddress", "-");
  setText("withdrawAddress", "-");
  setText("minerPublicKey", "-");
  setText("minerDifficulty", "-");
  setText("minerLastShareAgo", "-");
  setText("minerAccepted", "-");
  setText("minerStale", "-");
  setText("minerInvalid", "-");
  setText("minerHashrate", "-");
  setText("poolHashrate", "-");
  setText("networkHashrate", "-");
  renderLedgerHistory([]);
  setWithdrawEnabled(false);
}

function updateBalance(balance) {
  setText("balAvailable", balance.available);
  setText("balPendingWithdrawals", balance.pendingWithdrawal);
  setText("balPendingDeposits", balance.pendingDeposits || "0.0");
  setText("balMined", balance.totalMined);
  setText("balImmatureMining", balance.immatureMiningRewards || "0.0");
  setText("balDeposited", balance.totalDeposited);
  setText("balWithdrawn", balance.totalWithdrawn);
  setText("qadoPaySummaryAvailable", balance.available);
}

function setWithdrawEnabled(enabled) {
  const form = el("withdrawForm");
  const hint = el("withdrawHint");
  if (form) {
    form.querySelectorAll("input, button").forEach((node) => {
      node.disabled = !enabled;
    });
  }

  if (hint) {
    hint.textContent = enabled
      ? "Withdrawals are sent to yours wallet address, managed by the backend."
      : "Withdrawals are enabled automatically once your account wallet and miner token are ready.";
  }
}

function setWalletSendEnabled(enabled) {
  const form = el("walletSendForm");
  if (!form) {
    return;
  }

  form.querySelectorAll("input, button").forEach((node) => {
    node.disabled = !enabled;
  });
}

async function api(path, options = {}, mode = "user") {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };

  if (mode === "user" && state.sessionToken) {
    headers.Authorization = `Bearer ${state.sessionToken}`;
  }

  if (mode === "miner" && state.minerToken) {
    headers["X-Miner-Token"] = state.minerToken;
  }

  const response = await fetch(path, { ...options, headers });
  const data = await response.json().catch(() => ({}));

  if (response.status === 401 && mode === "user") {
    clearSessionState();

    if (page !== "login" && page !== "register") {
      setFlash("Session expired. Please sign in again.", true);
      navigate("/");
    }

    throw new Error("Session expired. Please sign in again.");
  }

  if (response.status === 401 && mode === "miner") {
    clearMinerState();
    throw new Error("Miner token expired. Reload the dashboard to refresh it.");
  }

  if (!response.ok) {
    throw new Error(data.error || `Request failed (${response.status})`);
  }

  return data;
}

async function loadMe() {
  const me = await api("/user/me");
  state.me = me;
  syncSessionIdentity(me.username);

  if (me.minerApiToken) {
    storeMinerTokenForUser(me.username, me.minerApiToken);
  } else {
    state.minerToken = getStoredMinerToken(me.username);
  }

  syncShellUser(me.username);
  setText("meUsername", me.username);
  setText("minerApiToken", me.minerApiToken || state.minerToken || "-");
  setText("depositAddress", me.depositAddress || "-");
  setText("withdrawAddress", me.withdrawalAddress || me.minerPublicKey || "-");
  setText("poolFeePercent", me.poolFeePercent || "0.00%");
  setText("minerPublicKey", me.minerPublicKey || "-");
  setText("minerDifficulty", formatMinerDifficulty(me.minerDifficulty));
  setWithdrawEnabled(Boolean(me.minerPublicKey));
  updateBalance(me.balance);

  return me;
}

async function loadMinerStats(options = {}) {
  const { silent = false } = options;
  if (!state.minerToken) {
    if (!silent) {
      setNotice("No miner token is available for this account yet.", true);
    }

    return false;
  }

  try {
    const stats = await api("/miner/stats", { method: "GET" }, "miner");
    setText("minerPublicKey", stats.publicKey);
    setText("minerDifficulty", formatMinerDifficulty(stats.shareDifficulty));
    setText("minerLastShareAgo", formatTimeAgo(stats.lastShareUtc));
    setText("minerAccepted", stats.acceptedSharesRound);
    setText("minerStale", stats.staleSharesRound);
    setText("minerInvalid", stats.invalidSharesRound);
    setText("minerHashrate", formatHashrate(stats.estimatedHashrate));
    setText("poolHashrate", formatHashrate(stats.poolHashrate));
    setText("networkHashrate", formatHashrate(stats.networkHashrate));

    if (!silent) {
      setNotice("Miner stats loaded.");
    }

    return true;
  } catch (error) {
    if (!silent) {
      setNotice(error.message, true);
    }

    return false;
  }
}

function stopMinerStatsAutoRefresh() {
  if (minerStatsAutoRefresh.intervalId) {
    window.clearInterval(minerStatsAutoRefresh.intervalId);
    minerStatsAutoRefresh.intervalId = 0;
  }
}

async function refreshMinerStatsSilently(force = false) {
  if (page !== "dashboard") {
    return;
  }

  if (!force && document.visibilityState !== "visible") {
    return;
  }

  if (minerStatsAutoRefresh.inFlight) {
    return;
  }

  minerStatsAutoRefresh.inFlight = true;
  try {
    await loadMinerStats({ silent: true });
  } finally {
    minerStatsAutoRefresh.inFlight = false;
  }
}

function startMinerStatsAutoRefresh() {
  stopMinerStatsAutoRefresh();

  if (!minerStatsAutoRefresh.visibilityBound) {
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") {
        void refreshMinerStatsSilently(true);
      }
    });
    minerStatsAutoRefresh.visibilityBound = true;
  }

  minerStatsAutoRefresh.intervalId = window.setInterval(() => {
    void refreshMinerStatsSilently();
  }, minerStatsRefreshMs);
}

async function loadLedgerHistory() {
  const items = await api("/ledger/history", { method: "GET" });
  renderLedgerHistory(items);
}

function renderLedgerHistory(items) {
  const list = el("ledgerHistoryList");
  if (!list) {
    return;
  }

  list.textContent = "";
  if (!Array.isArray(items) || items.length === 0) {
    const empty = document.createElement("p");
    empty.className = "history-empty";
    empty.textContent = "No pool transactions yet.";
    list.append(empty);
    return;
  }

  for (const item of items) {
    const row = document.createElement("article");
    row.className = "history-row";

    const copy = document.createElement("div");
    copy.className = "history-copy";

    const top = document.createElement("div");
    top.className = "history-top";

    const title = document.createElement("strong");
    title.textContent = item.kind || "Transaction";

    const date = document.createElement("span");
    date.className = "history-date";
    date.textContent = formatTimestamp(item.createdUtc);

    top.append(title, date);
    copy.append(top);

    const counterparty = document.createElement("p");
    counterparty.className = "history-meta";
    counterparty.textContent = `${item.counterpartyLabel || "Counterparty"}: ${item.counterparty || "-"}`;
    copy.append(counterparty);

    if (item.note) {
      const note = document.createElement("p");
      note.className = "history-note";
      note.textContent = `Note: ${item.note}`;
      copy.append(note);
    }

    if (item.txId) {
      const tx = document.createElement("code");
      tx.className = "history-txid";
      tx.textContent = `Tx: ${item.txId}`;
      copy.append(tx);
    }

    const amount = document.createElement("strong");
    amount.className = `history-amount ${item.isOutgoing ? "negative" : "positive"}`;
    amount.textContent = `${item.isOutgoing ? "-" : "+"}${item.amount} QADO`;

    row.append(copy, amount);
    list.append(row);
  }
}

function renderWalletTransactions(items) {
  const list = el("walletTransactionsList");
  if (!list) {
    return;
  }

  list.textContent = "";
  if (!Array.isArray(items) || items.length === 0) {
    const empty = document.createElement("p");
    empty.className = "history-empty";
    empty.textContent = "No wallet transactions yet.";
    list.append(empty);
    return;
  }

  for (const item of items) {
    const incoming = item.direction === "incoming";
    const row = document.createElement("article");
    row.className = "history-row";

    const copy = document.createElement("div");
    copy.className = "history-copy";

    const top = document.createElement("div");
    top.className = "history-top";

    const title = document.createElement("strong");
    title.textContent = incoming ? "Incoming transaction" : "Outgoing transaction";

    const date = document.createElement("span");
    date.className = "history-date";
    date.textContent = formatTimestamp(item.createdUtc);

    top.append(title, date);
    copy.append(top);

    const counterparty = document.createElement("p");
    counterparty.className = "history-meta";
    counterparty.textContent = `${incoming ? "From" : "To"}: ${item.counterparty || "-"}`;
    copy.append(counterparty);

    if (item.note) {
      const note = document.createElement("p");
      note.className = "history-note";
      note.textContent = `Note: ${item.note}`;
      copy.append(note);
    }

    const feeValue = parseFiniteNumber(item.fee);
    if (feeValue !== null && feeValue !== 0) {
      const fee = document.createElement("p");
      fee.className = "history-note";
      fee.textContent = `Fee: ${item.fee} QADO`;
      copy.append(fee);
    }

    const status = document.createElement("p");
    status.className = "history-note";
    status.textContent = `Status: ${item.status || "-"}`;
    copy.append(status);

    if (item.txId) {
      const tx = document.createElement("code");
      tx.className = "history-txid";
      tx.textContent = `Tx: ${item.txId}`;
      copy.append(tx);
    }

    const amount = document.createElement("strong");
    amount.className = `history-amount ${incoming ? "positive" : "negative"}`;
    amount.textContent = `${incoming ? "+" : "-"}${item.amount} QADO`;

    row.append(copy, amount);
    list.append(row);
  }
}

function renderWalletAddressBook(items) {
  const list = el("walletAddressBookList");
  if (!list) {
    return;
  }

  list.textContent = "";
  if (!Array.isArray(items) || items.length === 0) {
    const empty = document.createElement("p");
    empty.className = "history-empty";
    empty.textContent = "No saved addresses yet.";
    list.append(empty);
    return;
  }

  for (const item of items) {
    const row = document.createElement("article");
    row.className = "address-book-row";

    const copy = document.createElement("div");
    copy.className = "address-book-copy";

    const label = document.createElement("strong");
    label.textContent = item.label;

    const address = document.createElement("code");
    address.className = "address-book-value";
    address.textContent = abbreviateMiddle(item.address);
    address.title = item.address;

    copy.append(label, address);

    const actions = document.createElement("div");
    actions.className = "address-book-actions";

    const useButton = document.createElement("button");
    useButton.type = "button";
    useButton.className = "secondary-button";
    useButton.dataset.action = "fill";
    useButton.dataset.address = item.address;
    useButton.textContent = "Use";

    const deleteButton = document.createElement("button");
    deleteButton.type = "button";
    deleteButton.className = "danger-button";
    deleteButton.dataset.action = "delete";
    deleteButton.dataset.contactId = item.contactId;
    deleteButton.textContent = "Delete";

    actions.append(useButton, deleteButton);
    row.append(copy, actions);
    list.append(row);
  }
}

function renderQadoPayAddressBook(items) {
  const list = el("qadoPayAddressBookList");
  if (!list) {
    return;
  }

  list.textContent = "";
  if (!Array.isArray(items) || items.length === 0) {
    const empty = document.createElement("p");
    empty.className = "history-empty";
    empty.textContent = "No saved usernames yet.";
    list.append(empty);
    return;
  }

  for (const item of items) {
    const row = document.createElement("article");
    row.className = "address-book-row";

    const copy = document.createElement("div");
    copy.className = "address-book-copy";

    const label = document.createElement("strong");
    label.textContent = item.label;

    const username = document.createElement("code");
    username.className = "address-book-value";
    username.textContent = item.username;
    username.title = item.username;

    copy.append(label, username);

    const actions = document.createElement("div");
    actions.className = "address-book-actions";

    const useButton = document.createElement("button");
    useButton.type = "button";
    useButton.className = "secondary-button";
    useButton.dataset.action = "fill";
    useButton.dataset.username = item.username;
    useButton.textContent = "Use";

    const deleteButton = document.createElement("button");
    deleteButton.type = "button";
    deleteButton.className = "danger-button";
    deleteButton.dataset.action = "delete";
    deleteButton.dataset.contactId = item.contactId;
    deleteButton.textContent = "Delete";

    actions.append(useButton, deleteButton);
    row.append(copy, actions);
    list.append(row);
  }
}

function renderQadoPayPayments(items) {
  const list = el("qadoPayPaymentsList");
  if (!list) {
    return;
  }

  list.textContent = "";
  if (!Array.isArray(items) || items.length === 0) {
    const empty = document.createElement("p");
    empty.className = "history-empty";
    empty.textContent = "No Qado Pay payments yet.";
    list.append(empty);
    return;
  }

  for (const item of items) {
    const incoming = item.direction === "incoming";
    const row = document.createElement("article");
    row.className = "history-row";

    const copy = document.createElement("div");
    copy.className = "history-copy";

    const top = document.createElement("div");
    top.className = "history-top";

    const title = document.createElement("strong");
    title.textContent = incoming ? "Payment received" : "Payment sent";

    const date = document.createElement("span");
    date.className = "history-date";
    date.textContent = formatTimestamp(item.createdUtc);

    top.append(title, date);
    copy.append(top);

    const counterparty = document.createElement("p");
    counterparty.className = "history-meta";
    counterparty.textContent = `${incoming ? "From" : "To"}: ${item.username || "-"}`;
    copy.append(counterparty);

    if (item.note) {
      const note = document.createElement("p");
      note.className = "history-note";
      note.textContent = `Note: ${item.note}`;
      copy.append(note);
    }

    const amount = document.createElement("strong");
    amount.className = `history-amount ${incoming ? "positive" : "negative"}`;
    amount.textContent = `${incoming ? "+" : "-"}${item.amount} QADO`;

    row.append(copy, amount);
    list.append(row);
  }
}

function renderWalletSummary(wallet) {
  const hasKeyPair = Boolean(wallet?.hasKeyPair);
  setText("walletPublicKey", wallet?.publicKey || "-");
  setText("walletBalance", wallet?.balance?.available || "0.0");
  setText("walletPendingOutgoing", wallet?.balance?.pendingOutgoingCount ?? "0");
  setText("walletPendingIncoming", wallet?.balance?.pendingIncomingCount ?? "0");
  setWalletSendEnabled(hasKeyPair);

  renderWalletTransactions(wallet?.transactions || []);
  renderWalletAddressBook(wallet?.addressBook || []);
}

function renderQadoPaySummary(summary, store = true) {
  const effectiveSummary = store ? cloneQadoPaySummary(summary) : summary;
  state.qadoPaySummary = effectiveSummary;
  renderQadoPayOverview(effectiveSummary?.overview || null);
  renderQadoPayAddressBook(effectiveSummary?.addressBook || []);
  renderQadoPayPayments(effectiveSummary?.payments || []);
}

function renderQadoPayOverview(overview) {
  setText("qadoPaySummaryAvailable", state.me?.balance?.available || "0.0");
  setText("qadoPaySentToday", overview?.sentToday || "0.0");
  setText("qadoPayReceivedToday", overview?.receivedToday || "0.0");
  setText("qadoPayNetToday", formatSignedAmount(overview?.netToday || "0.0"));

  const value = el("qadoPayLastPaymentValue");
  const meta = el("qadoPayLastPaymentMeta");
  if (!value || !meta) {
    return;
  }

  const lastPayment = overview?.lastPayment;
  if (!lastPayment) {
    value.textContent = "No payments yet.";
    meta.textContent = "Send or receive an intra-pool payment to see it here.";
    return;
  }

  const incoming = lastPayment.direction === "incoming";
  value.textContent = `${incoming ? "From" : "To"} ${lastPayment.username || "-"}`;
  meta.textContent = `${incoming ? "+" : "-"}${lastPayment.amount} QADO | ${formatTimestamp(lastPayment.createdUtc)}`;
}

async function loadWalletSummary() {
  const wallet = await api("/wallet/summary", { method: "GET" });
  renderWalletSummary(wallet);
  return wallet;
}

async function loadQadoPaySummary() {
  const summary = await api("/qado-pay/summary", { method: "GET" });
  renderQadoPaySummary(summary);
  return summary;
}

async function restoreSessionOrRedirect() {
  if (!state.sessionToken) {
    return false;
  }

  try {
    await loadMe();
    return true;
  } catch (error) {
    if (error?.message === "Session expired. Please sign in again.") {
      return false;
    }

    throw error;
  }
}

async function requireAuthenticatedPage() {
  applyFlashNotice();
  bindProfileMenu();

  if (!state.sessionToken) {
    clearSessionState();
    setFlash("Please sign in.", true);
    navigate("/");
    return false;
  }

  return true;
}

async function onLogin(event) {
  event.preventDefault();

  const form = new FormData(event.target);
  const data = await api("/user/login", {
    method: "POST",
    body: JSON.stringify({
      username: form.get("username"),
      password: form.get("password"),
    }),
  });

  syncSessionIdentity(data.username);
  state.sessionToken = data.sessionToken;
  localStorage.setItem(storageKeys.sessionToken, state.sessionToken);
  setFlash("Signed in.");
  event.target.reset();
  navigate("/dashboard");
}

async function onRegister(event) {
  event.preventDefault();

  if (state.publicConfig.accountRegistrationEnabled === false) {
    applyAccountRegistrationAvailability();
    setNotice("Account creation is currently inactive.", true);
    return;
  }

  const form = new FormData(event.target);
  const username = `${form.get("username") || ""}`.trim();
  const password = `${form.get("password") || ""}`;
  const passwordConfirm = `${form.get("passwordConfirm") || ""}`;

  if (password !== passwordConfirm) {
    setNotice("Passwords do not match.", true);
    return;
  }

  await api("/user/register", {
    method: "POST",
    body: JSON.stringify({
      username,
      password,
    }),
  });

  clearSessionState();
  storePrefillUsername(username);
  setFlash("Account created. Wallet and miner token provisioning are handled automatically. Please sign in.");
  event.target.reset();
  navigate("/");
}

async function onWithdraw(event) {
  event.preventDefault();

  const formElement = event.target;
  if (formElement.dataset.busy === "1") {
    return;
  }

  const form = new FormData(formElement);
  const payload = {
    amount: normalizeDecimalInput(form.get("amount")),
    fee: normalizeDecimalInput(form.get("fee")),
  };

  formElement.dataset.busy = "1";
  const controls = Array.from(formElement.querySelectorAll("input, button")).map((node) => [node, node.disabled]);
  controls.forEach(([node]) => {
    node.disabled = true;
  });

  try {
    const data = await api("/withdraw", {
      method: "POST",
      body: JSON.stringify(payload),
    });

    await Promise.all([loadMe(), loadLedgerHistory()]);
    setNotice(`Withdrawal ${data.withdrawalId} sent as ${data.sentAmount} QADO with fee ${data.fee}.${data.txId ? ` txid: ${data.txId}` : ""}`);
    formElement.reset();
    const feeInput = formElement.querySelector("input[name='fee']");
    if (feeInput) {
      feeInput.value = "0";
    }
  } finally {
    delete formElement.dataset.busy;
    controls.forEach(([node, wasDisabled]) => {
      node.disabled = wasDisabled;
    });
  }
}

async function onTransfer(event) {
  event.preventDefault();

  const formElement = event.target;
  if (formElement.dataset.busy === "1") {
    return;
  }

  const form = new FormData(formElement);
  const username = normalizeUsername(form.get("username"));
  const amount = normalizeDecimalInput(form.get("amount"));
  const note = `${form.get("note") || ""}`.trim();
  const amountAtomic = parseQadoAtomic(amount);
  if (!username) {
    setNotice("Recipient username is required.", true);
    return;
  }

  if (amountAtomic === null || amountAtomic <= 0n) {
    setNotice("Amount must be a valid positive QADO value.", true);
    return;
  }

  const submitButton = formElement.querySelector("button[type='submit']");
  const defaultLabel = submitButton?.textContent || "Send payment";
  const controls = Array.from(formElement.querySelectorAll("input, button")).map((node) => [node, node.disabled]);
  const previousBalance = state.me?.balance ? { ...state.me.balance } : null;
  const previousSummary = cloneQadoPaySummary(state.qadoPaySummary);
  const optimisticTimestamp = new Date().toISOString();
  const requestStartedAt = performance.now();
  let sentStateVisible = false;
  let sendingTimerId = 0;

  formElement.dataset.busy = "1";
  controls.forEach(([node]) => {
    node.disabled = true;
  });
  setTransferButtonVisualState(submitButton, "sending", defaultLabel);

  if (state.me?.balance) {
    const availableAtomic = parseQadoAtomic(state.me.balance.available) ?? 0n;
    state.me = {
      ...state.me,
      balance: {
        ...state.me.balance,
        available: formatQadoAtomic(availableAtomic - amountAtomic),
      },
    };
    updateBalance(state.me.balance);
  }

  const optimisticSummary = cloneQadoPaySummary(state.qadoPaySummary) || {
    addressBook: [],
    payments: [],
    overview: {
      sentToday: "0.0",
      receivedToday: "0.0",
      netToday: "0.0",
      lastPayment: null,
    },
  };

  optimisticSummary.overview = optimisticSummary.overview || {
    sentToday: "0.0",
    receivedToday: "0.0",
    netToday: "0.0",
    lastPayment: null,
  };

  const currentSentAtomic = parseQadoAtomic(optimisticSummary.overview.sentToday) ?? 0n;
  const currentNetAtomic = parseQadoAtomic(optimisticSummary.overview.netToday) ?? 0n;
  optimisticSummary.overview.sentToday = formatQadoAtomic(currentSentAtomic + amountAtomic);
  optimisticSummary.overview.netToday = formatQadoAtomic(currentNetAtomic - amountAtomic);
  optimisticSummary.overview.lastPayment = {
    direction: "outgoing",
    username,
    amount: formatQadoAtomic(amountAtomic),
    createdUtc: optimisticTimestamp,
  };
  optimisticSummary.payments = [
    {
      ledgerEntryId: `optimistic-${Date.now()}`,
      direction: "outgoing",
      username,
      amount: formatQadoAtomic(amountAtomic),
      note: note || null,
      createdUtc: optimisticTimestamp,
    },
    ...optimisticSummary.payments,
  ];
  renderQadoPaySummary(optimisticSummary);

  sendingTimerId = window.setTimeout(() => {
    sentStateVisible = true;
    setTransferButtonVisualState(submitButton, "sent", defaultLabel);
  }, instantSubmitSendingMs);

  try {
    await api("/ledger/transfer", {
      method: "POST",
      body: JSON.stringify({
        username,
        amount,
        note,
      }),
    });

    const remainingSendingMs = instantSubmitSendingMs - (performance.now() - requestStartedAt);
    if (remainingSendingMs > 0) {
      await delay(remainingSendingMs);
    }

    if (!sentStateVisible) {
      clearTimeout(sendingTimerId);
      setTransferButtonVisualState(submitButton, "sent", defaultLabel);
    }

    setNotice(`Payment sent to ${username}.`);
    formElement.reset();
    void Promise.all([loadMe(), loadQadoPaySummary()]).catch(() => {});
    await delay(instantSubmitSuccessMs);
  } catch (error) {
    clearTimeout(sendingTimerId);
    if (previousBalance && state.me) {
      state.me = {
        ...state.me,
        balance: {
          ...state.me.balance,
          ...previousBalance,
        },
      };
      updateBalance(state.me.balance);
    }

    renderQadoPaySummary(previousSummary);
    setTransferButtonVisualState(submitButton, "default", defaultLabel);
    setNotice(`Payment failed. ${error.message}`, true);
    delete formElement.dataset.busy;
    controls.forEach(([node, wasDisabled]) => {
      node.disabled = wasDisabled;
    });
    return;
  }

  delete formElement.dataset.busy;
  controls.forEach(([node, wasDisabled]) => {
    node.disabled = wasDisabled;
  });
  setTransferButtonVisualState(submitButton, "default", defaultLabel);
}

async function onCreateWalletKeypair() {
  const data = await api("/wallet/keypair", {
    method: "POST",
    body: JSON.stringify({}),
  });

  await Promise.all([loadMe(), loadWalletSummary()]);
  setNotice(`Custodian wallet keypair is ready. Public key: ${data.publicKey}`);
}

async function onWalletSend(event) {
  event.preventDefault();

  const formElement = event.target;
  if (formElement.dataset.busy === "1") {
    return;
  }

  const form = new FormData(formElement);
  const payload = {
    address: form.get("address"),
    amount: normalizeDecimalInput(form.get("amount")),
    fee: normalizeDecimalInput(form.get("fee")),
    note: form.get("note"),
  };

  formElement.dataset.busy = "1";
  const controls = Array.from(formElement.querySelectorAll("input, button")).map((node) => [node, node.disabled]);
  controls.forEach(([node]) => {
    node.disabled = true;
  });

  try {
    const data = await api("/wallet/send", {
      method: "POST",
      body: JSON.stringify(payload),
    });

    await loadWalletSummary();
    setNotice(`Wallet transaction broadcast to ${data.address}. Amount: ${data.amount} QADO, fee: ${data.fee} QADO, txid: ${data.txId}`);
    formElement.reset();
    const feeInput = formElement.querySelector("input[name='fee']");
    if (feeInput) {
      feeInput.value = "0";
    }
  } finally {
    delete formElement.dataset.busy;
    controls.forEach(([node, wasDisabled]) => {
      node.disabled = wasDisabled;
    });
  }
}

async function onAddAddressBookEntry(event) {
  event.preventDefault();

  const form = new FormData(event.target);
  await api("/wallet/address-book", {
    method: "POST",
    body: JSON.stringify({
      label: form.get("label"),
      address: form.get("address"),
    }),
  });

  await loadWalletSummary();
  setNotice("Address book updated.");
  event.target.reset();
}

async function onAddQadoPayAddressBookEntry(event) {
  event.preventDefault();

  const form = new FormData(event.target);
  await api("/qado-pay/address-book", {
    method: "POST",
    body: JSON.stringify({
      label: form.get("label"),
      username: form.get("username"),
    }),
  });

  await loadQadoPaySummary();
  setNotice("Address book updated.");
  event.target.reset();
}

function bindWalletAddressBookActions() {
  const list = el("walletAddressBookList");
  if (!list || list.dataset.bound === "1") {
    return;
  }

  list.dataset.bound = "1";
  list.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) {
      return;
    }

    const action = button.dataset.action;
    if (action === "fill") {
      const addressInput = el("walletSendForm")?.querySelector("input[name='address']");
      if (addressInput) {
        addressInput.value = button.dataset.address || "";
        addressInput.focus();
      }

      setNotice("Recipient address copied into the send form.");
      return;
    }

    if (action === "delete") {
      api(`/wallet/address-book/${button.dataset.contactId || ""}`, { method: "DELETE" })
        .then(() => loadWalletSummary())
        .then(() => setNotice("Address book entry deleted."))
        .catch((error) => setNotice(error.message, true));
    }
  });
}

function bindQadoPayAddressBookActions() {
  const list = el("qadoPayAddressBookList");
  if (!list || list.dataset.bound === "1") {
    return;
  }

  list.dataset.bound = "1";
  list.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) {
      return;
    }

    const action = button.dataset.action;
    if (action === "fill") {
      const usernameInput = el("transferForm")?.querySelector("input[name='username']");
      if (usernameInput) {
        usernameInput.value = button.dataset.username || "";
        usernameInput.focus();
      }

      setNotice("Recipient username copied into the payment form.");
      return;
    }

    if (action === "delete") {
      api(`/qado-pay/address-book/${button.dataset.contactId || ""}`, { method: "DELETE" })
        .then(() => loadQadoPaySummary())
        .then(() => setNotice("Address book entry deleted."))
        .catch((error) => setNotice(error.message, true));
    }
  });
}

function onSignOut() {
  clearSessionState();
  setFlash("Signed out.");
  navigate("/");
}

function applyFlashNotice() {
  const flash = consumeFlash();
  if (flash.message) {
    setNotice(flash.message, flash.isError);
  }
}

function applyPrefillUsername() {
  if (page !== "login") {
    return;
  }

  const username = consumePrefillUsername();
  if (!username) {
    return;
  }

  const usernameInput = el("loginForm")?.querySelector("input[name='username']");
  const passwordInput = el("loginForm")?.querySelector("input[name='password']");

  if (usernameInput) {
    usernameInput.value = username;
  }

  if (passwordInput) {
    passwordInput.focus();
  }
}

async function bootstrapLoginPage() {
  applyFlashNotice();
  applyPrefillUsername();
  await loadPublicConfig();
  applyAccountRegistrationAvailability();

  if (await restoreSessionOrRedirect()) {
    navigate("/dashboard");
    return;
  }

  bindSubmit("loginForm", onLogin);
}

async function bootstrapRegisterPage() {
  applyFlashNotice();
  await loadPublicConfig();
  applyAccountRegistrationAvailability();

  if (await restoreSessionOrRedirect()) {
    navigate("/dashboard");
    return;
  }

  bindSubmit("registerForm", onRegister);
}

async function bootstrapDashboardPage() {
  resetDashboard();

  if (!await requireAuthenticatedPage()) {
    return;
  }

  bindSubmit("withdrawForm", onWithdraw);

  await Promise.all([loadMe(), loadLedgerHistory()]);
  await refreshMinerStatsSilently(true);
  startMinerStatsAutoRefresh();
}

async function bootstrapWalletPage() {
  renderWalletSummary(null);

  if (!await requireAuthenticatedPage()) {
    return;
  }

  bindSubmit("walletSendForm", onWalletSend);
  bindSubmit("walletAddressBookForm", onAddAddressBookEntry);
  bindWalletAddressBookActions();

  await loadMe();
  await loadWalletSummary();
}

async function bootstrapQadoPayPage() {
  renderQadoPaySummary(null);

  if (!await requireAuthenticatedPage()) {
    return;
  }

  bindSubmit("transferForm", onTransfer);
  bindSubmit("qadoPayAddressBookForm", onAddQadoPayAddressBookEntry);
  bindQadoPayAddressBookActions();

  await Promise.all([loadMe(), loadQadoPaySummary()]);
}

async function bootstrap() {
  if (page === "login") {
    await bootstrapLoginPage();
    return;
  }

  if (page === "register") {
    await bootstrapRegisterPage();
    return;
  }

  if (page === "dashboard") {
    await bootstrapDashboardPage();
    return;
  }

  if (page === "wallet") {
    await bootstrapWalletPage();
    return;
  }

  if (page === "qado-pay") {
    await bootstrapQadoPayPage();
  }
}

window.addEventListener("DOMContentLoaded", () => {
  bootstrap().catch((error) => setNotice(error.message, true));
});
