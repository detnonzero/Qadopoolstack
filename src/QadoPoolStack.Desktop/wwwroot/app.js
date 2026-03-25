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

function formatMinerHashrate(value) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) {
    return "-";
  }

  return `${Math.trunc(parsed / 1_000_000)} MH/s`;
}

function formatGigaHashrate(value) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) {
    return "-";
  }

  return `${Math.trunc(parsed / 1_000_000_000)} GH/s`;
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

function setSessionStatus(text = "") {
  setText("sessionStatus", text || (state.sessionToken ? "Authenticated" : "Signed out"));
}

function resetDashboard() {
  setText("meUsername", "-");
  setText("balAvailable", "0.0");
  setText("balPending", "0.0");
  setText("balMined", "0.0");
  setText("balDeposited", "0.0");
  setText("balWithdrawn", "0.0");
  setText("depositAddress", "-");
  setText("withdrawAddress", "-");
  setText("accountMinerToken", state.minerToken || "-");
  setText("challengeText", "-");
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
  setText("balPending", balance.pendingWithdrawal);
  setText("balMined", balance.totalMined);
  setText("balDeposited", balance.totalDeposited);
  setText("balWithdrawn", balance.totalWithdrawn);
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
      ? "Withdrawals are sent to your verified miner binding key. The fee is deducted from the entered amount."
      : "Verify a miner binding key first to enable withdrawals.";
  }
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

    if (page === "dashboard") {
      setFlash("Session expired. Please sign in again.", true);
      navigate("/");
    }

    throw new Error("Session expired. Please sign in again.");
  }

  if (response.status === 401 && mode === "miner") {
    clearMinerState();
    throw new Error("Miner token expired. Link the miner again.");
  }

  if (!response.ok) {
    throw new Error(data.error || `Request failed (${response.status})`);
  }

  return data;
}

async function loadMe() {
  const me = await api("/user/me");
  syncSessionIdentity(me.username);

  if (me.minerApiToken) {
    storeMinerTokenForUser(me.username, me.minerApiToken);
  } else {
    state.minerToken = getStoredMinerToken(me.username);
  }

  setSessionStatus(`Signed in as ${me.username}`);
  setText("meUsername", me.username);
  setText("depositAddress", me.depositAddress || "-");
  setText("withdrawAddress", me.withdrawalAddress || me.minerPublicKey || "-");
  setText("accountMinerToken", me.minerApiToken || state.minerToken || "-");
  setText("minerPublicKey", me.minerPublicKey || "-");
  setText("minerDifficulty", formatMinerDifficulty(me.minerDifficulty));
  setWithdrawEnabled(Boolean(me.minerPublicKey));
  updateBalance(me.balance);

  return me;
}

async function loadMinerStats() {
  if (!state.minerToken) {
    setNotice("No miner token stored yet. Link a miner first.", true);
    return;
  }

  const stats = await api("/miner/stats", { method: "GET" }, "miner");
  setText("minerPublicKey", stats.publicKey);
  setText("minerDifficulty", formatMinerDifficulty(stats.shareDifficulty));
  setText("minerLastShareAgo", formatTimeAgo(stats.lastShareUtc));
  setText("minerAccepted", stats.acceptedSharesRound);
  setText("minerStale", stats.staleSharesRound);
  setText("minerInvalid", stats.invalidSharesRound);
  setText("minerHashrate", formatMinerHashrate(stats.estimatedHashrate));
  setText("poolHashrate", formatGigaHashrate(stats.poolHashrate));
  setText("networkHashrate", formatGigaHashrate(stats.networkHashrate));
  setNotice("Miner stats loaded.");
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
    empty.textContent = "No transactions yet.";
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
    amount.textContent = `${item.isOutgoing ? "-" : ""}${item.amount} QADO`;

    row.append(copy, amount);
    list.append(row);
  }
}

function formatTimestamp(value) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function normalizeDecimalInput(value) {
  return `${value ?? ""}`.trim().replaceAll(" ", "").replace(",", ".");
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

  await api("/user/register", {
    method: "POST",
    body: JSON.stringify({
      username,
      password: form.get("password"),
    }),
  });

  clearSessionState();
  storePrefillUsername(username);
  setFlash("Account created. Please sign in.");
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

  const form = new FormData(event.target);
  await api("/ledger/transfer", {
    method: "POST",
    body: JSON.stringify({
      username: form.get("username"),
      amount: normalizeDecimalInput(form.get("amount")),
      note: form.get("note"),
    }),
  });

  await Promise.all([loadMe(), loadLedgerHistory()]);
  setNotice("Payment completed.");
  event.target.reset();
}

async function onChallenge(event) {
  event.preventDefault();

  const form = new FormData(event.target);
  const data = await api("/auth/challenge", {
    method: "POST",
    body: JSON.stringify({
      publicKey: form.get("publicKey"),
    }),
  });

  setText("challengeText", data.message);
  const challengeInput = el("verifyForm")?.querySelector("input[name='challengeId']");
  if (challengeInput) {
    challengeInput.value = data.challengeId;
  }

  setNotice("Challenge created. Sign the exact challenge text with your miner key.");
}

async function onVerify(event) {
  event.preventDefault();

  const form = new FormData(event.target);
  const data = await api("/auth/verify", {
    method: "POST",
    body: JSON.stringify({
      challengeId: form.get("challengeId"),
      signature: form.get("signature"),
    }),
  });

  storeMinerTokenForUser(state.sessionUsername, data.apiToken);
  setText("accountMinerToken", data.apiToken);
  setText("minerPublicKey", data.publicKey);
  setText("minerDifficulty", formatMinerDifficulty(data.shareDifficulty));
  setText("withdrawAddress", data.publicKey);
  setWithdrawEnabled(true);
  setNotice("Miner linked successfully. The same verified key is now used for deposits and withdrawals.");
  event.target.reset();
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

  setSessionStatus();
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
  applyFlashNotice();

  if (!state.sessionToken) {
    clearSessionState();
    setFlash("Please sign in.");
    navigate("/");
    return;
  }

  bindSubmit("withdrawForm", onWithdraw);
  bindSubmit("transferForm", onTransfer);
  bindSubmit("challengeForm", onChallenge);
  bindSubmit("verifyForm", onVerify);
  bindClick("loadMinerStatsButton", () => loadMinerStats().catch((error) => setNotice(error.message, true)));
  bindClick("signOutButton", onSignOut);

  if (state.minerToken) {
    setText("accountMinerToken", state.minerToken);
  }

  await Promise.all([loadMe(), loadLedgerHistory()]);
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
  }
}

window.addEventListener("DOMContentLoaded", () => {
  bootstrap().catch((error) => setNotice(error.message, true));
});
