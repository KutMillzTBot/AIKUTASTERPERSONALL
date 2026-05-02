const params = new URLSearchParams(window.location.search);

function readBridgeMetaPreset() {
  const meta = document.querySelector('meta[name="default-bridge"]');
  return String(meta?.getAttribute("content") || "").trim();
}

function inferBridgeFromHost() {
  const host = String(window.location.hostname || "").toLowerCase();
  const protocol = String(window.location.protocol || "https:");
  if (host === "127.0.0.1" || host === "localhost") return "http://127.0.0.1:5050";
  if (host.endsWith(".trycloudflare.com")) return `${protocol}//${window.location.host}`;
  return "";
}

const BRIDGE = String(
  params.get("bridge") ||
  readBridgeMetaPreset() ||
  inferBridgeFromHost() ||
  localStorage.getItem("fsb_bridge_url") ||
  "http://127.0.0.1:5050"
).replace(/\/+$/, "");

const $ = id => document.getElementById(id);
const set = (id, value) => {
  const el = $(id);
  if (el) el.textContent = value;
};

function usd(v) {
  return `$${Number(v || 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function setHealth(ok, text) {
  const dot = $("fsb-status-dot");
  const badge = $("fsb-health-badge");
  if (dot) {
    dot.style.background = ok ? "var(--accent-2)" : "var(--danger)";
    dot.style.boxShadow = ok
      ? "0 0 0 6px rgba(124,255,200,0.18)"
      : "0 0 0 6px rgba(255,107,129,0.18)";
  }
  set("fsb-status-text", text);
  set("fsb-bridge-health", ok ? "Online" : "Offline");
  if (badge) {
    badge.textContent = ok ? "Bridge Online" : "Bridge Offline";
    badge.style.color = ok ? "var(--accent-2)" : "var(--danger)";
  }
}

function renderWatchlist(items = [], mtSymbol = "") {
  const host = $("fsb-watchlist");
  if (!host) return;
  if (!items.length) {
    host.innerHTML = '<div class="note-card">No MT5 watchlist yet.</div>';
    return;
  }
  host.innerHTML = items.map(symbol => {
    const sym = String(symbol || "").toUpperCase();
    return `<div class="mini-chip">${sym}<small>${sym === mtSymbol ? "Active MT chart" : "Market Watch"}</small></div>`;
  }).join("");
}

function renderPositions(items = {}) {
  const host = $("fsb-positions");
  if (!host) return;
  const positions = Object.values(items || {});
  if (!positions.length) {
    host.innerHTML = '<div class="note-card">No open positions yet.</div>';
    return;
  }
  host.innerHTML = positions.map(pos => {
    const symbol = String(pos.symbol || "-").toUpperCase();
    const type = String(pos.order_type || pos.type || "-").toUpperCase();
    return `<div class="position-card"><strong>${symbol} · ${type}</strong><div class="position-meta"><span>Entry: ${Number(pos.entry || 0).toFixed(5)}</span><span>Lot: ${Number(pos.lot || 0).toFixed(2)}</span><span>SL: ${Number(pos.sl || 0).toFixed(5)}</span><span>TP: ${Number(pos.tp || 0).toFixed(5)}</span></div></div>`;
  }).join("");
}

function renderModels(items = {}) {
  const host = $("fsb-models");
  if (!host) return;
  const models = Object.entries(items || {}).slice(0, 6);
  if (!models.length) {
    host.innerHTML = '<div class="note-card">No model status yet.</div>';
    return;
  }
  host.innerHTML = models.map(([name, model]) => {
    const accuracy = Number(model?.accuracy || 0) * 100;
    const signal = Number(model?.signal || 0);
    return `<div class="model-card-mini"><strong>${name.replace(/_/g, " ")}</strong><small>Accuracy: ${accuracy.toFixed(1)}%<br>Signal: ${signal.toFixed(4)}<br>Enabled: ${model?.enabled ? "Yes" : "No"}</small></div>`;
  }).join("");
}

function renderLog(entries = []) {
  const host = $("fsb-live-log");
  if (!host) return;
  if (!entries.length) {
    host.innerHTML = '<div class="terminal-line">No recent bridge activity yet.</div>';
    return;
  }
  host.innerHTML = entries.map(entry => {
    const time = entry.ts || entry.time || "--:--:--";
    const text = entry.msg || entry.message || JSON.stringify(entry);
    return `<div class="terminal-line">[${time}] ${text}</div>`;
  }).join("");
}

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function getSupervisorUrl() {
  const target = new URL("../index.html", window.location.href);
  target.searchParams.set("bridge", BRIDGE);
  return target.toString();
}

function copyText(value, successText) {
  navigator.clipboard.writeText(value).then(() => {
    set("fsb-status-text", successText);
  }).catch(() => {
    set("fsb-status-text", "Copy failed, but the link is still shown above.");
  });
}

async function refresh() {
  set("fsb-bridge-url", BRIDGE);
  localStorage.setItem("fsb_bridge_url", BRIDGE);
  const supervisorLink = getSupervisorUrl();
  const link = $("fsb-open-supervisor");
  if (link) link.href = supervisorLink;

  try {
    const [snapshot, backtest, models, risk, logs] = await Promise.all([
      fetchJson(`${BRIDGE}/ui/snapshot`),
      fetchJson(`${BRIDGE}/backtest/status`).catch(() => ({ status: "unknown", progress: 0 })),
      fetchJson(`${BRIDGE}/models/status`).catch(() => ({})),
      fetchJson(`${BRIDGE}/risk/summary`).catch(() => ({ open_positions: 0, win_rate: 0 })),
      fetchJson(`${BRIDGE}/log?limit=12`).catch(() => ({ entries: [] })),
    ]);
    const status = snapshot.bridge || {};
    const account = snapshot.account || {};
    const mt = snapshot.mt || {};
    const portfolio = snapshot.portfolio || {};
    const positions = snapshot.positions || {};

    setHealth(true, "Bridge connected and ready");
    set("fsb-bridge-symbol", status.symbol || "-");
    set("fsb-mt-link", mt.connected ? "Attached" : "Waiting");
    set("fsb-mt-status", mt.connected ? "MT5 Live" : "Waiting");
    set("fsb-mode", String(status.mode || "-").toUpperCase());
    set("fsb-broker", account.broker || "-");
    set("fsb-selected-symbol", status.symbol || mt.symbol || "-");
    set("fsb-currency", account.currency || "-");
    set("fsb-leverage", account.leverage ? `1:${account.leverage}` : "-");
    set("fsb-broker-mode", String(status.mode || "auto").toUpperCase());
    set("fsb-balance", usd(account.balance));
    set("fsb-equity", usd(account.equity));
    set("fsb-float", usd(Number(account.equity || 0) - Number(account.balance || 0)));
    set("fsb-open", String(portfolio.open_trades ?? 0));
    set("fsb-backtest-status", String(backtest.status || "idle").toUpperCase());
    set("fsb-backtest-progress", `${Number(backtest.progress || 0)}%`);
    set("fsb-risk-open", String(risk.open_positions ?? 0));
    set("fsb-risk-winrate", `${Number(risk.win_rate || 0).toFixed(1)}%`);
    set("fsb-backtest-pill", String(backtest.status || "Idle").toUpperCase());
    renderWatchlist(mt.watchlist || [], mt.symbol || "");
    renderPositions(positions);
    renderModels(models);
    renderLog(logs.entries || logs.logs || []);
  } catch (err) {
    setHealth(false, `Bridge unreachable: ${err.message || "network"}`);
    set("fsb-bridge-symbol", "-");
    set("fsb-mt-link", "-");
    set("fsb-mt-status", "Offline");
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  $("fsb-refresh-btn")?.addEventListener("click", refresh);
  $("fsb-copy-bridge-btn")?.addEventListener("click", () => copyText(BRIDGE, "Bridge URL copied."));
  $("fsb-copy-page-btn")?.addEventListener("click", () => copyText(window.location.href, "Page URL copied."));
  $("fsb-open-deriv-btn")?.addEventListener("click", () => window.open("https://charts.deriv.com/deriv", "_blank", "noopener"));
  $("fsb-open-github-btn")?.addEventListener("click", () => window.open("https://github.com/KutMillzTBot/personalsystemai", "_blank", "noopener"));
  await refresh();
  setInterval(refresh, 5000);
});
