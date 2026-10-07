import "dotenv/config";
import WebSocket, { WebSocketServer } from "ws";
import fs from "node:fs";
import path from "node:path";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";

const env = (name, fallback) => process.env[name] ?? fallback;
const num = (name, fallback) => {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? value : fallback;
};
const integer = (name, fallback) =>
  Math.max(1, Math.trunc(num(name, fallback)));
const truthy = (name) => /^(1|true|yes|on)$/i.test(env(name, "false"));

const client = new SecretsManagerClient({ region: "us-east-1" });

let apikey = null;

try {
  const command = new GetSecretValueCommand({ SecretId: "massive-secret" });
  const response = await client.send(command);
  if (response.SecretString) {
    try {
      apikey = JSON.parse(response.SecretString);
    } catch {
      // Return raw string if not JSON formatted
      apikey = response.SecretString;
    }
  }

  if (response.SecretBinary) {
    const buff = Buffer.from(response.SecretBinary, "base64");
    apikey = JSON.parse(buff.toString("utf-8"));
  }
} catch (error) {
  console.error(`Failed to retrieve secret --> `, error);
}

const API_KEY = apikey;
console.log("api key is ", API_KEY.slice(0, 5) + "...");
const REST_BASE = env(
  "MASSIVE_REST_BASE_URL",
  "https://api.massive.com",
).replace(/\/$/, "");

console.log("api is ", REST_BASE);

const FEED_URL = env("MASSIVE_WS_BASE_URL", "wss://delayed.massive.com/stocks");
const TOP_COUNT = integer("TOP_STOCK_COUNT", 20);
const REFRESH_MS = integer("TOP_REFRESH_SECONDS", 10) * 1000;
const MIN_DAY_VOLUME = num("TOP_MIN_DAY_VOLUME", 500_000);
const MIN_DOLLAR_VOLUME = num("TOP_MIN_DOLLAR_VOLUME", 10_000_000);
const MIN_PRICE = num("TOP_MIN_PRICE", 5);
const MIN_VOLUME_RATIO = num("BREAKOUT_MIN_VOLUME_RATIO", 1.5);
const MIN_EMA_BARS = integer("BREAKOUT_MIN_EMA_BARS", 200);
const CONFIRMATION_BARS = integer("BREAKOUT_CONFIRMATION_BARS", 2);
const UTC_OFFSET_MINUTES = num("MARKET_UTC_OFFSET_MINUTES", -240);
const DEBUG = truthy("DEBUG") || truthy("DEBUG_LOGS");
const LOCAL_HOST = env("LOCAL_WS_HOST", "127.0.0.1");
const LOCAL_PORT = integer("LOCAL_WS_PORT", 9001);
const SHOW_ONLY_CANDIDATES = truthy("TOP_SHOW_CANDIDATES_ONLY");
const RECORD_EVENTS = !/^(0|false|no|off)$/i.test(env("RECORD_EVENTS", "true"));
const RECORD_FILE = env(
  "RECORD_FILE",
  `recordings/scanner-${new Date().toISOString().slice(0, 10)}.jsonl`,
);

let eventWriter = null;
if (RECORD_EVENTS) {
  fs.mkdirSync(path.dirname(RECORD_FILE), { recursive: true });
  eventWriter = fs.createWriteStream(RECORD_FILE, { flags: "a" });
  eventWriter.on("error", (error) =>
    console.error("[RECORDING] file error:", error.message),
  );
  console.log(`[RECORDING] writing list snapshots to ${RECORD_FILE}`);
}

if (!API_KEY) throw new Error("MASSIVE_API_KEY is required in .env");

const commonStocks = new Set();
const latest = new Map();
const minutes = new Map();
const states = new Map();
let topSymbols = new Set();
let candidateSymbols = new Set();
let currentSnapshot = {
  event: "TOP_STOCKS_CHANGED",
  count: 0,
  stocks: [],
  watchlist: [],
};
const clients = new Set();

const nowMs = () => Date.now();
const keyFor = (s) =>
  String(s || "")
    .trim()
    .toUpperCase();
const localMinute = (ts) => Math.floor(ts / 60000 + UTC_OFFSET_MINUTES);
const localDay = (ts) => Math.floor(localMinute(ts) / 1440);
const localMinuteOfDay = (ts) => ((localMinute(ts) % 1440) + 1440) % 1440;
const fmt = (v, digits = 4) =>
  Number.isFinite(v) ? Number(v.toFixed(digits)) : null;

async function fetchJson(url, params = {}) {
  const u = new URL(url);
  for (const [k, v] of Object.entries({ ...params, apiKey: API_KEY }))
    u.searchParams.set(k, String(v));
  const response = await fetch(u);
  if (!response.ok) throw new Error(`Massive returned HTTP ${response.status}`);
  const body = await response.json();
  if (body.status && !["OK", "DELAYED"].includes(body.status))
    throw new Error(`Massive returned status ${body.status}`);
  return body;
}

async function loadCommonStocks() {
  let url = `${REST_BASE}/v3/reference/tickers`;
  let first = true;
  while (url) {
    const body = await fetchJson(
      url,
      first
        ? {
            market: "stocks",
            type: "CS",
            active: true,
            limit: 1000,
            sort: "ticker",
            order: "asc",
          }
        : {},
    );
    for (const item of body.results || []) {
      const symbol = keyFor(item.ticker);
      if (symbol && symbol.length <= 8) commonStocks.add(symbol);
    }
    url = body.next_url || null;
    first = false;
  }
  if (!commonStocks.size)
    throw new Error("Massive returned an empty common-stock universe");
  console.log(
    `[UNIVERSE] loaded ${commonStocks.size} common stocks; ETFs excluded`,
  );
}

class Ema {
  constructor(period) {
    this.period = period;
    this.value = 0;
    this.initialized = false;
  }
  update(price) {
    if (!this.initialized) {
      this.value = price;
      this.initialized = true;
    } else this.value += (2 / (this.period + 1)) * (price - this.value);
    return this.value;
  }
}

class IntradayState {
  constructor() {
    this.minute = null;
    this.sessionDay = null;
    this.barsSeen = 0;
    this.ema = new Map([5, 9, 21, 55, 100, 200].map((p) => [p, new Ema(p)]));
    this.prior = new Map();
    this.volumes = [];
    this.openHigh = -Infinity;
    this.openLow = Infinity;
    this.stackStreak = 0;
    this.recentBurst = 0;
    this.setupSent = false;
    this.confirmed = false;
    this.pullbackArmed = false;
    this.pullbackHigh = 0;
  }
  onBar(bar) {
    const id = Math.floor(bar.timestampMs / 60000);
    if (!this.minute) {
      this.minute = {
        id,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.windowVolume,
        dayVolume: bar.dayVolume,
        dayVwap: bar.dayVwap,
        timestampMs: bar.timestampMs,
      };
      return null;
    }
    if (id === this.minute.id) {
      const m = this.minute;
      m.high = Math.max(m.high, bar.high);
      m.low = Math.min(m.low, bar.low);
      m.close = bar.close;
      m.volume += bar.windowVolume;
      m.dayVolume = bar.dayVolume;
      m.dayVwap = bar.dayVwap;
      m.timestampMs = bar.timestampMs;
      return null;
    }
    if (id < this.minute.id) return null;
    const closed = this.finalize(this.minute);
    this.minute = {
      id,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      volume: bar.windowVolume,
      dayVolume: bar.dayVolume,
      dayVwap: bar.dayVwap,
      timestampMs: bar.timestampMs,
    };
    return closed;
  }
  finalize(m) {
    const day = localDay(m.timestampMs);
    if (this.sessionDay !== day) {
      this.sessionDay = day;
      this.barsSeen = 0;
      this.ema = new Map([5, 9, 21, 55, 100, 200].map((p) => [p, new Ema(p)]));
      this.prior = new Map();
      this.volumes = [];
      this.openHigh = -Infinity;
      this.openLow = Infinity;
      this.stackStreak = 0;
      this.recentBurst = 0;
      this.setupSent = false;
      this.confirmed = false;
      this.pullbackArmed = false;
    }
    const tod = localMinuteOfDay(m.timestampMs);
    if (tod >= 570 && tod < 575) {
      this.openHigh = Math.max(this.openHigh, m.high);
      this.openLow = Math.min(this.openLow, m.low);
    }
    const avg = this.volumes.length
      ? this.volumes.reduce((a, b) => a + b, 0) / this.volumes.length
      : 0;
    const volumeRatio = avg > 0 ? m.volume / avg : 0;
    this.volumes.push(m.volume);
    if (this.volumes.length > 20) this.volumes.shift();
    for (const p of [5, 9, 21, 55, 100, 200])
      this.prior.set(p, this.ema.get(p).value);
    const e = Object.fromEntries(
      [5, 9, 21, 55, 100, 200].map((p) => [p, this.ema.get(p).update(m.close)]),
    );
    this.barsSeen++;
    const bullish =
      e[5] > e[9] &&
      e[9] > e[21] &&
      e[21] > e[55] &&
      e[55] > e[100] &&
      e[100] > e[200];
    const core = e[5] > e[9] && e[9] > e[21];
    const rising =
      this.barsSeen <= 1 ||
      [5, 9, 21].every((p) => e[p] > (this.prior.get(p) ?? -Infinity));
    const aboveVwap = m.dayVwap > 0 && m.close > m.dayVwap;
    const aboveOpen = Number.isFinite(this.openHigh) && m.close > this.openHigh;
    const regular = tod >= 570 && tod < 960;
    if (volumeRatio >= MIN_VOLUME_RATIO) this.recentBurst = 3;
    else this.recentBurst = Math.max(0, this.recentBurst - 1);
    this.stackStreak = bullish ? this.stackStreak + 1 : 0;
    const early =
      regular &&
      this.barsSeen >= MIN_EMA_BARS &&
      core &&
      rising &&
      aboveVwap &&
      aboveOpen &&
      this.recentBurst > 0;
    const confirmedNow =
      early && bullish && this.stackStreak >= CONFIRMATION_BARS;
    let signal = "MINUTE_UPDATE";
    if (early && !this.setupSent) {
      this.setupSent = true;
      signal = "BREAKOUT_SETUP";
    }
    if (confirmedNow && !this.confirmed) {
      this.confirmed = true;
      this.pullbackArmed = false;
      signal = "BULLISH_STACK_CONFIRMED";
    }
    if (this.confirmed) {
      const touches = m.low <= e[9] * 1.0025,
        holds = m.close >= e[9] && m.close >= e[21];
      if (touches && holds) {
        this.pullbackArmed = true;
        this.pullbackHigh = m.high;
      } else if (this.pullbackArmed && m.close > this.pullbackHigh) {
        signal = "PULLBACK_ENTRY";
        this.pullbackArmed = false;
      }
    }
    if (!core || m.close < e[21] || !aboveVwap) {
      this.setupSent = false;
      this.confirmed = false;
      this.pullbackArmed = false;
    }
    return {
      ...m,
      ema5: e[5],
      ema9: e[9],
      ema21: e[21],
      ema55: e[55],
      ema100: e[100],
      ema200: e[200],
      volumeRatio,
      stackStreak: this.stackStreak,
      bullishStack: bullish,
      aboveVwap,
      aboveOpeningRange: aboveOpen,
      signal,
    };
  }
}

function stockUpdate(bar, minute) {
  return {
    event: "STOCK_UPDATE",
    symbol: bar.symbol,
    price: bar.close,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
    vwap: bar.windowVwap,
    day_vwap: bar.dayVwap,
    window_volume: bar.windowVolume,
    day_volume: bar.dayVolume,
    ema_5: minute?.ema5 ?? null,
    ema_9: minute?.ema9 ?? null,
    ema_21: minute?.ema21 ?? null,
    ema_55: minute?.ema55 ?? null,
    ema_100: minute?.ema100 ?? null,
    ema_200: minute?.ema200 ?? null,
    bullish_stack: minute?.bullishStack ?? false,
    market_timestamp_ms: bar.timestampMs,
    sent_timestamp_ms: nowMs(),
  };
}
function summary(bar, rank, minute) {
  return {
    rank,
    symbol: bar.symbol,
    price: bar.close,
    day_volume: bar.dayVolume,
    dollar_volume: bar.close * bar.dayVolume,
    day_vwap: bar.dayVwap,
    ema_5: minute?.ema5 ?? null,
    ema_9: minute?.ema9 ?? null,
    ema_21: minute?.ema21 ?? null,
    ema_55: minute?.ema55 ?? null,
    ema_100: minute?.ema100 ?? null,
    ema_200: minute?.ema200 ?? null,
    volume_ratio: minute?.volumeRatio ?? 0,
    bullish_stack: minute?.bullishStack ?? false,
  };
}
function ranked() {
  const matches = [...latest.values()]
    .filter(
      (b) =>
        commonStocks.has(b.symbol) &&
        b.dayVolume >= MIN_DAY_VOLUME &&
        b.close >= MIN_PRICE &&
        b.close * b.dayVolume >= MIN_DOLLAR_VOLUME,
    )
    .sort(
      (a, b) => b.dayVolume - a.dayVolume || a.symbol.localeCompare(b.symbol),
    );
  return matches;
}
function refresh() {
  const matches = ranked();
  topSymbols = new Set(matches.map((b) => b.symbol));
  candidateSymbols = new Set(
    matches
      .filter((b) => {
        const m = minutes.get(b.symbol);
        return m?.bullishStack && m.aboveVwap && m.aboveOpeningRange;
      })
      .map((b) => b.symbol),
  );
  // This is the only outbound market message: the complete current list.
  currentSnapshot = {
    event: "TOP_STOCKS_CHANGED",
    count: matches.length,
    timestamp_ms: nowMs(),
    stocks: matches.map((b, i) => summary(b, i + 1, minutes.get(b.symbol))),
    watchlist: [],
  };
  recordSnapshot(currentSnapshot);
  broadcast(currentSnapshot);
  if (DEBUG)
    console.log(`[MATCHES] ${matches.map((x) => x.symbol).join(", ")}`);
}
function broadcast(payload) {
  const text = JSON.stringify(payload);
  for (const c of clients) if (c.readyState === WebSocket.OPEN) c.send(text);
}
function recordSnapshot(payload) {
  if (eventWriter) eventWriter.write(`${JSON.stringify(payload)}\n`);
}

async function loadReplay(symbol, date) {
  if (
    !/^[A-Za-z0-9.-]{1,16}$/.test(symbol) ||
    !/^\d{4}-\d{2}-\d{2}$/.test(date)
  ) {
    throw new Error("Ticker or date is invalid");
  }
  let url = `${REST_BASE}/v2/aggs/ticker/${encodeURIComponent(symbol.toUpperCase())}/range/1/second/${date}/${date}`;
  const bars = [];
  let first = true;
  while (url) {
    const body = await fetchJson(
      url,
      first ? { adjusted: true, sort: "asc", limit: 50000 } : {},
    );
    for (const b of body.results || []) bars.push(b);
    url = body.next_url || null;
    first = false;
  }
  let volume = 0;
  let notional = 0;
  return {
    event: "REPLAY_BARS",
    granularity: "second",
    symbol: symbol.toUpperCase(),
    date,
    bars: bars.map((b) => {
      const v = Math.max(0, Number(b.v || 0));
      const bvwap =
        Number.isFinite(Number(b.vw)) && Number(b.vw) > 0
          ? Number(b.vw)
          : (Number(b.h) + Number(b.l) + Number(b.c)) / 3;
      volume += v;
      notional += bvwap * v;
      return {
        time: Number(b.t),
        open: Number(b.o),
        high: Number(b.h),
        low: Number(b.l),
        close: Number(b.c),
        volume: v,
        vwap: volume ? notional / volume : bvwap,
      };
    }),
  };
}

function processAggregate(raw) {
  const symbol = keyFor(raw.sym);
  if (
    !symbol ||
    symbol.length > 8 ||
    raw.otc ||
    (!commonStocks.has(symbol) && symbol !== "TQQQ")
  )
    return;
  const bar = {
    symbol,
    open: Number(raw.o),
    high: Number(raw.h),
    low: Number(raw.l),
    close: Number(raw.c),
    windowVwap: Number(raw.vw || 0),
    dayVwap: Number(raw.a || 0),
    windowVolume: Number(raw.v || 0),
    dayVolume: Number(raw.av || 0),
    timestampMs: Number(raw.s || 0),
  };
  latest.set(symbol, bar);
  let state = states.get(symbol);
  if (!state) {
    state = new IntradayState();
    states.set(symbol, state);
  }
  const minute = state.onBar(bar);
  if (minute) minutes.set(symbol, minute);
  // Individual stock and breakout messages are intentionally not sent.
  // Clients receive the complete list on the refresh timer only.
}

async function startFeed() {
  let ws;
  let subscribed = false;
  const connect = () => {
    ws = new WebSocket(FEED_URL);
    ws.on("open", () => {
      console.log(`[MASSIVE] connected ${FEED_URL}`);
      ws.send(JSON.stringify({ action: "auth", params: API_KEY }));
    });
    ws.on("message", (data) => {
      let events;
      try {
        events = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (!Array.isArray(events)) events = [events];
      for (const event of events) {
        if (event.ev === "status") {
          console.log(`[MASSIVE] ${event.status}: ${event.message}`);
          if (event.status === "auth_success" && !subscribed) {
            ws.send(JSON.stringify({ action: "subscribe", params: "A.*" }));
            subscribed = true;
          }
        } else if (event.ev === "A") processAggregate(event);
      }
    });
    ws.on("error", (e) => console.error("[MASSIVE] socket error:", e.message));
    ws.on("close", () => {
      subscribed = false;
      console.error("[MASSIVE] disconnected; reconnecting in 2s");
      setTimeout(connect, 2000);
    });
  };
  connect();
}

function startLocalServer() {
  const server = new WebSocketServer({ host: LOCAL_HOST, port: LOCAL_PORT });
  server.on("connection", (client) => {
    clients.add(client);
    client.send(JSON.stringify(currentSnapshot));
    client.on("message", async (data) => {
      let command;
      try {
        command = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (command.action !== "LOAD_REPLAY") return;
      try {
        client.send(
          JSON.stringify(await loadReplay(command.symbol, command.date)),
        );
      } catch (error) {
        client.send(
          JSON.stringify({ event: "REPLAY_ERROR", message: error.message }),
        );
      }
    });
    client.on("close", () => clients.delete(client));
    client.on("error", () => clients.delete(client));
  });
  console.log(`[FEED WS] listening on ws://${LOCAL_HOST}:${LOCAL_PORT}`);
}

async function main() {
  await loadCommonStocks();
  startLocalServer();
  setInterval(refresh, REFRESH_MS);
  refresh();
  await startFeed();
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
