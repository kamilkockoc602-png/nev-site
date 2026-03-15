const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const Database = require("better-sqlite3");

const app = express();
const PORT = process.env.PORT || 3000;
const railwayMountPath = process.env.RAILWAY_VOLUME_MOUNT_PATH || "";
const rawDataDir = process.env.DATA_DIR || "";
const dataDirLooksUnresolvedTemplate = rawDataDir.includes("${{");
const configuredDataDir = dataDirLooksUnresolvedTemplate
  ? railwayMountPath
  : rawDataDir || railwayMountPath;
const DATA_DIR = configuredDataDir
  ? path.resolve(configuredDataDir)
  : path.join(__dirname, "data");
const DB_PATH = path.join(DATA_DIR, "app.db");
const UPDATE_INTERVAL_MS = 10 * 60 * 1000;
const PRICE_SOURCE_URL = process.env.PRICE_SOURCE_URL || "";
const ADMIN_USERNAME = (process.env.ADMIN_USERNAME || "admin").trim();
const ADMIN_PASSWORD = (process.env.ADMIN_PASSWORD || "1234").trim();
const TARIFF_CSV_PATH = path.join(
  __dirname,
  "tariffs",
  "BakanlikFiyatTarifesi_15.09.2025.csv"
);
const SUPPLEMENTARY_TARIFF_CSV_PATH = path.join(
  __dirname,
  "tariffs",
  "KilisEkTarife.csv"
);
const PRIMARY_TARIFF_JSON_PATH = path.join(
  __dirname,
  "tariffs",
  "bakanlik_yeni_guzergahlar_15.09.2025.json"
);

fs.mkdirSync(DATA_DIR, { recursive: true });

if (process.env.RAILWAY_PROJECT_ID && !configuredDataDir) {
  console.warn(
    "Railway volume algilanmadi. Veriler deploy sonrasi kaybolabilir."
  );
}
if (dataDirLooksUnresolvedTemplate) {
  console.warn(
    "DATA_DIR degiskeni template olarak cozulmedi. RAILWAY_VOLUME_MOUNT_PATH kullaniliyor."
  );
}
console.log(`Veri klasoru: ${DATA_DIR}`);
console.log(`Veritabani dosyasi: ${DB_PATH}`);

function normalizeSearchText(value) {
  return String(value || "")
    .toLocaleLowerCase("tr-TR")
    .replace(/ı/g, "i")
    .replace(/ğ/g, "g")
    .replace(/ü/g, "u")
    .replace(/ş/g, "s")
    .replace(/ö/g, "o")
    .replace(/ç/g, "c")
    .replace(/[-_/]+/g, " ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseTurkishNumber(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    return null;
  }

  let normalized = text.replace(/[^0-9,.-]/g, "");

  const hasComma = normalized.includes(",");
  const hasDot = normalized.includes(".");

  if (hasComma && hasDot) {
    const lastComma = normalized.lastIndexOf(",");
    const lastDot = normalized.lastIndexOf(".");
    if (lastComma > lastDot) {
      // 1.234,56 -> 1234.56
      normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
    } else {
      // 1,234.56 -> 1234.56
      normalized = normalized.replace(/,/g, "");
    }
  } else if (hasComma) {
    // 1234,56 -> 1234.56
    normalized = normalized.replace(/,/g, ".");
  } else if (hasDot) {
    // Keep dot decimal as-is (e.g. 633.5). If this is thousands grouping,
    // Number() still yields a finite value for common cases.
    normalized = normalized;
  }

  const number = Number(normalized);
  return Number.isFinite(number) ? number : null;
}

function findColumnIndex(headers, aliases) {
  const normalizedHeaders = headers.map((h) => normalizeSearchText(h));
  return normalizedHeaders.findIndex((header) =>
    aliases.some((alias) => header.includes(normalizeSearchText(alias)))
  );
}

function cleanRoutePart(text) {
  return String(text || "")
    .replace(/bus\s*station/gi, "")
    .replace(/otogar[iı]?/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function loadTariffRowsFromJson(jsonPath) {
  if (!fs.existsSync(jsonPath)) {
    return [];
  }

  try {
    const raw = fs.readFileSync(jsonPath, "utf8").replace(/^\uFEFF/, "");
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }

    const rows = [];
    for (const item of parsed) {
      const routeRaw = String(item?.route || "").trim();
      const price = parseTurkishNumber(item?.price);
      const discounted = parseTurkishNumber(item?.discounted);

      if (!routeRaw || price == null || discounted == null) {
        continue;
      }

      const parts = routeRaw.split("-");
      if (parts.length < 2) {
        continue;
      }

      const origin = cleanRoutePart(parts.shift());
      const destination = cleanRoutePart(parts.join("-"));
      if (!origin || !destination) {
        continue;
      }

      const route = `${origin} - ${destination}`;
      rows.push({
        route,
        tariffPrice: price,
        discountedPrice: discounted,
        routeSearch: normalizeSearchText(route),
      });
    }

    return rows;
  } catch (error) {
    console.warn(`Tarife JSON okunamadi: ${error.message}`);
    return [];
  }
}

function loadTariffRowsFromCsv() {
  if (fs.existsSync(PRIMARY_TARIFF_JSON_PATH)) {
    const jsonRows = loadTariffRowsFromJson(PRIMARY_TARIFF_JSON_PATH);
    jsonRows.sort((a, b) => a.route.localeCompare(b.route, "tr"));
    console.log(`Ana tarife JSON yüklendi: ${jsonRows.length} satir`);
    return jsonRows;
  }

  if (!fs.existsSync(TARIFF_CSV_PATH)) {
    console.warn(`Tarife CSV bulunamadi: ${TARIFF_CSV_PATH}`);
    return [];
  }

  const content = fs.readFileSync(TARIFF_CSV_PATH, "utf8").replace(/^\uFEFF/, "");
  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length < 2) {
    return [];
  }

  const headers = lines[0].split(";").map((h) => h.trim());
  const routeIndex = findColumnIndex(headers, ["kalkis-varis", "kalkis varis", "rota"]);
  const tariffPriceIndex = findColumnIndex(headers, ["tarife fiyati", "tarife fiyat"]);
  const discountedIndex = findColumnIndex(headers, ["tarife indirimli", "indirimli"]);

  if (routeIndex < 0 || tariffPriceIndex < 0 || discountedIndex < 0) {
    console.warn("Tarife CSV kolonlari beklenen formatta degil.");
    return [];
  }

  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = lines[i].split(";");
    const routeRaw = String(cols[routeIndex] || "").trim();
    const tariffPrice = parseTurkishNumber(cols[tariffPriceIndex]);
    const discountedPrice = parseTurkishNumber(cols[discountedIndex]);
    if (!routeRaw) {
      continue;
    }
    const route = routeRaw;

    rows.push({
      route,
      tariffPrice: tariffPrice == null ? 0 : tariffPrice,
      discountedPrice: discountedPrice == null ? 0 : discountedPrice,
      routeSearch: normalizeSearchText(route),
    });
  }

  rows.sort((a, b) => a.route.localeCompare(b.route, "tr"));

  if (fs.existsSync(SUPPLEMENTARY_TARIFF_CSV_PATH)) {
    const supContent = fs
      .readFileSync(SUPPLEMENTARY_TARIFF_CSV_PATH, "utf8")
      .replace(/^\uFEFF/, "");
    const supLines = supContent
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    const existing = new Set(rows.map((item) => normalizeSearchText(item.route)));
    let added = 0;

    for (const line of supLines) {
      const cols = line.split(";");
      if (cols.length < 3) {
        continue;
      }

      const origin = String(cols[0] || "").trim();
      const destination = String(cols[1] || "").trim();
      const price = parseTurkishNumber(cols[2]);
      if (!origin || !destination || price == null) {
        continue;
      }

      const route = `${origin} - ${destination}`;
      const key = normalizeSearchText(route);
      if (existing.has(key)) {
        continue;
      }

      rows.push({
        route,
        tariffPrice: price,
        discountedPrice: price,
        routeSearch: key,
      });
      existing.add(key);
      added += 1;
    }

    console.log(`Ek tarife CSV yüklendi: ${added} yeni satir`);
  }

  rows.sort((a, b) => a.route.localeCompare(b.route, "tr"));
  console.log(`Tarife CSV yuklendi: ${rows.length} satir`);
  return rows;
}

function scoreTariffRow(row, queryNorm, queryTokens) {
  if (!queryNorm) {
    return 1;
  }

  const haystack = row.routeSearch;
  let score = 0;

  if (haystack === queryNorm) {
    score += 140;
  }
  if (haystack.startsWith(queryNorm)) {
    score += 90;
  }
  if (haystack.includes(queryNorm)) {
    score += 70;
  }

  for (const token of queryTokens) {
    if (!token) {
      continue;
    }
    if (haystack.includes(token)) {
      score += 15;
    }
  }

  return score;
}

const tariffRows = loadTariffRowsFromCsv();

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

const MENUS = [
  "dashboard",
  "routes",
  "pricing",
  "reports",
  "ocr",
  "permissions",
  "logs",
];

const ADMIN_ONLY_MENUS = new Set(["permissions", "logs"]);

db.exec(`
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password TEXT NOT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  permissions TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS login_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT,
  ip TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  token TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS route_prices (
  route TEXT PRIMARY KEY,
  economy INTEGER NOT NULL,
  standard INTEGER NOT NULL,
  vip INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS price_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  route TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL,
  is_read INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS app_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pricing_uploads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uploaded_by_user_id TEXT NOT NULL,
  uploaded_by_username TEXT NOT NULL,
  direction_type TEXT NOT NULL,
  valid_from TEXT NOT NULL,
  valid_to TEXT NOT NULL,
  source_file_name TEXT,
  is_open INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pricing_upload_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  upload_id INTEGER NOT NULL,
  route TEXT NOT NULL,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  demand_price REAL NOT NULL,
  tariff_price REAL NOT NULL,
  discounted_price REAL NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (upload_id) REFERENCES pricing_uploads(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pricing_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL,
  is_read INTEGER NOT NULL DEFAULT 0
);
`);

const tariffRouteMap = new Map();
for (const row of tariffRows) {
  const key = normalizeSearchText(row.route);
  const existing = tariffRouteMap.get(key);
  if (!existing) {
    tariffRouteMap.set(key, row);
  }
}

function findTariffByRoute(origin, destination) {
  const direct = normalizeSearchText(`${origin} ${destination}`);
  const reverse = normalizeSearchText(`${destination} ${origin}`);
  return tariffRouteMap.get(direct) || tariffRouteMap.get(reverse) || null;
}

function parsePriceNumber(raw) {
  const value = Number(raw);
  if (Number.isFinite(value)) {
    return value;
  }
  return parseTurkishNumber(raw);
}

function addPricingNotification(username, message) {
  db.prepare(
    "INSERT INTO pricing_notifications (username, message, created_at, is_read) VALUES (?, ?, ?, 0)"
  ).run(username, message, nowStamp());
}

function nowStamp() {
  return new Date().toLocaleString("tr-TR");
}

function defaultPermissions(fullAccess) {
  const entries = MENUS.map((menuKey) => {
    if (ADMIN_ONLY_MENUS.has(menuKey)) {
      return [menuKey, false];
    }
    return [menuKey, Boolean(fullAccess) || menuKey === "dashboard"];
  });
  return Object.fromEntries(entries);
}

function ensureAdmin() {
  const existing = db
    .prepare("SELECT id FROM users WHERE username = ?")
    .get(ADMIN_USERNAME);
  if (existing) {
    db.prepare(
      "UPDATE users SET password = ?, is_admin = 1, is_active = 1 WHERE id = ?"
    ).run(ADMIN_PASSWORD, existing.id);
    return;
  }

  const insert = db.prepare(
    "INSERT INTO users (id, username, password, is_admin, is_active, permissions, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
  );
  insert.run(
    crypto.randomUUID(),
    ADMIN_USERNAME,
    ADMIN_PASSWORD,
    1,
    1,
    JSON.stringify(Object.fromEntries(MENUS.map((menuKey) => [menuKey, true]))),
    nowStamp()
  );
}

ensureAdmin();

function seedPricesIfEmpty() {
  const count = db.prepare("SELECT COUNT(*) AS total FROM route_prices").get().total;
  if (count > 0) {
    return;
  }

  const sample = [
    { route: "Istanbul - Ankara", economy: 640, standard: 760, vip: 980 },
    { route: "Istanbul - Izmir", economy: 700, standard: 835, vip: 1010 },
    { route: "Ankara - Antalya", economy: 675, standard: 790, vip: 930 },
  ];

  const insert = db.prepare(
    "INSERT INTO route_prices (route, economy, standard, vip, updated_at) VALUES (?, ?, ?, ?, ?)"
  );

  const trx = db.transaction((items) => {
    for (const item of items) {
      insert.run(item.route, item.economy, item.standard, item.vip, nowStamp());
    }
  });

  trx(sample);
  db.prepare("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('price_update_count', '1')").run();
  db.prepare("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('last_price_update', ?)").run(nowStamp());
}

seedPricesIfEmpty();

function getPrices() {
  return db
    .prepare(
      "SELECT route, economy, standard, vip, updated_at FROM route_prices ORDER BY route ASC"
    )
    .all();
}

function getMeta(key, fallback = "") {
  const row = db.prepare("SELECT value FROM app_meta WHERE key = ?").get(key);
  return row ? row.value : fallback;
}

function setMeta(key, value) {
  db.prepare("INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)").run(key, String(value));
}

function varyPrice(value) {
  const delta = Math.floor(Math.random() * 41) - 20;
  return Math.max(200, value + delta);
}

async function fetchPriceList() {
  if (PRICE_SOURCE_URL) {
    const res = await fetch(PRICE_SOURCE_URL);
    if (!res.ok) {
      throw new Error("Fiyat kaynagi yanit vermedi.");
    }

    const list = await res.json();
    if (!Array.isArray(list)) {
      throw new Error("Fiyat kaynagi veri formati gecersiz.");
    }

    return list
      .filter((item) => item && item.route)
      .map((item) => ({
        route: String(item.route),
        economy: Number(item.economy) || 0,
        standard: Number(item.standard) || 0,
        vip: Number(item.vip) || 0,
      }));
  }

  // Harici kaynak verilmezse local fiyatlar uzerinde simule guncelleme yapar.
  return getPrices().map((row) => ({
    route: row.route,
    economy: varyPrice(row.economy),
    standard: varyPrice(row.standard),
    vip: varyPrice(row.vip),
  }));
}

async function refreshPrices(reason = "interval") {
  const current = getPrices();
  const byRoute = new Map(current.map((item) => [item.route, item]));
  const incoming = await fetchPriceList();

  const upsert = db.prepare(
    `
    INSERT INTO route_prices (route, economy, standard, vip, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(route) DO UPDATE SET
      economy = excluded.economy,
      standard = excluded.standard,
      vip = excluded.vip,
      updated_at = excluded.updated_at
    `
  );

  const insertNotification = db.prepare(
    "INSERT INTO price_notifications (route, message, created_at, is_read) VALUES (?, ?, ?, 0)"
  );

  let changedCount = 0;
  const stamp = nowStamp();

  const trx = db.transaction((rows) => {
    for (const item of rows) {
      const prev = byRoute.get(item.route);
      upsert.run(item.route, item.economy, item.standard, item.vip, stamp);

      if (prev) {
        const fields = [
          ["Ekonomi", prev.economy, item.economy],
          ["Standart", prev.standard, item.standard],
          ["VIP", prev.vip, item.vip],
        ];

        for (const [label, oldVal, newVal] of fields) {
          if (oldVal !== newVal) {
            changedCount += 1;
            const direction = newVal > oldVal ? "yukseldi" : "dustu";
            const msg = `${item.route} - ${label} ${oldVal} TL -> ${newVal} TL (${direction})`;
            insertNotification.run(item.route, msg, stamp);
          }
        }
      } else {
        changedCount += 1;
        insertNotification.run(item.route, `${item.route} rotasi sisteme eklendi.`, stamp);
      }
    }
  });

  trx(incoming);

  const prevCount = Number(getMeta("price_update_count", "1")) || 1;
  setMeta("price_update_count", prevCount + 1);
  setMeta("last_price_update", stamp);
  setMeta("last_update_reason", reason);

  return { changedCount };
}

let refreshRunning = false;
async function safeRefreshPrices(reason) {
  if (refreshRunning) {
    return;
  }
  refreshRunning = true;
  try {
    await refreshPrices(reason);
  } catch (error) {
    console.error("Fiyat guncelleme hatasi:", error.message);
  } finally {
    refreshRunning = false;
  }
}

setInterval(() => {
  safeRefreshPrices("interval");
}, UPDATE_INTERVAL_MS);

safeRefreshPrices("startup");

app.use(express.json({ limit: "25mb" }));
app.use(express.static(__dirname));

function logAttempt({ username, status, reason, ip }) {
  db.prepare(
    "INSERT INTO login_logs (username, status, reason, ip, created_at) VALUES (?, ?, ?, ?, ?)"
  ).run(username, status, reason || null, ip || null, nowStamp());
}

function sanitizeUser(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    username: row.username,
    isAdmin: Boolean(row.is_admin),
    isActive: Boolean(row.is_active),
    permissions: JSON.parse(row.permissions || "{}"),
    createdAt: row.created_at,
  };
}

function getAuthUser(req) {
  const authHeader = req.headers.authorization || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  if (!token) {
    return null;
  }

  const session = db
    .prepare("SELECT user_id FROM sessions WHERE token = ?")
    .get(token);

  if (!session) {
    return null;
  }

  const user = db
    .prepare(
      "SELECT id, username, is_admin, is_active, permissions, created_at FROM users WHERE id = ?"
    )
    .get(session.user_id);

  if (!user) {
    db.prepare("DELETE FROM sessions WHERE token = ?").run(token);
    return null;
  }

  return { token, user: sanitizeUser(user) };
}

function requireAuth(req, res, next) {
  const auth = getAuthUser(req);
  if (!auth) {
    res.status(401).json({ message: "Oturum gecersiz." });
    return;
  }

  if (!auth.user.isActive && !auth.user.isAdmin) {
    res.status(403).json({ message: "Hesabin pasif durumda." });
    return;
  }

  req.auth = auth;
  next();
}

function requireAdmin(req, res, next) {
  if (!req.auth?.user?.isAdmin) {
    res.status(403).json({ message: "Bu islem sadece admin icindir." });
    return;
  }
  next();
}

app.post("/api/login", (req, res) => {
  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "").trim();

  if (!username || !password) {
    res.status(400).json({ message: "Kullanici adi ve sifre zorunlu." });
    return;
  }

  const user = db
    .prepare(
      "SELECT id, username, password, is_admin, is_active, permissions, created_at FROM users WHERE username = ?"
    )
    .get(username);

  if (!user || user.password !== password) {
    logAttempt({ username, status: "fail", reason: "Hatali kimlik", ip: req.ip });
    res.status(401).json({ message: "Kullanici adi veya sifre hatali." });
    return;
  }

  if (!user.is_admin && !user.is_active) {
    logAttempt({ username, status: "fail", reason: "Pasif hesap", ip: req.ip });
    res.status(403).json({ message: "Hesabiniz pasif durumda." });
    return;
  }

  const token = crypto.randomUUID();
  db.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)").run(
    token,
    user.id,
    nowStamp()
  );

  logAttempt({ username, status: "ok", reason: "", ip: req.ip });

  res.json({
    token,
    user: sanitizeUser(user),
  });
});

app.post("/api/logout", requireAuth, (req, res) => {
  db.prepare("DELETE FROM sessions WHERE token = ?").run(req.auth.token);
  res.json({ ok: true });
});

app.get("/api/me", requireAuth, (req, res) => {
  res.json({ user: req.auth.user });
});

app.get("/api/admin/users", requireAuth, requireAdmin, (req, res) => {
  const rows = db
    .prepare(
      "SELECT id, username, is_admin, is_active, permissions, created_at FROM users ORDER BY is_admin DESC, username ASC"
    )
    .all();

  res.json({
    users: rows.map(sanitizeUser),
  });
});

app.post("/api/admin/users", requireAuth, requireAdmin, (req, res) => {
  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "").trim();
  const fullAccess = Boolean(req.body?.fullAccess);

  if (!username || !password) {
    res.status(400).json({ message: "Kullanici adi ve sifre zorunlu." });
    return;
  }

  const existing = db.prepare("SELECT id FROM users WHERE username = ?").get(username);
  if (existing) {
    res.status(409).json({ message: "Bu kullanici adi zaten var." });
    return;
  }

  db.prepare(
    "INSERT INTO users (id, username, password, is_admin, is_active, permissions, created_at) VALUES (?, ?, ?, 0, 1, ?, ?)"
  ).run(crypto.randomUUID(), username, password, JSON.stringify(defaultPermissions(fullAccess)), nowStamp());

  res.json({ ok: true });
});

app.patch("/api/admin/users/:id", requireAuth, requireAdmin, (req, res) => {
  const userId = String(req.params.id || "").trim();
  const target = db
    .prepare(
      "SELECT id, username, password, is_admin, is_active, permissions, created_at FROM users WHERE id = ?"
    )
    .get(userId);

  if (!target) {
    res.status(404).json({ message: "Kullanici bulunamadi." });
    return;
  }

  if (target.is_admin) {
    res.status(400).json({ message: "Admin hesabi bu ekrandan duzenlenemez." });
    return;
  }

  const nextPermissions = req.body?.permissions;
  let normalizedPermissions = JSON.parse(target.permissions || "{}");

  if (nextPermissions && typeof nextPermissions === "object") {
    normalizedPermissions = { ...normalizedPermissions };
    for (const menuKey of MENUS) {
      if (ADMIN_ONLY_MENUS.has(menuKey)) {
        normalizedPermissions[menuKey] = false;
      } else if (Object.prototype.hasOwnProperty.call(nextPermissions, menuKey)) {
        normalizedPermissions[menuKey] = Boolean(nextPermissions[menuKey]);
      }
    }

    const hasAny = Object.entries(normalizedPermissions).some(
      ([menuKey, allowed]) => !ADMIN_ONLY_MENUS.has(menuKey) && Boolean(allowed)
    );

    if (!hasAny) {
      res.status(400).json({ message: "En az bir menu yetkisi verilmeli." });
      return;
    }
  }

  const newPassword =
    typeof req.body?.password === "string" ? req.body.password.trim() : "";
  const newUsername =
    typeof req.body?.username === "string" ? req.body.username.trim() : "";
  const shouldUpdatePassword = Boolean(newPassword);
  const shouldUpdateUsername = Boolean(newUsername) && newUsername !== target.username;

  if (shouldUpdateUsername) {
    const existing = db
      .prepare("SELECT id FROM users WHERE username = ? AND id != ?")
      .get(newUsername, userId);
    if (existing) {
      res.status(409).json({ message: "Bu kullanici adi zaten kullaniliyor." });
      return;
    }
  }

  const isActive =
    typeof req.body?.isActive === "boolean"
      ? (req.body.isActive ? 1 : 0)
      : target.is_active;

  db.prepare(
    "UPDATE users SET username = ?, password = ?, is_active = ?, permissions = ? WHERE id = ?"
  ).run(
    shouldUpdateUsername ? newUsername : target.username,
    shouldUpdatePassword ? newPassword : target.password,
    isActive,
    JSON.stringify(normalizedPermissions),
    userId
  );

  if (!isActive) {
    db.prepare("DELETE FROM sessions WHERE user_id = ?").run(userId);
  }

  res.json({ ok: true });
});

app.delete("/api/admin/users/:id", requireAuth, requireAdmin, (req, res) => {
  const userId = String(req.params.id || "").trim();
  const target = db.prepare("SELECT is_admin FROM users WHERE id = ?").get(userId);
  if (!target) {
    res.status(404).json({ message: "Kullanici bulunamadi." });
    return;
  }

  if (target.is_admin) {
    res.status(400).json({ message: "Admin silinemez." });
    return;
  }

  db.prepare("DELETE FROM sessions WHERE user_id = ?").run(userId);
  db.prepare("DELETE FROM users WHERE id = ?").run(userId);
  res.json({ ok: true });
});

app.get("/api/admin/logs", requireAuth, requireAdmin, (req, res) => {
  const logs = db
    .prepare(
      "SELECT username, status, reason, ip, created_at FROM login_logs ORDER BY id DESC LIMIT 200"
    )
    .all();

  res.json({
    logs: logs.map((log) => ({
      username: log.username,
      status: log.status,
      reason: log.reason,
      ip: log.ip,
      time: log.created_at,
    })),
  });
});

app.get("/api/prices", requireAuth, (req, res) => {
  const prices = getPrices();
  const avg =
    prices.length > 0
      ? Math.round(
          prices.reduce((acc, item) => acc + item.economy + item.standard + item.vip, 0) /
            (prices.length * 3)
        )
      : 0;

  res.json({
    prices: prices.map((item) => ({
      route: item.route,
      economy: item.economy,
      standard: item.standard,
      vip: item.vip,
      updatedAt: item.updated_at,
    })),
    avgFare: avg,
    totalRoutes: prices.length,
    updateCount: Number(getMeta("price_update_count", "1")) || 1,
    lastUpdated: getMeta("last_price_update", "-"),
  });
});

app.get("/api/tariff-prices", requireAuth, (req, res) => {
  const query = String(req.query.q || "").trim();
  const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 2000);
  const offset = Math.max(Number(req.query.offset) || 0, 0);
  const queryNorm = normalizeSearchText(query);
  const queryTokens = queryNorm.split(" ").filter(Boolean);

  let matched = tariffRows;
  if (queryNorm) {
    const exactMatches = tariffRows.filter((row) => row.routeSearch === queryNorm);
    if (exactMatches.length > 0) {
      matched = exactMatches;
    } else {
      matched = tariffRows
        .filter((row) => {
          if (queryTokens.length <= 1) {
            return row.routeSearch.includes(queryNorm);
          }
          return queryTokens.every((token) => row.routeSearch.includes(token));
        })
        .map((row) => ({ row, score: scoreTariffRow(row, queryNorm, queryTokens) }))
        .sort((a, b) => b.score - a.score || a.row.route.localeCompare(b.row.route, "tr"))
        .map((item) => item.row);
    }
  }

  const rows = matched.slice(offset, offset + limit).map((row) => ({
    route: row.route,
    tariffPrice: row.tariffPrice,
    discountedPrice: row.discountedPrice,
  }));

  res.json({
    query,
    total: matched.length,
    offset,
    limit,
    rows,
  });
});

app.get("/api/pricing-uploads", requireAuth, (req, res) => {
  const uploads = db
    .prepare(
      "SELECT id, uploaded_by_username, direction_type, valid_from, valid_to, source_file_name, is_open, created_at FROM pricing_uploads ORDER BY id DESC LIMIT 50"
    )
    .all();

  const itemQuery = db.prepare(
    "SELECT route, demand_price, tariff_price, discounted_price FROM pricing_upload_items WHERE upload_id = ? ORDER BY id ASC"
  );

  res.json({
    uploads: uploads.map((upload) => ({
      id: upload.id,
      uploadedBy: upload.uploaded_by_username,
      directionType: upload.direction_type,
      validFrom: upload.valid_from,
      validTo: upload.valid_to,
      sourceFileName: upload.source_file_name || "",
      isOpen: Boolean(upload.is_open),
      createdAt: upload.created_at,
      items: itemQuery.all(upload.id).map((item) => ({
        route: item.route,
        demandPrice: Number(item.demand_price),
        tariffPrice: Number(item.tariff_price),
        discountedPrice: Number(item.discounted_price),
      })),
    })),
  });
});

app.patch("/api/pricing-uploads/:id/toggle", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const isOpen = Boolean(req.body?.isOpen);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ message: "Gecersiz kayit." });
    return;
  }

  const found = db.prepare("SELECT id FROM pricing_uploads WHERE id = ?").get(id);
  if (!found) {
    res.status(404).json({ message: "Kayit bulunamadi." });
    return;
  }

  db.prepare("UPDATE pricing_uploads SET is_open = ? WHERE id = ?").run(isOpen ? 1 : 0, id);
  res.json({ ok: true });
});

app.delete("/api/pricing-uploads/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ message: "Gecersiz kayit." });
    return;
  }

  const upload = db
    .prepare("SELECT id, uploaded_by_user_id, uploaded_by_username FROM pricing_uploads WHERE id = ?")
    .get(id);

  if (!upload) {
    res.status(404).json({ message: "Kayit bulunamadi." });
    return;
  }

  const isOwner = upload.uploaded_by_user_id === req.auth.user.id;
  if (!isOwner && !req.auth.user.isAdmin) {
    res.status(403).json({ message: "Bu kaydi silme yetkin yok." });
    return;
  }

  const trx = db.transaction(() => {
    db.prepare("DELETE FROM pricing_upload_items WHERE upload_id = ?").run(id);
    db.prepare("DELETE FROM pricing_uploads WHERE id = ?").run(id);
  });
  trx();

  addPricingNotification(
    req.auth.user.username,
    `${req.auth.user.username} fiyat yuklemesini sildi (kayit #${id}).`
  );

  res.json({ ok: true });
});

app.post("/api/pricing-uploads", requireAuth, (req, res) => {
  const directionType = String(req.body?.directionType || "").trim();
  const validFrom = String(req.body?.validFrom || "").trim();
  const validTo = String(req.body?.validTo || "").trim();
  const sourceFileName = String(req.body?.sourceFileName || "").trim();
  const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];

  if (!directionType || !validFrom || !validTo) {
    res.status(400).json({ message: "Fiyat tipi ve tarih araligi zorunlu." });
    return;
  }

  if (!rows.length) {
    res.status(400).json({ message: "Excel verisinde satir bulunamadi." });
    return;
  }

  const startDate = new Date(validFrom);
  const endDate = new Date(validTo);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || startDate > endDate) {
    res.status(400).json({ message: "Tarih araligi gecersiz." });
    return;
  }

  const accepted = [];
  const rejected = [];

  rows.forEach((row, index) => {
    const origin = String(row?.origin || "").trim();
    const destination = String(row?.destination || "").trim();
    const demandPrice = parsePriceNumber(row?.demandPrice);
    const rowNumber = Number(row?.rowNumber) || index + 1;

    if (!origin || !destination) {
      rejected.push({ rowNumber, reason: "Nereden/Nereye bos olamaz." });
      return;
    }

    if (!Number.isFinite(demandPrice)) {
      rejected.push({ rowNumber, reason: "Talep fiyati sayi olmali." });
      return;
    }

    const tariff = findTariffByRoute(origin, destination);
    if (!tariff) {
      rejected.push({
        rowNumber,
        reason: `${origin} - ${destination} icin bakanlik fiyat kaydi bulunamadi. Guzergahi duzeltip tekrar yukleyin.`,
      });
      return;
    }

    const minPrice = Math.min(Number(tariff.discountedPrice), Number(tariff.tariffPrice));
    const maxPrice = Math.max(Number(tariff.discountedPrice), Number(tariff.tariffPrice));
    if (demandPrice < minPrice || demandPrice > maxPrice) {
      rejected.push({
        rowNumber,
        reason: `${origin} - ${destination} icin talep fiyati ${minPrice} - ${maxPrice} araliginda olmali.`,
      });
      return;
    }

    accepted.push({
      route: tariff.route,
      origin,
      destination,
      demandPrice,
      tariffPrice: Number(tariff.tariffPrice),
      discountedPrice: Number(tariff.discountedPrice),
    });
  });

  if (!accepted.length) {
    res.status(400).json({
      message: "Gecerli fiyat satiri yok. Lutfen aralik kurallarini kontrol et.",
      rejected,
    });
    return;
  }

  const trx = db.transaction(() => {
    const insertUpload = db.prepare(
      "INSERT INTO pricing_uploads (uploaded_by_user_id, uploaded_by_username, direction_type, valid_from, valid_to, source_file_name, is_open, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?)"
    );
    const result = insertUpload.run(
      req.auth.user.id,
      req.auth.user.username,
      directionType,
      validFrom,
      validTo,
      sourceFileName,
      nowStamp()
    );

    const uploadId = Number(result.lastInsertRowid);
    const insertItem = db.prepare(
      "INSERT INTO pricing_upload_items (upload_id, route, origin, destination, demand_price, tariff_price, discounted_price, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    );

    for (const row of accepted) {
      insertItem.run(
        uploadId,
        row.route,
        row.origin,
        row.destination,
        row.demandPrice,
        row.tariffPrice,
        row.discountedPrice,
        nowStamp()
      );
    }

    addPricingNotification(
      req.auth.user.username,
      `${req.auth.user.username} fiyat yukledi (${accepted.length} satir, ${directionType}, ${validFrom} - ${validTo})`
    );

    return uploadId;
  });

  const uploadId = trx();
  res.json({
    ok: true,
    uploadId,
    acceptedCount: accepted.length,
    rejected,
  });
});

app.post("/api/admin/prices/refresh", requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await refreshPrices("manual");
    res.json({ ok: true, changedCount: result.changedCount });
  } catch (error) {
    res.status(500).json({ message: `Fiyatlar guncellenemedi: ${error.message}` });
  }
});

app.get("/api/notifications", requireAuth, (req, res) => {
  const priceNotifications = db
    .prepare(
      "SELECT id, route, message, created_at, is_read FROM price_notifications ORDER BY id DESC LIMIT 100"
    )
    .all();

  const pricingNotifications = db
    .prepare(
      "SELECT id, username, message, created_at, is_read FROM pricing_notifications ORDER BY id DESC LIMIT 100"
    )
    .all();

  const notifications = [
    ...priceNotifications.map((n) => ({
      id: `price-${n.id}`,
      route: n.route,
      message: n.message,
      time: n.created_at,
      isRead: Boolean(n.is_read),
    })),
    ...pricingNotifications.map((n) => ({
      id: `upload-${n.id}`,
      route: n.username,
      message: n.message,
      time: n.created_at,
      isRead: Boolean(n.is_read),
    })),
  ]
    .sort((a, b) => String(b.time).localeCompare(String(a.time), "tr"))
    .slice(0, 120);

  res.json({
    notifications,
    unreadCount: notifications.filter((n) => !n.isRead).length,
  });
});

app.post("/api/notifications/read-all", requireAuth, (req, res) => {
  db.prepare("UPDATE price_notifications SET is_read = 1 WHERE is_read = 0").run();
  db.prepare("UPDATE pricing_notifications SET is_read = 1 WHERE is_read = 0").run();
  res.json({ ok: true });
});

app.use((error, req, res, next) => {
  if (error && error.type === "entity.too.large") {
    res.status(413).json({ message: "Excel dosyasi cok buyuk. Daha kucuk parcaya bolerek yukleyin." });
    return;
  }

  if (error) {
    res.status(500).json({ message: "Sunucu hatasi olustu." });
    return;
  }

  next();
});

app.listen(PORT, () => {
  console.log(`Sunucu calisiyor: http://localhost:${PORT}`);
});
