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
  if (!fs.existsSync(PRIMARY_TARIFF_JSON_PATH)) {
    console.warn(`Ana tarife JSON bulunamadi: ${PRIMARY_TARIFF_JSON_PATH}`);
    return [];
  }

  const rows = loadTariffRowsFromJson(PRIMARY_TARIFF_JSON_PATH);
  rows.sort((a, b) => a.route.localeCompare(b.route, "tr"));
  console.log(`Ana tarife JSON yüklendi: ${rows.length} satir`);
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
  "reporting",
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
  description TEXT NOT NULL DEFAULT '',
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
  row_direction TEXT NOT NULL DEFAULT 'Gidis-Donus',
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

CREATE TABLE IF NOT EXISTS operations_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  report_date TEXT NOT NULL,
  origin_name TEXT NOT NULL,
  destination_name TEXT NOT NULL,
  route_label TEXT NOT NULL,
  ride_uuid TEXT NOT NULL,
  from_station_id TEXT NOT NULL DEFAULT '',
  to_station_id TEXT NOT NULL DEFAULT '',
  line_code TEXT,
  trip_number TEXT,
  departure_time TEXT,
  arrival_time TEXT,
  seats_available INTEGER,
  occupancy_percent INTEGER,
  occupancy_level TEXT,
  is_delayed INTEGER NOT NULL DEFAULT 0,
  delay_minutes INTEGER NOT NULL DEFAULT 0,
  vehicle_plate TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  payload_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(report_date, ride_uuid)
);
`);

const pricingItemColumns = db.prepare("PRAGMA table_info(pricing_upload_items)").all();
const hasRowDirectionColumn = pricingItemColumns.some((col) => col.name === "row_direction");
if (!hasRowDirectionColumn) {
  db.exec(
    "ALTER TABLE pricing_upload_items ADD COLUMN row_direction TEXT NOT NULL DEFAULT 'Gidis-Donus'"
  );
}

const pricingUploadColumns = db.prepare("PRAGMA table_info(pricing_uploads)").all();
const hasDescriptionColumn = pricingUploadColumns.some((col) => col.name === "description");
if (!hasDescriptionColumn) {
  db.exec("ALTER TABLE pricing_uploads ADD COLUMN description TEXT NOT NULL DEFAULT ''");
}

const reportingColumns = db.prepare("PRAGMA table_info(operations_reports)").all();
const hasFromStationColumn = reportingColumns.some((col) => col.name === "from_station_id");
if (!hasFromStationColumn) {
  db.exec("ALTER TABLE operations_reports ADD COLUMN from_station_id TEXT NOT NULL DEFAULT ''");
}
const hasToStationColumn = reportingColumns.some((col) => col.name === "to_station_id");
if (!hasToStationColumn) {
  db.exec("ALTER TABLE operations_reports ADD COLUMN to_station_id TEXT NOT NULL DEFAULT ''");
}

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
  const parsed = parseTurkishNumber(raw);
  return parsed == null ? NaN : parsed;
}

function addPricingNotification(username, message) {
  db.prepare(
    "INSERT INTO pricing_notifications (username, message, created_at, is_read) VALUES (?, ?, ?, 0)"
  ).run(username, message, nowStamp());
}

function nowStamp() {
  return new Date().toLocaleString("tr-TR", {
    timeZone: "Europe/Istanbul",
    hour12: false,
  });
}

function toDotDate(dateText) {
  const raw = String(dateText || "").trim();
  const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    return "";
  }
  return `${m[3]}.${m[2]}.${m[1]}`;
}

function todayIsoInIstanbul() {
  const raw = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/Istanbul" });
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
  }
  return raw;
}

function parseIsoToStamp(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }

  const date = new Date(text);
  if (Number.isNaN(date.getTime())) {
    return text;
  }

  return date.toLocaleString("tr-TR", {
    timeZone: "Europe/Istanbul",
    hour12: false,
  });
}

function estimateOccupancyPercent(item) {
  const seats = Number(item?.available?.seats);
  if (Number.isFinite(seats)) {
    const estimated = Math.round((1 - seats / 46) * 100);
    return Math.max(0, Math.min(100, estimated));
  }

  const level = String(item?.remaining?.capacity || "").toLocaleLowerCase("tr-TR");
  if (level === "low") return 85;
  if (level === "medium") return 65;
  if (level === "high") return 35;
  return null;
}

const REPORTING_SOURCE = {
  originName: "Siirt",
  fromStationId: "45e1a606-8cd1-46bc-aebd-7f67a4909a4d",
};

const REPORTING_TARGETS = [
  { destinationName: "Marmaris", toStationId: "66615e19-06cb-4c6f-93df-c3bf05b40e6a" },
  { destinationName: "Antalya", toStationId: "9b6c0630-3ecb-11ea-8017-02437075395e" },
  { destinationName: "Esenler", toStationId: "9b6a8316-3ecb-11ea-8017-02437075395e" },
  { destinationName: "Ankara", toStationId: "9b6a837a-3ecb-11ea-8017-02437075395e" },
  { destinationName: "Izmir", toStationId: "9b6a8396-3ecb-11ea-8017-02437075395e" },
];

const REPORTING_TARGET_MAP = new Map(
  REPORTING_TARGETS.map((item) => [String(item.toStationId), item.destinationName])
);

function getIstanbulDayRangeMs(reportDateIso) {
  const raw = String(reportDateIso || "").trim();
  const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    return null;
  }

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  const from = Date.parse(`${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T00:00:00+03:00`);
  const to = Date.parse(`${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T23:59:59+03:00`);

  if (!Number.isFinite(from) || !Number.isFinite(to)) {
    return null;
  }

  return { from, to };
}

function fromUnixSecondsToStamp(seconds, tz = "Europe/Istanbul") {
  const sec = Number(seconds);
  if (!Number.isFinite(sec) || sec <= 0) {
    return "";
  }

  return new Date(sec * 1000).toLocaleString("tr-TR", {
    timeZone: String(tz || "Europe/Istanbul"),
    hour12: false,
  });
}

async function fetchRouteAvailabilityMap(reportDateIso, origin, target) {
  const dotDate = toDotDate(reportDateIso);
  if (!dotDate) {
    throw new Error("Rapor tarihi gecersiz.");
  }

  const params = new URLSearchParams({
    from_station_id: origin.fromStationId,
    to_station_id: target.toStationId,
    departure_date: dotDate,
    products: '{"adult":1}',
    currency: "TRY",
    locale: "tr",
    search_by: "stations",
    include_after_midnight_rides: "1",
  });

  const url = `https://global.api.flixbus.com/search/service/v4/search?${params.toString()}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`${origin.originName} - ${target.destinationName} seferleri alinamadi.`);
  }

  const data = await res.json();
  const trips = Array.isArray(data?.trips) ? data.trips : [];
  const map = new Map();

  for (const trip of trips) {
    const resultObj = trip?.results && typeof trip.results === "object" ? trip.results : {};
    for (const item of Object.values(resultObj)) {
      if (!item || String(item?.status || "") === "unavailable") {
        continue;
      }

      const leg = Array.isArray(item?.legs) && item.legs.length ? item.legs[0] : null;
      const rideUuid = String(leg?.ride_id || "").trim();
      if (!rideUuid) {
        continue;
      }

      const departureTime = parseIsoToStamp(item?.departure?.date || "");
      const arrivalTime = parseIsoToStamp(item?.arrival?.date || "");
      const seatsAvailable = Number(item?.available?.seats);
      const occupancyPercent = estimateOccupancyPercent(item);
      const occupancyLevel = String(item?.remaining?.capacity || "").trim();

      map.set(rideUuid, {
        seatsAvailable: Number.isFinite(seatsAvailable) ? seatsAvailable : null,
        occupancyPercent: Number.isFinite(occupancyPercent) ? occupancyPercent : null,
        occupancyLevel,
        payloadJson: JSON.stringify(item || {}),
      });
    }
  }

  return map;
}

async function fetchLiveDelayMinutes(rideUuid, fromStationId, toStationId) {
  const url = `https://global.api.flixbus.com/gis/v1/ride/${encodeURIComponent(rideUuid)}/trip-info/live?from_station_uuid=${encodeURIComponent(fromStationId)}&to_station_uuid=${encodeURIComponent(toStationId)}`;
  const res = await fetch(url);
  if (!res.ok) {
    return 0;
  }

  const data = await res.json();
  const stops = Array.isArray(data?.stops) ? data.stops : [];
  const originStop = stops.find((stop) => String(stop?.stop_uuid || "") === String(fromStationId));
  const sec = Number(originStop?.departure?.deviation_seconds);
  if (!Number.isFinite(sec)) {
    return 0;
  }
  return Math.round(sec / 60);
}

async function fetchTripDetailsTiming(rideUuid, fromStationId, toStationId) {
  const uid = `direct:${rideUuid}:${fromStationId}:${toStationId}`;
  const url = `https://global.api.flixbus.com/search/service/v2/trip/details?trip=${encodeURIComponent(uid)}`;
  const res = await fetch(url);
  if (!res.ok) {
    return { departureTime: "", arrivalTime: "", tripNumber: "", lineCode: "" };
  }

  const data = await res.json();
  const itinerary = Array.isArray(data?.itinerary) ? data.itinerary : [];
  const ridePart = itinerary.find((part) => String(part?.type || "") === "ride") || {};
  const segments = Array.isArray(ridePart?.segments) ? ridePart.segments : [];
  const firstSeg = segments[0] || {};
  const lastSeg = segments[segments.length - 1] || {};

  return {
    departureTime: parseIsoToStamp(firstSeg?.departure_date || data?.departure?.date || ""),
    arrivalTime: parseIsoToStamp(lastSeg?.arrival_date || data?.arrival?.date || ""),
    tripNumber: String(data?.uid || "").trim(),
    lineCode: String(ridePart?.line?.code || "").trim(),
  };
}

async function fetchSiirtDepartures(reportDateIso) {
  const range = getIstanbulDayRangeMs(reportDateIso);
  if (!range) {
    throw new Error("Rapor tarihi gecersiz.");
  }

  const url = `https://global.api.flixbus.com/gis/v1/station/${REPORTING_SOURCE.fromStationId}/timetable/departures/from-timestamp/${range.from}/to-timestamp/${range.to}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error("Siirt kalkisli seferler alinamadi.");
  }

  const data = await res.json();
  const rides = Array.isArray(data?.rides) ? data.rides : [];
  return rides.filter((ride) => REPORTING_TARGET_MAP.has(String(ride?.to_stop_uuid || "")));
}

async function collectOperationsReportRows(reportDateIso) {
  const departures = await fetchSiirtDepartures(reportDateIso);
  const availabilityMaps = new Map();
  for (const target of REPORTING_TARGETS) {
    const map = await fetchRouteAvailabilityMap(reportDateIso, REPORTING_SOURCE, target);
    availabilityMaps.set(target.toStationId, map);
  }

  const rows = [];
  for (const ride of departures) {
    const rideUuid = String(ride?.ride_uuid || "").trim();
    const toStationId = String(ride?.to_stop_uuid || "").trim();
    if (!rideUuid || !toStationId) {
      continue;
    }

    const destinationName = REPORTING_TARGET_MAP.get(toStationId) || "Hedef";
    const base = availabilityMaps.get(toStationId)?.get(rideUuid) || {};
    const timing = await fetchTripDetailsTiming(rideUuid, REPORTING_SOURCE.fromStationId, toStationId);
    const delayMinutes = await fetchLiveDelayMinutes(rideUuid, REPORTING_SOURCE.fromStationId, toStationId);

    rows.push({
      reportDate: reportDateIso,
      originName: REPORTING_SOURCE.originName,
      destinationName,
      routeLabel: `${REPORTING_SOURCE.originName} -> ${destinationName}`,
      rideUuid,
      lineCode: timing.lineCode || String(ride?.line_code || "").trim(),
      tripNumber: timing.tripNumber || "",
      departureTime: timing.departureTime || fromUnixSecondsToStamp(ride?.planned?.timestamp, ride?.planned?.tz),
      arrivalTime: timing.arrivalTime || "",
      seatsAvailable: Number.isFinite(Number(base.seatsAvailable)) ? Number(base.seatsAvailable) : null,
      occupancyPercent: Number.isFinite(Number(base.occupancyPercent)) ? Number(base.occupancyPercent) : null,
      occupancyLevel: String(base.occupancyLevel || "veri-yok"),
      delayMinutes,
      isDelayed: delayMinutes !== 0 ? 1 : 0,
      payloadJson: base.payloadJson || JSON.stringify(ride || {}),
    });
  }

  rows.sort((a, b) => String(a.departureTime || "").localeCompare(String(b.departureTime || ""), "tr"));
  return rows;
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

const permissionRows = db
  .prepare("SELECT id, is_admin, permissions FROM users")
  .all();
for (const row of permissionRows) {
  let parsed = {};
  try {
    parsed = JSON.parse(row.permissions || "{}");
  } catch {
    parsed = {};
  }

  let changed = false;
  for (const menuKey of MENUS) {
    if (Object.prototype.hasOwnProperty.call(parsed, menuKey)) {
      continue;
    }

    if (row.is_admin) {
      parsed[menuKey] = true;
    } else if (ADMIN_ONLY_MENUS.has(menuKey)) {
      parsed[menuKey] = false;
    } else if (menuKey === "dashboard") {
      parsed[menuKey] = true;
    } else if (menuKey === "reporting") {
      parsed[menuKey] = Boolean(parsed.pricing || parsed.reports || parsed.routes);
    } else {
      parsed[menuKey] = false;
    }
    changed = true;
  }

  if (changed) {
    db.prepare("UPDATE users SET permissions = ? WHERE id = ?")
      .run(JSON.stringify(parsed), row.id);
  }
}

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
          }
        }
      } else {
        changedCount += 1;
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

setInterval(() => {
  const today = todayIsoInIstanbul();
  syncOperationsReportsForDate(today, { emitNotification: false }).catch((error) => {
    console.error("Raporlama otomatik senkron hatasi:", error.message);
  });
}, 15 * 60 * 1000);

syncOperationsReportsForDate(todayIsoInIstanbul(), { emitNotification: false }).catch(() => null);

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
      "SELECT id, uploaded_by_user_id, uploaded_by_username, direction_type, description, valid_from, valid_to, source_file_name, is_open, created_at FROM pricing_uploads ORDER BY id DESC LIMIT 50"
    )
    .all();

  const itemQuery = db.prepare(
    "SELECT id, route, origin, destination, row_direction, demand_price, tariff_price, discounted_price FROM pricing_upload_items WHERE upload_id = ? ORDER BY id ASC"
  );

  res.json({
    uploads: uploads.map((upload) => ({
      id: upload.id,
      uploadedByUserId: upload.uploaded_by_user_id,
      uploadedBy: upload.uploaded_by_username,
      directionType: upload.direction_type,
      description: String(upload.description || ""),
      validFrom: upload.valid_from,
      validTo: upload.valid_to,
      sourceFileName: upload.source_file_name || "",
      isOpen: Boolean(upload.is_open),
      createdAt: upload.created_at,
      items: itemQuery.all(upload.id).map((item) => ({
        id: Number(item.id),
        route: item.route,
        origin: item.origin,
        destination: item.destination,
        directionLabel: String(item.row_direction || "Gidis-Donus"),
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

app.patch("/api/pricing-uploads/:id/description", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const description = String(req.body?.description || "").trim().slice(0, 500);

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
    res.status(403).json({ message: "Bu kaydin aciklamasini guncelleme yetkin yok." });
    return;
  }

  db.prepare("UPDATE pricing_uploads SET description = ? WHERE id = ?").run(description, id);

  addPricingNotification(
    req.auth.user.username,
    `${req.auth.user.username} fiyat yuklemesi aciklamasini guncelledi (kayit #${id}).`
  );

  res.json({ ok: true, description });
});

app.delete("/api/pricing-upload-items/:itemId", requireAuth, (req, res) => {
  const itemId = Number(req.params.itemId);
  if (!Number.isInteger(itemId) || itemId <= 0) {
    res.status(400).json({ message: "Gecersiz satir." });
    return;
  }

  const item = db
    .prepare(
      `
      SELECT
        i.id,
        i.route,
        i.upload_id,
        u.uploaded_by_user_id,
        u.uploaded_by_username
      FROM pricing_upload_items i
      INNER JOIN pricing_uploads u ON u.id = i.upload_id
      WHERE i.id = ?
      `
    )
    .get(itemId);

  if (!item) {
    res.status(404).json({ message: "Satir bulunamadi." });
    return;
  }

  const isOwner = item.uploaded_by_user_id === req.auth.user.id;
  if (!isOwner && !req.auth.user.isAdmin) {
    res.status(403).json({ message: "Bu satiri silme yetkin yok." });
    return;
  }

  db.prepare("DELETE FROM pricing_upload_items WHERE id = ?").run(itemId);

  addPricingNotification(
    req.auth.user.username,
    `${req.auth.user.username} fiyat satiri sildi (${item.route}, satir #${itemId}).`
  );

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
  const description = String(req.body?.description || "").trim().slice(0, 500);
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

  const overlappingItems = db
    .prepare(
      `
      SELECT
        i.origin,
        i.destination,
        i.demand_price,
        u.id AS upload_id,
        u.direction_type,
        u.valid_from,
        u.valid_to
      FROM pricing_upload_items i
      INNER JOIN pricing_uploads u ON u.id = i.upload_id
      WHERE u.is_open = 1
      ORDER BY u.id DESC
      `
    )
    .all();

  const overlapMap = new Map();
  for (const item of overlappingItems) {
    const key = `${normalizeSearchText(item.origin)}|${normalizeSearchText(item.destination)}`;
    if (!overlapMap.has(key)) {
      overlapMap.set(key, item);
    }
  }

  rows.forEach((row, index) => {
    const origin = String(row?.origin || "").trim();
    const destination = String(row?.destination || "").trim();
    const directionLabel = String(row?.directionLabel || "Gidis-Donus").trim() || "Gidis-Donus";
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

    const routeKey = `${normalizeSearchText(origin)}|${normalizeSearchText(destination)}`;
    const existing = overlapMap.get(routeKey);
    if (existing) {
      rejected.push({
        rowNumber,
        reason: `${origin} - ${destination} icin zaten aktif fiyat var: ${Number(existing.demand_price)} TL (${existing.direction_type}, ${existing.valid_from} - ${existing.valid_to}, kayit #${existing.upload_id}).`,
      });
      return;
    }

    accepted.push({
      route: `${origin} - ${destination}`,
      origin,
      destination,
      directionLabel,
      demandPrice,
      tariffPrice: 0,
      discountedPrice: 0,
    });

    overlapMap.set(routeKey, {
      origin,
      destination,
      demand_price: demandPrice,
      upload_id: "yeni",
      valid_from: validFrom,
      valid_to: validTo,
    });
  });

  if (!accepted.length) {
    res.status(400).json({
      message: "Gecerli fiyat satiri yok. Yinelenen guzergah/tarih kayitlarini kontrol edin.",
      rejected,
    });
    return;
  }

  const trx = db.transaction(() => {
    const insertUpload = db.prepare(
      "INSERT INTO pricing_uploads (uploaded_by_user_id, uploaded_by_username, direction_type, description, valid_from, valid_to, source_file_name, is_open, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)"
    );
    const result = insertUpload.run(
      req.auth.user.id,
      req.auth.user.username,
      directionType,
      description,
      validFrom,
      validTo,
      sourceFileName,
      nowStamp()
    );

    const uploadId = Number(result.lastInsertRowid);
    const insertItem = db.prepare(
      "INSERT INTO pricing_upload_items (upload_id, route, origin, destination, row_direction, demand_price, tariff_price, discounted_price, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    for (const row of accepted) {
      insertItem.run(
        uploadId,
        row.route,
        row.origin,
        row.destination,
        row.directionLabel,
        row.demandPrice,
        row.tariffPrice,
        row.discountedPrice,
        nowStamp()
      );
    }

    addPricingNotification(
      req.auth.user.username,
      `${req.auth.user.username} fiyat yukledi (${accepted.length} satir, ${directionType}, ${validFrom} - ${validTo}${description ? `, Aciklama: ${description}` : ""})`
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

let reportingSyncRunning = false;
async function syncOperationsReportsForDate(reportDate, options = {}) {
  if (reportingSyncRunning) {
    return { date: reportDate, count: 0, skipped: true };
  }

  reportingSyncRunning = true;
  try {
    const collected = await collectOperationsReportRows(reportDate);
    const stamp = nowStamp();

    const upsert = db.prepare(
      `
      INSERT INTO operations_reports (
        report_date,
        origin_name,
        destination_name,
        route_label,
        ride_uuid,
        from_station_id,
        to_station_id,
        line_code,
        trip_number,
        departure_time,
        arrival_time,
        seats_available,
        occupancy_percent,
        occupancy_level,
        is_delayed,
        delay_minutes,
        payload_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(report_date, ride_uuid) DO UPDATE SET
        destination_name = excluded.destination_name,
        route_label = excluded.route_label,
        from_station_id = excluded.from_station_id,
        to_station_id = excluded.to_station_id,
        line_code = excluded.line_code,
        trip_number = excluded.trip_number,
        departure_time = excluded.departure_time,
        arrival_time = excluded.arrival_time,
        seats_available = excluded.seats_available,
        occupancy_percent = excluded.occupancy_percent,
        occupancy_level = excluded.occupancy_level,
        is_delayed = excluded.is_delayed,
        delay_minutes = excluded.delay_minutes,
        payload_json = excluded.payload_json,
        updated_at = excluded.updated_at
      `
    );

    const selectExisting = db.prepare(
      "SELECT id, ride_uuid FROM operations_reports WHERE report_date = ? AND origin_name = ?"
    );
    const deleteById = db.prepare("DELETE FROM operations_reports WHERE id = ?");

    const trx = db.transaction(() => {
      if (!collected.length) {
        db.prepare("DELETE FROM operations_reports WHERE report_date = ? AND origin_name = ?")
          .run(reportDate, REPORTING_SOURCE.originName);
        return;
      }

      const keepSet = new Set(collected.map((row) => String(row.rideUuid || "")));
      const existing = selectExisting.all(reportDate, REPORTING_SOURCE.originName);
      for (const oldRow of existing) {
        if (!keepSet.has(String(oldRow.ride_uuid || ""))) {
          deleteById.run(oldRow.id);
        }
      }

      for (const row of collected) {
        upsert.run(
          row.reportDate,
          row.originName,
          row.destinationName,
          row.routeLabel,
          row.rideUuid,
          REPORTING_SOURCE.fromStationId,
          REPORTING_TARGETS.find((x) => x.destinationName === row.destinationName)?.toStationId || "",
          row.lineCode,
          row.tripNumber,
          row.departureTime,
          row.arrivalTime,
          row.seatsAvailable,
          row.occupancyPercent,
          row.occupancyLevel,
          row.isDelayed,
          row.delayMinutes,
          row.payloadJson,
          stamp,
          stamp
        );
      }
    });

    trx();

    if (options.emitNotification && options.actor) {
      addPricingNotification(
        options.actor,
        `${options.actor} raporlama verisini yeniledi (${reportDate}, ${collected.length} sefer).`
      );
    }

    return { date: reportDate, count: collected.length, skipped: false };
  } finally {
    reportingSyncRunning = false;
  }
}

app.get("/api/operations-reports", requireAuth, (req, res) => {
  const date = String(req.query.date || todayIsoInIstanbul()).trim();
  const origin = String(req.query.origin || "Siirt").trim();

  const rows = db
    .prepare(
      `
      SELECT
        id,
        report_date,
        origin_name,
        destination_name,
        route_label,
        ride_uuid,
        line_code,
        trip_number,
        departure_time,
        arrival_time,
        seats_available,
        occupancy_percent,
        occupancy_level,
        is_delayed,
        delay_minutes,
        vehicle_plate,
        note,
        updated_at
      FROM operations_reports
      WHERE report_date = ? AND origin_name = ?
      ORDER BY departure_time ASC, id ASC
      `
    )
    .all(date, origin);

  res.json({
    date,
    origin,
    rows: rows.map((row) => ({
      id: Number(row.id),
      reportDate: row.report_date,
      originName: row.origin_name,
      destinationName: row.destination_name,
      routeLabel: row.route_label,
      rideUuid: row.ride_uuid,
      lineCode: row.line_code || "",
      tripNumber: row.trip_number || "",
      departureTime: row.departure_time || "",
      arrivalTime: row.arrival_time || "",
      seatsAvailable: Number.isFinite(Number(row.seats_available)) ? Number(row.seats_available) : null,
      occupancyPercent: Number.isFinite(Number(row.occupancy_percent)) ? Number(row.occupancy_percent) : null,
      occupancyLevel: row.occupancy_level || "",
      isDelayed: Boolean(row.is_delayed),
      delayMinutes: Number(row.delay_minutes) || 0,
      vehiclePlate: row.vehicle_plate || "",
      note: row.note || "",
      updatedAt: row.updated_at,
    })),
  });
});

app.post("/api/operations-reports/sync", requireAuth, async (req, res) => {
  const reportDate = String(req.body?.date || todayIsoInIstanbul()).trim();
  const dotDate = toDotDate(reportDate);
  if (!dotDate) {
    res.status(400).json({ message: "Rapor tarihi gecersiz." });
    return;
  }

  try {
    const result = await syncOperationsReportsForDate(reportDate, {
      emitNotification: true,
      actor: req.auth.user.username,
    });
    res.json({ ok: true, date: result.date, count: result.count, skipped: result.skipped });
  } catch (error) {
    res.status(500).json({ message: error.message || "Rapor verisi senkronize edilemedi." });
  }
});

app.patch("/api/operations-reports/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ message: "Gecersiz rapor kaydi." });
    return;
  }

  const delayMinutesRaw = Number(req.body?.delayMinutes);
  const delayMinutes = Number.isFinite(delayMinutesRaw)
    ? Math.max(-720, Math.min(720, Math.round(delayMinutesRaw)))
    : 0;
  const vehiclePlate = String(req.body?.vehiclePlate || "").trim().slice(0, 32);
  const note = String(req.body?.note || "").trim().slice(0, 400);
  const isDelayed = delayMinutes !== 0 ? 1 : 0;

  const found = db.prepare("SELECT id FROM operations_reports WHERE id = ?").get(id);
  if (!found) {
    res.status(404).json({ message: "Rapor kaydi bulunamadi." });
    return;
  }

  db.prepare(
    `
    UPDATE operations_reports
    SET delay_minutes = ?,
        is_delayed = ?,
        vehicle_plate = ?,
        note = ?,
        updated_at = ?
    WHERE id = ?
    `
  ).run(delayMinutes, isDelayed, vehiclePlate, note, nowStamp(), id);

  res.json({ ok: true });
});

app.get("/api/notifications", requireAuth, (req, res) => {
  const pricingNotifications = db
    .prepare(
      "SELECT id, username, message, created_at, is_read FROM pricing_notifications ORDER BY id DESC LIMIT 100"
    )
    .all();

  const notifications = pricingNotifications
    .map((n) => ({
      id: `upload-${n.id}`,
      route: n.username,
      message: n.message,
      time: n.created_at,
      isRead: Boolean(n.is_read),
    }))
    .sort((a, b) => String(b.time).localeCompare(String(a.time), "tr"))
    .slice(0, 120);

  res.json({
    notifications,
    unreadCount: notifications.filter((n) => !n.isRead).length,
  });
});

app.post("/api/notifications/read-all", requireAuth, (req, res) => {
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
