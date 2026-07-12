require("dotenv").config();
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
const OBILET_CHECK_INTERVAL_MINUTES = Number.parseInt(
  process.env.OBILET_CHECK_INTERVAL_MINUTES || "5",
  10
);
const OBILET_CHECK_INTERVAL_MS =
  (Number.isFinite(OBILET_CHECK_INTERVAL_MINUTES) && OBILET_CHECK_INTERVAL_MINUTES > 0
    ? OBILET_CHECK_INTERVAL_MINUTES
    : 5) * 60 * 1000;
const OBILET_EMAIL_MODE = String(process.env.OBILET_EMAIL_MODE || "changes")
  .trim()
  .toLocaleLowerCase("tr-TR");
const OBILET_EMAIL_INTERVAL_HOURS = Number.parseInt(
  process.env.OBILET_EMAIL_INTERVAL_HOURS || "1",
  10
);
const OBILET_PRICE_CONFIRM_RUNS_RAW = Number.parseInt(
  process.env.OBILET_PRICE_CONFIRM_RUNS || "1",
  10
);
const OBILET_PRICE_CONFIRM_RUNS =
  Number.isFinite(OBILET_PRICE_CONFIRM_RUNS_RAW) && OBILET_PRICE_CONFIRM_RUNS_RAW > 0
    ? OBILET_PRICE_CONFIRM_RUNS_RAW
    : 1;
// Gelecek tarihli bir sefer kac tur UST USTE gorunmezse fiyat satiri silinsin (iptal tespiti).
// >1 tutulur ki tek aksak/eksik tarama fiyat satirlarini SILMESIN. Gorulunce sayac sifirlanir.
const OBILET_ABSENT_DELETE_RUNS = 3;
const OBILET_SUBJECT_CHANGE = String(process.env.OBILET_SUBJECT_CHANGE || "oBilet Fiyat Raporu").trim();
const OBILET_SUBJECT_NO_CHANGE = String(process.env.OBILET_SUBJECT_NO_CHANGE || "oBilet Fiyat Raporu").trim();
const OBILET_SUBJECT_PRICE_ALERT = String(process.env.OBILET_SUBJECT_PRICE_ALERT || "oBilet Fiyat Degisikligi").trim();
const OBILET_SUBJECT_TEST = String(process.env.OBILET_SUBJECT_TEST || "oBilet Test E-postasi").trim();
// Fiyat degisiklik kayitlari kac gun saklansin (varsayilan 3). 0 = otomatik temizlik kapali.
const PRICE_HISTORY_RETENTION_DAYS_RAW = Number.parseInt(
  process.env.PRICE_HISTORY_RETENTION_DAYS || "3",
  10
);
const PRICE_HISTORY_RETENTION_DAYS =
  Number.isFinite(PRICE_HISTORY_RETENTION_DAYS_RAW) && PRICE_HISTORY_RETENTION_DAYS_RAW >= 0
    ? PRICE_HISTORY_RETENTION_DAYS_RAW
    : 3;
const EMAIL_SIGNATURE_HTML = String(process.env.EMAIL_SIGNATURE_HTML || "").trim();
const EMAIL_SIGNATURE_TEXT = String(process.env.EMAIL_SIGNATURE_TEXT || "").trim();
// Telegram bildirim botu. Token + varsayilan sohbet ID'leri env'den okunur.
const TELEGRAM_BOT_TOKEN = String(process.env.TELEGRAM_BOT_TOKEN || "").trim();
const TELEGRAM_DEFAULT_CHAT_IDS = String(process.env.TELEGRAM_CHAT_ID || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const TELEGRAM_ENABLED = TELEGRAM_BOT_TOKEN.length > 0;
// Fiyat ayiklama loglari. Kapatmak icin env'e DEBUG_OBILET_PRICE=0
const DEBUG_OBILET_PRICE = String(process.env.DEBUG_OBILET_PRICE ?? "1").trim() !== "0";
const DEBUG_OBILET_API = String(process.env.DEBUG_OBILET_API || "").trim() === "1";
const DEBUG_OBILET_XHR = String(process.env.DEBUG_OBILET_XHR || "").trim() === "1";
const DEBUG_OBILET_XHR_BODY = String(process.env.DEBUG_OBILET_XHR_BODY || "").trim() === "1";
const OBILET_OPERATOR_CATALOG = [
  "Adıyaman Ünal Turizm",
  "Aksakal Seyahat",
  "Ali Osman Ulusoy",
  "Anadolu Ulaşım",
  "Balıkesir Uludağ Turizm",
  "Ben Turizm",
  "Best Van Turizm",
  "Beydagi Turizm",
  "Buzlu Turizm",
  "Can Diyarbakir Turizm",
  "Cizre Nuh",
  "CSR Seyahat",
  "Çanakkale Truva Turizm",
  "Çayırağası Vip",
  "Dadaş Turizm",
  "DK Köksallar Seyahat",
  "Efetur",
  "Enver Geçgel Turizm",
  "Esadaş Turizm",
  "Güney Akdeniz Seyahat",
  "Has Karayolu",
  "Hatay Gokbey",
  "Hatay Günsas Turizm",
  "Iğdırlı Turizm",
  "Isparta Petrol",
  "İstanbul Seyahat",
  "Jet Turizm",
  "Kamil Koç",
  "Kanberoğlu Turizm",
  "Kontur Turizm",
  "Lider Adana",
  "Lüks Adana",
  "Lüks Artvin Seyahat",
  "Lüks Batman Seyahat",
  "Lüks Ereğli",
  "Lüks Karadeniz",
  "Lüks Nur Seyahat",
  "Lüks Yalova Seyahat",
  "Malatya Medine",
  "Malatya Zafer Turizm",
  "Mardin Seyahat",
  "Martur",
  "Mersin Nur",
  "Metro Turizm",
  "Muş Yolu Turizm",
  "Niğde Aydoğanlar",
  "Nilüfer Turizm",
  "Öz Diyarbakır",
  "Öz Erciş",
  "Öz Has Bingöl",
  "Öz Sivas Seyahat",
  "Özkaymak",
  "Özlem Adana",
  "Özlem Cizre Nuh",
  "Özlem Diyarbakır",
  "Pamukkale Turizm",
  "Sakarya Vib Turizm",
  "Seç Turizm",
  "Siirt Petrol",
  "Star Diyarbakır",
  "Süha Turizm",
  "Şanlıurfa Astor",
  "Tokat Seyahat",
  "Tuncelililer Turizm",
  "Ulusoy",
  "Van Gölü Turizm",
  "Van Kalesi",
  "Varan Turizm",
  "Yeni Aksaray Seyahat",
  "Yeni Diyarbakır",
  "Yeni Özlem Adana",
  "Yeşil Artvin Ekspres",
  "Yeşil Muş Ovası",
  "Yüksekova Seyahat",
  // Sonradan eklenenler (liste runtime'da alfabetik siralanir):
  "Villa Seyahat",
  "Niğde İnan Turizm",
  "Lüks Mersin",
  "Mersin Sahil Seyahat",
  "Noktadan Noktaya Seyahat",
].sort((a, b) => a.localeCompare(b, "tr-TR"));
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

function slugTr(value) {
  return String(value || "")
    .toLocaleLowerCase("tr-TR")
    .replace(/ı/g, "i")
    .replace(/ğ/g, "g")
    .replace(/ü/g, "u")
    .replace(/ş/g, "s")
    .replace(/ö/g, "o")
    .replace(/ç/g, "c")
    .replace(/[^a-z0-9\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toTurkishTitleCase(text) {
  return String(text || "")
    .split(" ")
    .filter(Boolean)
    .map((word) => {
      const first = word.charAt(0).toLocaleUpperCase("tr-TR");
      const rest = word.slice(1).toLocaleLowerCase("tr-TR");
      return `${first}${rest}`;
    })
    .join(" ");
}

function renderEmailSignature() {
  if (!EMAIL_SIGNATURE_HTML && !EMAIL_SIGNATURE_TEXT) {
    return "";
  }

  const signatureBody = EMAIL_SIGNATURE_HTML
    ? EMAIL_SIGNATURE_HTML
    : EMAIL_SIGNATURE_TEXT.replace(/\n/g, "<br/>");

  return `
    <div style="margin-top: 18px; padding-top: 12px; border-top: 1px dashed #e0e0e0; color: #555; font-size: 12px; line-height: 1.5; font-family: sans-serif;">
      ${signatureBody}
    </div>
  `;
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
  "oneops",
  "ocr",
  "obilet_tracker",
  "occupancy",
  "sefer_takip",
  "sure_hesap",
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
  operator_name TEXT NOT NULL DEFAULT '',
  vehicle_model TEXT NOT NULL DEFAULT '',
  revenue_try REAL,
  revenue_eur REAL,
  note TEXT NOT NULL DEFAULT '',
  payload_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(report_date, ride_uuid)
);

CREATE TABLE IF NOT EXISTS user_error_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  description TEXT NOT NULL,
  username TEXT NOT NULL,
  photos_json TEXT NOT NULL,
  priority TEXT NOT NULL DEFAULT 'Orta',
  status TEXT NOT NULL DEFAULT 'Yeni',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS obilet_targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  date TEXT NOT NULL,
  end_date TEXT NOT NULL DEFAULT '',
  departure_stop_filter TEXT NOT NULL DEFAULT '',
  operators TEXT NOT NULL,
  email_notifications TEXT NOT NULL,
  telegram_chat_ids TEXT NOT NULL DEFAULT '',
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS obilet_prices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL,
  journey_date TEXT NOT NULL DEFAULT '',
  operator TEXT NOT NULL,
  departure_time TEXT NOT NULL,
  departure_stop TEXT NOT NULL DEFAULT '',
  arrival_stop TEXT NOT NULL DEFAULT '',
  price INTEGER NOT NULL,
  pending_price INTEGER,
  pending_seen_count INTEGER NOT NULL DEFAULT 0,
  last_updated TEXT NOT NULL,
  FOREIGN KEY (target_id) REFERENCES obilet_targets(id) ON DELETE CASCADE
);
`);

try { db.exec("ALTER TABLE user_error_reports ADD COLUMN priority TEXT NOT NULL DEFAULT 'Orta'"); } catch(e) {}
try { db.exec("ALTER TABLE user_error_reports ADD COLUMN status TEXT NOT NULL DEFAULT 'Yeni'"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN telegram_chat_ids TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN end_date TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN departure_stop_filter TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN last_sync_status TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN last_sync_at TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN last_email_sent_at TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN route_id TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_targets ADD COLUMN created_by TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE users ADD COLUMN full_name TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE users ADD COLUMN title TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE users ADD COLUMN last_seen TEXT NOT NULL DEFAULT ''"); } catch(e) {}

// Fiyat degisikligi gecmisi tablosu: her bir onaylanmis fiyat degisikligini kayit altina alir.
// Raporlama ekraninda kronolojik liste olarak gosterilir.
db.exec(`
CREATE TABLE IF NOT EXISTS obilet_price_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  journey_date TEXT NOT NULL,
  operator TEXT NOT NULL,
  departure_time TEXT NOT NULL,
  departure_stop TEXT NOT NULL DEFAULT '',
  old_price INTEGER NOT NULL,
  new_price INTEGER NOT NULL,
  changed_at TEXT NOT NULL,
  detected_by TEXT NOT NULL DEFAULT 'auto'
);
CREATE INDEX IF NOT EXISTS idx_obilet_price_history_changed_at ON obilet_price_history(changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_obilet_price_history_target ON obilet_price_history(target_id);
`);

// Telegram bot kullanicilari: self-servis kayit + admin onayi.
// status: pending (onay bekliyor) | approved (onayli) | blocked (engelli)
db.exec(`
CREATE TABLE IF NOT EXISTS telegram_users (
  chat_id TEXT PRIMARY KEY,
  first_name TEXT NOT NULL DEFAULT '',
  last_name TEXT NOT NULL DEFAULT '',
  username TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending',
  requested_at TEXT NOT NULL DEFAULT '',
  approved_at TEXT NOT NULL DEFAULT '',
  approved_by TEXT NOT NULL DEFAULT ''
);
`);

// Telegram bildirim abonelikleri: kullanici hangi hatlarin (target) bildirimini alacak.
// Abonelik yoksa bildirim gitmez (adminler haric — onlar her zaman alir).
db.exec(`
CREATE TABLE IF NOT EXISTS telegram_subscriptions (
  chat_id TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT '',
  UNIQUE(chat_id, target_id)
);
CREATE INDEX IF NOT EXISTS idx_telegram_subs_target ON telegram_subscriptions(target_id);
`);

// Doluluk takibi (oBilet Takip'ten BAGIMSIZ ayri tablo). Her sefer icin anlik koltuk durumu.
// total-seats/available-seats oBilet sefer listesinde dogrudan geliyor (piggyback ile yazilir).
db.exec(`
CREATE TABLE IF NOT EXISTS obilet_occupancy (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL,
  journey_date TEXT NOT NULL,
  operator TEXT NOT NULL,
  departure_time TEXT NOT NULL,
  departure_stop TEXT NOT NULL DEFAULT '',
  total_seats INTEGER,
  available_seats INTEGER,
  occupancy_percent INTEGER,
  last_updated TEXT NOT NULL,
  UNIQUE(target_id, journey_date, operator, departure_time, departure_stop)
);
CREATE INDEX IF NOT EXISTS idx_obilet_occupancy_target ON obilet_occupancy(target_id);
`);

// KOKTEN DUZELTME: Doluluk kimligini fiyat tablosuyla ayni yap — (target, tarih, firma, saat).
// Eski UNIQUE'e departure_stop dahildi; durak yazimi degisince ayni sefere MUKERRER doluluk
// satiri olusuyordu (obilet_prices durak olmadan kimliklerken doluluk durakla kimlikliyordu),
// okuma tarafi da duragi yok sayinca eski/yanlis satir cekilebiliyordu.
// 1) Once mevcut mukerrerleri temizle (her sefer icin EN GUNCEL, esitlikte en buyuk id kalsin),
// 2) sonra 4-kolonlu UNIQUE index olustur (upsert artik hep tek satiri gunceller).
try {
  db.exec(`
    DELETE FROM obilet_occupancy WHERE id IN (
      SELECT o1.id FROM obilet_occupancy o1 WHERE EXISTS (
        SELECT 1 FROM obilet_occupancy o2
         WHERE o2.target_id = o1.target_id AND o2.journey_date = o1.journey_date
           AND o2.operator = o1.operator AND o2.departure_time = o1.departure_time
           AND o2.id <> o1.id
           AND (
             (substr(o2.last_updated,7,4)||substr(o2.last_updated,4,2)||substr(o2.last_updated,1,2)||substr(o2.last_updated,12,8))
               > (substr(o1.last_updated,7,4)||substr(o1.last_updated,4,2)||substr(o1.last_updated,1,2)||substr(o1.last_updated,12,8))
             OR (
               (substr(o2.last_updated,7,4)||substr(o2.last_updated,4,2)||substr(o2.last_updated,1,2)||substr(o2.last_updated,12,8))
                 = (substr(o1.last_updated,7,4)||substr(o1.last_updated,4,2)||substr(o1.last_updated,1,2)||substr(o1.last_updated,12,8))
               AND o2.id > o1.id
             )
           )
      )
    );
  `);
} catch (e) { console.warn("[Doluluk] mukerrer temizleme (migrasyon) hatasi:", e.message); }
try {
  db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_obilet_occupancy_identity ON obilet_occupancy(target_id, journey_date, operator, departure_time);");
} catch (e) { console.warn("[Doluluk] kimlik index olusturma hatasi:", e.message); }
// Kaynak: 'gercek' (koltuk haritasi) | 'liste' (available-seats yedek). Gercek cekilemezse onceki
// 'gercek' satiri korunur, 'liste' satiri silinir (yanlis deger gostermemek icin).
try { db.exec("ALTER TABLE obilet_occupancy ADD COLUMN source TEXT NOT NULL DEFAULT ''"); } catch(e) {}
// Atanan otobus plakasi — /json/sefer yanitindan gelir (kalkisa yakin atanir; ileri tarihte bos).
// Yalnizca GERCEK koltuk haritasi cekildiginde (source='gercek') dolar; liste yedeginde bos kalir.
try { db.exec("ALTER TABLE obilet_occupancy ADD COLUMN plate TEXT NOT NULL DEFAULT ''"); } catch(e) {}
// Seferin GERCEK guzergahi (ilk->son durak). Sorgulanan segment (orn. Adana->Nigde) yerine gercek
// bas-bitis (Adana->Esenler/Istanbul) gosterilir. journey.stops'tan gelir, ek istek yok.
try { db.exec("ALTER TABLE obilet_occupancy ADD COLUMN route_from TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_occupancy ADD COLUMN route_to TEXT NOT NULL DEFAULT ''"); } catch(e) {}
// Servis/sefer kimligi: journey.code'un ilk parcasi (orn "1212852-1376-1387" -> "1212852"). AYNI fiziksel
// aracin farkli segmentleri (Adana->Ankara, Sanliurfa->Ankara) bu id'yi PAYLASIR. Ana sefer (gercek kalkis)
// bununla eslestirilir. Sadece okuma/eslestirme icin; fiyat mantigini etkilemez.
try { db.exec("ALTER TABLE obilet_occupancy ADD COLUMN service_id TEXT NOT NULL DEFAULT ''"); } catch(e) {}

// ANA SEFER KESIF tablosu: feeder sehirlerden (Sanliurfa, Gaziantep...) yapilan taramada bulunan her servisin
// GERCEK kalkis sehri/saati ve bitisini serviceId ile saklar. Sefer Takip, segmentin serviceId'siyle buraya
// JOIN yaparak "Sanliurfa 18:30 -> Ankara" gibi ana seferi gosterir. Fiyat/tarama dongusunden BAGIMSIZ.
try { db.exec(`CREATE TABLE IF NOT EXISTS obilet_service_routes (
  service_id TEXT PRIMARY KEY,
  operator TEXT NOT NULL DEFAULT '',
  origin_city TEXT NOT NULL DEFAULT '',
  origin_time TEXT NOT NULL DEFAULT '',
  dest_city TEXT NOT NULL DEFAULT '',
  stops_json TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
)`); } catch(e) {}

// AG & KAPASITE DIFF: her taramada bir rotanin YAPI fotografini (hangi firma, hangi saatte, kac koltuk,
// kac arac) saklar; sonraki tarama oncekiyle kiyaslanip yapisal degisiklik (yeni rakip / yeni-kalkan sefer /
// kapasite / ek arac / saat kaydirma) tespit edilir. FIYAT mantigina dokunmaz — ayni tarama verisini OKUR,
// ayri tablolara yazar. obilet_route_structure = son fotograf; obilet_structure_changes = tespit edilen farklar.
try { db.exec(`CREATE TABLE IF NOT EXISTS obilet_route_structure (
  target_id INTEGER NOT NULL,
  journey_date TEXT NOT NULL,
  snapshot_json TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (target_id, journey_date)
)`); } catch(e) {}
try { db.exec(`CREATE TABLE IF NOT EXISTS obilet_structure_changes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  target_id INTEGER NOT NULL,
  journey_date TEXT NOT NULL DEFAULT '',
  route_label TEXT NOT NULL DEFAULT '',
  change_type TEXT NOT NULL DEFAULT '',
  operator TEXT NOT NULL DEFAULT '',
  message TEXT NOT NULL DEFAULT '',
  detected_at TEXT NOT NULL DEFAULT '',
  is_read INTEGER NOT NULL DEFAULT 0
)`); } catch(e) {}
try { db.exec("CREATE INDEX IF NOT EXISTS idx_obilet_structure_changes_id ON obilet_structure_changes(id DESC)"); } catch(e) {}
// Kapasite artti/azaldi bildirimleri kaldirildi (gurultu) — mevcut kayitli olanlari da temizle ki gorunmesin.
try { db.exec("DELETE FROM obilet_structure_changes WHERE change_type IN ('capacity_up','capacity_down')"); } catch(e) {}

// KULLANICI-BAZLI bildirim "gorüldü" takibi: her kullanici (user_id) icin, her tur (kind: 'price'|'structure')
// EN SON GORDUGU degisim id'si. Boylece bir kullanici giris yapmadigi surece degisimleri kacirmaz; giris
// yapinca gormedikleri sag-ust STICKY bildirim olarak cikar, kendi eliyle kapatir (kapatinca id ilerler).
// Tarayici/cihaz bagimsiz (localStorage degil, sunucu) — paylasimli bilgisayarda da her kullanici kendi durumunu gorur.
try { db.exec(`CREATE TABLE IF NOT EXISTS notification_reads (
  user_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  last_seen_id INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (user_id, kind)
)`); } catch(e) {}

// Kayitli PKM guzergahlari (Pkm Form menusu) — PAYLASIMLI: herkes gorur/acar. plan_json = {stops,legMin,peronMin,depMin}.
try { db.exec(`CREATE TABLE IF NOT EXISTS pkm_routes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  created_by TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
)`); } catch(e) {}

try { db.exec("ALTER TABLE obilet_prices ADD COLUMN journey_date TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_prices ADD COLUMN departure_stop TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_prices ADD COLUMN arrival_stop TEXT NOT NULL DEFAULT ''"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_prices ADD COLUMN pending_price INTEGER"); } catch(e) {}
try { db.exec("ALTER TABLE obilet_prices ADD COLUMN pending_seen_count INTEGER NOT NULL DEFAULT 0"); } catch(e) {}
// Kac tur UST USTE gorunmedi (iptal tespiti icin). Tek aksak taramada sefer silinmesin diye.
try { db.exec("ALTER TABLE obilet_prices ADD COLUMN absent_count INTEGER NOT NULL DEFAULT 0"); } catch(e) {}

// Sehir adi -> oBilet station ID eslemesi (otomatik ogrenilen). Her basarili taramada doldurulur.
db.exec(`
CREATE TABLE IF NOT EXISTS obilet_station_ids (
  city_key TEXT PRIMARY KEY,
  city_name TEXT NOT NULL,
  station_id INTEGER NOT NULL,
  hits INTEGER NOT NULL DEFAULT 1,
  last_seen TEXT NOT NULL DEFAULT ''
);
`);

// ===================== TALEP RADARI (Otonom Yogunluk Analizi) =====================
// oBilet Takip sisteminden (obilet_targets / obilet_occupancy) TAMAMEN BAGIMSIZ ayri altyapi.
// analysis_routes  : sistemin hazir "populer rota" havuzu (boot'ta seed'lenir, route_id backfill'lenir).
// analysis_occupancy_history : her tarama olcumu APPEND edilir (UZERINE YAZILMAZ) — boylece
//   ayni seferin zaman icindeki dolulugu (gun-deseni / talep trendi) cikarilabilir.
db.exec(`
CREATE TABLE IF NOT EXISTS analysis_routes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  route_id TEXT NOT NULL DEFAULT '',
  is_active INTEGER NOT NULL DEFAULT 1,
  is_seed INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(origin, destination)
);
CREATE TABLE IF NOT EXISTS analysis_occupancy_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  route_ref TEXT NOT NULL,
  origin TEXT NOT NULL DEFAULT '',
  destination TEXT NOT NULL DEFAULT '',
  journey_date TEXT NOT NULL,
  operator TEXT NOT NULL,
  departure_time TEXT NOT NULL,
  total_seats INTEGER,
  available_seats INTEGER,
  occupancy_percent INTEGER,
  source TEXT NOT NULL DEFAULT 'gercek',
  measured_at TEXT NOT NULL,
  measured_at_iso TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_analysis_hist_route_date ON analysis_occupancy_history(route_ref, journey_date);
CREATE INDEX IF NOT EXISTS idx_analysis_hist_measured ON analysis_occupancy_history(measured_at_iso);
CREATE INDEX IF NOT EXISTS idx_analysis_hist_jdate ON analysis_occupancy_history(journey_date);
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
const hasOperatorNameColumn = reportingColumns.some((col) => col.name === "operator_name");
if (!hasOperatorNameColumn) {
  db.exec("ALTER TABLE operations_reports ADD COLUMN operator_name TEXT NOT NULL DEFAULT ''");
}
const hasVehicleModelColumn = reportingColumns.some((col) => col.name === "vehicle_model");
if (!hasVehicleModelColumn) {
  db.exec("ALTER TABLE operations_reports ADD COLUMN vehicle_model TEXT NOT NULL DEFAULT ''");
}
const hasRevenueTryColumn = reportingColumns.some((col) => col.name === "revenue_try");
if (!hasRevenueTryColumn) {
  db.exec("ALTER TABLE operations_reports ADD COLUMN revenue_try REAL");
}
const hasRevenueEurColumn = reportingColumns.some((col) => col.name === "revenue_eur");
if (!hasRevenueEurColumn) {
  db.exec("ALTER TABLE operations_reports ADD COLUMN revenue_eur REAL");
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
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Istanbul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
  return formatter.format(new Date()); // "2026-05-28"
}

function currentTimeInIstanbul() {
  const now = new Date();
  const timeStr = now.toLocaleTimeString("tr-TR", {
    timeZone: "Europe/Istanbul",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });
  return timeStr; // e.g., "20:36"
}

function isJourneyInFuture(journeyDate, departureTime) {
  // journeyDate: "2026-05-28", departureTime: "17:00"
  const today = todayIsoInIstanbul();
  const now = currentTimeInIstanbul(); // "HH:mm"
  
  // If journey date is in the future, always include it
  if (journeyDate > today) {
    return true;
  }
  
  // If journey date is in the past, exclude it
  if (journeyDate < today) {
    return false;
  }
  
  // Today: compare departure time with current time
  const depTimeStr = String(departureTime || "").trim().substring(0, 5); // "17:00"
  return depTimeStr > now; // "17:00" > "20:36"? false - excluded
}

function shiftIsoDate(dateIso, dayOffset) {
  const m = String(dateIso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) {
    return dateIso;
  }

  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const base = new Date(Date.UTC(y, mo - 1, d));
  base.setUTCDate(base.getUTCDate() + Number(dayOffset || 0));
  const yy = base.getUTCFullYear();
  const mm = String(base.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(base.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "").trim());
}

function buildIsoDateRange(startIso, endIso) {
  const start = String(startIso || "").trim();
  const end = String(endIso || "").trim() || start;
  if (!isIsoDate(start) || !isIsoDate(end)) {
    return [];
  }

  if (start > end) {
    return [];
  }

  const items = [];
  let cursor = start;
  let safety = 0;
  while (cursor <= end && safety < 370) {
    items.push(cursor);
    cursor = shiftIsoDate(cursor, 1);
    safety += 1;
  }
  return items;
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
  const seats = toFiniteNumber(item?.available?.seats);
  if (seats != null) {
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

const REPORTING_ORIGIN_STATIC = new Map([
  [slugTr(REPORTING_SOURCE.originName), { originName: REPORTING_SOURCE.originName, fromStationId: REPORTING_SOURCE.fromStationId }],
]);

const REPORTING_KNOWN_TARGETS = [
  { destinationName: "Marmaris", toStationId: "66615e19-06cb-4c6f-93df-c3bf05b40e6a" },
  { destinationName: "Antalya", toStationId: "9b6c0630-3ecb-11ea-8017-02437075395e" },
  { destinationName: "Esenler", toStationId: "9b6a8316-3ecb-11ea-8017-02437075395e" },
  { destinationName: "Ankara", toStationId: "9b6a837a-3ecb-11ea-8017-02437075395e" },
  { destinationName: "Izmir", toStationId: "9b6a8396-3ecb-11ea-8017-02437075395e" },
];

const REPORTING_KNOWN_TARGET_MAP = new Map(
  REPORTING_KNOWN_TARGETS.map((item) => [String(item.toStationId), item.destinationName])
);

function resolveDestinationNameFromRide(ride, toStationId) {
  const directCandidates = [
    ride?.to_stop_name,
    ride?.to_stop?.name,
    ride?.to_stop?.city_name,
    ride?.to_stop_city_name,
    ride?.destination_name,
    ride?.destination?.name,
    ride?.destination?.city_name,
    ride?.destination,
    ride?.to_name,
    ride?.toStopName,
  ];

  for (const candidate of directCandidates) {
    const text = String(candidate || "").trim();
    if (text) {
      return text;
    }
  }

  return REPORTING_KNOWN_TARGET_MAP.get(String(toStationId || "")) || "Hedef";
}

function getControlAuthHeaders() {
  const cookieHeader = String(getMeta("control_cookie_header", "") || "").trim();
  const csrfToken = String(getMeta("control_csrf_token", "") || "").trim();
  if (!cookieHeader) {
    return null;
  }

  return {
    Cookie: cookieHeader,
    "X-CSRF-Token": csrfToken,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    Accept: "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
  };
}

function collectStationCandidates(node, out = []) {
  if (!node || typeof node !== "object") {
    return out;
  }

  if (Array.isArray(node)) {
    for (const item of node) {
      collectStationCandidates(item, out);
    }
    return out;
  }

  const id = String(node.id || node.uuid || node.station_id || node.stationId || node.stop_uuid || "").trim();
  const name = String(node.name || node.city_name || node.display_name || node.label || node.title || "").trim();
  if (id && name && /^[0-9a-fA-F-]{32,36}$/.test(id)) {
    out.push({ id, name });
  }

  for (const value of Object.values(node)) {
    if (value && typeof value === "object") {
      collectStationCandidates(value, out);
    }
  }

  return out;
}

async function lookupOriginStationByName(originName) {
  const query = String(originName || "").trim();
  if (!query) {
    return null;
  }

  const headers = getControlAuthHeaders();
  if (!headers) {
    return null;
  }

  const endpoints = [
    `https://global.api.flixbus.com/gis/v1/station/search/${encodeURIComponent(query)}?locale=tr`,
    `https://global.api.flixbus.com/gis/v1/station/search?query=${encodeURIComponent(query)}&locale=tr`,
  ];

  const querySlug = slugTr(query);
  for (const url of endpoints) {
    try {
      const res = await fetch(url, { headers, redirect: "follow" });
      if (!res.ok) {
        continue;
      }

      const data = await res.json();
      const candidates = collectStationCandidates(data, []);
      if (!candidates.length) {
        continue;
      }

      const exact = candidates.find((item) => slugTr(item.name) === querySlug);
      if (exact) {
        return { originName: toTurkishTitleCase(exact.name), fromStationId: exact.id };
      }

      const close = candidates.find((item) => slugTr(item.name).includes(querySlug) || querySlug.includes(slugTr(item.name)));
      const selected = close || candidates[0];
      if (selected) {
        return { originName: toTurkishTitleCase(selected.name), fromStationId: selected.id };
      }
    } catch {
      // next endpoint
    }
  }

  return null;
}

function lookupOriginStationFromHistory(originName) {
  const query = slugTr(originName);
  if (!query) {
    return null;
  }

  const byOrigin = db
    .prepare(
      `
      SELECT origin_name AS city_name, from_station_id AS station_id, COUNT(*) AS cnt
      FROM operations_reports
      WHERE COALESCE(TRIM(from_station_id), '') <> ''
      GROUP BY origin_name, from_station_id
      ORDER BY cnt DESC
      `
    )
    .all();

  const byDestination = db
    .prepare(
      `
      SELECT destination_name AS city_name, to_station_id AS station_id, COUNT(*) AS cnt
      FROM operations_reports
      WHERE COALESCE(TRIM(to_station_id), '') <> ''
      GROUP BY destination_name, to_station_id
      ORDER BY cnt DESC
      `
    )
    .all();

  const candidates = [...byOrigin, ...byDestination]
    .map((row) => ({
      cityName: String(row.city_name || "").trim(),
      stationId: String(row.station_id || "").trim(),
      count: Number(row.cnt || 0),
    }))
    .filter((row) => row.cityName && row.stationId);

  if (!candidates.length) {
    return null;
  }

  const exact = candidates.find((row) => slugTr(row.cityName) === query);
  if (exact) {
    return {
      originName: toTurkishTitleCase(exact.cityName),
      fromStationId: exact.stationId,
    };
  }

  const partial = candidates.find((row) => slugTr(row.cityName).includes(query) || query.includes(slugTr(row.cityName)));
  if (partial) {
    return {
      originName: toTurkishTitleCase(partial.cityName),
      fromStationId: partial.stationId,
    };
  }

  return null;
}

async function resolveReportingOrigin(originInput) {
  const raw = String(originInput || REPORTING_SOURCE.originName).trim();
  if (!raw) {
    return { originName: REPORTING_SOURCE.originName, fromStationId: REPORTING_SOURCE.fromStationId };
  }

  if (/^[0-9a-fA-F-]{32,36}$/.test(raw)) {
    return { originName: raw, fromStationId: raw };
  }

  const fromStatic = REPORTING_ORIGIN_STATIC.get(slugTr(raw));
  if (fromStatic) {
    return fromStatic;
  }

  const fromHistory = lookupOriginStationFromHistory(raw);
  if (fromHistory) {
    REPORTING_ORIGIN_STATIC.set(slugTr(raw), fromHistory);
    REPORTING_ORIGIN_STATIC.set(slugTr(fromHistory.originName), fromHistory);
    return fromHistory;
  }

  const looked = await lookupOriginStationByName(raw);
  if (looked) {
    REPORTING_ORIGIN_STATIC.set(slugTr(raw), looked);
    REPORTING_ORIGIN_STATIC.set(slugTr(looked.originName), looked);
    return looked;
  }

  throw new Error(`Kalkis sehri bulunamadi: ${raw}. OneOps baglantisi acik oldugunda tekrar dene.`);
}

async function fetchStationNameById(stationId) {
  const id = String(stationId || "").trim();
  if (!id) {
    return "";
  }

  const urls = [
    `https://global.api.flixbus.com/gis/v1/station/${encodeURIComponent(id)}`,
    `https://global.api.flixbus.com/gis/v1/station/${encodeURIComponent(id)}?locale=tr`,
  ];

  for (const url of urls) {
    try {
      const res = await fetch(url);
      if (!res.ok) {
        continue;
      }

      const data = await res.json();
      const candidates = [
        data?.name,
        data?.city_name,
        data?.city?.name,
        data?.display_name,
      ];

      for (const candidate of candidates) {
        const name = String(candidate || "").trim();
        if (name) {
          return name;
        }
      }
    } catch {
      // Try next endpoint variation.
    }
  }

  return "";
}

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
      const seatsAvailable = toFiniteNumber(item?.available?.seats);
      const occupancyPercent = estimateOccupancyPercent(item);
      const occupancyLevel = String(item?.remaining?.capacity || "").trim();

      map.set(rideUuid, {
        seatsAvailable: seatsAvailable != null ? seatsAvailable : null,
        occupancyPercent: toFiniteNumber(occupancyPercent),
        occupancyLevel,
        payloadJson: JSON.stringify(item || {}),
      });
    }
  }

  return map;
}

function normalizeVehiclePlate(raw) {
  const text = String(raw || "")
    .toLocaleUpperCase("tr-TR")
    .replace(/[^0-9A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!text) {
    return "";
  }

  // Common compact Turkish format: 34LSS22
  const compact = text.replace(/\s+/g, "");
  const compactMatch = compact.match(/^(\d{2})([A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC]{1,3})(\d{2,4})$/);
  if (compactMatch) {
    return `${compactMatch[1]} ${compactMatch[2]} ${compactMatch[3]}`;
  }

  // Extended OneOps formats also appear as 01HAZER01 or 34ABCD12345.
  const compactExtended = compact.match(/^(\d{2})([A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC]{1,8})(\d{2,6})$/);
  if (compactExtended) {
    return `${compactExtended[1]} ${compactExtended[2]} ${compactExtended[3]}`;
  }

  const spaced = text.match(/\b(\d{2})\s*([A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC]{1,3})\s*(\d{2,4})\b/);
  if (spaced) {
    return `${spaced[1]} ${spaced[2]} ${spaced[3]}`;
  }

  const spacedExtended = text.match(/\b(\d{2})[\s-]*([A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC]{1,8})[\s-]*(\d{2,6})\b/);
  if (spacedExtended) {
    return `${spacedExtended[1]} ${spacedExtended[2]} ${spacedExtended[3]}`;
  }

  return "";
}

function collectCandidatePlateValues(obj, out = []) {
  if (!obj || typeof obj !== "object") {
    return out;
  }

  if (Array.isArray(obj)) {
    for (const item of obj) {
      collectCandidatePlateValues(item, out);
    }
    return out;
  }

  for (const [key, value] of Object.entries(obj)) {
    const keyNorm = String(key || "").toLocaleLowerCase("tr-TR");
    const isPlateKey = /plate|license|registration|plaka/.test(keyNorm);

    if (isPlateKey && typeof value === "string") {
      out.push(value);
    }

    if (value && typeof value === "object") {
      collectCandidatePlateValues(value, out);
    }
  }

  return out;
}

function extractVehiclePlateFromPayload(payload) {
  const candidates = collectCandidatePlateValues(payload, []);
  for (const item of candidates) {
    const normalized = normalizeVehiclePlate(item);
    if (normalized) {
      return normalized;
    }
  }

  return "";
}

function getOneOpsOrigin() {
  const fromMeta = String(getMeta("control_session_test_url", "") || "").trim();
  if (fromMeta) {
    try {
      return new URL(fromMeta).origin;
    } catch {
      // fallback below
    }
  }
  return "https://app.oneops.flixbus.com";
}

function getControlBaseOrigin() {
  const fromMeta = String(getMeta("control_base_url", "") || "").trim();
  if (fromMeta) {
    try {
      return new URL(fromMeta).origin;
    } catch {
      // fallback below
    }
  }
  return "https://backend.flixbus.com";
}

function extractVehiclePlateFromText(text) {
  const source = String(text || "");
  if (!source) {
    return "";
  }

  const namedPatterns = [
    /"licensePlate"\s*:\s*"([^"]+)"/gi,
    /"vehiclePlate"\s*:\s*"([^"]+)"/gi,
    /"plateNumber"\s*:\s*"([^"]+)"/gi,
    /"registrationNumber"\s*:\s*"([^"]+)"/gi,
    /"plaka"\s*:\s*"([^"]+)"/gi,
    /"vehicle_registration_number"\s*:\s*"([^"]+)"/gi,
    /\"licensePlate\"\s*:\s*\"([^\"]+)\"/gi,
    /\"vehiclePlate\"\s*:\s*\"([^\"]+)\"/gi,
  ];

  for (const regex of namedPatterns) {
    let match = regex.exec(source);
    while (match) {
      const normalized = normalizeVehiclePlate(match[1]);
      if (normalized) {
        return normalized;
      }
      match = regex.exec(source);
    }
  }

  return "";
}

function htmlToPlainText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&quot;/gi, '"')
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function extractPlateFromOneOpsAssignmentArea(htmlOrText) {
  const text = htmlToPlainText(htmlOrText);
  if (!text) {
    return "";
  }

  const sectionPatterns = [
    /Plaka\s*Atama[\s\S]{0,260}?Plaka\s*yaz\s*:?\s*([0-9A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC\s-]{4,40})/i,
    /Plaka\s*yaz\s*:?\s*([0-9A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC\s-]{4,40})/i,
  ];

  for (const regex of sectionPatterns) {
    const match = regex.exec(text);
    if (!match) {
      continue;
    }
    const normalized = normalizeVehiclePlate(match[1]);
    if (normalized) {
      return normalized;
    }
  }

  return "";
}

function extractPlateNearChangeButton(htmlOrText) {
  const raw = String(htmlOrText || "");
  const text = htmlToPlainText(raw);
  if (!text && !raw) {
    return "";
  }

  const sources = [raw, text.replace(/\s+/g, " ")];

  for (const src of sources) {
    // OneOps Vehicle Info genelde: "209756 - 34LSS22 - Tourismo ... Active ... Change"
    const nearChangePattern = /([0-9]{2}[A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC]{2,8}[0-9]{2,6})[\s\S]{0,2000}?Change/i;
    const nearChangeMatch = nearChangePattern.exec(src);
    if (nearChangeMatch) {
      const normalized = normalizeVehiclePlate(nearChangeMatch[1]);
      if (normalized) {
        return normalized;
      }
    }

    // Vehicle Info satirinda tire ile ayrilan parca icinde plaka bulunur.
    const vehicleInfoLine = /\b\d{3,8}\s*[\-\u2013\u2014]\s*([0-9A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC]{6,20})\s*[\-\u2013\u2014]\s*/i.exec(src);
    if (vehicleInfoLine) {
      const normalized = normalizeVehiclePlate(vehicleInfoLine[1]);
      if (normalized) {
        return normalized;
      }
    }

    // Bazi sayfalarda ayiraclar kaybolabiliyor: "209756 34LSS22 Tourismo..."
    const compactLine = /\b\d{3,8}\s+([0-9]{2}[A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC]{2,8}[0-9]{2,6})\b/i.exec(src);
    if (compactLine) {
      const normalized = normalizeVehiclePlate(compactLine[1]);
      if (normalized) {
        return normalized;
      }
    }
  }

  return "";
}

function parseMoneyAmount(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    return null;
  }

  const cleaned = text.replace(/[^0-9,.-]/g, "");
  if (!cleaned) {
    return null;
  }

  let normalized = cleaned;
  const hasComma = normalized.includes(",");
  const hasDot = normalized.includes(".");
  if (hasComma && hasDot) {
    const lastComma = normalized.lastIndexOf(",");
    const lastDot = normalized.lastIndexOf(".");
    if (lastComma > lastDot) {
      normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
    } else {
      normalized = normalized.replace(/,/g, "");
    }
  } else if (hasComma) {
    normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
  }

  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function emptyOneOpsSummary() {
  return {
    plate: "",
    revenueTry: null,
    revenueEur: null,
    operatorName: "",
    vehicleModel: "",
  };
}

function mergeOneOpsSummary(base, incoming) {
  return {
    plate: String(incoming?.plate || base?.plate || "").trim(),
    revenueTry: incoming?.revenueTry != null ? incoming.revenueTry : (base?.revenueTry ?? null),
    revenueEur: incoming?.revenueEur != null ? incoming.revenueEur : (base?.revenueEur ?? null),
    operatorName: String(incoming?.operatorName || base?.operatorName || "").trim(),
    vehicleModel: String(incoming?.vehicleModel || base?.vehicleModel || "").trim(),
  };
}

function extractOneOpsSummaryFromText(htmlOrText) {
  const raw = String(htmlOrText || "");
  const text = htmlToPlainText(raw);
  const summary = emptyOneOpsSummary();

  summary.plate = extractPlateNearChangeButton(raw) || extractPlateFromOneOpsAssignmentArea(raw) || "";

  const tryBlockMatch = /Total\s*Revenue\s*\(TRY\)\s*:?[\s\u00A0]*([\s\S]{0,120}?)(?:Total\s*Revenue\s*\(EUR\)|$)/i.exec(text);
  if (tryBlockMatch) {
    const amount = parseMoneyAmount(tryBlockMatch[1]);
    if (amount != null) {
      summary.revenueTry = amount;
    }
  }

  const eurBlockMatch = /Total\s*Revenue\s*\(EUR\)\s*:?[\s\u00A0]*([\s\S]{0,120}?)(?:$|Comments|Vehicle\s*Info|Radar)/i.exec(text);
  if (eurBlockMatch) {
    const amount = parseMoneyAmount(eurBlockMatch[1]);
    if (amount != null) {
      summary.revenueEur = amount;
    }
  }

  const modelMatch = /\b\d{3,8}\s*-\s*[0-9A-Z\s]{4,20}\s*-\s*([^\n-]{3,80})\s*-\s*(?:Active|Inactive)/i.exec(text);
  if (modelMatch) {
    summary.vehicleModel = String(modelMatch[1] || "").replace(/\s+/g, " ").trim();
  }

  const operatorMatch = /([A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC0-9\s\.,&\-]{6,}(?:A\.\s?\u015E\.|LTD\.?|\u015ET\.?\u0130\.?|TUR\u0130ZM|TA\u015EIMACILIK)[A-Z\u00C7\u011E\u0130\u00D6\u015E\u00DC0-9\s\.,&\-]{0,80})/i.exec(text);
  if (operatorMatch) {
    summary.operatorName = String(operatorMatch[1] || "").replace(/\s+/g, " ").trim();
  }

  return summary;
}

function extractOneOpsSummaryFromPayload(payload) {
  const summary = emptyOneOpsSummary();
  if (!payload || typeof payload !== "object") {
    return summary;
  }

  summary.plate = extractVehiclePlateFromPayload(payload);

  const text = JSON.stringify(payload || {});
  const tryMatch = /"totalRevenueTry"\s*:\s*([0-9.,-]+)/i.exec(text) || /"total_revenue_try"\s*:\s*([0-9.,-]+)/i.exec(text);
  if (tryMatch) {
    summary.revenueTry = parseMoneyAmount(tryMatch[1]);
  }
  const eurMatch = /"totalRevenueEur"\s*:\s*([0-9.,-]+)/i.exec(text) || /"total_revenue_eur"\s*:\s*([0-9.,-]+)/i.exec(text);
  if (eurMatch) {
    summary.revenueEur = parseMoneyAmount(eurMatch[1]);
  }

  const modelMatch = /"vehicleModel"\s*:\s*"([^"]+)"/i.exec(text);
  if (modelMatch) {
    summary.vehicleModel = String(modelMatch[1] || "").trim();
  }
  const operatorMatch = /"operatorName"\s*:\s*"([^"]+)"/i.exec(text) || /"carrierName"\s*:\s*"([^"]+)"/i.exec(text);
  if (operatorMatch) {
    summary.operatorName = String(operatorMatch[1] || "").trim();
  }

  return summary;
}

function extractJsonObjectsFromHtml(html) {
  const source = String(html || "");
  if (!source) {
    return [];
  }

  const out = [];
  const scriptPatterns = [
    /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/gi,
    /<script[^>]*type=["']application\/json["'][^>]*>([\s\S]*?)<\/script>/gi,
    /window\.__INITIAL_STATE__\s*=\s*({[\s\S]*?});/gi,
    /window\.__PRELOADED_STATE__\s*=\s*({[\s\S]*?});/gi,
  ];

  for (const pattern of scriptPatterns) {
    let match = pattern.exec(source);
    while (match) {
      const raw = String(match[1] || "").trim();
      if (raw) {
        try {
          out.push(JSON.parse(raw));
        } catch {
          // Ignore non-JSON blobs.
        }
      }
      match = pattern.exec(source);
    }
  }

  return out;
}

async function fetchVehiclePlateFromUrl(url, headers) {
  const debug = {
    url,
    status: null,
    finalUrl: "",
    contentType: "",
    matchedBy: "",
    plate: "",
    revenueTry: null,
    revenueEur: null,
    operatorName: "",
    vehicleModel: "",
    note: "",
  };

  let summary = emptyOneOpsSummary();

  try {
    const response = await fetch(url, {
      method: "GET",
      headers,
      redirect: "follow",
    });

    debug.status = response.status;
    debug.finalUrl = String(response.url || "");

    if (!response.ok) {
      debug.note = "HTTP hata";
      return { plate: "", summary, debug };
    }

    if (/\/users\/login|\/login/i.test(debug.finalUrl)) {
      debug.note = "Login sayfasina yonlendi";
      return { plate: "", summary, debug };
    }

    const contentType = String(response.headers.get("content-type") || "").toLocaleLowerCase("tr-TR");
    debug.contentType = contentType;

    if (contentType.includes("application/json")) {
      const payload = await response.json();
      summary = mergeOneOpsSummary(summary, extractOneOpsSummaryFromPayload(payload));
      if (summary.plate) {
        debug.matchedBy = "json:plate-key";
        debug.plate = summary.plate;
        debug.revenueTry = summary.revenueTry;
        debug.revenueEur = summary.revenueEur;
        debug.operatorName = summary.operatorName;
        debug.vehicleModel = summary.vehicleModel;
        return { plate: summary.plate, summary, debug };
      }

      const fromJsonText = extractVehiclePlateFromText(JSON.stringify(payload || {}));
      if (fromJsonText) {
        debug.matchedBy = "json:text-scan";
        summary.plate = fromJsonText;
        debug.plate = summary.plate;
      } else {
        debug.note = "JSON var ama plaka yok";
      }
      debug.revenueTry = summary.revenueTry;
      debug.revenueEur = summary.revenueEur;
      debug.operatorName = summary.operatorName;
      debug.vehicleModel = summary.vehicleModel;
      return { plate: summary.plate, summary, debug };
    }

    const text = await response.text();
    const embeddedPayloads = extractJsonObjectsFromHtml(text);
    for (const payload of embeddedPayloads) {
      summary = mergeOneOpsSummary(summary, extractOneOpsSummaryFromPayload(payload));
    }

    summary = mergeOneOpsSummary(summary, extractOneOpsSummaryFromText(text));
    const fromChangeArea = extractPlateNearChangeButton(text);
    if (fromChangeArea) {
      debug.matchedBy = "html:vehicle-info-change";
      summary.plate = fromChangeArea;
      debug.plate = summary.plate;
      debug.revenueTry = summary.revenueTry;
      debug.revenueEur = summary.revenueEur;
      debug.operatorName = summary.operatorName;
      debug.vehicleModel = summary.vehicleModel;
      return { plate: summary.plate, summary, debug };
    }

    const fromAssignmentArea = extractPlateFromOneOpsAssignmentArea(text);
    if (fromAssignmentArea) {
      debug.matchedBy = "html:plaka-atama";
      summary.plate = fromAssignmentArea;
      debug.plate = summary.plate;
      debug.revenueTry = summary.revenueTry;
      debug.revenueEur = summary.revenueEur;
      debug.operatorName = summary.operatorName;
      debug.vehicleModel = summary.vehicleModel;
      return { plate: summary.plate, summary, debug };
    }

    debug.note = "HTML var ama guvenilir plaka alani bulunamadi";

    debug.revenueTry = summary.revenueTry;
    debug.revenueEur = summary.revenueEur;
    debug.operatorName = summary.operatorName;
    debug.vehicleModel = summary.vehicleModel;
    return { plate: summary.plate, summary, debug };
  } catch {
    debug.note = "Istek hatasi";
    return { plate: "", summary, debug };
  }
}

function buildPlateProbeUrls(rideUuid) {
  const oneOpsOrigin = getOneOpsOrigin();
  const controlBaseOrigin = getControlBaseOrigin();

  return [
    `${oneOpsOrigin}/ops-portal/ride-control/ride/${encodeURIComponent(rideUuid)}`,
    `${oneOpsOrigin}/ops-portal/ride-control/ride/${encodeURIComponent(rideUuid)}/summary`,
    `${oneOpsOrigin}/ops-portal/ride-control/ride/${encodeURIComponent(rideUuid)}/vehicle-info`,
    `${oneOpsOrigin}/ops-portal/ride-control/ride/${encodeURIComponent(rideUuid)}/revenue`,
    `${oneOpsOrigin}/ops-portal/ride-control/api/ride/${encodeURIComponent(rideUuid)}`,
    `${oneOpsOrigin}/ops-portal/ride-control/api/rides/${encodeURIComponent(rideUuid)}`,
    `${oneOpsOrigin}/ops-portal/api/ride-control/rides/${encodeURIComponent(rideUuid)}`,
    `${oneOpsOrigin}/ops-portal/api/ride-control/ride/${encodeURIComponent(rideUuid)}`,
    `${controlBaseOrigin}/ops-portal/ride-control/api/ride/${encodeURIComponent(rideUuid)}`,
    `${controlBaseOrigin}/ops-portal/ride-control/api/rides/${encodeURIComponent(rideUuid)}`,
    `${controlBaseOrigin}/ride-control/api/ride/${encodeURIComponent(rideUuid)}`,
    `${controlBaseOrigin}/ride-control/api/rides/${encodeURIComponent(rideUuid)}`,
    `${controlBaseOrigin}/api/ride-control/ride/${encodeURIComponent(rideUuid)}`,
  ];
}

async function probeOneOpsPlateByRideUuid(rideUuid) {
  const cookieHeader = String(getMeta("control_cookie_header", "") || "").trim();
  if (!cookieHeader) {
    return {
      summary: emptyOneOpsSummary(),
      probes: [],
      note: "Kayitli cookie yok.",
    };
  }

  const csrfToken = String(getMeta("control_csrf_token", "") || "").trim();
  const headers = {
    Cookie: cookieHeader,
    "X-CSRF-Token": csrfToken,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    Accept: "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
  };

  const probeUrls = buildPlateProbeUrls(rideUuid);
  const probes = [];
  let mergedSummary = emptyOneOpsSummary();
  for (const url of probeUrls) {
    const result = await fetchVehiclePlateFromUrl(url, headers);
    probes.push(result.debug);
    mergedSummary = mergeOneOpsSummary(mergedSummary, result.summary);
  }

  const hasAnyData = Boolean(
    mergedSummary.plate ||
    mergedSummary.operatorName ||
    mergedSummary.vehicleModel ||
    mergedSummary.revenueTry != null ||
    mergedSummary.revenueEur != null
  );

  return {
    summary: mergedSummary,
    probes,
    note: hasAnyData ? "OneOps verisi bulundu." : "OneOps verisi bulunamadi.",
  };
}

async function fetchOneOpsVehiclePlate(rideUuid) {
  const probe = await probeOneOpsPlateByRideUuid(rideUuid);
  return probe.summary?.plate || "";
}

function toFiniteNumber(value) {
  if (value == null) {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function secondsOrMinutesToMinutes(rawValue) {
  const n = toFiniteNumber(rawValue);
  if (n == null) {
    return null;
  }

  // Buyuk ihtimalle saniye: 180 => 3 dk, 540 => 9 dk
  if (Math.abs(n) > 120) {
    return Math.round(n / 60);
  }

  return Math.round(n);
}

function extractStopDelayMinutes(stop) {
  if (!stop || typeof stop !== "object") {
    return null;
  }

  const departure = stop.departure && typeof stop.departure === "object" ? stop.departure : {};
  const arrival = stop.arrival && typeof stop.arrival === "object" ? stop.arrival : {};

  const directCandidates = [
    departure.deviation_seconds,
    departure.delay_seconds,
    departure.deviation_minutes,
    departure.delay_minutes,
    departure.deviation,
    departure.delay,
    stop.deviation_seconds,
    stop.delay_seconds,
    stop.deviation_minutes,
    stop.delay_minutes,
    stop.deviation,
    stop.delay,
    arrival.deviation_seconds,
    arrival.delay_seconds,
    arrival.deviation_minutes,
    arrival.delay_minutes,
    arrival.deviation,
    arrival.delay,
  ];

  for (const value of directCandidates) {
    const m = secondsOrMinutesToMinutes(value);
    if (m != null) {
      return m;
    }
  }

  const realtimeTs = toFiniteNumber(departure.realtime_timestamp ?? departure.actual_timestamp ?? departure.live_timestamp);
  const plannedTs = toFiniteNumber(departure.timestamp ?? departure.planned_timestamp ?? departure.scheduled_timestamp);
  if (realtimeTs != null && plannedTs != null) {
    const deltaSec = realtimeTs - plannedTs;
    return Math.round(deltaSec / 60);
  }

  return null;
}

async function fetchLiveTripSnapshot(rideUuid, fromStationId, toStationId) {
  const url = `https://global.api.flixbus.com/gis/v1/ride/${encodeURIComponent(rideUuid)}/trip-info/live?from_station_uuid=${encodeURIComponent(fromStationId)}&to_station_uuid=${encodeURIComponent(toStationId)}`;
  const res = await fetch(url);
  if (!res.ok) {
    return { delayMinutes: 0, vehiclePlate: "" };
  }

  const data = await res.json();
  const stops = Array.isArray(data?.stops) ? data.stops : [];
  const originStop = stops.find((stop) => String(stop?.stop_uuid || "") === String(fromStationId));
  const stopDelay = extractStopDelayMinutes(originStop);
  const fallbackCandidates = [
    data?.deviation_seconds,
    data?.delay_seconds,
    data?.deviation_minutes,
    data?.delay_minutes,
    data?.delay,
  ];

  let delayMinutes = stopDelay;
  if (delayMinutes == null) {
    for (const value of fallbackCandidates) {
      const m = secondsOrMinutesToMinutes(value);
      if (m != null) {
        delayMinutes = m;
        break;
      }
    }
  }

  if (delayMinutes == null) {
    delayMinutes = 0;
  }

  const vehiclePlate = extractVehiclePlateFromPayload(data);
  return { delayMinutes, vehiclePlate };
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

async function fetchOriginDepartures(origin, reportDateIso) {
  const range = getIstanbulDayRangeMs(reportDateIso);
  if (!range) {
    throw new Error("Rapor tarihi gecersiz.");
  }

  const url = `https://global.api.flixbus.com/gis/v1/station/${encodeURIComponent(origin.fromStationId)}/timetable/departures/from-timestamp/${range.from}/to-timestamp/${range.to}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`${origin.originName} kalkisli seferler alinamadi.`);
  }

  const data = await res.json();
  const rides = Array.isArray(data?.rides) ? data.rides : [];
  return rides;
}

async function collectOperationsReportRows(reportDateIso, origin = REPORTING_SOURCE) {
  const departures = await fetchOriginDepartures(origin, reportDateIso);
  const availabilityMaps = new Map();
  const oneOpsSummaryCache = new Map();
  const stationNameCache = new Map();

  const targetByStation = new Map();
  for (const ride of departures) {
    const toStationId = String(ride?.to_stop_uuid || "").trim();
    if (!toStationId) {
      continue;
    }

    if (!targetByStation.has(toStationId)) {
      targetByStation.set(toStationId, {
        toStationId,
        destinationName: resolveDestinationNameFromRide(ride, toStationId),
      });
    }
  }

  for (const target of targetByStation.values()) {
    try {
      const map = await fetchRouteAvailabilityMap(reportDateIso, origin, target);
      availabilityMaps.set(target.toStationId, map);
    } catch {
      availabilityMaps.set(target.toStationId, new Map());
    }
  }

  const rows = [];
  for (const ride of departures) {
    const rideUuid = String(ride?.ride_uuid || "").trim();
    const toStationId = String(ride?.to_stop_uuid || "").trim();
    if (!rideUuid || !toStationId) {
      continue;
    }

    let destinationName = resolveDestinationNameFromRide(ride, toStationId);
    if (destinationName === "Hedef") {
      if (!stationNameCache.has(toStationId)) {
        stationNameCache.set(toStationId, fetchStationNameById(toStationId));
      }

      const stationName = String(await stationNameCache.get(toStationId) || "").trim();
      if (stationName) {
        destinationName = stationName;
      }
    }
    const base = availabilityMaps.get(toStationId)?.get(rideUuid) || {};
    const timing = await fetchTripDetailsTiming(rideUuid, origin.fromStationId, toStationId);
    const liveSnapshot = await fetchLiveTripSnapshot(rideUuid, origin.fromStationId, toStationId);
    let vehiclePlate = "";
    let oneOpsSummary = emptyOneOpsSummary();

    if (!oneOpsSummaryCache.has(rideUuid)) {
      oneOpsSummaryCache.set(rideUuid, probeOneOpsPlateByRideUuid(rideUuid));
    }

    const oneOpsProbe = await oneOpsSummaryCache.get(rideUuid);
    oneOpsSummary = oneOpsProbe?.summary || emptyOneOpsSummary();

    vehiclePlate = String(oneOpsSummary.plate || "").trim();

    const hasAnyOneOpsData = Boolean(
      oneOpsSummary.operatorName ||
      oneOpsSummary.vehicleModel ||
      oneOpsSummary.revenueTry != null ||
      oneOpsSummary.revenueEur != null
    );
    if (hasAnyOneOpsData && !vehiclePlate) {
      // OneOps'a erisip plaka bulamadiysak eski yanlis plakayi temizlemek icin bos gec.
      vehiclePlate = "";
    }

    const operatorName = String(oneOpsSummary.operatorName || "").trim();
    const vehicleModel = String(oneOpsSummary.vehicleModel || "").trim();
    const revenueTry = toFiniteNumber(oneOpsSummary.revenueTry);
    const revenueEur = toFiniteNumber(oneOpsSummary.revenueEur);

    const delayMinutes = Number(liveSnapshot.delayMinutes) || 0;

    rows.push({
      reportDate: reportDateIso,
      originName: origin.originName,
      destinationName,
      routeLabel: `${origin.originName} -> ${destinationName}`,
      rideUuid,
      toStationId,
      lineCode: timing.lineCode || String(ride?.line_code || "").trim(),
      tripNumber: timing.tripNumber || "",
      departureTime: timing.departureTime || fromUnixSecondsToStamp(ride?.planned?.timestamp, ride?.planned?.tz),
      arrivalTime: timing.arrivalTime || "",
      seatsAvailable: toFiniteNumber(base.seatsAvailable),
      occupancyPercent: toFiniteNumber(base.occupancyPercent),
      occupancyLevel: String(base.occupancyLevel || "veri-yok"),
      delayMinutes,
      isDelayed: delayMinutes !== 0 ? 1 : 0,
      vehiclePlate,
      operatorName,
      vehicleModel,
      revenueTry,
      revenueEur,
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

  // Legacy anahtar uyumlulugu: control -> oneops
  if (
    !Object.prototype.hasOwnProperty.call(parsed, "oneops") &&
    Object.prototype.hasOwnProperty.call(parsed, "control")
  ) {
    parsed.oneops = Boolean(parsed.control);
    delete parsed.control;
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

let reportingSyncRunning = false;

async function autoSyncReportingDates() {
  const today = todayIsoInIstanbul();
  const tomorrow = shiftIsoDate(today, 1);

  for (const date of [today, tomorrow]) {
    try {
      await syncOperationsReportsForDate(date, { emitNotification: false });
    } catch (error) {
      console.error(`Raporlama otomatik senkron hatasi (${date}):`, error.message);
    }
  }
}

setInterval(() => {
  autoSyncReportingDates().catch(() => null);
}, 5 * 60 * 1000);

autoSyncReportingDates().catch(() => null);

// Eski fiyat degisiklik kayitlarini otomatik temizle.
// changed_at formati: DD.MM.YYYY HH:mm:ss - SQL'de YYYY-MM-DD'ye cevirip kesim tarihiyle kiyasla.
function cleanupOldPriceHistory() {
  if (!PRICE_HISTORY_RETENTION_DAYS || PRICE_HISTORY_RETENTION_DAYS <= 0) {
    return; // 0 veya gecersiz = temizlik kapali
  }
  try {
    const cutoffIso = shiftIsoDate(todayIsoInIstanbul(), -PRICE_HISTORY_RETENTION_DAYS);
    const changedAtDateIso =
      "substr(changed_at, 7, 4) || '-' || substr(changed_at, 4, 2) || '-' || substr(changed_at, 1, 2)";
    const info = db
      .prepare(`DELETE FROM obilet_price_history WHERE ${changedAtDateIso} < ?`)
      .run(cutoffIso);
    if (info.changes > 0) {
      console.log(
        `[temizlik] ${info.changes} eski fiyat degisiklik kaydi silindi (${PRICE_HISTORY_RETENTION_DAYS} gunden eski, kesim < ${cutoffIso}).`
      );
    }
  } catch (error) {
    console.error("Eski fiyat gecmisi temizleme hatasi:", error.message);
  }
}

// Acilista bir kez + her 6 saatte bir calistir.
setInterval(cleanupOldPriceHistory, 6 * 60 * 60 * 1000);
cleanupOldPriceHistory();

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
    fullName: row.full_name || "",
    role: row.role || "",
    title: row.title || "",
    isAdmin: Boolean(row.is_admin),
    isActive: Boolean(row.is_active),
    permissions: JSON.parse(row.permissions || "{}"),
    createdAt: row.created_at,
    lastSeen: row.last_seen || "",
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
      "SELECT id, username, full_name, role, title, is_admin, is_active, permissions, created_at FROM users WHERE id = ?"
    )
    .get(session.user_id);

  if (!user) {
    db.prepare("DELETE FROM sessions WHERE token = ?").run(token);
    return null;
  }

  return { token, user: sanitizeUser(user) };
}

// Kullanici basina son last_seen yazim zamani (ms) — throttle icin bellek ici.
const lastSeenWrites = new Map();

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

  // Son aktiflik damgasi: her yetkili istekte guncelle (admin panelinde "su an aktif / X dk once").
  // Disk yazimini azaltmak icin kullanici basina 15 sn throttle.
  try {
    const uid = auth.user.id;
    const nowMs = Date.now();
    const prev = lastSeenWrites.get(uid) || 0;
    if (nowMs - prev >= 15000) {
      lastSeenWrites.set(uid, nowMs);
      db.prepare("UPDATE users SET last_seen = ? WHERE id = ?").run(nowStamp(), uid);
    }
  } catch (e) { /* yok say — aktiflik takibi asla istegi bozmasin */ }

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

// Menu-key bazli yetki: admin her seye erisir; degilse kullanicinin permissions[menuKey]'i truthy olmali.
// requireAuth'tan SONRA middleware olarak kullanilir: app.get("/api/x", requireAuth, requirePermission("demand"), ...)
function requirePermission(menuKey) {
  return (req, res, next) => {
    const u = req.auth?.user;
    if (!u) { res.status(401).json({ message: "Oturum gecersiz." }); return; }
    if (u.isAdmin || (u.permissions && u.permissions[menuKey])) { next(); return; }
    res.status(403).json({ message: "Bu bolum icin yetkiniz yok." });
  };
}

app.post("/api/login", (req, res) => {
  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "").trim();

  if (!username || !password) {
    res.status(400).json({ message: "Username and password are required." });
    return;
  }

  const user = db
    .prepare(
      "SELECT id, username, password, full_name, role, title, is_admin, is_active, permissions, created_at FROM users WHERE username = ?"
    )
    .get(username);

  if (!user || user.password !== password) {
    logAttempt({ username, status: "fail", reason: "Hatali kimlik", ip: req.ip });
    res.status(401).json({ message: "Invalid username or password." });
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
      "SELECT id, username, full_name, role, title, is_admin, is_active, permissions, created_at, last_seen FROM users ORDER BY is_admin DESC, username ASC"
    )
    .all();

  res.json({
    users: rows.map(sanitizeUser),
  });
});

// Kullanici KENDI hesabini gunceller (kullanici adi + sifre). Giris yapan HERKES kullanabilir.
// Guvenlik: mevcut sifre dogrulamasi zorunlu. Sadece kendi hesabini degistirir; rol/yetki degismez.
app.patch("/api/account", requireAuth, (req, res) => {
  try {
    const userId = req.auth.user.id;
    const row = db.prepare("SELECT id, username, password FROM users WHERE id = ?").get(userId);
    if (!row) return res.status(404).json({ message: "Kullanici bulunamadi." });

    const currentPassword = typeof req.body?.currentPassword === "string" ? req.body.currentPassword : "";
    const newUsername = typeof req.body?.newUsername === "string" ? req.body.newUsername.trim() : "";
    const newPassword = typeof req.body?.newPassword === "string" ? req.body.newPassword : "";

    // Mevcut sifre dogrulamasi (duz metin — mevcut sistemle tutarli).
    if (row.password !== currentPassword) {
      return res.status(403).json({ message: "Mevcut sifreniz yanlis." });
    }

    const wantUsername = Boolean(newUsername) && newUsername !== row.username;
    const wantPassword = Boolean(newPassword);
    if (!wantUsername && !wantPassword) {
      return res.status(400).json({ message: "Degistirilecek yeni bir kullanici adi veya sifre girin." });
    }

    if (wantUsername) {
      if (newUsername.length < 3) {
        return res.status(400).json({ message: "Kullanici adi en az 3 karakter olmali." });
      }
      const taken = db.prepare("SELECT id FROM users WHERE username = ? AND id != ?").get(newUsername, userId);
      if (taken) {
        return res.status(409).json({ message: "Bu kullanici adi zaten kullaniliyor." });
      }
    }
    if (wantPassword && String(newPassword).length < 4) {
      return res.status(400).json({ message: "Yeni sifre en az 4 karakter olmali." });
    }

    db.prepare("UPDATE users SET username = ?, password = ? WHERE id = ?").run(
      wantUsername ? newUsername : row.username,
      wantPassword ? newPassword : row.password,
      userId
    );

    const updated = db.prepare(
      "SELECT id, username, full_name, role, title, is_admin, is_active, permissions, created_at, last_seen FROM users WHERE id = ?"
    ).get(userId);
    res.json({ ok: true, user: sanitizeUser(updated), usernameChanged: wantUsername, passwordChanged: wantPassword });
  } catch (e) {
    res.status(500).json({ message: e.message || "Hesap guncellenemedi." });
  }
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
      "SELECT id, username, password, full_name, role, title, is_admin, is_active, permissions, created_at FROM users WHERE id = ?"
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
  const newFullName =
    typeof req.body?.fullName === "string" ? req.body.fullName.trim() : null;
  const newRole =
    typeof req.body?.role === "string" ? req.body.role.trim() : null;
  const newTitle =
    typeof req.body?.title === "string" ? req.body.title.trim() : null;
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
    "UPDATE users SET username = ?, password = ?, full_name = ?, role = ?, title = ?, is_active = ?, permissions = ? WHERE id = ?"
  ).run(
    shouldUpdateUsername ? newUsername : target.username,
    shouldUpdatePassword ? newPassword : target.password,
    newFullName !== null ? newFullName : (target.full_name || ""),
    newRole !== null ? newRole : (target.role || ""),
    newTitle !== null ? newTitle : (target.title || ""),
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

async function syncOperationsReportsForDate(reportDate, options = {}) {
  if (reportingSyncRunning) {
    return { date: reportDate, count: 0, skipped: true };
  }

  reportingSyncRunning = true;
  try {
    const origin = await resolveReportingOrigin(options.originName || REPORTING_SOURCE.originName);
    const collected = await collectOperationsReportRows(reportDate, origin);
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
        vehicle_plate,
        operator_name,
        vehicle_model,
        revenue_try,
        revenue_eur,
        payload_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        vehicle_plate = CASE
          WHEN COALESCE(TRIM(excluded.vehicle_plate), '') <> '' THEN excluded.vehicle_plate
          WHEN (
            COALESCE(TRIM(excluded.operator_name), '') <> '' OR
            COALESCE(TRIM(excluded.vehicle_model), '') <> '' OR
            excluded.revenue_try IS NOT NULL OR
            excluded.revenue_eur IS NOT NULL
          ) THEN ''
          WHEN COALESCE(TRIM(operations_reports.vehicle_plate), '') = '' THEN ''
          WHEN operations_reports.vehicle_plate GLOB '[0-9][0-9] [A-Z] [0-9]' THEN ''
          ELSE operations_reports.vehicle_plate
        END,
        operator_name = CASE
          WHEN COALESCE(TRIM(excluded.operator_name), '') <> '' THEN excluded.operator_name
          ELSE operations_reports.operator_name
        END,
        vehicle_model = CASE
          WHEN COALESCE(TRIM(excluded.vehicle_model), '') <> '' THEN excluded.vehicle_model
          ELSE operations_reports.vehicle_model
        END,
        revenue_try = COALESCE(excluded.revenue_try, operations_reports.revenue_try),
        revenue_eur = COALESCE(excluded.revenue_eur, operations_reports.revenue_eur),
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
          .run(reportDate, origin.originName);
        return;
      }

      const keepSet = new Set(collected.map((row) => String(row.rideUuid || "")));
      const existing = selectExisting.all(reportDate, origin.originName);
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
          origin.fromStationId,
          String(row.toStationId || ""),
          row.lineCode,
          row.tripNumber,
          row.departureTime,
          row.arrivalTime,
          row.seatsAvailable,
          row.occupancyPercent,
          row.occupancyLevel,
          row.isDelayed,
          row.delayMinutes,
          row.vehiclePlate,
          row.operatorName,
          row.vehicleModel,
          row.revenueTry,
          row.revenueEur,
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
        `${options.actor} raporlama verisini yeniledi (${origin.originName}, ${reportDate}, ${collected.length} sefer).`
      );
    }

    return { date: reportDate, origin: origin.originName, count: collected.length, skipped: false };
  } finally {
    reportingSyncRunning = false;
  }
}

app.get("/api/operations-reports", requireAuth, async (req, res) => {
  const date = String(req.query.date || todayIsoInIstanbul()).trim();
  const originInput = String(req.query.origin || REPORTING_SOURCE.originName).trim();

  let origin;
  try {
    origin = await resolveReportingOrigin(originInput);
  } catch (error) {
    res.status(400).json({ message: error.message || "Kalkis sehri gecersiz." });
    return;
  }

  // Frontend'de butona basmadan veri güncel olsun.
  try {
    await syncOperationsReportsForDate(date, { emitNotification: false, originName: origin.originName });
  } catch (error) {
    console.error(`GET /api/operations-reports otomatik sync hatasi (${origin.originName}, ${date}):`, error.message);
  }

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
        operator_name,
        vehicle_model,
        revenue_try,
        revenue_eur,
        note,
        updated_at
      FROM operations_reports
      WHERE report_date = ? AND origin_name = ?
      ORDER BY departure_time ASC, id ASC
      `
    )
    .all(date, origin.originName);

  res.json({
    date,
    origin: origin.originName,
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
      seatsAvailable: toFiniteNumber(row.seats_available),
      occupancyPercent: toFiniteNumber(row.occupancy_percent),
      occupancyLevel: row.occupancy_level || "",
      isDelayed: Boolean(row.is_delayed),
      delayMinutes: Number(row.delay_minutes) || 0,
      vehiclePlate: row.vehicle_plate || "",
      operatorName: row.operator_name || "",
      vehicleModel: row.vehicle_model || "",
      revenueTry: toFiniteNumber(row.revenue_try),
      revenueEur: toFiniteNumber(row.revenue_eur),
      note: row.note || "",
      updatedAt: row.updated_at,
    })),
  });
});

app.post("/api/operations-reports/sync", requireAuth, async (req, res) => {
  const reportDate = String(req.body?.date || todayIsoInIstanbul()).trim();
  const originInput = String(req.body?.origin || REPORTING_SOURCE.originName).trim();
  const dotDate = toDotDate(reportDate);
  if (!dotDate) {
    res.status(400).json({ message: "Rapor tarihi gecersiz." });
    return;
  }

  try {
    const result = await syncOperationsReportsForDate(reportDate, {
      emitNotification: true,
      actor: req.auth.user.username,
      originName: originInput,
    });
    res.json({ ok: true, date: result.date, origin: result.origin, count: result.count, skipped: result.skipped });
  } catch (error) {
    res.status(500).json({ message: error.message || "Rapor verisi senkronize edilemedi." });
  }
});

app.post("/api/operations-reports/plate-debug", requireAuth, requireAdmin, async (req, res) => {
  const rideUuid = String(req.body?.rideUuid || "").trim();
  if (!rideUuid) {
    res.status(400).json({ message: "ride_uuid zorunlu." });
    return;
  }

  try {
    const probe = await probeOneOpsPlateByRideUuid(rideUuid);
    const lastRow = db.prepare(
      "SELECT id, report_date, route_label, vehicle_plate FROM operations_reports WHERE ride_uuid = ? ORDER BY id DESC LIMIT 1"
    ).get(rideUuid);

    res.json({
      ok: true,
      rideUuid,
      extractedPlate: probe.summary?.plate || "",
      extractedRevenueTry: probe.summary?.revenueTry ?? null,
      extractedRevenueEur: probe.summary?.revenueEur ?? null,
      extractedOperatorName: probe.summary?.operatorName || "",
      extractedVehicleModel: probe.summary?.vehicleModel || "",
      note: probe.note || "",
      probes: probe.probes || [],
      currentRow: lastRow
        ? {
            id: Number(lastRow.id),
            reportDate: lastRow.report_date,
            routeLabel: lastRow.route_label,
            vehiclePlate: lastRow.vehicle_plate || "",
          }
        : null,
    });
  } catch (error) {
    res.status(500).json({ message: error.message || "Plaka debug islemi basarisiz." });
  }
});

app.get("/api/control-integration", requireAuth, requireAdmin, (req, res) => {
  const baseUrl = getMeta("control_base_url", "https://backend.flixbus.com");
  const loginUrl = getMeta("control_login_url", "https://app.oneops.flixbus.com/users/login");
  const cookieHeader = getMeta("control_cookie_header", "");
  const csrfToken = getMeta("control_csrf_token", "");
  const sessionTestUrl = getMeta("control_session_test_url", "https://app.oneops.flixbus.com/ops-portal/");
  const updatedAt = getMeta("control_updated_at", "");
  const lastCheckAt = getMeta("control_last_check_at", "");
  const lastCheckStatus = getMeta("control_last_check_status", "");

  res.json({
    baseUrl,
    loginUrl,
    cookieHeader,
    csrfToken,
    sessionTestUrl,
    hasCookie: Boolean(cookieHeader),
    updatedAt,
    lastCheckAt,
    lastCheckStatus,
  });
});

app.patch("/api/control-integration", requireAuth, requireAdmin, (req, res) => {
  const baseUrl = String(req.body?.baseUrl || "https://backend.flixbus.com").trim().slice(0, 400);
  const loginUrl = String(req.body?.loginUrl || "https://app.oneops.flixbus.com/users/login").trim().slice(0, 500);
  const cookieHeader = String(req.body?.cookieHeader || "").trim().slice(0, 12000);
  const csrfToken = String(req.body?.csrfToken || "").trim().slice(0, 1000);
  const sessionTestUrl = String(req.body?.sessionTestUrl || "https://app.oneops.flixbus.com/ops-portal/").trim().slice(0, 1000);

  if (baseUrl && !/^https?:\/\//i.test(baseUrl)) {
    res.status(400).json({ message: "Control Base URL gecersiz." });
    return;
  }
  if (loginUrl && !/^https?:\/\//i.test(loginUrl)) {
    res.status(400).json({ message: "Login URL gecersiz." });
    return;
  }
  if (sessionTestUrl && !/^https?:\/\//i.test(sessionTestUrl)) {
    res.status(400).json({ message: "Oturum Test URL gecersiz." });
    return;
  }

  setMeta("control_base_url", baseUrl);
  setMeta("control_login_url", loginUrl);
  setMeta("control_cookie_header", cookieHeader);
  setMeta("control_csrf_token", csrfToken);
  setMeta("control_session_test_url", sessionTestUrl);
  setMeta("control_updated_at", nowStamp());

  addPricingNotification(
    req.auth.user.username,
    `${req.auth.user.username} Control baglanti ayarlarini guncelledi.`
  );

  res.json({ ok: true });
});

app.post("/api/control-integration/test", requireAuth, requireAdmin, async (req, res) => {
  const cookieHeader = getMeta("control_cookie_header", "");
  const csrfToken = getMeta("control_csrf_token", "");
  const fallbackUrl = getMeta("control_session_test_url", "https://app.oneops.flixbus.com/ops-portal/");
  const testUrl = String(req.body?.testUrl || fallbackUrl).trim().slice(0, 1000);

  if (!cookieHeader) {
    res.status(400).json({ message: "Kayitli OneOps cookie bulunamadi." });
    return;
  }

  if (!/^https?:\/\//i.test(testUrl)) {
    res.status(400).json({ message: "Oturum Test URL gecersiz." });
    return;
  }

  try {
    const response = await fetch(testUrl, {
      method: "GET",
      headers: {
        Cookie: cookieHeader,
        "X-CSRF-Token": csrfToken,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        Accept: "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
      },
      redirect: "follow",
    });

    const finalUrl = String(response.url || testUrl);
    const redirectedToLogin = /\/users\/login|\/login/i.test(finalUrl);
    const unauthorized = response.status === 401 || response.status === 403;
    const authenticated = !redirectedToLogin && !unauthorized && response.status < 500;

    setMeta("control_last_check_at", nowStamp());
    setMeta("control_last_check_status", authenticated ? "ok" : "invalid");

    res.json({
      ok: true,
      authenticated,
      statusCode: response.status,
      finalUrl,
      checkedAt: getMeta("control_last_check_at", ""),
      message: authenticated
        ? "OneOps oturumu gecerli gorunuyor."
        : "OneOps oturumu gecersiz veya suresi dolmus olabilir. Yeniden giris yapip kaydet.",
    });
  } catch (error) {
    setMeta("control_last_check_at", nowStamp());
    setMeta("control_last_check_status", "error");
    res.status(502).json({ message: error.message || "OneOps oturum testi basarisiz." });
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

app.get("/api/error-reports", requireAuth, (req, res) => {
  const reports = db
    .prepare("SELECT id, description, username, photos_json, priority, status, created_at FROM user_error_reports ORDER BY id DESC LIMIT 100")
    .all();

  res.json({
    errors: reports.map(r => ({
      id: r.id,
      desc: r.description,
      user: r.username,
      photos: JSON.parse(r.photos_json || "[]"),
      priority: r.priority,
      status: r.status,
      date: r.created_at
    }))
  });
});

app.post("/api/error-reports", requireAuth, (req, res) => {
  const desc = String(req.body?.desc || "").trim();
  const user = String(req.body?.user || "").trim();
  const priority = String(req.body?.priority || "Orta");
  const photos = Array.isArray(req.body?.photos) ? req.body.photos : [];

  if (!desc || !user) {
    res.status(400).json({ message: "Aciklama ve kullanici adi zorunlu." });
    return;
  }

  const result = db.prepare(
    "INSERT INTO user_error_reports (description, username, photos_json, priority, created_at) VALUES (?, ?, ?, ?, ?)"
  ).run(desc, user, JSON.stringify(photos), priority, new Date().toISOString());

  res.json({ ok: true, id: result.lastInsertRowid });
});

app.patch("/api/error-reports/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  const status = String(req.body?.status || "Yeni");
  db.prepare("UPDATE user_error_reports SET status = ? WHERE id = ?").run(status, id);
  res.json({ ok: true });
});

app.delete("/api/error-reports/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  db.prepare("DELETE FROM user_error_reports WHERE id = ?").run(id);
  res.json({ ok: true });
});

// ==========================================
// oBiLET FIYAT TAKIP VE E-POSTA ENTEGRASYONU
// ==========================================

// Puppeteer + stealth plugin: bilinen headless Chrome fingerprint'lerini gizler.
// Cloudflare/anti-bot taramalarini yenmek icin en yaygin cozum.
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(StealthPlugin());
const nodemailer = require("nodemailer");

// Browser pool (kaynak optimizasyonu için tek instance)
let sharedBrowser = null;
let browserLaunchInProgress = false;

// Browser instance'ı al veya yeni oluştur
async function getBrowserInstance() {
  // Eğer mevcut browser varsa ve bağlı ise onu kullan
  if (sharedBrowser && sharedBrowser.isConnected()) {
    return sharedBrowser;
  }
  
  // Eğer başka bir browser launch işlemi devam ediyorsa bekle
  while (browserLaunchInProgress) {
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  // Hala browser yoksa yeni oluştur
  if (!sharedBrowser || !sharedBrowser.isConnected()) {
    browserLaunchInProgress = true;
    try {
      console.log("[Browser Pool] Yeni browser instance oluşturuluyor...");
      // Production'da Dockerfile google-chrome-stable kuruyor ve
      // PUPPETEER_EXECUTABLE_PATH env'i set ediyor. Local geliştirmede env yoksa
      // Puppeteer'in indirdigi Chromium kullanilir.
      const executablePath = process.env.PUPPETEER_EXECUTABLE_PATH || undefined;
      sharedBrowser = await puppeteer.launch({
        headless: "new",
        executablePath,
        // NOT: --single-process + --no-zygote sistem Chrome'unda detached-frame hatasi yaratiyor.
        // O flag'ler Puppeteer'in bundled Chromium'u icin gerekliydi (RAM koruma). System Chrome
        // multi-process'i guvenli sekilde yonetir, RAM'i optimize eder. obiletTaskRunning lock'u
        // sequential calismayi zaten garantiledigi icin concurrent tab tear-down riski yok.
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-dev-shm-usage",
          "--disable-blink-features=AutomationControlled",
          "--disable-gpu",
          "--disable-software-rasterizer",
          "--disable-extensions",
          "--disable-background-networking",
          "--disable-default-apps",
          "--disable-sync",
          "--disable-translate",
          "--hide-scrollbars",
          "--metrics-recording-only",
          "--mute-audio",
          "--no-first-run",
          "--disable-crash-reporter",
          "--disable-breakpad"
        ]
      });
      console.log("[Browser Pool] Browser instance oluşturuldu.");
    } finally {
      browserLaunchInProgress = false;
    }
  }
  
  return sharedBrowser;
}

// Browser instance'ı temizle
async function closeBrowserInstance() {
  if (sharedBrowser) {
    try {
      console.log("[Browser Pool] Browser instance kapatılıyor...");
      await sharedBrowser.close();
      sharedBrowser = null;
      console.log("[Browser Pool] Browser instance kapatıldı.");
    } catch (error) {
      console.error("[Browser Pool] Browser kapatma hatası:", error.message);
    }
  }
}

function parseCsvList(raw) {
  return String(raw || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function createSmtpTransportsWithFallback() {
  const smtpHost = String(process.env.SMTP_HOST || "smtp.gmail.com").trim();
  const smtpPort = parseInt(process.env.SMTP_PORT || "465", 10);
  const smtpUser = String(process.env.SMTP_USER || "").trim();
  const smtpPass = String(process.env.SMTP_PASS || "").trim();

  if (!smtpUser || !smtpPass) {
    throw new Error("SMTP_USER ve SMTP_PASS tanimli degil.");
  }

  const candidatePorts = [];
  if (Number.isFinite(smtpPort)) {
    candidatePorts.push(smtpPort);
  }
  if (!candidatePorts.includes(465)) {
    candidatePorts.push(465);
  }
  if (!candidatePorts.includes(587)) {
    candidatePorts.push(587);
  }

  const transporters = candidatePorts.map((port) => {
    const secure = port === 465;
    const transporter = nodemailer.createTransport({
      host: smtpHost,
      port,
      secure,
      requireTLS: !secure,
      auth: {
        user: smtpUser,
        pass: smtpPass,
      },
      connectionTimeout: 20000,
      greetingTimeout: 15000,
      socketTimeout: 30000,
      tls: {
        servername: smtpHost,
        minVersion: "TLSv1.2",
      },
    });

    return { transporter, port, secure };
  });

  return {
    transporters,
    smtpHost,
    smtpUser,
  };
}

async function sendMailWithSmtpFallback(mailOptions) {
  const { transporters, smtpHost, smtpUser } = createSmtpTransportsWithFallback();
  const errors = [];

  for (const entry of transporters) {
    try {
      await entry.transporter.verify();
      const info = await entry.transporter.sendMail(mailOptions);
      return {
        info,
        smtpHost,
        smtpUser,
        smtpPort: entry.port,
      };
    } catch (error) {
      errors.push(`${entry.port}/${entry.secure ? "SSL" : "STARTTLS"}: ${error.message}`);
    }
  }

  const smtpErrorMessage = `SMTP baglanti basarisiz (${smtpHost}). Denenen portlar: ${errors.join(" | ")}`;

  const resendApiKey = String(process.env.RESEND_API_KEY || "").trim();
  if (resendApiKey) {
    try {
      const resendResponse = await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${resendApiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          from: mailOptions.from,
          to: Array.isArray(mailOptions.to)
            ? mailOptions.to
            : String(mailOptions.to || "")
                .split(",")
                .map((x) => x.trim())
                .filter(Boolean),
          subject: mailOptions.subject,
          html: mailOptions.html,
        }),
      });

      const resendData = await resendResponse.json().catch(() => ({}));
      if (resendResponse.ok && resendData?.id) {
        return {
          info: { messageId: resendData.id },
          smtpHost: "resend-api",
          smtpUser,
          smtpPort: 443,
        };
      }
      errors.push(`Resend API: ${resendData?.message || resendData?.name || resendResponse.status}`);
    } catch (error) {
      errors.push(`Resend API: ${error.message}`);
    }
  }

  const brevoApiKey = String(process.env.BREVO_API_KEY || "").trim();
  if (brevoApiKey) {
    try {
      const match = String(mailOptions.from || "").match(/^(.*?)\s*<([^>]+)>$/);
      const senderName = match ? match[1].replace(/^"|"$/g, "").trim() : "oBilet Fiyat Takip";
      const senderEmail = match ? match[2].trim() : smtpUser;
      const recipients = (Array.isArray(mailOptions.to)
        ? mailOptions.to
        : String(mailOptions.to || "")
            .split(",")
            .map((x) => x.trim())
            .filter(Boolean)).map((email) => ({ email }));

      const brevoResponse = await fetch("https://api.brevo.com/v3/smtp/email", {
        method: "POST",
        headers: {
          "api-key": brevoApiKey,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          sender: { name: senderName || "oBilet Fiyat Takip", email: senderEmail },
          to: recipients,
          subject: mailOptions.subject,
          htmlContent: mailOptions.html,
        }),
      });

      const brevoData = await brevoResponse.json().catch(() => ({}));
      if (brevoResponse.ok && brevoData?.messageId) {
        return {
          info: { messageId: brevoData.messageId },
          smtpHost: "brevo-api",
          smtpUser,
          smtpPort: 443,
        };
      }
      errors.push(`Brevo API: ${brevoData?.message || brevoResponse.status}`);
    } catch (error) {
      errors.push(`Brevo API: ${error.message}`);
    }
  }

  throw new Error(`${smtpErrorMessage}${errors.length ? ` | Fallbackler: ${errors.join(" | ")}` : ""}`);
}

function setObiletTargetSyncStatus(targetId, statusText) {
  try {
    db.prepare("UPDATE obilet_targets SET last_sync_status = ?, last_sync_at = ? WHERE id = ?")
      .run(String(statusText || "").slice(0, 500), nowStamp(), targetId);
  } catch (error) {
    console.warn(`[oBilet] Durum kaydi basarisiz (target: ${targetId}): ${error.message}`);
  }
}

function buildObiletRouteLabel(target) {
  return `${String(target?.origin || "").toLocaleUpperCase("tr-TR")} - ${String(target?.destination || "").toLocaleUpperCase("tr-TR")}`;
}

function buildObiletSubject(prefix, target, dateLabel) {
  return `${prefix}: ${buildObiletRouteLabel(target)} (${dateLabel})`;
}

function normalizeObiletOperatorName(value) {
  return toTurkishTitleCase(String(value || "").replace(/\s+/g, " ").trim());
}

function toObiletOperatorMatchKey(value) {
  return normalizeSearchText(value).replace(/\s+/g, "");
}

function isObiletOperatorMatch(selectedOperator, scrapedOperator) {
  const selectedKey = toObiletOperatorMatchKey(selectedOperator);
  const scrapedKey = toObiletOperatorMatchKey(scrapedOperator);
  if (!selectedKey || !scrapedKey) {
    return false;
  }

  // Bosluk, Turkce karakter ve kucuk yazim farklarini tolere ederek eslestir.
  return scrapedKey.includes(selectedKey) || selectedKey.includes(scrapedKey);
}

function buildJourneyIdentityKey(operator, departureTime, departureStop = "", price = 0) {
  // Aynı firma + aynı saat = aynı sefer (fiyat değişse de aynı sefer!)
  // price parametresi artık kullanılmıyor - fiyat değişikliğini pending mekanizması izliyor
  const parts = [
    toObiletOperatorMatchKey(operator),
    String(departureTime || "").trim(),
  ];
  
  // departureStop varsa ekle (normalize edilmiş halde)
  if (departureStop) {
    parts.push(normalizeSearchText(departureStop));
  }
  
  return parts.join("|");
}

// oBilet İstasyon ID Önbelleği
// ============================================================
// oBiLET GERÇEK API ENTEGRASYonu
// oBilet'in resmi mobile/web API'sini kullanır
// ============================================================

// Session önbelleği - her istekte yeni session açmayız
let obiletSessionCache = null;
let obiletSessionExpiry = 0;

// oBilet API Session Al
async function getObiletSession() {
  const now = Date.now();
  if (obiletSessionCache && now < obiletSessionExpiry) {
    return obiletSessionCache;
  }

  try {
    const res = await fetch("https://api.obilet.com/api/client/getsession", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "Basic T25seUFQSTo3OEI0QzZFMy1BNDI1LTRFQUQtQTZFOS04QzA0OUZGNzBCMDY=",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0"
      },
      body: JSON.stringify({
        "type": 4,
        "connection": {
          "ip-address": "1.1.1.1",
          "port": "5000"
        },
        "browser": {
          "name": "Chrome",
          "version": "124"
        }
      })
    });

    if (res.ok) {
      const data = await res.json();
      const session = data?.data;
      if (session?.["session-id"] && session?.["device-id"]) {
        obiletSessionCache = session;
        // 20 dakika geçerli
        obiletSessionExpiry = now + 20 * 60 * 1000;
        console.log(`[oBilet] Session alındı: ${session["session-id"].substring(0, 8)}...`);
        return session;
      }
    }
  } catch (e) {
    console.log(`[oBilet] Session alma hatası: ${e.message}`);
  }

  // Fallback session
  const fallback = {
    "session-id": crypto.randomUUID(),
    "device-id": crypto.randomUUID()
  };
  obiletSessionCache = fallback;
  obiletSessionExpiry = now + 5 * 60 * 1000;
  return fallback;
}

// oBilet İstasyon ID önbelleği
const obiletStationIdCache = new Map();

// oBilet API: Şehir - İstasyon ID bul
async function getObiletStationId(cityName, session) {
  const key = slugTr(cityName);
  if (obiletStationIdCache.has(key)) return obiletStationIdCache.get(key);

  try {
    const res = await fetch("https://api.obilet.com/api/location/getbuslocations", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "Basic T25seUFQSTo3OEI0QzZFMy1BNDI1LTRFQUQtQTZFOS04QzA0OUZGNzBCMDY="
      },
      body: JSON.stringify({
        "data": cityName,
        "device-session": session,
        "date": new Date().toISOString(),
        "language": "tr-TR"
      })
    });

    if (res.ok) {
      const data = await res.json();
      const locations = Array.isArray(data?.data) ? data.data : [];
      
      // Tam eşleşme önce, sonra içeren
      let match = locations.find(loc => slugTr(loc?.name || "") === key);
      if (!match) match = locations.find(loc => slugTr(loc?.name || "").includes(key) || key.includes(slugTr(loc?.name || "")));
      
      if (match?.id) {
        obiletStationIdCache.set(key, match.id);
        console.log(`[oBilet] İstasyon bulundu: ${cityName} - ID:${match.id} (${match.name})`);
        return match.id;
      }
    }
  } catch (e) {
    console.log(`[oBilet] İstasyon arama hatası (${cityName}): ${e.message}`);
  }

  return null;
}

// oBilet Gerçek API ile sefer çek
async function fetchObiletJourneysViaApi(origin, destination, dateIso) {
  try {
    const session = await getObiletSession();
    
    const [originId, destId] = await Promise.all([
      getObiletStationId(origin, session),
      getObiletStationId(destination, session)
    ]);

    if (!originId || !destId) {
      console.log(`[oBilet API] İstasyon ID bulunamadı: ${origin}(${originId}) - ${destination}(${destId})`);
      return [];
    }

    const dateParts = dateIso.split("-");
    const dateFormatted = `${dateParts[2]}.${dateParts[1]}.${dateParts[0]}`;

    const res = await fetch("https://api.obilet.com/api/journey/getbusjourneys", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "Basic T25seUFQSTo3OEI0QzZFMy1BNDI1LTRFQUQtQTZFOS04QzA0OUZGNzBCMDY="
      },
      body: JSON.stringify({
        "data": {
          "origin-id": originId,
          "destination-id": destId,
          "departure-date": dateFormatted,
          "currency": "TRY"
        },
        "device-session": session,
        "date": new Date().toISOString(),
        "language": "tr-TR"
      })
    });

    if (!res.ok) {
      console.log(`[oBilet API] HTTP ${res.status} - ${origin} - ${destination}`);
      return [];
    }

    const data = await res.json();
    const items = Array.isArray(data?.data) ? data.data : [];

    if (!items.length) {
      console.log(`[oBilet API] Sonuç boş: ${origin} - ${destination} ${dateIso}`);
      return [];
    }

    const journeys = [];
    const seen = new Set();

    for (const item of items) {
      const operator = toTurkishTitleCase(String(
        item?.partner_name ||
        item?.["partner-name"] ||
        item?.busCompany?.name ||
        item?.bus_company?.name ||
        ""
      ).trim());

      const rawTime = String(
        item?.departure_time ||
        item?.["departure-time"] ||
        item?.departureTime ||
        ""
      );
      const timeMatch = rawTime.match(/([01]\d|2[0-3]):[0-5]\d/);
      if (!timeMatch) continue;
      const depTime = timeMatch[0];

      // KRITIK: oBilet'in sayfada gosterdigi satis fiyati = internet-price.
      // original-price firmanin yayinladigi yuksek fiyat — ASLA fallback olarak kullanma.
      // Bu sira degisirse "1100 yerine 1200" hatasi geri gelir.
      const priceRaw =
        item?.["internet-price"] ??
        item?.internet_price ??
        item?.internetPrice ??
        item?.price ??
        0;
      const price = Math.round(Number(priceRaw));

      const depStop = String(
        item?.origin_location ||
        item?.["origin-location"] ||
        item?.originLocation ||
        ""
      ).replace(/\s+/g, " ").trim();

      const arrStop = String(
        item?.destination_location ||
        item?.["destination-location"] ||
        item?.destinationLocation ||
        ""
      ).replace(/\s+/g, " ").trim();

      if (!operator || !depTime || price < 50) continue;

      const key = `${toObiletOperatorMatchKey(operator)}|${depTime}`;
      if (seen.has(key)) continue;
      seen.add(key);

      journeys.push({ operator, time: depTime, price, departureStop: depStop, arrivalStop: arrStop });
    }

    console.log(`[oBilet API] ${origin}-${destination} ${dateIso}: ${journeys.length} sefer bulundu`);
    if (journeys.length > 0) {
      journeys.slice(0, 5).forEach(j => 
        console.log(`  - ${j.operator} ${j.time}: ${j.price} TL (${j.departureStop})`)
      );
    }
    return journeys;

  } catch (e) {
    console.log(`[oBilet API] Hata: ${e.message}`);
    return [];
  }
}

// oBilet Ana Scraper
// Onceligi: target'in elindeki route_id -> statik tablo -> Node API -> Puppeteer.
// Onemli: Ayni route ID iki kez denenmesin — verilen ID ile statik tablo aynisi olabilir.
// Browser/protocol hatalarini icerde yakalar, isleyen hatti devirmemek icin [] dondurur.
async function scrapeObilet(origin, destination, dateIso, routeId = null, seatOperators = null) {
  const triedRouteIds = new Set();
  const cleanRouteId = routeId && /^\d+-\d+$/.test(String(routeId).trim()) ? String(routeId).trim() : null;

  const tryScrapeSeferler = async (rid) => {
    // Transient Puppeteer hatalari icin tek retry. ONEMLI: browser instance'i KAPATMIYORUZ —
    // browser kapanirsa Cloudflare cookie'leri kaybolur, sonraki istekler her seferinde challenge
    // alir. Sadece newPage yeniden denenir; gercek crash olursa setupObiletPage zaten icinde
    // browser'i yeniden baslatir.
    const isRetriable = (msg) =>
      /detached Frame|Target\.createTarget|Session with given id not found|Protocol error|disconnected|Navigation timeout|frame got detached/i.test(msg);

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const browser = await getBrowserInstance();
        return await scrapeObiletSeferlerPage(browser, rid, dateIso, seatOperators);
      } catch (err) {
        const msg = String(err?.message || "").substring(0, 200);
        if (attempt === 1 && isRetriable(msg)) {
          console.warn(`[oBilet] /seferler/${rid} gecici hata, 3s sonra retry: ${msg}`);
          await new Promise(r => setTimeout(r, 3000));
          continue;
        }
        console.warn(`[oBilet] /seferler/${rid} hatasi (yutuldu): ${msg}`);
        return [];
      }
    }
    return [];
  };

  // 1) Elde route_id varsa direkt /seferler/ sayfasini kazi — en saglam yol
  if (cleanRouteId) {
    console.log(`[oBilet] Route ID kullanilarak direkt cekiliyor: ${cleanRouteId}`);
    triedRouteIds.add(cleanRouteId);
    const journeys = await tryScrapeSeferler(cleanRouteId);
    if (journeys.length > 0) return journeys;
    console.log(`[oBilet] Verilen route_id (${cleanRouteId}) sonuc dondurmedi. Yedek yontemler atlanir.`);
    return [];
  }

  // 2) Statik tablodan route_id cikar (Kadirli=595, Ankara=356 gibi bilinenler)
  const localRouteId = buildObiletRouteIdLocal(origin, destination);
  if (localRouteId && !triedRouteIds.has(localRouteId)) {
    console.log(`[oBilet] Yerel station tablosundan route_id: ${origin}->${destination} = ${localRouteId}`);
    triedRouteIds.add(localRouteId);
    const journeys = await tryScrapeSeferler(localRouteId);
    if (journeys.length > 0) return journeys;
  }

  // 3) Resmi API dene (Node.js'den) — Railway egress engellerse fail eder
  const apiResult = await fetchObiletJourneysViaApi(origin, destination, dateIso).catch(() => []);
  if (apiResult.length > 0) return apiResult;

  // 4) Puppeteer fallback (browser-API + route discovery)
  console.log(`[oBilet] API başarısız, Puppeteer deneniyor: ${origin}-${destination} ${dateIso}`);
  return scrapeObiletViaPuppeteer(origin, destination, dateIso);
}

// oBilet station ID seed tablosu — kullanici tarafindan dogrulanmis ID'ler.
// Yeni ID eklemek icin: panelden "oBilet Sehir Kodlari" bolumunden ekleyin (kalici DB'ye gider),
// veya direkt buraya gomun. Key'ler slugTr ile uretilir: turkce karakterler donusur, bosluk silinir.
const OBILET_STATION_IDS_SEED = {
  "adana": 348,
  "adiyaman": 352,     // Adıyaman
  "afyon": 353,
  "agri": 354,         // Ağrı
  "aksaray": 414,
  "ankara": 356,
  "antalya": 357,
  "balikesir": 360,    // Balıkesir
  "bolu": 364,
  "bursa": 366,
  "ceyhan": 488,
  "diyarbakir": 371,   // Diyarbakır
  "eskisehir": 376,    // Eskişehir
  "gaziantep": 377,
  "hatay": 430,
  "isparta": 381,
  "istanbul": 349,     // İstanbul
  "izmir": 383,        // İzmir
  "kadirli": 595,
  "kayseri": 386,
  "kilis": 425,
  "konya": 389,
  "kozan": 617,
  "mersin": 382,
  "nigde": 398,        // Niğde
  "osmaniye": 426,
  "sanliurfa": 409,    // Şanlıurfa
  "siirt": 402,
  "trabzon": 407,
  "usak": 410,         // Uşak
  "van": 411,
};

function cityKey(cityName) {
  return slugTr(cityName).replace(/\s+/g, "");
}

// Sehir -> station ID lookup. Once DB'deki ogrenilen ID'lere, sonra seed tablosuna bakar.
function findObiletStationIdLocal(cityName) {
  const key = cityKey(cityName);
  if (!key) return null;

  // 1) Tam eslesme: DB
  try {
    const row = db.prepare("SELECT station_id FROM obilet_station_ids WHERE city_key = ?").get(key);
    if (row?.station_id) return row.station_id;
  } catch (e) { /* DB henuz hazir degilse seed'e dus */ }

  // 2) Tam eslesme: seed
  if (Object.prototype.hasOwnProperty.call(OBILET_STATION_IDS_SEED, key)) {
    return OBILET_STATION_IDS_SEED[key];
  }

  // 3) Kismi eslesme: DB (orn. "kadirli otogari" -> "kadirli")
  try {
    const all = db.prepare("SELECT city_key, station_id FROM obilet_station_ids").all();
    for (const row of all) {
      if (key.includes(row.city_key) || row.city_key.includes(key)) return row.station_id;
    }
  } catch (e) {}

  // 4) Kismi eslesme: seed
  for (const known of Object.keys(OBILET_STATION_IDS_SEED)) {
    if (key.includes(known) || known.includes(key)) return OBILET_STATION_IDS_SEED[known];
  }

  return null;
}

function buildObiletRouteIdLocal(origin, destination) {
  const o = findObiletStationIdLocal(origin);
  const d = findObiletStationIdLocal(destination);
  if (o && d) return `${o}-${d}`;
  return null;
}

// XHR'den gelen item'lardan sehir->ID eslemesini ogren ve DB'ye yaz.
const learnStationStmt = db.prepare(`
  INSERT INTO obilet_station_ids (city_key, city_name, station_id, hits, last_seen)
  VALUES (?, ?, ?, 1, ?)
  ON CONFLICT(city_key) DO UPDATE SET
    station_id = excluded.station_id,
    city_name = excluded.city_name,
    hits = hits + 1,
    last_seen = excluded.last_seen
`);

function learnObiletStationId(cityName, stationId) {
  if (!cityName || !stationId) return;
  const key = cityKey(cityName);
  if (!key) return;
  try {
    learnStationStmt.run(key, String(cityName).trim(), Number(stationId), nowStamp());
  } catch (e) { /* sessizce gec */ }
}

// oBilet route ID cache: "kadirli|ankara" -> "595-356"
// Landing sayfasinda "Otobus Ara"ya basinca acilan /seferler/X-Y/DATE URL'inden ogrenilir.
const obiletRouteIdCache = new Map();

// Puppeteer ile bir sayfa hazirla — Stealth plugin webdriver/navigator/chrome maskelenmesini
// otomatik yapar. Burada ek olarak sadece reklam istegi engelleme ve Turkce locale.
// Browser cokmusse (Target.createTarget hatasi) bir kez yeniden baslat ve tekrar dene.
async function setupObiletPage(browser) {
  const buildPage = async (b) => {
    const page = await b.newPage();
    await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36");
    await page.setViewport({ width: 1366, height: 768 });
    await page.setExtraHTTPHeaders({ "Accept-Language": "tr-TR,tr;q=0.9" });
    await page.setRequestInterception(true);
    page.on("request", (req) => {
      const reqUrl = req.url();
      if (/(doubleclick|googleads|googletagmanager|google-analytics|facebook\.com|fbcdn|hotjar|tiktok|yandex|criteo|taboola|outbrain|clarity|useinsider)/i.test(reqUrl)) {
        return req.abort();
      }
      return req.continue();
    });
    return page;
  };

  try {
    return await buildPage(browser);
  } catch (err) {
    const msg = String(err?.message || "");
    // Browser cokmesi sinyalleri.
    const isCrash = /Target\.createTarget|Session with given id not found|Protocol error|disconnected|detached Frame|frame got detached/i.test(msg);
    if (!isCrash) throw err;

    console.warn(`[Browser Pool] newPage hatasi (${msg.substring(0, 120)}). Browser yeniden baslatiliyor...`);
    try { await closeBrowserInstance(); } catch {}
    const fresh = await getBrowserInstance();
    return await buildPage(fresh);
  }
}

// Landing sayfasini ac, Cloudflare challenge gercekten gecene kadar bekle.
// Sabit 12s bekleme yerine waitForFunction ile koşulu sorgular — bu daha guvenli
// (gercek bekleme suresi 2-30 sn arasi degisiyor, sabit 12s bazen yetmiyor).
async function gotoAndHandleCloudflare(page, url) {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 }).catch(() => {});

  const isStillChallenge = async () => {
    try {
      return await page.evaluate(() =>
        document.title.includes("Just a moment") ||
        (document.body?.innerText || "").includes("Checking your browser") ||
        (document.body?.innerText || "").includes("challenge-platform") ||
        !!document.querySelector("#challenge-running")
      );
    } catch { return false; }
  };

  if (!(await isStillChallenge())) return;

  console.log("[oBilet Puppeteer] Cloudflare challenge, gecmesi bekleniyor (en fazla 30s)...");
  try {
    await page.waitForFunction(
      () => !document.title.includes("Just a moment") &&
            !(document.body?.innerText || "").includes("Checking your browser") &&
            !document.querySelector("#challenge-running"),
      { timeout: 30000, polling: 1000 }
    );
    console.log("[oBilet Puppeteer] Cloudflare gecildi.");
  } catch {
    console.log("[oBilet Puppeteer] Cloudflare 30s'de gecilemedi, mevcut DOM ile devam edilir.");
  }
}

// Browser context icinden oBilet API'sine fetch at.
// Railway konteynerinden Node.js fetch'i api.obilet.com'a cikamiyor (DNS/egress engeli).
// Ama Puppeteer browser'i kendi network stack'ini kullaniyor + Cloudflare cookie'lerine sahip,
// bu yuzden ayni endpoint'ler sayfa context'inden cagrildiginda calisir.
async function fetchObiletJourneysViaBrowser(browser, origin, destination, dateIso) {
  const originSlug = slugTr(origin).replace(/\s+/g, "-");
  const destSlug = slugTr(destination).replace(/\s+/g, "-");
  // Once www.obilet.com'da herhangi bir sayfa ac ki Cloudflare cookie'leri otursun.
  const landingUrl = `https://www.obilet.com/otobus-bileti/${originSlug}-${destSlug}?date=${dateIso}`;

  console.log(`[oBilet Browser-API] Baslatiliyor: ${origin}->${destination} ${dateIso}`);
  const page = await setupObiletPage(browser);

  try {
    await gotoAndHandleCloudflare(page, landingUrl);

    const dateParts = dateIso.split("-");
    const dateFormatted = `${dateParts[2]}.${dateParts[1]}.${dateParts[0]}`; // DD.MM.YYYY

    // Sayfa context'inde tum API akisini tek seferde calistir
    const result = await page.evaluate(async (originName, destName, dateFmt) => {
      const AUTH = "Basic T25seUFQSTo3OEI0QzZFMy1BNDI1LTRFQUQtQTZFOS04QzA0OUZGNzBCMDY=";
      const slugTr = (s) => String(s || "").toLocaleLowerCase("tr-TR")
        .replace(/ı/g, "i").replace(/ğ/g, "g").replace(/ü/g, "u")
        .replace(/ş/g, "s").replace(/ö/g, "o").replace(/ç/g, "c")
        .replace(/[^a-z0-9]/g, "").trim();

      try {
        // 1) Session
        const sessionRes = await fetch("https://api.obilet.com/api/client/getsession", {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": AUTH },
          body: JSON.stringify({
            type: 4,
            connection: { "ip-address": "1.1.1.1", port: "5000" },
            browser: { name: "Chrome", version: "124" }
          })
        });
        if (!sessionRes.ok) return { error: `session HTTP ${sessionRes.status}` };
        const sessionData = await sessionRes.json();
        const session = sessionData?.data;
        if (!session?.["session-id"]) return { error: "session-id yok" };

        // 2) Station ID lookup
        const findStation = async (cityName) => {
          const res = await fetch("https://api.obilet.com/api/location/getbuslocations", {
            method: "POST",
            headers: { "Content-Type": "application/json", "Authorization": AUTH },
            body: JSON.stringify({
              data: cityName,
              "device-session": session,
              date: new Date().toISOString(),
              language: "tr-TR"
            })
          });
          if (!res.ok) return null;
          const data = await res.json();
          const list = Array.isArray(data?.data) ? data.data : [];
          const target = slugTr(cityName);
          // Tam eslesme onceligi
          let m = list.find(loc => slugTr(loc?.name) === target);
          if (!m) m = list.find(loc => slugTr(loc?.name).includes(target) || target.includes(slugTr(loc?.name)));
          return m?.id ?? null;
        };

        const [originId, destId] = await Promise.all([findStation(originName), findStation(destName)]);
        if (!originId || !destId) {
          return { error: `Istasyon ID bulunamadi: ${originName}=${originId} ${destName}=${destId}` };
        }

        // 3) Seferleri al
        const journeyRes = await fetch("https://api.obilet.com/api/journey/getbusjourneys", {
          method: "POST",
          headers: { "Content-Type": "application/json", "Authorization": AUTH },
          body: JSON.stringify({
            data: {
              "origin-id": originId,
              "destination-id": destId,
              "departure-date": dateFmt,
              currency: "TRY"
            },
            "device-session": session,
            date: new Date().toISOString(),
            language: "tr-TR"
          })
        });
        if (!journeyRes.ok) return { error: `journey HTTP ${journeyRes.status}` };
        const journeyData = await journeyRes.json();
        return {
          originId,
          destId,
          items: Array.isArray(journeyData?.data) ? journeyData.data : []
        };
      } catch (e) {
        return { error: e.message };
      }
    }, origin, destination, dateFormatted);

    if (result?.error) {
      console.log(`[oBilet Browser-API] Hata: ${result.error}`);
      return [];
    }

    const items = result?.items || [];
    console.log(`[oBilet Browser-API] ${origin}->${destination} ${dateIso}: ${items.length} ham sefer`);

    if (DEBUG_OBILET_API && items[0]) {
      console.log(`[oBilet Browser-API DEBUG] Ornek alanlar: ${Object.keys(items[0]).join(", ")}`);
    }

    const journeys = [];
    const seen = new Set();
    for (const item of items) {
      const operator = toTurkishTitleCase(String(
        item?.["partner-name"] || item?.partner_name || item?.partnerName ||
        item?.busCompany?.name || item?.bus_company?.name || ""
      ).trim());
      const rawTime = String(item?.["departure-time"] || item?.departure_time || item?.departureTime || "");
      const tm = rawTime.match(/([01]\d|2[0-3]):[0-5]\d/);
      if (!tm) continue;

      // KRITIK: Sadece internet-price (oBilet satis fiyati). original-price'a ASLA dusme.
      const price = Math.round(Number(
        item?.["internet-price"] ?? item?.internet_price ?? item?.internetPrice ?? item?.price ?? 0
      ));
      if (!operator || price < 50) continue;

      const depStop = String(item?.["origin-location"] || item?.origin_location || "").trim();
      const arrStop = String(item?.["destination-location"] || item?.destination_location || "").trim();
      const key = `${toObiletOperatorMatchKey(operator)}|${tm[0]}`;
      if (seen.has(key)) continue;
      seen.add(key);
      journeys.push({ operator, time: tm[0], price, departureStop: depStop, arrivalStop: arrStop });
    }

    console.log(`[oBilet Browser-API] ${journeys.length} sefer parse edildi`);
    if (DEBUG_OBILET_PRICE) {
      journeys.slice(0, 8).forEach(j =>
        console.log(`  -> ${j.operator} ${j.time}: ${j.price} TL`)
      );
    }
    return journeys;
  } catch (e) {
    console.error(`[oBilet Browser-API] Beklenmeyen hata: ${e.message}`);
    return [];
  } finally {
    await page.close().catch(() => {});
  }
}

// /seferler/{routeId}/{date} sayfasindan gercek satis fiyatlarini cek.
async function scrapeObiletSeferlerPage(browser, routeId, dateIso, seatOperators = null) {
  const url = `https://www.obilet.com/seferler/${routeId}/${dateIso}?t=${Date.now()}`;
  console.log(`[oBilet Puppeteer] Seferler sayfasi: ${url}`);

  const page = await setupObiletPage(browser);

  // XHR yakalama — eger oBilet sayfa icinde API cagiriyorsa
  const capturedJourneys = [];
  const seenKeys = new Set();
  const idsByKey = new Map(); // operator+saat -> [tum listeleme id'leri] (gercek doluluk icin)
  page.on("response", async (response) => {
    try {
      if (response.status() < 200 || response.status() >= 300) return;
      const ct = response.headers()["content-type"] || "";
      if (!ct.includes("json")) return;
      if (!/(getbusjourneys|busjourneys|journeys)/i.test(response.url())) return;

      const text = await response.text().catch(() => "");
      if (!text) return;
      let data; try { data = JSON.parse(text); } catch { return; }
      const list = Array.isArray(data?.data) ? data.data :
        Array.isArray(data?.journeys) ? data.journeys :
        Array.isArray(data) ? data : [];
      console.log(`[oBilet Puppeteer] XHR yakalandi: ${list.length} sefer`);

      // ILK item'in KOLTUK ile ilgili alanlarini kisaca logla (dev dump kaldirildi — gurultu yapiyordu).
      if (list.length > 0 && DEBUG_OBILET_PRICE) {
        const it = list[0];
        console.log(`[oBilet XHR] ornek: ${it["partner-name"]} total=${it["total-seats"]} avail=${it["available-seats"]} hasSeatInfo=${it["has-available-seat-info"]} dyn=${it["has-dynamic-pricing"]} zero=${it?.journey?.["should-set-seats-to-zero"]}`);
      }

      let parsed = 0, skipped_op = 0, skipped_time = 0, skipped_price = 0;
      for (const item of list) {
        // Operator: partner-name kesin alani
        const operator = toTurkishTitleCase(String(
          item?.["partner-name"] || item?.partner_name || item?.partnerName ||
          item?.["company-name"] || item?.company_name || item?.companyName ||
          item?.partner?.name || item?.busCompany?.name || ""
        ).trim());

        // Time: en hizli yol journey.departure (ISO "2026-06-09T08:30:00"), sonra stops dizisi
        let tm = null;
        let depStop = String(item?.journey?.origin || item?.["origin-location"] || "").trim();
        let arrStop = String(item?.journey?.destination || item?.["destination-location"] || "").trim();

        const directDeparture = item?.journey?.departure || item?.departure || item?.["departure-time"] || item?.departure_time;
        if (directDeparture) {
          const m = String(directDeparture).match(/T?(\d{2}):(\d{2})/);
          if (m) tm = [`${m[1]}:${m[2]}`, m[1], m[2]];
        }
        // Yedek: stops dizisinden is-origin olani bul
        if (!tm) {
          const stops = item?.journey?.stops || item?.stops || [];
          if (Array.isArray(stops) && stops.length) {
            const originStop = stops.find(s => s?.["is-origin"] === true || s?.is_origin === true || s?.isOrigin === true) || stops[0];
            const destStop = stops.find(s => s?.["is-destination"] === true || s?.is_destination === true || s?.isDestination === true) || stops[stops.length - 1];
            if (originStop?.time) {
              const m = String(originStop.time).match(/T?(\d{2}):(\d{2})/);
              if (m) tm = [`${m[1]}:${m[2]}`, m[1], m[2]];
            }
            if (!depStop) depStop = String(originStop?.name || "").trim();
            if (!arrStop) arrStop = String(destStop?.name || "").trim();
          }
        }

        // Price: GERCEK yapida fiyat journey.internet-price icinde (log'dan teyit edildi).
        // Oncelik: journey.internet-price -> top-level internet-price -> sale/discounted -> son care original-price
        let price = 0;
        const priceCandidates = [
          item?.journey?.["internet-price"], item?.journey?.internet_price, item?.journey?.internetPrice,
          item?.["internet-price"], item?.internet_price, item?.internetPrice,
          item?.journey?.["sale-price"], item?.["sale-price"], item?.sale_price,
          item?.journey?.["discounted-price"], item?.["discounted-price"],
          item?.journey?.["partner-price"], item?.["partner-price"],
          item?.journey?.price, item?.price, item?.amount, item?.fare,
          // Son care — original-price (yuksek liste fiyati). Hicbir yerde internet-price yoksa bunu kullan.
          item?.journey?.["original-price"], item?.["original-price"], item?.original_price,
        ];
        for (const c of priceCandidates) {
          if (c == null) continue;
          const n = Math.round(Number(c));
          if (Number.isFinite(n) && n >= 50 && n <= 50000) { price = n; break; }
        }

        if (!operator) { skipped_op++; continue; }
        if (!tm) { skipped_time++; continue; }
        if (price < 50) { skipped_price++; continue; }

        // OGREN: API yanitinda her item sehir<->station-id eslemesini tasiyor.
        // Bunlari DB'ye kaydet ki yeni hatlar elle ID girmeden eklenebilsin.
        const oLoc = item?.["origin-location"];
        const oLocId = item?.["origin-location-id"];
        const dLoc = item?.["destination-location"];
        const dLocId = item?.["destination-location-id"];
        if (oLoc && oLocId) learnObiletStationId(oLoc, oLocId);
        if (dLoc && dLocId) learnObiletStationId(dLoc, dLocId);

        // DOLULUK (piggyback): oBilet sefer listesi total-seats/available-seats'i dogrudan veriyor.
        // Fiyat mantigini etkilemez — sadece sefer nesnesine ek alan; takip dongusu ayri tabloya yazar.
        const totalSeats = toFiniteNumber(item?.["total-seats"] ?? item?.total_seats ?? item?.totalSeats);
        const availableSeats = toFiniteNumber(item?.["available-seats"] ?? item?.available_seats ?? item?.availableSeats);
        // KRITIK: oBilet gercek koltuk bilgisini SADECE has-available-seat-info=true iken verir.
        // False olan seferlerde available-seats guvenilmez (cogu zaman 0 -> sahte %100). Bu seferler
        // tam da dinamik-fiyatlamasi olmayan yani FIYATI DEGISMEYEN seferler. Doluluk YALNIZCA
        // bu bayrak true iken kaydedilir; degilse "bilinmiyor" birakilir (yanlis sayi gosterilmez).
        const hasSeatInfo =
          (item?.["has-available-seat-info"] ?? item?.journey?.["has-available-seat-info"] ??
           item?.has_available_seat_info ?? item?.hasAvailableSeatInfo) === true;
        // should-set-seats-to-zero=true iken oBilet koltuklari SIFIR gostertir (yapay %100). Bu seferlerde
        // available-seats guvenilmez — dolulugu kaydetmeyiz.
        const shouldZeroSeats =
          (item?.journey?.["should-set-seats-to-zero"] ?? item?.["should-set-seats-to-zero"]) === true;
        const seatInfoReliable = hasSeatInfo && !shouldZeroSeats;

        const key = `${toObiletOperatorMatchKey(operator)}|${tm[0]}`;
        // GERCEK doluluk icin: ayni operator+saat'in TUM listeleme id'lerini topla (mukerrer olabilir;
        // gecerli koltuk haritasi doneni gercek otobustur).
        const seferId = item?.id ?? item?.journey?.id;
        if (seferId != null) {
          if (!idsByKey.has(key)) idsByKey.set(key, []);
          if (!idsByKey.get(key).includes(seferId)) idsByKey.get(key).push(seferId);
        }
        if (seenKeys.has(key)) {
          // KOPYA arac: oBilet ayni firma+saate 2. (bazen 3.) otobus koyar (orn. 00:30'da biri 37/41
          // DOLU, biri 15/40). Occupancy TEK satirda gosterildigi icin kullanici oBilet'te DOLU araci
          // gorur; bos kopyayi gostermek "yanlis" gorunur. Bu yuzden EN DOLU = EN AZ BOS (min available,
          // yani "Son X Koltuk"taki X en kucuk olan) aracin koltuk degerini tutariz. NOT: yalnizca occupancy
          // alanlari guncellenir; iki arac ayni ucret oldugu icin FIYAT/degisiklik mantigi HIC etkilenmez.
          if (Number.isFinite(totalSeats) && Number.isFinite(availableSeats)) {
            const existing = capturedJourneys.find((cj) => cj.matchKey === key);
            if (existing) {
              const exAvail = Number.isFinite(existing.availableSeats) ? existing.availableSeats : Infinity;
              if (availableSeats < exAvail) { // daha az bos = daha dolu arac -> onu tut
                existing.totalSeats = totalSeats;
                existing.availableSeats = availableSeats;
                existing.seatInfoReliable = seatInfoReliable;
              }
            }
          }
          continue;
        }
        seenKeys.add(key);
        // Seferin GERCEK guzergahi: journey.stops[0] = gercek baslangic, stops[son] = gercek bitis (segment DEGIL).
        // Orn. Adana->Nigde sorgusu: stops[0]=Adana, stops[son]=Esenler (Istanbul). is-destination bayragi
        // sorgulanan segment'i (Nigde) isaretler; biz SON duragi (gercek varis) aliriz.
        let routeFrom = "", routeTo = "";
        let originStopTime = "";
        {
          const allStops = item?.journey?.stops || item?.stops || [];
          if (Array.isArray(allStops) && allStops.length) {
            routeFrom = String(allStops[0]?.name || "").trim();
            routeTo = String(allStops[allStops.length - 1]?.name || "").trim();
            const m0 = String(allStops[0]?.time || "").match(/T?(\d{2}):(\d{2})/);
            if (m0) originStopTime = `${m0[1]}:${m0[2]}`;
          }
        }
        // Servis kimligi: code "1212852-1376-1387" -> "1212852". Ana sefer eslestirmesi icin. (Fiyat mantigi kullanmaz.)
        const serviceId = String(item?.journey?.code || item?.code || "").split("-")[0].trim();
        capturedJourneys.push({ operator, time: tm[0], price, departureStop: depStop, arrivalStop: arrStop, routeFrom, routeTo, originStopTime, serviceId, totalSeats, availableSeats, seatInfoReliable, hasSeatInfoRaw: hasSeatInfo, shouldZeroRaw: shouldZeroSeats, matchKey: key });
        parsed++;
      }
      if (list.length > 0) {
        console.log(`[oBilet Puppeteer XHR] parse: ${parsed}, atlandi: op=${skipped_op} time=${skipped_time} price=${skipped_price}`);
        if (DEBUG_OBILET_PRICE && capturedJourneys.length > 0) {
          capturedJourneys.slice(-Math.min(parsed, 5)).forEach(j =>
            console.log(`  -> ${j.operator} ${j.time}: ${j.price} TL`)
          );
        }
      }
    } catch (e) {
      if (DEBUG_OBILET_PRICE) console.log(`[oBilet Puppeteer XHR] handler hatasi: ${e.message}`);
    }
  });

  try {
    // ONCE ANA SAYFA: Cloudflare cookie'lerini insan benzeri akisla otur. Direkt /seferler/'e gitmek
    // bot pattern'i — kullanici tarayicida her zaman once ana sayfayi acar.
    await gotoAndHandleCloudflare(page, "https://www.obilet.com/");
    // Kullanici davranisi simulasyonu: kucuk bir bekleme (insan ana sayfayi inceler)
    await new Promise(r => setTimeout(r, 1500 + Math.random() * 1000));

    // Asil hedef sayfaya navigate et — ayni page, dolayisiyla cookie/session devam ediyor
    await gotoAndHandleCloudflare(page, url);

    // Fiyat etiketinin (Schema.org veya text) gelmesini bekle — daha uzun timeout
    await page.waitForFunction(
      () => /\d+\s*(TL|₺)/i.test(document.body?.innerText || ""),
      { timeout: 40000 }
    ).catch(() => {
      console.log("[oBilet Puppeteer] /seferler/ fiyat beklemesi zaman asti.");
    });

    // XHR'larin tamamlanmasi icin ekstra bekleme (JS challenge bazen geride kaliyor)
    await new Promise(r => setTimeout(r, 3000));

    if (capturedJourneys.length > 0) {
      console.log(`[oBilet Puppeteer] ${capturedJourneys.length} sefer (XHR)`);
      // EK ARAC tespiti icin: her operator+saat kacinci arac (idsByKey ayni anahtarda tum listeleme id'lerini
      // topluyor). busCount = o saatte kac ayri otobus. Yalnizca yapi-diff icin ek alan; fiyat mantigini etkilemez.
      capturedJourneys.forEach((cj) => {
        const ids = idsByKey.get(cj.matchKey);
        cj.busCount = (ids && ids.length) ? ids.length : 1;
      });
      // GERCEK DOLULUK (opsiyonel): takip edilen operatorlerin seferleri icin koltuk haritasindan
      // gercek dolu say. Tamamen izole — hata olsa bile fiyat/sefer verisini etkilemez.
      if (seatOperators && seatOperators.length) {
        try { await attachRealOccupancy(page, capturedJourneys, idsByKey, seatOperators, dateIso); }
        catch (e) { if (DEBUG_OBILET_PRICE) console.warn(`[Doluluk] gercek koltuk cekme hatasi: ${e.message}`); }
      }
      return capturedJourneys;
    }

    // DOM parse — /seferler/ sayfasinda fiyatlar net (1.100 TL gibi)
    const domJourneys = await page.evaluate(() => {
      const results = [];
      const seen = new Set();
      const parseNumber = (raw) => {
        const n = parseFloat(String(raw || "").replace(/\./g, "").replace(",", ".").replace(/[^0-9.]/g, ""));
        return Number.isFinite(n) ? Math.round(n) : 0;
      };
      const isStruck = (el) => {
        if (!el) return false;
        if (el.closest("del, s, strike")) return true;
        try {
          if (window.getComputedStyle(el).textDecorationLine.includes("line-through")) return true;
        } catch {}
        return false;
      };

      const cards = document.querySelectorAll("li[itemprop='busTrip'], .journeys li, .journey-list li, .journey-item");
      for (const card of cards) {
        const op = card.querySelector("[itemprop='provider'] meta[itemprop='name']")?.getAttribute("content") ||
          card.querySelector("[itemprop='provider'] [itemprop='name']")?.textContent?.trim() ||
          card.querySelector("img[alt]")?.getAttribute("alt") || "";
        const timeText = card.querySelector("[itemprop='departureTime']")?.getAttribute("content") ||
          card.querySelector("[itemprop='departureTime']")?.textContent ||
          card.querySelector(".departure-time, .time")?.textContent || "";
        const tm = String(timeText).match(/([01]\d|2[0-3]):[0-5]\d/);
        if (!op || !tm) continue;

        let price = 0;

        // 1) Schema.org itemprop=price (cizik degilse)
        for (const node of card.querySelectorAll("[itemprop='price']")) {
          if (isStruck(node)) continue;
          const c = parseNumber(node.getAttribute("content") || node.textContent);
          if (c >= 100 && c <= 10000) { price = c; break; }
        }

        // 2) .price, .amount tipindeki cizilmemis elemanlar — EN DUSUK
        if (!price) {
          const candidates = [];
          card.querySelectorAll(".amount, .price, .ticket-price, .fare, [class*='price']").forEach(node => {
            if (isStruck(node)) return;
            const c = parseNumber(node.textContent);
            if (c >= 100 && c <= 10000) candidates.push(c);
          });
          if (candidates.length) price = Math.min(...candidates);
        }

        // 3) Son care: kartin tum textContent'inden cizilmemis TL'leri bul
        if (!price) {
          const struckSet = new Set();
          card.querySelectorAll("del, s, strike, [style*='line-through']").forEach(el => {
            struckSet.add((el.textContent || "").trim());
          });
          const matches = [...(card.textContent || "").matchAll(/(\d{1,3}(?:\.\d{3})*|\d+)\s*(?:TL|₺)/gi)]
            .map(m => ({ raw: m[0], val: parseNumber(m[1]) }))
            .filter(x => x.val >= 100 && x.val <= 10000)
            .filter(x => ![...struckSet].some(s => s.includes(x.raw)));
          if (matches.length) price = Math.min(...matches.map(x => x.val));
        }

        if (!price) continue;

        const depStop = card.querySelector("[itemprop='departureBusStop'] [itemprop='name']")?.textContent?.trim() ||
          card.querySelector("[itemprop='departureBusStop']")?.textContent?.trim() || "";
        const arrStop = card.querySelector("[itemprop='arrivalBusStop'] [itemprop='name']")?.textContent?.trim() ||
          card.querySelector("[itemprop='arrivalBusStop']")?.textContent?.trim() || "";

        const key = `${op}|${tm[0]}`;
        if (seen.has(key)) continue;
        seen.add(key);
        results.push({ operator: op, time: tm[0], price, departureStop: depStop, arrivalStop: arrStop });
      }
      return results;
    }).catch(() => []);

    if (domJourneys.length > 0) {
      console.log(`[oBilet Puppeteer] /seferler/'den DOM: ${domJourneys.length} sefer`);
      if (DEBUG_OBILET_PRICE) {
        domJourneys.slice(0, 8).forEach(j =>
          console.log(`  -> ${j.operator} ${j.time}: ${j.price} TL`)
        );
      }
      return domJourneys;
    }

    console.log(`[oBilet Puppeteer] /seferler/ sayfasinda sefer bulunamadi.`);
    return [];
  } catch (e) {
    console.error(`[oBilet Puppeteer] /seferler/ hatasi: ${e.message}`);
    return [];
  } finally {
    await page.close().catch(() => {});
  }
}

// oBilet Puppeteer Scraper (Node.js fetch'i api.obilet.com'a cikamadiginda kullanilir).
// Akis:
//  1. Browser context'inden direkt oBilet API'sini cagir (en kesin/hizli yol — Cloudflare bypass cookies ile).
//  2. Olmazsa /seferler/{routeId}/{date} sayfasini kazi (DOM'dan internet-price oku).
async function scrapeObiletViaPuppeteer(origin, destination, dateIso) {
  const browser = await getBrowserInstance();

  // BIRINCIL: Browser context-icinden API
  const apiJourneys = await fetchObiletJourneysViaBrowser(browser, origin, destination, dateIso);
  if (apiJourneys.length > 0) return apiJourneys;

  // IKINCIL: /seferler/ sayfasi kazima (route ID cache'le)
  const cacheKey = `${slugTr(origin)}|${slugTr(destination)}`;
  let routeId = obiletRouteIdCache.get(cacheKey);

  // Browser API'den originId-destId ogrendiysek route ID'sini ondan kur
  // (fetchObiletJourneysViaBrowser hata dondururken bile bazen ID'leri bulmus olabilir,
  // ama mevcut akista geri donmuyor — yeni implementasyona donus icin acik birak)

  if (!routeId) {
    console.log(`[oBilet Puppeteer] Browser API basarisiz ve route ID cache'de yok — veri yok.`);
    return [];
  }

  const journeys = await scrapeObiletSeferlerPage(browser, routeId, dateIso);
  if (journeys.length === 0) {
    obiletRouteIdCache.delete(cacheKey);
  }
  return journeys;
}

// Nodemailer ile E-posta Gönderim Servisi
async function sendPriceChangeEmail(emailList, target, changes) {
  const { smtpUser } = createSmtpTransportsWithFallback();

  const smtpFrom = process.env.SMTP_FROM || `"oBilet Fiyat Takip" <${smtpUser}>`;

  const changeRows = changes.map(c => {
    const isDrop = c.newPrice < c.oldPrice;
    const direction = isDrop ? "Fiyat DÜŞTÜ" : "Fiyat YÜKSELDİ";
    // Firma perspektifi: rakip artisi = YESIL (iyi), rakip dususu = KIRMIZI (rekabet baskisi)
    const directionColor = isDrop ? "#c0392b" : "#27ae60";
    return `
      <tr style="border-bottom: 1px solid #eaeaea;">
        <td style="padding: 12px; font-weight: bold; color: #2c3e50; font-family: sans-serif;">${c.operator}</td>
        <td style="padding: 12px; color: #7f8c8d; font-family: sans-serif; text-align: center;">${c.departure_time}</td>
        <td style="padding: 12px; color: #7f8c8d; font-family: sans-serif; text-decoration: line-through; text-align: center;">${c.oldPrice} TL</td>
        <td style="padding: 12px; font-weight: bold; color: ${directionColor}; font-family: sans-serif; text-align: center;">${c.newPrice} TL</td>
        <td style="padding: 12px; font-weight: bold; color: ${directionColor}; font-family: sans-serif; text-align: center;">${direction}</td>
      </tr>
    `;
  }).join("");

  const formattedDate = target.date.split("-").reverse().join("."); // YYYY-MM-DD -> DD.MM.YYYY

  const htmlContent = `
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 650px; margin: 0 auto; border: 1px solid #e0e0e0; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.05);">
      <!-- Header -->
      <div style="background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); padding: 25px; text-align: center; color: white;">
        <h1 style="margin: 0; font-size: 22px; font-weight: 600; letter-spacing: 0.5px; font-family: sans-serif;">oBilet Fiyat Değişiklik Bildirimi</h1>
        <p style="margin: 5px 0 0 0; opacity: 0.9; font-size: 14px; font-family: sans-serif;">Kamil Koç Fiyat Takip Servisi</p>
      </div>
      
      <!-- Content -->
      <div style="padding: 30px; background-color: #ffffff;">
        <div style="background-color: #f8f9fa; border-left: 4px solid #2a5298; padding: 15px; border-radius: 4px; margin-bottom: 25px;">
          <h3 style="margin: 0 0 5px 0; color: #2c3e50; font-size: 15px; font-family: sans-serif;">Güzergah Bilgileri</h3>
          <p style="margin: 0; color: #555555; font-size: 14px; font-family: sans-serif; line-height: 1.5;">
            <strong>Hat:</strong> ${target.origin.toUpperCase()} - ${target.destination.toUpperCase()} <br/>
            <strong>Tarih:</strong> ${formattedDate}
          </p>
        </div>
        
        <h4 style="margin: 0 0 12px 0; color: #2c3e50; font-size: 14px; font-family: sans-serif; border-bottom: 2px solid #2a5298; padding-bottom: 6px;">Fiyat Değişiklik Detayları</h4>
        <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px;">
          <thead>
            <tr style="background-color: #f1f3f5; color: #495057; border-bottom: 2px solid #ddd;">
              <th style="padding: 12px; font-family: sans-serif;">Firma</th>
              <th style="padding: 12px; font-family: sans-serif; text-align: center;">Saat</th>
              <th style="padding: 12px; font-family: sans-serif; text-align: center;">Eski Fiyat</th>
              <th style="padding: 12px; font-family: sans-serif; text-align: center;">Yeni Fiyat</th>
              <th style="padding: 12px; font-family: sans-serif; text-align: center;">Durum</th>
            </tr>
          </thead>
          <tbody>
            ${changeRows}
          </tbody>
        </table>
        
        <div style="margin-top: 30px; text-align: center;">
          <a href="http://localhost:3000" style="background-color: #2a5298; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 13px; display: inline-block; box-shadow: 0 2px 5px rgba(0,0,0,0.1); font-family: sans-serif;">
            Yönetim Paneline Git
          </a>
        </div>
        ${renderEmailSignature()}
      </div>
      
      <!-- Footer -->
      <div style="background-color: #f1f3f5; padding: 15px; text-align: center; color: #7f8c8d; font-size: 11px; border-top: 1px solid #e0e0e0; font-family: sans-serif; line-height: 1.4;">
        Bu e-posta otobüs fiyat yönetim paneli tarafından otomatik olarak gönderilmiştir. <br/>
        © 2026 Kamil Koç Fiyat Takip Sistemi. Tüm Hakları Saklıdır.
      </div>
    </div>
  `;

  const mailOptions = {
    from: smtpFrom,
    to: emailList,
    subject: buildObiletSubject(OBILET_SUBJECT_PRICE_ALERT, target, formattedDate),
    html: htmlContent
  };

  const { info, smtpPort } = await sendMailWithSmtpFallback(mailOptions);
  console.log(`[E-posta] SMTP portu kullanildi: ${smtpPort}`);
  console.log(`[E-posta] Basariyla gonderildi: ${info.messageId}`);
  return info;
}

// ============================================================
// Telegram Bildirim Botu
// ============================================================
const tgHttps = require("https");

// HTML ozel karakterlerini kacir (parse_mode=HTML icin).
function tgEscape(s) {
  return String(s == null ? "" : s).replace(/[&<>]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c])
  );
}

// YYYY-MM-DD -> DD.MM.YYYY (eslesmiyorsa oldugu gibi birakir).
function tgDateDot(s) {
  const m = String(s || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[3]}.${m[2]}.${m[1]}` : String(s || "");
}

// Cerceveli (Excel hucresi gibi) monospace tablo (<pre> icinde).
// cols: [{key,label,align:'l'|'r',max?}]. opts.compact: kolon ici bosluk yok (dar, telefona sigsin).
function tgTable(cols, rows, opts = {}) {
  const padN = opts.compact ? 0 : 1;
  const pad = " ".repeat(padN);
  const widths = cols.map((c) => {
    let w = c.label.length;
    for (const r of rows) w = Math.max(w, String(r[c.key] == null ? "" : r[c.key]).length);
    return c.max ? Math.min(w, c.max) : w;
  });
  const cell = (val, i) => {
    let s = String(val == null ? "" : val);
    const w = widths[i];
    if (s.length > w) s = s.slice(0, w);
    return cols[i].align === "r" ? s.padStart(w) : s.padEnd(w);
  };
  const border = (l, m, r) => l + widths.map((w) => "─".repeat(w + padN * 2)).join(m) + r;
  const rowLine = (vals) => "│" + vals.map((v, i) => pad + cell(v, i) + pad).join("│") + "│";
  const top = border("┌", "┬", "┐");
  const head = rowLine(cols.map((c) => c.label));
  const mid = border("├", "┼", "┤");
  const bottom = border("└", "┴", "┘");
  // Dis cerceve + baslik cizgisi (satir arasi cizgi yok — 50 satir tek mesaja sigsin).
  const body = rows.map((r) => rowLine(cols.map((c) => r[c.key])));
  return "<pre>" + tgEscape([top, head, mid, ...body, bottom].join("\n")) + "</pre>";
}

// YYYY-MM-DD -> DD.MM.YY (kisa ama yilli).
function tgDate2(s) {
  const m = String(s || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[3]}.${m[2]}.${m[1].slice(2)}` : String(s || "");
}

// Degisiklik/sefer satirlarini DOLULUK ile zenginlestir (obilet_occupancy tablosundan).
// Satirlarda target_id, operator, journey_date, departure_time olmali.
function tgAttachOccupancy(rows) {
  try {
    if (!rows || !rows.length) return rows;
    const occ = db.prepare("SELECT target_id, operator, journey_date, departure_time, total_seats, available_seats, plate FROM obilet_occupancy").all();
    if (!occ.length) return rows;
    const map = new Map();
    for (const o of occ) map.set(`${o.target_id}|${o.operator}|${o.journey_date}|${o.departure_time}`, o);
    for (const r of rows) {
      const m = map.get(`${r.target_id}|${r.operator}|${r.journey_date}|${r.departure_time}`);
      if (m) { r.total_seats = m.total_seats; r.available_seats = m.available_seats; r.plate = m.plate; }
    }
  } catch (e) { /* yok say */ }
  return rows;
}

// Degisiklikleri Hat+Firma'ya gore gruplar; her grup icinde sefer tarih+saatine gore siralar.
// Satirlarda doluluk varsa (total_seats) "Boş/Top" kolonu eklenir.
function tgChangesGrouped(rows) {
  const groups = new Map();
  let order = 0;
  for (const r of rows) {
    const route = `${(r.origin || "").toUpperCase()}>${(r.destination || "").toUpperCase()}`;
    const key = route + "|" + (r.operator || "");
    if (!groups.has(key)) {
      groups.set(key, { route, operator: r.operator || "", order: order++, items: [] });
    }
    groups.get(key).items.push(r);
  }
  // Gruplar: en son degisen grup ustte (rows zaten id DESC geldigi icin order = ilk gorulme).
  const ordered = [...groups.values()].sort((a, b) => a.order - b.order);
  const MAX_ROWS = 60; // 50 satirlik liste tek blokta kalsin (satir arasi cizgi yok)
  const blocks = [];
  for (const g of ordered) {
    // Grup ici: sefer tarih+saatine gore artan sira.
    g.items.sort((a, b) => {
      const ka = `${a.journey_date || ""} ${a.departure_time || ""}`;
      const kb = `${b.journey_date || ""} ${b.departure_time || ""}`;
      return ka < kb ? -1 : ka > kb ? 1 : 0;
    });
    const hasOcc = g.items.some((r) => r.total_seats != null);
    const cols = [
      { key: "sefer", label: "Sefer", align: "l", max: 11 },
      { key: "eski", label: "Eski", align: "r" },
      { key: "yeni", label: "Yeni", align: "r" },
      { key: "fark", label: "Fark", align: "r" },
    ];
    if (hasOcc) cols.push({ key: "dol", label: "Dolu", align: "r" });
    // Plaka: atanmis otobus plakasi; atanmamis (ileri tarih) seferde "NULL". Her zaman gosterilir.
    cols.push({ key: "plaka", label: "Plaka", align: "l", max: 11 });

    // Sefer: yil olmadan "DD.MM HH:MM" (yer kazanir, telefona sigsin).
    const dm = (s) => { const m = String(s || "").match(/^(\d{4})-(\d{2})-(\d{2})$/); return m ? `${m[3]}.${m[2]}` : String(s || ""); };
    const tableRows = g.items.map((r) => {
      const diff = r.new_price - r.old_price;
      const row = {
        sefer: `${dm(r.journey_date)}${r.departure_time ? " " + r.departure_time : ""}`.trim(),
        eski: String(r.old_price),
        yeni: String(r.new_price),
        fark: (diff > 0 ? "+" : "") + diff,
      };
      // Dolu = araçtaki yolcu (total - available). "38/41" => 41 koltukta 38 dolu.
      if (hasOcc) {
        if (r.total_seats != null && r.available_seats != null) {
          row.dol = `${Math.max(0, r.total_seats - r.available_seats)}/${r.total_seats}`;
        } else { row.dol = "—"; }
      }
      // Plaka: varsa yaz, yoksa NULL (istenen davranis).
      row.plaka = (r.plate && String(r.plate).trim()) ? String(r.plate).trim() : "NULL";
      return row;
    });
    // Grup ortalama dolulugu — basliga kalin yazilir (tablo icinde kalin mumkun degil).
    let occLabel = "";
    if (hasOcc) {
      const wo = g.items.filter((r) => r.total_seats != null && r.available_seats != null && r.total_seats > 0);
      if (wo.length) {
        const avg = Math.round(wo.reduce((s, r) => s + ((r.total_seats - r.available_seats) / r.total_seats) * 100, 0) / wo.length);
        occLabel = ` · <b>%${avg} dolu</b>`;
      }
    }
    const partCount = Math.ceil(tableRows.length / MAX_ROWS);
    for (let i = 0; i < tableRows.length; i += MAX_ROWS) {
      const slice = tableRows.slice(i, i + MAX_ROWS);
      const partLabel = partCount > 1 ? ` (${Math.floor(i / MAX_ROWS) + 1}/${partCount})` : "";
      const header = `<b>${tgEscape(g.route)}</b> · ${tgEscape(g.operator)}${partLabel}${occLabel}`;
      blocks.push(header + "\n" + tgTable(cols, slice, { compact: true }));
    }
  }
  return blocks.join("\n\n");
}

// Telegram'a tek bir mesaj gonderir. Hata olsa bile uygulamayi dusurmez (resolve(false)).
function sendTelegramMessage(chatId, htmlText) {
  return new Promise((resolve) => {
    if (!TELEGRAM_ENABLED || !chatId) { resolve(false); return; }
    const payload = JSON.stringify({
      chat_id: String(chatId),
      parse_mode: "HTML",
      disable_web_page_preview: true,
      text: htmlText,
    });
    const req = tgHttps.request(
      {
        hostname: "api.telegram.org",
        path: `/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload),
        },
        timeout: 15000,
      },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          if (res.statusCode === 200) {
            resolve(true);
          } else {
            console.warn(`[Telegram] Gonderim hatasi (${res.statusCode}): ${body}`);
            resolve(false);
          }
        });
      }
    );
    req.on("error", (e) => { console.warn(`[Telegram] Baglanti hatasi: ${e.message}`); resolve(false); });
    req.on("timeout", () => { req.destroy(); console.warn("[Telegram] Zaman asimi."); resolve(false); });
    req.write(payload);
    req.end();
  });
}

// Uzun metni Telegram limitine gore birden fazla mesaja bolup sirayla gonderir.
// Blok (\n\n) sinirlarindan boler; tgChangesGrouped her blogu limit altinda tuttugu icin
// hicbir <pre> tablosu ortadan bolunmez.
async function sendTelegramLong(chatId, text, limit = 4090) {
  if (!text) return;
  if (text.length <= limit) { await sendTelegramMessage(chatId, text); return; }
  const blocks = String(text).split("\n\n");
  let buf = "";
  for (const block of blocks) {
    const piece = buf ? buf + "\n\n" + block : block;
    if (piece.length > limit && buf) {
      await sendTelegramMessage(chatId, buf);
      buf = block;
    } else {
      buf = piece;
    }
  }
  if (buf) await sendTelegramMessage(chatId, buf);
}

// Bir hattaki fiyat degisikliklerini Telegram'a bildirir.
// Hedefe ozel telegram_chat_ids varsa onlar, yoksa global TELEGRAM_CHAT_ID kullanilir.
async function sendObiletTelegramAlert(target, changes) {
  if (!TELEGRAM_ENABLED || !changes || changes.length === 0) return;
  // Alicilar: adminler + onayli kullanicilar + (varsa) hatta tanimli ID'ler.
  const chatIds = telegramNotifyChatIds(target);
  if (chatIds.length === 0) return;

  // changes camelCase (oldPrice/newPrice) -> tgChangesGrouped'in bekledigi sekle cevir.
  const rows = changes.map((c) => ({
    target_id: target.id,
    origin: target.origin,
    destination: target.destination,
    journey_date: c.journey_date,
    operator: c.operator,
    departure_time: c.departure_time,
    old_price: c.oldPrice,
    new_price: c.newPrice,
  }));
  tgAttachOccupancy(rows); // her degisikligin yanina bos/dolu koltuk
  const text =
    `<b>Fiyat Değişikliği</b> · ${changes.length} değişim\n\n` +
    tgChangesGrouped(rows);

  for (const chatId of chatIds) {
    await sendTelegramLong(chatId, text);
  }
}

// ---- Komutlu bot (long polling) ----

// Genel POST helper (setMyCommands gibi cagrilar icin). Parse edilmis cevabi doner.
function telegramPost(method, obj) {
  return new Promise((resolve) => {
    if (!TELEGRAM_ENABLED) { resolve(null); return; }
    const payload = JSON.stringify(obj || {});
    const req = tgHttps.request(
      {
        hostname: "api.telegram.org",
        path: `/bot${TELEGRAM_BOT_TOKEN}/${method}`,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) },
        timeout: 15000,
      },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => { try { resolve(JSON.parse(body)); } catch { resolve(null); } });
      }
    );
    req.on("error", () => resolve(null));
    req.on("timeout", () => { req.destroy(); resolve(null); });
    req.write(payload);
    req.end();
  });
}

// getUpdates icin GET helper (uzun bekleme destekli).
function telegramGetUpdates(offset, timeoutSec) {
  return new Promise((resolve) => {
    if (!TELEGRAM_ENABLED) { resolve(null); return; }
    const path = `/bot${TELEGRAM_BOT_TOKEN}/getUpdates?timeout=${timeoutSec}&offset=${offset}`;
    const req = tgHttps.request(
      { hostname: "api.telegram.org", path, method: "GET", timeout: (timeoutSec + 10) * 1000 },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => { try { resolve(JSON.parse(body)); } catch { resolve(null); } });
      }
    );
    req.on("error", () => resolve(null));
    req.on("timeout", () => { req.destroy(); resolve(null); });
    req.end();
  });
}

// ---- Kullanici yonetimi (self-servis + admin onayi) ----
// Admin = env TELEGRAM_CHAT_ID'deki ID'ler. Her zaman tam yetkili + onaylama yapabilir.
function telegramIsAdmin(chatId) {
  return TELEGRAM_DEFAULT_CHAT_IDS.map(String).includes(String(chatId));
}
function telegramGetUser(chatId) {
  try { return db.prepare("SELECT * FROM telegram_users WHERE chat_id = ?").get(String(chatId)); }
  catch { return null; }
}
// Komut kullanabilir mi? Admin veya status='approved'.
function telegramCanUse(chatId) {
  if (telegramIsAdmin(chatId)) return true;
  const u = telegramGetUser(chatId);
  return !!(u && u.status === "approved");
}
// Yeni kullaniciyi 'pending' olarak kaydet (varsa dokunma).
function telegramRegisterPending(chat, from) {
  try {
    db.prepare(
      "INSERT OR IGNORE INTO telegram_users (chat_id, first_name, last_name, username, status, requested_at) VALUES (?, ?, ?, ?, 'pending', ?)"
    ).run(String(chat.id), from.first_name || "", from.last_name || "", from.username || "", nowStamp());
  } catch (e) { console.warn(`[Telegram] pending kayit hatasi: ${e.message}`); }
}
// Bildirim alicilari: adminler + onayli kullanicilar + (varsa) hatta tanimli ID'ler.
function telegramNotifyChatIds(target) {
  // Adminler her zaman tum bildirimleri alir.
  const set = new Set(TELEGRAM_DEFAULT_CHAT_IDS.map(String));
  try {
    // Bu hatta ABONE olan onayli kullanicilar.
    if (target && target.id != null) {
      const rows = db.prepare("SELECT chat_id FROM telegram_subscriptions WHERE target_id = ?").all(target.id);
      for (const r of rows) {
        const cid = String(r.chat_id);
        if (telegramIsAdmin(cid)) { set.add(cid); continue; }
        const u = telegramGetUser(cid);
        if (u && u.status === "approved") set.add(cid);
      }
    }
  } catch { /* yok say */ }
  if (target && target.telegram_chat_ids) {
    for (const id of parseCsvList(target.telegram_chat_ids)) set.add(String(id));
  }
  return [...set];
}
// Abonelik klavyesi: her hat icin (abone) / (degil) butonu.
function tgBuildAboneKeyboard(chatId) {
  const routes = db.prepare("SELECT id, origin, destination FROM obilet_targets ORDER BY origin, destination").all();
  const subs = new Set(
    db.prepare("SELECT target_id FROM telegram_subscriptions WHERE chat_id = ?").all(String(chatId)).map((r) => r.target_id)
  );
  return {
    inline_keyboard: routes.map((r) => [{
      text: `${subs.has(r.id) ? "" : ""} ${(r.origin || "").toUpperCase()} - ${(r.destination || "").toUpperCase()}`,
      callback_data: "sub:" + r.id,
    }]),
  };
}
function tgFullName(u) {
  return `${(u.first_name || "")} ${(u.last_name || "")}`.trim() || "(isimsiz)";
}

// ---- Komut cevaplari ----
function tgCmdYardim(isAdmin) {
  let t =
    "<b>KK oBilet Fiyat Takip Botu</b>\n\n" +
    "Kullanabileceğin komutlar:\n\n" +
    "/durum — Sistem özeti (kaç hat, son değişiklik)\n" +
    "/takip — Toplam izlenen sefer (hat bazında)\n" +
    "/hatlar — Takip edilen hatların listesi\n" +
    "/son — Son 50 fiyat değişikliği\n" +
    "/dusenler — Sadece son fiyat düşüşleri\n" +
    "/firma — Firmaya göre değişiklikler (listeden seç)\n" +
    "/fiyatlar — Hat bazında güncel fiyat aralığı\n" +
    "/hat — Hata göre değişiklikler (listeden seç)\n" +
    "/guncel — Hattın değişmeyen fiyatları (listeden seç)\n" +
    "/sefer — Firmanın sefer bazlı fiyat değişim geçmişi + doluluk (firma seç)\n" +
    "/abone — Bildirim almak istediğin hatları seç\n" +
    "/aboneliklerim — Abone olduğun hatlar\n" +
    "/yardim — Bu menü";
  if (isAdmin) {
    t +=
      "\n\n<b>Yönetici komutları:</b>\n" +
      "/bekleyenler — Onay bekleyen kullanıcılar\n" +
      "/kullanicilar — Onaylı kullanıcılar\n" +
      "/onayla &lt;id&gt; — Kullanıcıyı onayla\n" +
      "/engelle &lt;id&gt; — Kullanıcıyı engelle";
  }
  return t;
}

// Onay bekleyenleri listele (admin).
function tgListPending() {
  const rows = db.prepare("SELECT chat_id, first_name, last_name, username, requested_at FROM telegram_users WHERE status = 'pending' ORDER BY requested_at").all();
  if (!rows.length) return "Onay bekleyen kullanıcı yok.";
  return "<b>Onay Bekleyenler</b>\n\n" + rows.map((r) => {
    const uname = r.username ? ` @${tgEscape(r.username)}` : "";
    return `${tgEscape(tgFullName(r))}${uname}\n<code>${tgEscape(r.chat_id)}</code>\n   /onayla ${tgEscape(r.chat_id)}`;
  }).join("\n\n");
}

// Onayli kullanicilari listele (admin).
function tgListApproved() {
  const rows = db.prepare("SELECT chat_id, first_name, last_name, username FROM telegram_users WHERE status = 'approved' ORDER BY approved_at").all();
  const adminCount = TELEGRAM_DEFAULT_CHAT_IDS.length;
  let t = `<b>Onaylı Kullanıcılar (${rows.length} + ${adminCount} yönetici)</b>\n\n`;
  if (rows.length) {
    t += rows.map((r) => {
      const uname = r.username ? ` @${tgEscape(r.username)}` : "";
      return `${tgEscape(tgFullName(r))}${uname} — <code>${tgEscape(r.chat_id)}</code>`;
    }).join("\n");
  } else {
    t += "(Henüz onaylı kullanıcı yok, sadece yöneticiler.)";
  }
  return t;
}

// Admin: kullaniciyi onayla. Telegram'a bildirim de gonderir.
async function tgAdminApprove(adminChatId, targetIdRaw) {
  const targetId = String(targetIdRaw || "").trim();
  if (!targetId) return sendTelegramMessage(adminChatId, "Kullanım: /onayla &lt;chat_id&gt;");
  const u = telegramGetUser(targetId);
  if (!u) return sendTelegramMessage(adminChatId, `Kayıt bulunamadı: ${tgEscape(targetId)}`);
  db.prepare("UPDATE telegram_users SET status = 'approved', approved_at = ?, approved_by = ? WHERE chat_id = ?")
    .run(nowStamp(), String(adminChatId), targetId);
  await sendTelegramMessage(adminChatId, `Onaylandı: ${tgEscape(tgFullName(u))} (${tgEscape(targetId)})`);
  await sendTelegramMessage(targetId,
    "<b>Erişimin onaylandı!</b>\n\n" +
    "Şimdi hangi hatların fiyat değişikliklerinde bildirim almak istediğini seç. <b>Seçmezsen bildirim gelmez.</b>\n\n" +
    "Aşağıdaki listeden hatlara dokun (açık / kapalı). İstediğin zaman /abone ile değiştirebilirsin. Komutlar: /yardim");
  // Onay sonrasi dogrudan abonelik klavyesini gonder — kisi elle yazmadan secsin.
  await telegramPost("sendMessage", {
    chat_id: String(targetId),
    parse_mode: "HTML",
    text: "<b>Bildirim almak istediğin hatları seç:</b>",
    reply_markup: tgBuildAboneKeyboard(targetId),
  });
}

// Admin: kullaniciyi engelle.
async function tgAdminBlock(adminChatId, targetIdRaw) {
  const targetId = String(targetIdRaw || "").trim();
  if (!targetId) return sendTelegramMessage(adminChatId, "Kullanım: /engelle &lt;chat_id&gt;");
  const u = telegramGetUser(targetId);
  if (!u) {
    // Kaydi yoksa engelli olarak ekle ki bir daha yazinca pending olmasin.
    db.prepare("INSERT OR REPLACE INTO telegram_users (chat_id, status, requested_at) VALUES (?, 'blocked', ?)").run(targetId, nowStamp());
  } else {
    db.prepare("UPDATE telegram_users SET status = 'blocked' WHERE chat_id = ?").run(targetId);
  }
  await sendTelegramMessage(adminChatId, `Engellendi: ${tgEscape(targetId)}`);
}

// Admin: /duyuru <metin> — mesaji TUM onayli kullanicilara (+ adminlere) gonderir.
async function tgAdminBroadcast(adminChatId, fullText) {
  const text = String(fullText || "").replace(/^\/duyuru(@\S+)?\s*/i, "").trim();
  if (!text) {
    return sendTelegramMessage(adminChatId,
      "Kullanım: <b>/duyuru</b> sonrasına mesajını yaz.\nÖrnek:\n/duyuru Merhaba, bildirim sistemi değişti...\n\n(Mesaj tüm onaylı kullanıcılara gider.)");
  }
  // Alicilar: adminler + onayli kullanicilar (engelliler haric).
  const recipients = new Set(TELEGRAM_DEFAULT_CHAT_IDS.map(String));
  try {
    const rows = db.prepare("SELECT chat_id FROM telegram_users WHERE status = 'approved'").all();
    for (const r of rows) recipients.add(String(r.chat_id));
  } catch { /* yok say */ }

  let sent = 0, failed = 0;
  for (const cid of recipients) {
    const ok = await sendTelegramMessage(cid, text);
    if (ok) sent++; else failed++;
    await new Promise((r) => setTimeout(r, 50)); // Telegram rate-limit dostu
  }
  await sendTelegramMessage(adminChatId, `Duyuru gönderildi: <b>${sent}</b> kişi${failed ? ` (${failed} başarısız)` : ""}.`);
}

// Yeni talebi adminlere butonlu mesajla bildir.
function tgNotifyAdminsNewRequest(chat, from) {
  const name = `${from.first_name || ""} ${from.last_name || ""}`.trim() || "(isimsiz)";
  const uname = from.username ? `@${from.username}` : "(kullanıcı adı yok)";
  const text =
    "<b>Yeni erişim talebi</b>\n\n" +
    `${tgEscape(name)} ${tgEscape(uname)}\n` +
    `<code>${tgEscape(String(chat.id))}</code>\n\n` +
    "Bu kişi botu kullanmak istiyor. Onaylıyor musun?";
  const reply_markup = {
    inline_keyboard: [[
      { text: "Onayla", callback_data: `approve:${chat.id}` },
      { text: "Reddet", callback_data: `block:${chat.id}` },
    ]],
  };
  for (const adminId of TELEGRAM_DEFAULT_CHAT_IDS) {
    telegramPost("sendMessage", { chat_id: String(adminId), parse_mode: "HTML", text, reply_markup });
  }
}

function tgCmdDurum() {
  try {
    const targets = db.prepare("SELECT COUNT(*) AS c FROM obilet_targets").get().c;
    const prices = db.prepare("SELECT COUNT(*) AS c FROM obilet_prices").get().c;
    const lastRows = db.prepare("SELECT target_id, origin, destination, journey_date, departure_time, operator, old_price, new_price FROM obilet_price_history ORDER BY id DESC LIMIT 10").all();
    tgAttachOccupancy(lastRows);
    const todayPrefix = (() => {
      try { return shiftIsoDate(todayIsoInIstanbul(), 0).split("-").reverse().join("."); } catch { return ""; }
    })();
    let changesToday = 0;
    if (todayPrefix) {
      changesToday = db.prepare("SELECT COUNT(*) AS c FROM obilet_price_history WHERE substr(changed_at,1,10) = ?").get(todayPrefix).c;
    }
    const lastBlock = lastRows.length
      ? tgChangesGrouped(lastRows)
      : "Henüz değişiklik kaydı yok.";
    return (
      "<b>Sistem Durumu</b>\n\n" +
      `Takip edilen hat: <b>${targets}</b>\n` +
      `İzlenen sefer/fiyat: <b>${prices}</b>\n` +
      `Bugünkü değişiklik: <b>${changesToday}</b>\n\n` +
      "<b>Son 10 değişiklik:</b>\n" + lastBlock
    );
  } catch (e) {
    return "Durum alınamadı: " + tgEscape(e.message);
  }
}

function tgCmdHatlar() {
  try {
    const rows = db.prepare("SELECT origin, destination FROM obilet_targets ORDER BY origin, destination").all();
    if (!rows.length) return "Takip edilen hat yok.";
    const lines = rows.map((r, i) => `${i + 1}. ${tgEscape((r.origin || "").toUpperCase())} - ${tgEscape((r.destination || "").toUpperCase())}`);
    return `<b>Takip Edilen Hatlar (${rows.length})</b>\n\n` + lines.join("\n");
  } catch (e) {
    return "Hatlar alınamadı: " + tgEscape(e.message);
  }
}

function tgCmdSon() {
  try {
    const rows = db.prepare(
      "SELECT target_id, origin, destination, journey_date, operator, departure_time, old_price, new_price FROM obilet_price_history ORDER BY id DESC LIMIT 50"
    ).all();
    if (!rows.length) return "Henüz fiyat değişikliği kaydı yok.";
    tgAttachOccupancy(rows);
    return "<b>Son 50 Fiyat Değişikliği</b>\n\n" + tgChangesGrouped(rows);
  } catch (e) {
    return "Kayıtlar alınamadı: " + tgEscape(e.message);
  }
}

function tgCmdFiyatlar() {
  try {
    const rows = db.prepare(`
      SELECT t.origin, t.destination, MIN(p.price) AS min_price, MAX(p.price) AS max_price, COUNT(p.id) AS n
        FROM obilet_targets t
        JOIN obilet_prices p ON p.target_id = t.id
       GROUP BY t.id
       ORDER BY t.origin, t.destination
    `).all();
    if (!rows.length) return "Henüz fiyat verisi yok.";
    const tableRows = rows.map((r) => ({
      hat: `${(r.origin || "").toUpperCase()}>${(r.destination || "").toUpperCase()}`,
      fiyat: r.min_price === r.max_price ? `${r.min_price}` : `${r.min_price}-${r.max_price}`,
      sefer: String(r.n),
    }));
    return "<b>Güncel Fiyatlar</b>\n" + tgTable(
      [
        { key: "hat", label: "Hat", align: "l", max: 16 },
        { key: "fiyat", label: "Fiyat", align: "r" },
        { key: "sefer", label: "Sefer", align: "r" },
      ],
      tableRows
    );
  } catch (e) {
    return "Fiyatlar alınamadı: " + tgEscape(e.message);
  }
}

// Bir hattin (target) fiyat degisikliklerini gosterir — firma bazinda gruplu tablo.
function tgCmdHatChangesByTarget(targetId) {
  try {
    const t = db.prepare("SELECT id, origin, destination FROM obilet_targets WHERE id = ?").get(targetId);
    if (!t) return "Hat bulunamadı.";
    const head = `<b>${tgEscape((t.origin || "").toUpperCase())} - ${tgEscape((t.destination || "").toUpperCase())}</b> — Fiyat Değişiklikleri`;
    const rows = db.prepare(
      "SELECT target_id, origin, destination, journey_date, operator, departure_time, old_price, new_price FROM obilet_price_history WHERE target_id = ? ORDER BY id DESC LIMIT 50"
    ).all(targetId);
    if (!rows.length) return head + "\n\nBu hatta kayıtlı fiyat değişikliği yok.";
    tgAttachOccupancy(rows);
    return `${head} (${rows.length})\n\n` + tgChangesGrouped(rows);
  } catch (e) {
    return "Hat değişiklikleri alınamadı: " + tgEscape(e.message);
  }
}

// /hat <kalkis> <varis> — yazarak hat secimi; eslesirse o hattin degisikliklerini gosterir.
function tgCmdHat(args) {
  try {
    if (!args || args.length < 2) {
      return "Kullanım: <b>/hat &lt;kalkış&gt; &lt;varış&gt;</b>\nÖrnek: /hat ADANA BURSA\n\nveya sadece /hat yazıp listeden seç.";
    }
    const norm = (s) => String(s || "").toLocaleLowerCase("tr-TR").trim();
    const origin = norm(args[0]);
    const destination = norm(args[args.length - 1]);
    const targets = db.prepare("SELECT id, origin, destination FROM obilet_targets").all();
    const t =
      targets.find((x) => norm(x.origin) === origin && norm(x.destination) === destination) ||
      targets.find((x) => norm(x.origin).includes(origin) && norm(x.destination).includes(destination));
    if (!t) {
      return `"${tgEscape(args[0].toUpperCase())} - ${tgEscape(args[args.length - 1].toUpperCase())}" hattı takipte değil.\n/hatlar ile listeyi görebilirsin.`;
    }
    return tgCmdHatChangesByTarget(t.id);
  } catch (e) {
    return "Hat detayı alınamadı: " + tgEscape(e.message);
  }
}

// /hat (argumansiz) — hat butonlarini gonderir, tiklayinca o hattin degisiklikleri gelir.
async function tgSendHatSecimi(chatId) {
  const routes = db.prepare("SELECT id, origin, destination FROM obilet_targets ORDER BY origin, destination").all();
  if (!routes.length) {
    await sendTelegramMessage(chatId, "Takip edilen hat yok.");
    return;
  }
  const inline_keyboard = routes.map((r) => [{
    text: `${(r.origin || "").toUpperCase()} - ${(r.destination || "").toUpperCase()}`,
    callback_data: "h:" + r.id,
  }]);
  await telegramPost("sendMessage", {
    chat_id: String(chatId),
    parse_mode: "HTML",
    text: "<b>Hat seç</b> — değişikliklerini görmek istediğin hatta dokun:",
    reply_markup: { inline_keyboard },
  });
}

// Bir hattin DEGISMEYEN seferleri — fiyati hic degismemis (gecmiste kaydi olmayan) seferler.
// Degisen seferler /hat ve bildirimle zaten geliyor; burada sadece "degismedi" olanlar.
function tgCmdGuncelByTarget(targetId) {
  try {
    const t = db.prepare("SELECT id, origin, destination FROM obilet_targets WHERE id = ?").get(targetId);
    if (!t) return "Hat bulunamadı.";
    const routeUp = `${(t.origin || "").toUpperCase()} - ${(t.destination || "").toUpperCase()}`;
    const head = `<b>${tgEscape(routeUp)}</b> — Değişmeyen Fiyatlar`;

    // Fiyati degismis sefer kimlikleri (gecmiste kaydi olanlar).
    const changedKeys = new Set(
      db.prepare("SELECT DISTINCT operator, journey_date, departure_time FROM obilet_price_history WHERE target_id = ?")
        .all(targetId)
        .map((r) => `${r.operator}|${r.journey_date}|${r.departure_time}`)
    );

    // Tum guncel seferler; degismeyenleri (gecmiste olmayan) ayikla.
    const prices = db.prepare(
      "SELECT operator, journey_date, departure_time, price FROM obilet_prices WHERE target_id = ?"
    ).all(targetId);
    const unchanged = prices.filter(
      (p) => !changedKeys.has(`${p.operator}|${p.journey_date}|${p.departure_time}`)
    );
    if (!unchanged.length) return head + "\n\nBu hatta değişmemiş sefer yok (hepsinin fiyatı değişmiş).";

    // Firma bazinda grupla.
    const groups = new Map();
    for (const p of unchanged) {
      if (!groups.has(p.operator)) groups.set(p.operator, []);
      groups.get(p.operator).push(p);
    }
    const operators = [...groups.keys()].sort((a, b) => String(a).localeCompare(String(b), "tr"));

    const CAP = 50;
    const total = unchanged.length;
    let shown = 0;
    const cols = [
      { key: "sefer", label: "Sefer", align: "l", max: 14 },
      { key: "fiyat", label: "Fiyat", align: "r" },
      { key: "not", label: "Durum", align: "l" },
    ];
    const blocks = [];
    for (const op of operators) {
      if (shown >= CAP) break;
      const items = groups.get(op).sort((a, b) => {
        const ka = `${a.journey_date} ${a.departure_time}`;
        const kb = `${b.journey_date} ${b.departure_time}`;
        return ka < kb ? -1 : ka > kb ? 1 : 0;
      });
      const slice = items.slice(0, CAP - shown);
      shown += slice.length;
      const tableRows = slice.map((p) => ({
        sefer: `${tgDate2(p.journey_date)}${p.departure_time ? " " + p.departure_time : ""}`.trim(),
        fiyat: String(p.price),
        not: "değişmedi",
      }));
      blocks.push(`<b>${tgEscape(op)}</b>\n` + tgTable(cols, tableRows));
    }
    const countLabel = total > CAP ? `${CAP}/${total}` : `${total}`;
    return `${head} (${countLabel} sefer)\n\n` + blocks.join("\n\n");
  } catch (e) {
    return "Değişmeyen fiyatlar alınamadı: " + tgEscape(e.message);
  }
}

// /guncel <kalkis> <varis> — yazarak hat secimi; eslesirse o hattin guncel fiyatlari.
function tgCmdGuncel(args) {
  try {
    if (!args || args.length < 2) {
      return "Kullanım: <b>/guncel &lt;kalkış&gt; &lt;varış&gt;</b>\nÖrnek: /guncel ADANA BURSA\n\nveya sadece /guncel yazıp listeden seç.";
    }
    const norm = (s) => String(s || "").toLocaleLowerCase("tr-TR").trim();
    const origin = norm(args[0]);
    const destination = norm(args[args.length - 1]);
    const targets = db.prepare("SELECT id, origin, destination FROM obilet_targets").all();
    const t =
      targets.find((x) => norm(x.origin) === origin && norm(x.destination) === destination) ||
      targets.find((x) => norm(x.origin).includes(origin) && norm(x.destination).includes(destination));
    if (!t) {
      return `"${tgEscape(args[0].toUpperCase())} - ${tgEscape(args[args.length - 1].toUpperCase())}" hattı takipte değil.\n/hatlar ile listeyi görebilirsin.`;
    }
    return tgCmdGuncelByTarget(t.id);
  } catch (e) {
    return "Güncel fiyatlar alınamadı: " + tgEscape(e.message);
  }
}

// /guncel (argumansiz) — hat butonlarini gonderir, tiklayinca o hattin guncel fiyatlari gelir.
async function tgSendGuncelSecimi(chatId) {
  const routes = db.prepare("SELECT id, origin, destination FROM obilet_targets ORDER BY origin, destination").all();
  if (!routes.length) {
    await sendTelegramMessage(chatId, "Takip edilen hat yok.");
    return;
  }
  const inline_keyboard = routes.map((r) => [{
    text: `${(r.origin || "").toUpperCase()} - ${(r.destination || "").toUpperCase()}`,
    callback_data: "g:" + r.id,
  }]);
  await telegramPost("sendMessage", {
    chat_id: String(chatId),
    parse_mode: "HTML",
    text: "<b>Hat seç</b> — değişmeyen fiyatlarını görmek istediğin hatta dokun:",
    reply_markup: { inline_keyboard },
  });
}

// /sefer — bir FIRMANIN son 3 gundeki tum guzergahlardaki sefer takibi (TABLO).
function tgCmdSeferTakip(firmaQuery) {
  try {
    const norm = (s) => String(s || "").toLocaleLowerCase("tr-TR").trim();
    const q = norm(firmaQuery);
    if (!q) {
      return "Kullanım: <b>/sefer &lt;firma&gt;</b>\nÖrnek: /sefer Enver Geçgel\n\nveya sadece /sefer yazıp listeden seç.";
    }
    const ops = db.prepare("SELECT DISTINCT operator FROM obilet_prices WHERE operator != '' ORDER BY operator").all().map((r) => r.operator);
    const matched = ops.find((o) => norm(o) === q) || ops.find((o) => norm(o).includes(q));
    if (!matched) return `"${tgEscape(firmaQuery)}" için sefer takip verisi yok.`;

    const { journeys } = computeJourneyTracking({ operator: matched });
    if (!journeys.length) return `<b>${tgEscape(matched)}</b>\n\nSon 3 günde fiyat değişikliği yok.`;

    const dm = (s) => { const m = String(s || "").match(/^(\d{4})-(\d{2})-(\d{2})$/); return m ? `${m[3]}.${m[2]}` : String(s || ""); };
    const tableRows = journeys.slice(0, 40).map((j) => ({
      guzergah: `${(j.origin || "").toUpperCase()}>${(j.destination || "").toUpperCase()}`,
      sefer: `${dm(j.journey_date)} ${j.departure_time || ""}`.trim(),
      deg: `${j.changeCount}x`,
      gecmis: (j.prices || []).join("-"),
      guncel: String(j.currentPrice),
      dolu: (j.totalSeats != null && j.yolcu != null) ? `${j.yolcu}/${j.totalSeats}` : "-",
    }));
    const cols = [
      { key: "guzergah", label: "Güzergah", align: "l", max: 15 },
      { key: "sefer", label: "Sefer", align: "l", max: 11 },
      { key: "deg", label: "Değ", align: "r" },
      { key: "gecmis", label: "Fiyat Geçmişi", align: "l" },
      { key: "guncel", label: "Güncel", align: "r" },
      { key: "dolu", label: "Dolu", align: "r" },
    ];
    const MAX = 25;
    const blocks = [];
    for (let i = 0; i < tableRows.length; i += MAX) {
      blocks.push(tgTable(cols, tableRows.slice(i, i + MAX), { compact: true }));
    }
    const head = `<b>${tgEscape(matched)}</b> · Sefer Takip (${journeys.length} sefer${journeys.length > 40 ? ", ilk 40" : ""})`;
    return head + "\n" + blocks.join("\n\n");
  } catch (e) {
    return "Sefer takip alınamadı: " + tgEscape(e.message);
  }
}

// /sefer (argumansiz) — firma butonlari; tiklayinca o firmanin sefer takibi.
async function tgSendSeferSecimi(chatId) {
  const ops = db.prepare("SELECT DISTINCT operator FROM obilet_prices WHERE operator != '' ORDER BY operator").all().map((r) => r.operator);
  if (!ops.length) {
    await sendTelegramMessage(chatId, "Henüz sefer takip verisi yok.");
    return;
  }
  const inline_keyboard = [];
  for (const op of ops) {
    const data = "st:" + op;
    if (Buffer.byteLength(data) > 64) continue;
    inline_keyboard.push([{ text: op, callback_data: data }]);
  }
  await telegramPost("sendMessage", {
    chat_id: String(chatId),
    parse_mode: "HTML",
    text: "<b>Firma seç</b> — sefer takibini görmek istediğin firmaya dokun:",
    reply_markup: { inline_keyboard },
  });
}

// /takip — panel kartiyla ayni: aktif hat + toplam izlenen sefer + hat bazinda dagilim.
function tgCmdTakip() {
  try {
    const activeTargets = db.prepare("SELECT COUNT(*) AS c FROM obilet_targets").get().c;
    const totalJourneys = db.prepare("SELECT COUNT(*) AS c FROM obilet_prices").get().c;
    const perRoute = db.prepare(`
      SELECT t.origin, t.destination, COUNT(p.id) AS n
        FROM obilet_targets t
        LEFT JOIN obilet_prices p ON p.target_id = t.id
       GROUP BY t.id
       ORDER BY n DESC
    `).all();
    let t =
      "<b>Takip Özeti</b>\n\n" +
      `Aktif hat: <b>${activeTargets}</b>\n` +
      `Toplam izlenen sefer: <b>${totalJourneys}</b>\n`;
    if (perRoute.length) {
      t += "\n<b>Hat bazında:</b>\n" + perRoute
        .map((r) => `${tgEscape((r.origin || "").toUpperCase())} - ${tgEscape((r.destination || "").toUpperCase())}: <b>${r.n}</b> sefer`)
        .join("\n");
    }
    return t;
  } catch (e) {
    return "Takip özeti alınamadı: " + tgEscape(e.message);
  }
}

// /dusenler — sadece son fiyat dususleri (rakip indirimi yakalamak icin).
function tgCmdDusenler() {
  try {
    const rows = db.prepare(
      "SELECT target_id, origin, destination, journey_date, operator, departure_time, old_price, new_price FROM obilet_price_history WHERE new_price < old_price ORDER BY id DESC LIMIT 50"
    ).all();
    if (!rows.length) return "Son dönemde fiyat düşüşü kaydı yok.";
    tgAttachOccupancy(rows);
    return "<b>Son Fiyat Düşüşleri</b>\n\n" + tgChangesGrouped(rows);
  } catch (e) {
    return "Düşüşler alınamadı: " + tgEscape(e.message);
  }
}

// Gecmiste degisikligi olan firmalarin listesi (buton secimi icin).
// Firma butonlari icin kaynak: sistemde TAKIP EDILEN hatlarda secilen firmalar.
// "Tum firmalar" (*) secili hat varsa, o rotalarda gercekten gozuken firmalar da eklenir.
function tgGetTrackedOperators() {
  try {
    const set = new Set();
    let hasWildcard = false;
    const rows = db.prepare("SELECT operators FROM obilet_targets").all();
    for (const r of rows) {
      const list = parseCsvList(r.operators || "");
      if (list.length === 0 || list.includes("*")) hasWildcard = true;
      for (const op of list) {
        if (op && op !== "*") set.add(normalizeObiletOperatorName(op) || op);
      }
    }
    // "*" (tum firmalar) secili ise, takip edilen seferlerde gozuken gercek firmalari da ekle.
    if (hasWildcard) {
      const opRows = db.prepare("SELECT DISTINCT operator FROM obilet_prices WHERE operator != ''").all();
      for (const o of opRows) if (o.operator) set.add(o.operator);
    }
    return [...set].sort((a, b) => String(a).localeCompare(String(b), "tr"));
  } catch { return []; }
}

// /firma <isim> — sadece o firmanin fiyat degisikliklerini gosterir.
function tgCmdFirma(name) {
  try {
    const q = String(name || "").toLocaleLowerCase("tr-TR").trim();
    if (!q) return "Kullanım: <b>/firma &lt;firma adı&gt;</b>\nÖrnek: /firma Has Karayolu\n\nveya sadece /firma yazıp listeden seç.";
    const rows = db.prepare(
      "SELECT target_id, origin, destination, journey_date, operator, departure_time, old_price, new_price FROM obilet_price_history ORDER BY id DESC LIMIT 1000"
    ).all();
    const matched = rows.filter((r) => String(r.operator || "").toLocaleLowerCase("tr-TR").includes(q));
    if (!matched.length) return `"${tgEscape(name)}" için fiyat değişikliği bulunamadı.`;
    const title = tgEscape((matched[0].operator || name));
    const shown = matched.slice(0, 50); // son 50 (tek mesaja sigsin)
    tgAttachOccupancy(shown);
    const countLabel = matched.length > 50 ? `son 50 / ${matched.length}` : `${matched.length}`;
    return `<b>${title}</b> — Fiyat Değişiklikleri (${countLabel})\n\n` +
      tgChangesGrouped(shown);
  } catch (e) {
    return "Firma değişiklikleri alınamadı: " + tgEscape(e.message);
  }
}

// /firma (argumansiz) — firma butonlarini gonderir, tiklayinca o firmanin degisiklikleri gelir.
async function tgSendFirmaSecimi(chatId) {
  const ops = tgGetTrackedOperators();
  if (!ops.length) {
    await sendTelegramMessage(chatId, "Takip edilen hatlarda seçili firma bulunamadı.");
    return;
  }
  const inline_keyboard = [];
  for (const op of ops) {
    const data = "f:" + op;
    if (Buffer.byteLength(data) > 64) continue; // Telegram callback_data limiti
    inline_keyboard.push([{ text: op, callback_data: data }]);
  }
  await telegramPost("sendMessage", {
    chat_id: String(chatId),
    parse_mode: "HTML",
    text: "<b>Firma seç</b> — değişikliklerini görmek istediğin firmaya dokun:",
    reply_markup: { inline_keyboard },
  });
}

// Onayla/Reddet butonuna basilinca (callback_query) calisir.
async function handleTelegramCallback(cb) {
  const fromId = cb.from && cb.from.id;
  // Spinner'i kapat.
  await telegramPost("answerCallbackQuery", { callback_query_id: cb.id });
  const data = String(cb.data || "");

  const targetChat = (cb.message && cb.message.chat && cb.message.chat.id) || fromId;

  // Firma secimi butonu — tum onayli kullanicilar kullanabilir.
  if (data.startsWith("f:")) {
    if (!telegramCanUse(fromId)) return;
    await sendTelegramLong(targetChat, tgCmdFirma(data.slice(2)));
    return;
  }
  // Hat secimi butonu — o hattin degisikliklerini gosterir.
  if (data.startsWith("h:")) {
    if (!telegramCanUse(fromId)) return;
    const id = parseInt(data.slice(2), 10);
    await sendTelegramLong(targetChat, tgCmdHatChangesByTarget(id));
    return;
  }
  // Guncel fiyat butonu — o hattin anlik tum fiyatlari (firma ozeti).
  if (data.startsWith("g:")) {
    if (!telegramCanUse(fromId)) return;
    const id = parseInt(data.slice(2), 10);
    await sendTelegramLong(targetChat, tgCmdGuncelByTarget(id));
    return;
  }
  // Sefer takip butonu — o FIRMANIN sefer bazli fiyat degisiklik gecmisi.
  if (data.startsWith("st:")) {
    if (!telegramCanUse(fromId)) return;
    await sendTelegramLong(targetChat, tgCmdSeferTakip(data.slice(3)));
    return;
  }
  // Abonelik toggle butonu — bu hatta abone ol/cik, klavyeyi guncelle.
  if (data.startsWith("sub:")) {
    if (!telegramCanUse(fromId)) return;
    const tid = parseInt(data.slice(4), 10);
    try {
      const exists = db.prepare("SELECT 1 FROM telegram_subscriptions WHERE chat_id = ? AND target_id = ?").get(String(fromId), tid);
      if (exists) {
        db.prepare("DELETE FROM telegram_subscriptions WHERE chat_id = ? AND target_id = ?").run(String(fromId), tid);
      } else {
        db.prepare("INSERT OR IGNORE INTO telegram_subscriptions (chat_id, target_id, created_at) VALUES (?, ?, ?)").run(String(fromId), tid, nowStamp());
      }
      if (cb.message) {
        await telegramPost("editMessageReplyMarkup", {
          chat_id: cb.message.chat.id,
          message_id: cb.message.message_id,
          reply_markup: tgBuildAboneKeyboard(fromId),
        });
      }
    } catch (e) { console.warn(`[Telegram] abonelik hatasi: ${e.message}`); }
    return;
  }

  // Onayla/Reddet — sadece yoneticiler.
  if (!telegramIsAdmin(fromId)) {
    await telegramPost("answerCallbackQuery", { callback_query_id: cb.id, text: "Bu islem sadece yoneticiler icindir.", show_alert: true });
    return;
  }
  const [action, targetId] = data.split(":");
  if (!targetId) return;
  if (action === "approve") {
    await tgAdminApprove(fromId, targetId);
  } else if (action === "block") {
    await tgAdminBlock(fromId, targetId);
  }
}

// Tek bir guncellemeyi isle (mesaj veya buton).
async function handleTelegramUpdate(update) {
  if (update && update.callback_query) return handleTelegramCallback(update.callback_query);

  const msg = update && update.message;
  if (!msg || !msg.text) return;
  const chat = msg.chat || {};
  const chatId = chat.id;
  if (chatId == null) return;
  const from = msg.from || {};

  const parts = msg.text.trim().split(/\s+/);
  const cmd = parts[0].toLowerCase().split("@")[0]; // "/komut@bot arg" -> "/komut"
  const arg = parts[1] || "";
  const isAdmin = telegramIsAdmin(chatId);

  // Yonetici komutlari (sadece adminler).
  if (isAdmin) {
    if (cmd === "/bekleyenler") return sendTelegramMessage(chatId, tgListPending());
    if (cmd === "/kullanicilar") return sendTelegramMessage(chatId, tgListApproved());
    if (cmd === "/onayla") return tgAdminApprove(chatId, arg);
    if (cmd === "/engelle" || cmd === "/reddet") return tgAdminBlock(chatId, arg);
    if (cmd === "/duyuru") return tgAdminBroadcast(chatId, msg.text);
  }

  // Yetki kontrolu — onaylanmamis kullanicilar komut kullanamaz.
  if (!telegramCanUse(chatId)) {
    const u = telegramGetUser(chatId);
    if (!u) {
      // Ilk temas: pending kaydet + adminlere butonlu bildirim gonder.
      telegramRegisterPending(chat, from);
      await sendTelegramMessage(chatId,
        "<b>Merhaba!</b> Burası KK oBilet fiyat takip botu.\n\n" +
        "Bu bot özeldir; kullanabilmen için önce <b>yönetici onayı</b> gerekiyor. Talebin <b>iletildi</b> \n\n" +
        "Onaylanınca sana haber vereceğim ve hangi hatların bildirimini almak istediğini seçeceksin. ");
      tgNotifyAdminsNewRequest(chat, from);
      console.log(`[Telegram] Yeni erisim talebi: ${tgFullName(from)} (chat ${chatId})`);
    } else if (u.status === "blocked") {
      await sendTelegramMessage(chatId, "Erişimin engellenmiş.");
    } else {
      await sendTelegramMessage(chatId, "Onayın hâlâ bekleniyor. Yönetici onaylayınca haber vereceğim.");
    }
    return;
  }

  // /firma — argumanli ise direkt sonuc, argumansiz ise firma secim butonlari.
  if (cmd === "/firma") {
    const rest = parts.slice(1).join(" ").trim();
    if (rest) {
      await sendTelegramLong(chatId, tgCmdFirma(rest));
    } else {
      await tgSendFirmaSecimi(chatId);
    }
    return;
  }

  // /hat — argumanli ise o hattin degisiklikleri, argumansiz ise hat secim butonlari.
  if (cmd === "/hat") {
    const rest = parts.slice(1);
    if (rest.length >= 2) {
      await sendTelegramLong(chatId, tgCmdHat(rest));
    } else {
      await tgSendHatSecimi(chatId);
    }
    return;
  }

  // /guncel — argumanli ise o hattin guncel fiyatlari, argumansiz ise hat secim butonlari.
  if (cmd === "/guncel") {
    const rest = parts.slice(1);
    if (rest.length >= 2) {
      await sendTelegramLong(chatId, tgCmdGuncel(rest));
    } else {
      await tgSendGuncelSecimi(chatId);
    }
    return;
  }

  // /sefer — argumanli ise o firmanin sefer takibi, argumansiz ise firma secim butonlari.
  if (cmd === "/sefer") {
    const rest = parts.slice(1).join(" ").trim();
    if (rest) {
      await sendTelegramLong(chatId, tgCmdSeferTakip(rest));
    } else {
      await tgSendSeferSecimi(chatId);
    }
    return;
  }

  // /abone — bildirim almak istedigi hatlari sec (/toggle).
  if (cmd === "/abone") {
    await telegramPost("sendMessage", {
      chat_id: String(chatId),
      parse_mode: "HTML",
      text: "<b>Bildirim Aboneliği</b>\nFiyat değişikliği bildirimi almak istediğin hatlara dokun (açık, kapalı):",
      reply_markup: tgBuildAboneKeyboard(chatId),
    });
    return;
  }

  // /aboneliklerim — abone olunan hatlarin listesi.
  if (cmd === "/aboneliklerim") {
    const subs = db.prepare(`
      SELECT t.origin, t.destination FROM telegram_subscriptions s
        JOIN obilet_targets t ON t.id = s.target_id
       WHERE s.chat_id = ? ORDER BY t.origin, t.destination
    `).all(String(chatId));
    const reply = subs.length
      ? "<b>Aboneliklerin</b>\n\n" + subs.map((r) => `${tgEscape((r.origin || "").toUpperCase())} - ${tgEscape((r.destination || "").toUpperCase())}`).join("\n") + "\n\nDeğiştirmek için /abone"
      : "Henüz hiçbir hatta abone değilsin, bu yüzden bildirim almıyorsun.\n/abone ile hat seç.";
    await sendTelegramMessage(chatId, reply);
    return;
  }

  // Onayli/admin -> komutlar.
  let reply;
  switch (cmd) {
    case "/start":
    case "/yardim":
    case "/help":
      reply = tgCmdYardim(isAdmin); break;
    case "/durum":
      reply = tgCmdDurum(); break;
    case "/hatlar":
      reply = tgCmdHatlar(); break;
    case "/son":
      reply = tgCmdSon(); break;
    case "/fiyatlar":
      reply = tgCmdFiyatlar(); break;
    case "/dusenler":
      reply = tgCmdDusenler(); break;
    case "/takip":
      reply = tgCmdTakip(); break;
    default:
      reply = "Bilinmeyen komut. /yardim yazarak komutları görebilirsin.";
  }
  // Uzun cevaplari otomatik birden fazla mesaja bol (kirpma yok).
  await sendTelegramLong(chatId, reply);
}

// Polling dongusu — tek instance icin guvenli.
let telegramPollingStarted = false;
async function startTelegramPolling() {
  if (!TELEGRAM_ENABLED || telegramPollingStarted) return;
  telegramPollingStarted = true;

  // Telegram'a komut menusunu tanit (kullaniciya "/" yazinca liste cikar).
  const userCommands = [
    { command: "durum", description: "Sistem özeti" },
    { command: "takip", description: "Toplam izlenen sefer (hat bazında)" },
    { command: "hatlar", description: "Takip edilen hatlar" },
    { command: "son", description: "Son 50 fiyat değişikliği" },
    { command: "dusenler", description: "Son fiyat düşüşleri" },
    { command: "firma", description: "Firmaya göre değişiklikler" },
    { command: "fiyatlar", description: "Güncel fiyat aralıkları" },
    { command: "hat", description: "Hata göre değişiklikler" },
    { command: "guncel", description: "Hattın değişmeyen fiyatları" },
    { command: "sefer", description: "Sefer bazlı değişim + doluluk" },
    { command: "abone", description: "Bildirim hatlarını seç" },
    { command: "aboneliklerim", description: "Abone olduğun hatlar" },
    { command: "yardim", description: "Komut menüsü" },
  ];
  await telegramPost("setMyCommands", { commands: userCommands });
  // Bot acilis aciklamasi (kisi botu ilk actiginda gorur).
  await telegramPost("setMyDescription", {
    description: "KK oBilet fiyat takip botu. Kullanmak için /start yaz, yönetici onayından sonra /abone ile hatlarını seçip fiyat değişikliği bildirimleri al.",
  });
  await telegramPost("setMyShortDescription", {
    short_description: "KK oBilet fiyat takip botu — /start ile başla.",
  });
  // Yoneticilere ek komutlari sadece kendi sohbetlerinde goster (scope: chat).
  const adminCommands = userCommands.concat([
    { command: "bekleyenler", description: "Onay bekleyen kullanıcılar" },
    { command: "kullanicilar", description: "Onaylı kullanıcılar" },
    { command: "onayla", description: "Kullanıcı onayla: /onayla <id>" },
    { command: "engelle", description: "Kullanıcı engelle: /engelle <id>" },
    { command: "duyuru", description: "Tüm kullanıcılara duyuru gönder" },
  ]);
  for (const adminId of TELEGRAM_DEFAULT_CHAT_IDS) {
    await telegramPost("setMyCommands", { commands: adminCommands, scope: { type: "chat", chat_id: String(adminId) } });
  }

  // Acilista eski/birikmis mesajlari atla: en son update_id'yi bul, sonrasini dinle.
  let offset = 0;
  try {
    const init = await telegramGetUpdates(-1, 0);
    if (init && init.ok && init.result.length) {
      offset = init.result[init.result.length - 1].update_id + 1;
    }
  } catch { /* yok say */ }

  console.log("[Telegram] Komut dinleyici basladi (long polling).");
  // Sonsuz dongu — her turda 30 sn uzun bekleme yapar.
  /* eslint-disable no-constant-condition */
  while (true) {
    try {
      const data = await telegramGetUpdates(offset, 30);
      if (data && data.ok && Array.isArray(data.result)) {
        for (const upd of data.result) {
          offset = upd.update_id + 1;
          try { await handleTelegramUpdate(upd); }
          catch (e) { console.warn(`[Telegram] Komut isleme hatasi: ${e.message}`); }
        }
      } else if (data && data.ok === false) {
        // 409 Conflict (baska poller/webhook) gibi durumlar — biraz bekle.
        console.warn(`[Telegram] getUpdates reddedildi: ${data.description || "?"}`);
        await new Promise((r) => setTimeout(r, 5000));
      }
    } catch (e) {
      console.warn(`[Telegram] Poll dongu hatasi: ${e.message}`);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

// Fiyat degisikligi raporu maili. Sadece changes.length > 0 oldugunda cagrilir.
async function sendObiletCycleStatusEmail(emailList, target, trackedJourneys, changes = []) {
  const { smtpUser } = createSmtpTransportsWithFallback();
  const smtpFrom = process.env.SMTP_FROM || `"oBilet Fiyat Takip" <${smtpUser}>`;

  const formattedDate = String(target.date || "").split("-").reverse().join(".");
  const formattedEndDate = String(target.end_date || "").split("-").reverse().join(".");
  const dateLabel = formattedEndDate && formattedEndDate !== formattedDate
    ? `${formattedDate} - ${formattedEndDate}`
    : formattedDate;
  const checkedAt = nowStamp();
  const hasChanges = changes.length > 0;

  const currentPriceRows = trackedJourneys.map(item => `
    <tr style="background:#ffffff;">
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;color:#333333;">${toDotDate(item.journey_date) || "-"}</td>
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;color:#333333;">${item.operator}</td>
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;text-align:center;color:#333333;">${item.time}</td>
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;text-align:right;color:#1a73e8;font-weight:600;font-size:14px;">${item.price} TL</td>
    </tr>`).join("");

  const changesRows = changes.map(c => {
    const isDrop = c.newPrice < c.oldPrice;
    // Firma perspektifi: rakip artisi = YESIL (iyi), rakip dususu = KIRMIZI (rekabet baskisi)
    const newColor = isDrop ? "#d32f2f" : "#27ae60";
    return `
    <tr style="background:#fff8f8;">
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;color:#333333;">${toDotDate(c.journey_date) || "-"}</td>
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;color:#333333;">${c.operator}</td>
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;text-align:center;color:#333333;">${c.departure_time}</td>
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;text-align:right;color:#999999;text-decoration:line-through;">${c.oldPrice} TL</td>
      <td style="padding:10px;border-bottom:1px solid #e8e8e8;text-align:right;color:${newColor};font-weight:700;font-size:14px;">${c.newPrice} TL</td>
    </tr>`;
  }).join("");

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;background:#ffffff;">
      <div style="padding:20px;background:${hasChanges ? "#c0392b" : "#1a73e8"};color:#ffffff;text-align:center;">
        <h2 style="margin:0;font-size:20px;font-weight:600;">oBilet Fiyat Raporu</h2>
      </div>
      <div style="padding:20px;background:#ffffff;">
        <p style="margin:0 0 12px 0;color:#333333;">Merhaba,</p>
        <p style="margin:0 0 10px 0;color:#333333;"><strong>Hat:</strong> ${target.origin.toUpperCase()} - ${target.destination.toUpperCase()}</p>
        <p style="margin:0 0 10px 0;color:#333333;"><strong>Tarih Araligi:</strong> ${dateLabel}</p>
        <p style="margin:0 0 16px 0;color:#333333;"><strong>Kontrol Zamani:</strong> ${checkedAt}</p>

        <p style="margin:0 0 12px 0;color:#333333;"><strong>Durum:</strong>
          <span style="color:${hasChanges ? "#c0392b" : "#27ae60"};font-weight:600;">
            ${hasChanges ? `${changes.length} fiyat değişikliği` : "Değişiklik yok"}
          </span>
        </p>

        ${hasChanges ? `
          <h3 style="margin:20px 0 10px 0;font-size:16px;color:#c0392b;font-weight:600;">Fiyat Değişiklikleri</h3>
          <table style="width:100%;border-collapse:collapse;font-size:13px;border:1px solid #e0e0e0;border-radius:4px;">
            <thead><tr style="background:#f8f9fa;">
              <th style="padding:10px;text-align:left;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Tarih</th>
              <th style="padding:10px;text-align:left;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Firma</th>
              <th style="padding:10px;text-align:center;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Saat</th>
              <th style="padding:10px;text-align:right;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Eski</th>
              <th style="padding:10px;text-align:right;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Yeni</th>
            </tr></thead>
            <tbody>${changesRows}</tbody>
          </table>` : ""}

        <h3 style="margin:20px 0 10px 0;font-size:16px;color:#333333;font-weight:600;">Anlık Fiyat Listesi</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px;border:1px solid #e0e0e0;border-radius:4px;">
          <thead><tr style="background:#f8f9fa;">
            <th style="padding:10px;text-align:left;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Tarih</th>
            <th style="padding:10px;text-align:left;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Firma</th>
            <th style="padding:10px;text-align:center;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Saat</th>
            <th style="padding:10px;text-align:right;color:#555555;font-weight:600;border-bottom:2px solid #e0e0e0;">Fiyat</th>
          </tr></thead>
          <tbody>${currentPriceRows || '<tr><td colspan="4" style="padding:12px;text-align:center;color:#999999;">Sefer verisi bulunamadı.</td></tr>'}</tbody>
        </table>
        ${renderEmailSignature()}
      </div>
    </div>
  `;

  const { info, smtpPort } = await sendMailWithSmtpFallback({
    from: smtpFrom,
    to: emailList,
    subject: buildObiletSubject(hasChanges ? OBILET_SUBJECT_CHANGE : OBILET_SUBJECT_NO_CHANGE, target, dateLabel),
    html,
  });
  console.log(`[E-posta] SMTP portu kullanildi: ${smtpPort}`);
  console.log(`[E-posta] Rapor gonderildi: ${info.messageId}`);
  return info;
}


// Tek oBilet Target İşleme Fonksiyonu (Ortak)
async function processObiletTarget(target) {
  try {
    setObiletTargetSyncStatus(target.id, "Kontrol ediliyor...");
    
    // Duplicate kayitlari temizle (ayni target + journey_date + operator + departure_time)
    // Yeni sistemde departure_stop kullanmiyoruz, eski duplicate kayitlar sorun olabilir
    try {
      const duplicates = db.prepare(`
        SELECT journey_date, operator, departure_time
        FROM obilet_prices
        WHERE target_id = ?
        GROUP BY journey_date, operator, departure_time
        HAVING COUNT(*) > 1
      `).all(target.id);
      
      for (const dup of duplicates) {
        // Her duplicate grup icin, en son guncellenen ID'yi bul
        const keepId = db.prepare(`
          SELECT id FROM obilet_prices
          WHERE target_id = ? AND journey_date = ? AND operator = ? AND departure_time = ?
          ORDER BY last_updated DESC LIMIT 1
        `).get(target.id, dup.journey_date, dup.operator, dup.departure_time);
        
        if (keepId) {
          // Digerlerini sil
          db.prepare(`
            DELETE FROM obilet_prices
            WHERE target_id = ? AND journey_date = ? AND operator = ? AND departure_time = ? AND id != ?
          `).run(target.id, dup.journey_date, dup.operator, dup.departure_time, keepId.id);
        }
      }
    } catch (cleanErr) {
      console.error(`[oBilet] Duplicate temizleme hatasi: ${cleanErr.message}`);
    }
    
    const dateList = buildIsoDateRange(target.date, target.end_date || target.date);
    if (!dateList.length) {
      setObiletTargetSyncStatus(target.id, "Hata: Tarih araligi gecersiz. Baslangic ve bitis tarihini kontrol edin.");
      return;
    }

    const periodLabel = dateList.length > 1
      ? `${toDotDate(dateList[0])} - ${toDotDate(dateList[dateList.length - 1])}`
      : toDotDate(dateList[0]);

    console.log(`[Takip Görevi] Sorgulanıyor: ${target.origin} -> ${target.destination} (${periodLabel})`);

    const targetOperators = parseCsvList(target.operators)
      .map(normalizeObiletOperatorName)
      .filter(Boolean);
    // Eğer firma seçilmemişse veya "*" varsa, TÜM firmaları kabul et
    const acceptAllOperators = targetOperators.length === 0 || targetOperators.includes("*");
    if (acceptAllOperators) {
      console.log(`[Takip Görevi] TÜM firmalar kabul ediliyor (operator filtresi yok)`);
    } else {
      console.log(`[Takip Görevi] Sadece seçili firmalar: ${targetOperators.join(", ")}`);
    }
    const departureStopFilter = String(target.departure_stop_filter || "").trim();
    const departureStopFilterKey = normalizeSearchText(departureStopFilter);
    const queryOriginPrimary = departureStopFilter || target.origin;
    // GERCEK doluluk icin koltuk haritasi cekilecek operatorler — SADECE belirli firma seciliyse.
    // ACIK: POST("{}") govdesi calisiyor (canli dogrulandi: 15:30 seferi liste 5 derken gercek 14).
    // Liste available-seats BAZI otobuslerde BAYAT; gercek koltuk haritasi (SVG) her zaman dogru.
    // attachRealOccupancy tiklamasiz POST ile ceker, takip edilen firmalarla + tarih basi ust
    // sinirla (asagida) yuku bounded tutar. "*" (tum firma) hatlarinda cok agir olacagi icin atlanir.
    // GERCEK koltuk cekimi ACIK (2026-07, tekrar). Neden: LISTE degeri (available-seats) GECIS SEFERLERINDE
    // (rakip baska sehirden gelip Adana'dan gecen seferler) CIDDI EKSIK gosteriyor. Canli kanit: 13.07 19:30
    // Adana->Ankara Enver, LISTE 34 bos diyor (biz "7 dolu %17"), ama GERCEK koltuk haritasi 13 erkek + 5 kadin
    // = 18 DOLU (%44). Fark: arac Sanliurfa 13:30 kalkisli, Adana'dan gecerken uzerinde 18 yolcu var; oBilet'in
    // segment "available-seats"i bunlari yansitmiyor. GERCEK koltuk haritasi (SVG) fiziki dolulugu dogru verir.
    // Bayatlik korumasi (satir ~6548): real cekim basarisiz olursa eski 'gercek' korunur (liste ile ezilmez) —
    // birkac dk eski dogru deger, taze YANLIS liste degerinden iyidir. attachRealOccupancy tiklamasiz POST ile,
    // takip edilen firmalarla + tarih basi 25 sefer ust siniriyla ceker (yuku/Cloudflare'i bounded tutar).
    // Kapatmak icin: true -> false.
    const REAL_SEATMAP_ENABLED = true;
    // HIZ: gercek koltuk haritasini SADECE ilk N gunde cek (kalkisa yakin -> otobus dolu -> dogruluk kritik).
    // Uzak tarihler cogunlukla bos, liste degeri yeterli. Boylece tarama hizlanir + Cloudflare yuku duser
    // (daha az istek -> daha az takilma). 2 = bugun + yarin. Artir/azalt: dogruluk<->hiz dengesi.
    const REAL_SEATMAP_NEAR_DAYS = 2;
    const seatOps = !REAL_SEATMAP_ENABLED ? null : (acceptAllOperators ? ["*"] : targetOperators);

    const trackedJourneys = [];
    const changes = [];     // [{ journey_date, operator, departure_time, oldPrice, newPrice }] — mail tetikleyici tek liste
    const now = nowStamp();
    const scrapedOperators = new Set();

    const targetRouteId = String(target.route_id || "").trim();
    // Yapi-diff icin: her hedefte SADECE ILK GECERLI GELECEK tarihi bir kez fotografla (bugun degil —
    // bugunku seferler gun ici kalktikca "kaldirildi" yanlis alarmini onlemek icin strict future).
    const structureTodayIso = todayIsoInIstanbul();
    let structureCaptured = false;
    for (let dayIdx = 0; dayIdx < dateList.length; dayIdx++) {
        const journeyDate = dateList[dayIdx];
        // Gercek koltuk haritasi YALNIZCA ilk N gunde (yakin tarih); uzak tarihlerde liste degeri (hizli).
        const daySeatOps = (dayIdx < REAL_SEATMAP_NEAR_DAYS) ? seatOps : null;
        // Ayni hatta ardisik tarihler arasinda kucuk bir bekleme — Cloudflare burst tespitini onler.
        if (dayIdx > 0) {
          await new Promise(r => setTimeout(r, 3000));
        }
        let journeys = await scrapeObilet(queryOriginPrimary, target.destination, journeyDate, targetRouteId, daySeatOps);
        if (departureStopFilter && !journeys.length) {
          // Durak bazli sorgu bos donerse sehir bazli rotaya geri dus.
          journeys = await scrapeObilet(target.origin, target.destination, journeyDate, targetRouteId, daySeatOps);
        }
        // Bos dondu (Cloudflare challenge / timeout olabilir) — 8 sn bekleyip 1 kez daha dene.
        // Boylece o gunun fiyat+doluluk verisi eski kalmaz.
        if (!journeys.length) {
          console.log(`[Takip Görevi] ${journeyDate} bos dondu, 8 sn sonra 1 kez daha denenecek...`);
          await new Promise(r => setTimeout(r, 8000));
          journeys = await scrapeObilet(queryOriginPrimary, target.destination, journeyDate, targetRouteId, daySeatOps);
          if (departureStopFilter && !journeys.length) {
            journeys = await scrapeObilet(target.origin, target.destination, journeyDate, targetRouteId, daySeatOps);
          }
        }
        journeys.forEach((journey) => {
          const normalized = normalizeObiletOperatorName(journey.operator);
          if (normalized) scrapedOperators.add(normalized);
        });

        // Filtreler: firma, kalkis duragi, gelecek tarih
        const dayTrackedRaw = journeys
          .filter((j) => acceptAllOperators || targetOperators.some((op) => isObiletOperatorMatch(op, j.operator)))
          .filter((j) => !departureStopFilterKey || normalizeSearchText(j.departureStop || "").includes(departureStopFilterKey))
          .filter((j) => isJourneyInFuture(journeyDate, j.time))
          .map((j) => ({ ...j, journey_date: journeyDate }));

        // Ayni firma+saat+durak seferinde duplicate kayit cikarsa tek kayda indir.
        const dayTrackedMap = new Map();
        for (const journey of dayTrackedRaw) {
          const key = buildJourneyIdentityKey(journey.operator, journey.time, journey.departureStop);
          if (!dayTrackedMap.has(key)) dayTrackedMap.set(key, journey);
        }
        const dayTracked = Array.from(dayTrackedMap.values());

        const currentKeys = new Set(
          dayTracked.map((j) => buildJourneyIdentityKey(j.operator, j.time, j.departureStop))
        );

        // VAR OLAN DB SATIRLARI temizligi — VERI KAYBINI ONLE:
        //  1) YALNIZCA scrape BASARILIYSA (journeys.length>0) temizle. Tarama Cloudflare/timeout
        //     yuzunden BOS donduyse HICBIR SEY SILME (yoksa o tarihin tum fiyatlari gider).
        //  2) Gecmis kalkan seferi HEMEN silme: 10 gun tut (Sefer Takip'te kalkis dolulugu gorunsun).
        //  3) Gelecek seferi TEK turda gorulmedi diye HEMEN silme -> yoklama sayaci; ust uste
        //     OBILET_ABSENT_DELETE_RUNS tur gorunmezse sil. Gorulunce sayac sifirlanir. Boylece
        //     tek aksak/eksik tarama fiyat satirlarini SILEMEZ.
        if (journeys.length > 0) {
          const existingRows = db
            .prepare("SELECT id, operator, departure_time, journey_date, departure_stop, arrival_stop, price, absent_count FROM obilet_prices WHERE target_id = ? AND journey_date = ?")
            .all(target.id, journeyDate);
          const pastKeepFrom = shiftIsoDate(todayIsoInIstanbul(), -10);
          for (const row of existingRows) {
            // Geçmiş kalkış: 10 gunden ESKI ise sil; degilse KORU (yakin gecmisi Sefer Takip'te goster).
            if (!isJourneyInFuture(row.journey_date, row.departure_time)) {
              if (row.journey_date < pastKeepFrom) {
                db.prepare("DELETE FROM obilet_prices WHERE id = ?").run(row.id);
              }
              continue;
            }
            // Filtre dışı satırlar (firma/durak): hiç değerlendirme (sayaca da dokunma).
            const operatorMatched = acceptAllOperators || targetOperators.some((op) => isObiletOperatorMatch(op, row.operator));
            if (!operatorMatched) continue;
            if (departureStopFilterKey) {
              const rowStopKey = normalizeSearchText(row.departure_stop || "");
              if (!rowStopKey.includes(departureStopFilterKey)) continue;
            }

            const rowKey = buildJourneyIdentityKey(row.operator, row.departure_time, row.departure_stop);
            if (currentKeys.has(rowKey)) {
              // Sefer bu turda GORULDU -> yoklama sayacini sifirla (silinme adayligindan cikar).
              if ((row.absent_count || 0) !== 0) {
                db.prepare("UPDATE obilet_prices SET absent_count = 0 WHERE id = ?").run(row.id);
              }
            } else {
              // Bu turda YOK -> sayaci artir; sadece ust uste yeterince tur gorunmezse sil.
              const nextAbsent = (row.absent_count || 0) + 1;
              if (nextAbsent >= OBILET_ABSENT_DELETE_RUNS) {
                db.prepare("DELETE FROM obilet_prices WHERE id = ?").run(row.id);
              } else {
                db.prepare("UPDATE obilet_prices SET absent_count = ? WHERE id = ?").run(nextAbsent, row.id);
              }
            }
          }
        }

        trackedJourneys.push(...dayTracked);

        // DOLULUK (piggyback) — her seferin anlik koltuk durumunu AYRI tabloya yazar.
        // Tamamen izole: hata olsa bile fiyat takibini etkilemez.
        // YALNIZCA has-available-seat-info=true olan seferler yazilir (gercek koltuk verisi).
        // writtenOccKeys: gercekten yazdigimiz seferler — temizlik bunu baz alir ki koltuk bilgisi
        // olmayan seferlerin ESKI (yanlis) satirlari da silinsin.
        const writtenOccKeys = new Set();
        let occSkippedNoInfo = 0;
        for (const j of dayTracked) {
          try {
            const occOp = normalizeObiletOperatorName(j.operator) || j.operator;
            let total, avail, occ, source;
            const rT = Number(j.realTotal), rS = Number(j.realSold);
            if (Number.isFinite(rT) && rT > 0 && Number.isFinite(rS)) {
              // 1) BIRINCIL: GERCEK koltuk haritasi (/json/sefer SVG sayimi). Her zaman dogru (canli dogrulandi:
              //    21:29 => 35/6). Mevcut her degerin (liste dahil) UZERINE yazar.
              total = rT; const sold = Math.max(0, Math.min(rT, rS));
              avail = rT - sold; occ = Math.round((sold / rT) * 100); source = "gercek";
            } else if (j.seatInfoReliable === true && Number.isFinite(Number(j.totalSeats)) && Number(j.totalSeats) > 0 && Number.isFinite(Number(j.availableSeats))) {
              // 2) ANA KAYNAK (gercek cekim kapaliyken): oBilet liste available-seats. Her taramada TAZE gelir.
              //    KORUMA yalnizca GERCEK CEKIM ACIKKEN gecerli: taze gercek liste ile bozulmasin. Gercek
              //    KAPALIYKEN eski 'gercek' satirlar bayatladigi icin liste ONLARI da serbestce guncellemeli
              //    (bayat 5 -> taze 29). Aksi halde dolu otobus eski dusuk degerde takili kalir.
              if (REAL_SEATMAP_ENABLED) {
                const ex = db.prepare("SELECT source FROM obilet_occupancy WHERE target_id=? AND journey_date=? AND operator=? AND departure_time=?")
                  .get(target.id, j.journey_date, occOp, j.time);
                if (ex && ex.source === "gercek") { continue; } // taze gercegi liste ile bozma
              }
              total = Number(j.totalSeats); avail = Number(j.availableSeats);
              occ = Math.max(0, Math.min(100, Math.round((1 - avail / total) * 100))); source = "liste";
            } else {
              // 3) Hicbir guvenilir veri yok -> yazma ("-").
              occSkippedNoInfo++;
              continue;
            }
            if (DEBUG_OBILET_PRICE) console.log(`[Doluluk] ${journeyDate} ${j.time} ${occOp}: ${total - avail}/${total} dolu (%${occ}) [${source}]`);
            // Doluluk YALNIZCA gercek koltuk haritasi (source='gercek') veya guvenilir liste (source='liste')
            // ile yazilir. GERCEK her zaman ustune yazabilir; LISTE mevcut 'gercek' satiri BOZMAZ (yukaridaki
            // koruma, satir 6445). Fiyat takibinden tamamen bagimsiz ayri tablo (obilet_occupancy).
            // Plaka yalnizca GERCEK harita cekiminde gelir; liste yedeginde bos. Mevcut plakayi
            // bos ile silmemek icin: gercek+plaka varsa yaz, yoksa mevcut plakayi KORU (COALESCE).
            const plateVal = (source === "gercek" && j.realPlate) ? String(j.realPlate).trim() : "";
            db.prepare(`
              INSERT INTO obilet_occupancy (target_id, journey_date, operator, departure_time, departure_stop, total_seats, available_seats, occupancy_percent, last_updated, source, plate, route_from, route_to, service_id)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(target_id, journey_date, operator, departure_time)
              DO UPDATE SET departure_stop = excluded.departure_stop, total_seats = excluded.total_seats, available_seats = excluded.available_seats, occupancy_percent = excluded.occupancy_percent, last_updated = excluded.last_updated, source = excluded.source, plate = CASE WHEN excluded.plate != '' THEN excluded.plate ELSE obilet_occupancy.plate END, route_from = CASE WHEN excluded.route_from != '' THEN excluded.route_from ELSE obilet_occupancy.route_from END, route_to = CASE WHEN excluded.route_to != '' THEN excluded.route_to ELSE obilet_occupancy.route_to END, service_id = CASE WHEN excluded.service_id != '' THEN excluded.service_id ELSE obilet_occupancy.service_id END
            `).run(target.id, j.journey_date, occOp, j.time, j.departureStop || "", total, avail, occ, now, source, plateVal, String(j.routeFrom || ""), String(j.routeTo || ""), String(j.serviceId || ""));
            writtenOccKeys.add(`${occOp}|${j.time}`);
          } catch (occErr) {
            if (DEBUG_OBILET_PRICE) console.warn(`[Doluluk] yazma hatasi: ${occErr.message}`);
          }
        }
        if (journeys.length > 0 && occSkippedNoInfo > 0) {
          console.log(`[Doluluk] ${journeyDate}: ${writtenOccKeys.size} sefer koltuk yazildi, ${occSkippedNoInfo} sefer koltuk bilgisi YOK (atlandi, "-" gosterilecek).`);
        }

        // Bu tarih icin doluluk tablosunda olup bu taramada YAZILMAYAN satirlari sil
        // (iptal/tasinmis sefer + koltuk-bilgisi-yok seferlerin eski yanlis kayitlari + durak mukerrerleri).
        // GUVENLIK: yalnizca scrape BASARILIYSA (journeys.length>0) temizle — bos donen taramada
        // (Cloudflare/timeout) o tarihin dolulugunu YANLISLIKLA silmemek icin.
        if (journeys.length > 0) {
          try {
            // Bu taramada GORULEN tum seferler (gercek deger yazilamasa bile). Cleanup bunu baz alir:
            // sefer HALA listede ise onceki degerini KORU (gecici cekim hatasinda YANLISLIKLA silme,
            // "-" gostermeyelim); yalnizca listede OLMAYAN (iptal/tasinmis) gelecek seferi sil.
            const currentOccKeys = new Set(
              dayTracked.map((j) => `${normalizeObiletOperatorName(j.operator) || j.operator}|${j.time}`)
            );
            const occRowsForDate = db.prepare(
              "SELECT id, operator, departure_time FROM obilet_occupancy WHERE target_id = ? AND journey_date = ?"
            ).all(target.id, journeyDate);
            for (const o of occRowsForDate) {
              // KALKAN (gecmis) sefer: kalkmadan onceki SON doluluk degeri DONSUN, SILME.
              // Boylece kullanici "bu arac 30/41 ile cikmis" diye kalkis dolulugunu gorebilir.
              if (!isJourneyInFuture(journeyDate, o.departure_time)) continue;
              // GELECEK sefer bu turda listede yok (iptal/tasinmis) -> sil. Listede olan ama bu tur
              // degeri tazelenemeyen (gecici hata) satir KORUNUR — onceki deger gosterilmeye devam eder.
              if (!currentOccKeys.has(`${o.operator}|${o.departure_time}`)) {
                db.prepare("DELETE FROM obilet_occupancy WHERE id = ?").run(o.id);
              }
            }
          } catch (occCleanErr) {
            if (DEBUG_OBILET_PRICE) console.warn(`[Doluluk] temizleme hatasi: ${occCleanErr.message}`);
          }
        }

        // YENI/MEVCUT SEFERLER: fiyat değişimi, yeni ekleme tespiti
        for (const journey of dayTracked) {
          const normalizedOperator = normalizeObiletOperatorName(journey.operator) || journey.operator;
          const journeyKey = buildJourneyIdentityKey(normalizedOperator, journey.time, journey.departureStop);
          const candidates = db.prepare(
            "SELECT * FROM obilet_prices WHERE target_id = ? AND journey_date = ? AND departure_time = ? ORDER BY last_updated DESC"
          ).all(target.id, journey.journey_date, journey.time);
          const previous = candidates.find(
            (row) => buildJourneyIdentityKey(row.operator, row.departure_time, row.departure_stop) === journeyKey
          );

          if (!previous) {
            // YENI SEFER: sessizce DB'ye ekle, mail/bildirim yok (sade mod).
            db.prepare(
              "INSERT INTO obilet_prices (target_id, journey_date, operator, departure_time, departure_stop, arrival_stop, price, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            ).run(target.id, journey.journey_date, normalizedOperator, journey.time, journey.departureStop || "", journey.arrivalStop || "", journey.price, now);
            continue;
          }

          // Mevcut sefer: fiyat aynı mı?
          if (previous.price === journey.price) {
            // Aynı fiyat — pending sıfırla, metadata tazele.
            db.prepare(
              "UPDATE obilet_prices SET operator = ?, departure_stop = ?, arrival_stop = ?, last_updated = ?, pending_price = NULL, pending_seen_count = 0 WHERE id = ?"
            ).run(normalizedOperator, journey.departureStop || "", journey.arrivalStop || "", now, previous.id);
            continue;
          }

          // Fiyat değişti: pending counter mantığı (ardışık tur doğrulama).
          const newPrice = journey.price;
          const previousPendingPrice = Number(previous.pending_price || 0);
          const previousPendingCount = Number(previous.pending_seen_count || 0);
          const pendingCount = previousPendingPrice === newPrice ? previousPendingCount + 1 : 1;

          if (pendingCount >= OBILET_PRICE_CONFIRM_RUNS) {
            changes.push({
              journey_date: journey.journey_date,
              operator: normalizedOperator,
              departure_time: journey.time,
              oldPrice: previous.price,
              newPrice,
            });
            db.prepare(
              "UPDATE obilet_prices SET operator = ?, price = ?, departure_stop = ?, arrival_stop = ?, last_updated = ?, pending_price = NULL, pending_seen_count = 0 WHERE id = ?"
            ).run(normalizedOperator, newPrice, journey.departureStop || "", journey.arrivalStop || "", now, previous.id);

            // Kalıcı geçmiş kaydı — raporlama sayfası bu tablodan okur.
            try {
              db.prepare(
                "INSERT INTO obilet_price_history (target_id, origin, destination, journey_date, operator, departure_time, departure_stop, old_price, new_price, changed_at, detected_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
              ).run(
                target.id,
                target.origin,
                target.destination,
                journey.journey_date,
                normalizedOperator,
                journey.time,
                journey.departureStop || "",
                previous.price,
                newPrice,
                now,
                "auto"
              );
            } catch (histErr) {
              console.warn(`[oBilet] Fiyat gecmisi kaydı basarisiz: ${histErr.message}`);
            }

            addPricingNotification(
              "oBilet Fiyat Takip",
              `${toDotDate(journey.journey_date)} ${normalizedOperator} (${journey.time}, ${journey.departureStop || "-"}) fiyatı değişti: ${previous.price} TL -> ${newPrice} TL (${target.origin.toUpperCase()} - ${target.destination.toUpperCase()})`
            );
          } else {
            if (DEBUG_OBILET_PRICE) {
              console.log(`[oBilet ${target.origin}-${target.destination}] ${journeyKey}: Pending ${previousPendingCount}-${pendingCount} (${previous.price}-${newPrice}TL)`);
            }
            db.prepare(
              "UPDATE obilet_prices SET operator = ?, departure_stop = ?, arrival_stop = ?, last_updated = ?, pending_price = ?, pending_seen_count = ? WHERE id = ?"
            ).run(normalizedOperator, journey.departureStop || "", journey.arrivalStop || "", now, newPrice, pendingCount, previous.id);
          }
        }

        // AG & KAPASITE DIFF: gunun ham sefer listesinden (TUM firmalar) yapi fotografi + onceki tur ile kiyas.
        // Fiyat/degisiklik/Telegram mantigina DOKUNMAZ — ayni 'journeys' verisini OKUR, ayri tabloya yazar.
        // SADECE ilk GELECEK (bugunden sonraki) tarihi bir kez: bugun ise gun ici kalkan seferler "kaldirildi"
        // yanlis alarmi uretir; gelecek tarihte tum seferler sabit. Cogullamayi da onler (tek tarih).
        if (STRUCTURE_DIFF_ENABLED && !structureCaptured && journeyDate > structureTodayIso && Array.isArray(journeys) && journeys.length) {
          try { captureStructureSnapshot(target, journeyDate, journeys); structureCaptured = true; }
          catch (e) { if (DEBUG_OBILET_PRICE) console.warn(`[Yapi Diff] kanca hatasi: ${e.message}`); }
        }
    }

    if (trackedJourneys.length === 0) {
      // Iki farkli durum: (a) scraper hic sefer cekemedi (sayfa acilmadi/Cloudflare/yanlis ID),
      // (b) cekti ama secilen firmalar arasinda yok. Mesajlari ayri tutalim ki kullanici neyi
      // duzeltecegini bilsin.
      const stopDetail = departureStopFilter ? ` (Kalkis duragi filtresi: ${departureStopFilter})` : "";
      let statusMsg;
      let notifMsg;
      if (scrapedOperators.size === 0) {
        // (a) Hic veri gelmedi.
        statusMsg = `oBilet sayfasindan sefer cekilemedi. Olasi nedenler: yanlis sehir ID, Cloudflare engeli, tarihte sefer yok${stopDetail}.`;
        notifMsg = `${target.origin.toUpperCase()} - ${target.destination.toUpperCase()} icin oBilet'ten sefer cekilemedi.`;
      } else {
        // (b) Veri var ama filtre tutmadi.
        const scrapedSample = Array.from(scrapedOperators).slice(0, 12).join(", ");
        const selectedSample = acceptAllOperators ? "tum firmalar" : targetOperators.join(", ");
        statusMsg = `Secilen firmalar bu hatta sefer islemiyor olabilir.${stopDetail} Secili firmalar: ${selectedSample}. Bu hatta bulunan firmalar: ${scrapedSample}${scrapedOperators.size > 12 ? "..." : ""}.`;
        notifMsg = `${target.origin.toUpperCase()} - ${target.destination.toUpperCase()}: Secili firmalar bulunamadi. Hatta gozuken: ${scrapedSample}.`;
      }
      setObiletTargetSyncStatus(target.id, statusMsg);
      addPricingNotification("oBilet Fiyat Takip", notifMsg);
    }
    
    if (changes.length > 0) {
      console.log(`[Takip Görevi] ${changes.length} fiyat değişikliği tespit edildi.`);
    } else {
      console.log(`[Takip Görevi] Fiyat değişikliği yok: ${target.origin} - ${target.destination}`);
    }

    const emails = parseCsvList(target.email_notifications);
    let mailWarning = "";
    if (emails.length > 0) {
      // Mail SADECE fiyat degisikligi olunca atilir (kullanici tercihi: sade mod).
      // "always" modu env ile yine acilabilir ama varsayilan kapali.
      const shouldSendEmail = changes.length > 0 || OBILET_EMAIL_MODE === "always";

      if (!shouldSendEmail) {
        console.log(`[Takip Görevi] Fiyat değişikliği yok, mail atlanıyor.`);
      } else {
        console.log(`[Takip Görevi] Mail gönderiliyor (${changes.length} fiyat değişikliği)`);
        try {
          await sendObiletCycleStatusEmail(emails, target, trackedJourneys, changes);
          db.prepare("UPDATE obilet_targets SET last_email_sent_at = ? WHERE id = ?").run(now, target.id);
          console.log(`[Takip Görevi] Mail gönderildi: ${emails.join(", ")}`);
        } catch (mailError) {
          mailWarning = " E-posta gonderilemedi: " + mailError.message;
          console.error(`[Takip Görevi] Mail hatasi (${target.origin} -> ${target.destination}): ${mailError.message}`);
          addPricingNotification("oBilet Fiyat Takip", `${target.origin.toUpperCase()} - ${target.destination.toUpperCase()} rapor maili gönderilemedi: ${mailError.message}`);
        }
      }
    } else {
      console.log(`[Takip Görevi] Mail adresi tanimli degil: ${target.origin} -> ${target.destination}`);
    }

    // Telegram bildirimi — sadece fiyat degisikligi oldugunda. Hata olsa bile akisi bozmaz.
    if (changes.length > 0 && TELEGRAM_ENABLED) {
      try {
        await sendObiletTelegramAlert(target, changes);
        console.log(`[Takip Görevi] Telegram bildirimi gönderildi (${changes.length} değişiklik).`);
      } catch (tgErr) {
        console.error(`[Takip Görevi] Telegram hatasi (${target.origin} -> ${target.destination}): ${tgErr.message}`);
      }
    }

    if (trackedJourneys.length > 0) {
      const changeText = changes.length > 0 ? `${changes.length} fiyat degisti` : "Degisiklik yok";
      setObiletTargetSyncStatus(
        target.id,
        `Basarili. ${dateList.length} gun tarandi, ${trackedJourneys.length} sefer izlendi, ${changeText}.${mailWarning}`
      );
    }
  } catch (err) {
    console.error(`[Takip Görevi] Hata oluştu (${target.origin} -> ${target.destination}): ${err.message}`);
    setObiletTargetSyncStatus(target.id, `Hata: ${err.message}`);
  }
}

// Job lock değişkeni (aynı anda birden fazla task çalışmasını engeller).
// Plus manuel istek kuyrugu: kullanicilar ayni hattı tekrar tekrar tetiklerse veya
// auto task surerken manuel basarsa kuyrukta beklerler.
let obiletTaskRunning = false;
const obiletPendingManualTargets = new Set(); // target.id'leri — ayni hatta duplicate basmayi engeller
let obiletPendingFullRefresh = false;          // "tum hatlari yenile" tek bir kuyrukta tutulur
const obiletPriorityQueue = [];                // Anlik Tara: siranin ONUNE gecen hatlar (paralel degil!)

// Acquire / release helpers: lock'u atomic almak icin.
async function acquireObiletLock(timeoutMs = 120000) {
  const start = Date.now();
  while (obiletTaskRunning) {
    if (Date.now() - start > timeoutMs) return false;
    await new Promise(r => setTimeout(r, 1000));
  }
  obiletTaskRunning = true;
  return true;
}
function releaseObiletLock() {
  obiletTaskRunning = false;
}

// ===================== AG & KAPASITE DIFF (yapisal degisiklik tespiti) =====================
// FIYAT/DEGISIKLIK/TELEGRAM mantigina DOKUNMAZ. Her taramada bir rotanin yapi fotografini alir,
// onceki tur ile kiyaslar; yeni rakip / yeni-kalkan sefer / kapasite / ek arac / saat kaydirmasini
// obilet_structure_changes'a yazar (sag-ust sticky bildirim buradan beslenir).
const STRUCTURE_DIFF_ENABLED = true;

// HH:MM -> dakika.
function hhmmToMin(t) {
  const m = String(t || "").match(/^(\d{2}):(\d{2})$/);
  return m ? parseInt(m[1], 10) * 60 + parseInt(m[2], 10) : -1;
}

// Ham sefer listesinden yapi fotografi: { ops: { [firma]: { [saat]: {seats, buses} } }, hasBusInfo }.
function buildStructureSnapshot(journeys) {
  const ops = {};
  let hasBusInfo = false;
  for (const j of (journeys || [])) {
    const op = String(normalizeObiletOperatorName(j.operator) || j.operator || "").trim();
    const t = String(j.time || "").trim();
    if (!op || !/^\d{2}:\d{2}$/.test(t)) continue;
    if (!ops[op]) ops[op] = {};
    const seats = Number(j.totalSeats);
    const buses = Number(j.busCount);
    if (Number.isFinite(buses)) hasBusInfo = true; // busCount yalnizca XHR path'te var (DOM fallback'te yok)
    const prev = ops[op][t];
    ops[op][t] = {
      seats: Math.max(Number.isFinite(seats) && seats > 0 ? seats : 0, prev ? prev.seats : 0),
      buses: Math.max(Number.isFinite(buses) && buses > 0 ? buses : 1, prev ? prev.buses : 1),
    };
  }
  return { ops, hasBusInfo };
}

// Fotograftaki toplam sefer (firma×saat) sayisi — kismi-tarama korumasi icin.
function countSnapshotDepartures(snap) {
  let n = 0;
  const ops = (snap && snap.ops) || {};
  for (const op of Object.keys(ops)) n += Object.keys(ops[op]).length;
  return n;
}

// Iki fotograf arasi yapisal farklar. [{type, operator, message}]
function computeStructureDiff(prev, cur, routeLabel) {
  const changes = [];
  const prevOps = (prev && prev.ops) || {};
  const curOps = (cur && cur.ops) || {};
  const prevNames = new Set(Object.keys(prevOps));
  const curNames = new Set(Object.keys(curOps));

  for (const op of curNames) if (!prevNames.has(op)) {
    changes.push({ type: "new_operator", operator: op, message: `${routeLabel}: ${op} bu hatta GİRDİ (yeni rakip)` });
  }
  for (const op of prevNames) if (!curNames.has(op)) {
    changes.push({ type: "removed_operator", operator: op, message: `${routeLabel}: ${op} bu hattan ÇEKİLDİ` });
  }

  for (const op of curNames) {
    if (!prevNames.has(op)) continue; // yeni firmayi ustte bildirdik
    const pT = prevOps[op] || {}, cT = curOps[op] || {};
    const added = Object.keys(cT).filter((t) => !pT[t]);
    const removed = Object.keys(pT).filter((t) => !cT[t]);

    // Saat kaydirma: kaldirilan + eklenen ±25 dk icinde eslesiyorsa "kaydirma".
    const usedAdd = new Set(), usedRem = new Set();
    for (const rt of removed) {
      let best = null, bestDiff = 26;
      for (const at of added) {
        if (usedAdd.has(at)) continue;
        const d = Math.abs(hhmmToMin(at) - hhmmToMin(rt));
        if (d > 0 && d <= 25 && d < bestDiff) { best = at; bestDiff = d; }
      }
      if (best) { usedRem.add(rt); usedAdd.add(best);
        changes.push({ type: "time_shift", operator: op, message: `${routeLabel}: ${op} ${rt} seferini ${best}'e KAYDIRDI` });
      }
    }
    for (const at of added) if (!usedAdd.has(at)) {
      changes.push({ type: "new_departure", operator: op, message: `${routeLabel}: ${op} ${at} seferi EKLEDİ` });
    }
    for (const rt of removed) if (!usedRem.has(rt)) {
      changes.push({ type: "removed_departure", operator: op, message: `${routeLabel}: ${op} ${rt} seferini KALDIRDI` });
    }
    // Ek arac (ayni saatte 2. otobus). NOT: "kapasite artti/azaldi" (koltuk sayisi 1-3 oynamasi) KALDIRILDI —
    // oBilet total-seats degeri tur tur ufak dalgalaniyordu, gercek kapasite degisimi degil, gurultu.
    for (const t of Object.keys(cT)) {
      if (!pT[t]) continue;
      const pb = pT[t].buses, cb = cT[t].buses;
      // Ek arac SADECE iki fotograf da busCount tasiyorsa (XHR path) — DOM fallback'te busCount=1 varsayilir,
      // DOM->XHR gecisi yanlis "ek arac" uretmesin diye.
      if (cb > pb && prev && prev.hasBusInfo && cur && cur.hasBusInfo) {
        changes.push({ type: "extra_bus", operator: op, message: `${routeLabel}: ${op} ${t} seferine EK ARAÇ koydu (${cb} otobüs)` });
      }
    }
  }
  return changes;
}

// Bir gunun ham seferlerinden yapi fotografini al, onceki tur ile kiyasla, farklari kaydet.
function captureStructureSnapshot(target, journeyDate, journeys) {
  if (!STRUCTURE_DIFF_ENABLED) return;
  if (!Array.isArray(journeys) || journeys.length === 0) return; // bos/basarisiz tarama -> dokunma
  const cur = buildStructureSnapshot(journeys);
  const curCount = Object.keys(cur.ops).length;
  if (curCount === 0) return;
  const routeLabel = `${String(target.origin || "").trim()} → ${String(target.destination || "").trim()}`;
  const now = nowStamp();

  const row = db.prepare("SELECT snapshot_json FROM obilet_route_structure WHERE target_id=? AND journey_date=?").get(target.id, journeyDate);
  let prev = null;
  if (row && row.snapshot_json) { try { prev = JSON.parse(row.snapshot_json); } catch (e) { prev = null; } }

  if (prev && prev.ops) {
    // KISMI/BASARISIZ TARAMA KORUMASI: toplam SEFER sayisi oncekinin %60'indan azsa DIFF ETME + fotografi BOZMA.
    // Ani buyuk kayip (Cloudflare'in eksik dondurdugu tur) = scrape hatasi, gercek tarife degisikligi degil.
    // Sefer-sayisi bazli (operator-sayisi degil) — boylece "2 firmadan 1'i dustu" veya "duraklar inceldi" de yakalanir.
    const prevDep = countSnapshotDepartures(prev), curDep = countSnapshotDepartures(cur);
    if (prevDep > 0 && curDep < prevDep * 0.6) {
      console.log(`[Yapi Diff] ${routeLabel} kismi tarama (${curDep}/${prevDep} sefer) — atlandi.`);
      return;
    }
    const diffs = computeStructureDiff(prev, cur, routeLabel);
    if (diffs.length) {
      const ins = db.prepare("INSERT INTO obilet_structure_changes (target_id, journey_date, route_label, change_type, operator, message, detected_at, is_read) VALUES (?,?,?,?,?,?,?,0)");
      const insMany = db.transaction((list) => { for (const c of list) ins.run(target.id, journeyDate, routeLabel, c.type, c.operator || "", c.message, now); });
      insMany(diffs);
      console.log(`[Yapi Diff] ${routeLabel}: ${diffs.length} yapisal degisiklik kaydedildi.`);
    }
  }
  // Fotografi guncelle (ilk kez -> sadece kaydet, diff yok; normal tur -> tazele).
  db.prepare(`INSERT INTO obilet_route_structure (target_id, journey_date, snapshot_json, updated_at) VALUES (?,?,?,?)
    ON CONFLICT(target_id, journey_date) DO UPDATE SET snapshot_json=excluded.snapshot_json, updated_at=excluded.updated_at`)
    .run(target.id, journeyDate, JSON.stringify(cur), now);
}

// oBilet Fiyat Senkronizasyonu Arka Plan Görevi (Tüm Hatlar)
async function refreshObiletPricesTask() {
  // Eğer başka bir task çalışıyorsa atla
  if (obiletTaskRunning) {
    console.log(`[Takip Görevi] Zaten bir task çalışıyor, bu çalıştırma atlanıyor.`);
    return;
  }
  
  obiletTaskRunning = true;
  try {
    console.log(`[Takip Görevi] oBilet fiyat kontrolü başlatılıyor: ${nowStamp()}`);
    
    // EN UZUN SUREDIR TARANMAYAN HAT ONCE. Boylece surec restart olsa (deploy/crash) bile
    // yarim kalan turdaki atlanan/eski hatlar bir sonraki turda ILK sirada taranir — hicbir hat ac kalmaz.
    // last_sync_at "DD.MM.YYYY HH:mm:ss" -> kronolojik sortable hale cevrilir; bos (hic taranmamis) en basta.
    const targets = db.prepare(`
      SELECT * FROM obilet_targets WHERE is_active = 1
      ORDER BY (
        substr(last_sync_at, 7, 4) || substr(last_sync_at, 4, 2) ||
        substr(last_sync_at, 1, 2) || substr(last_sync_at, 12, 8)
      ) ASC, id ASC
    `).all();
    if (targets.length === 0) {
      console.log("[Takip Görevi] Aktif takip edilecek hat bulunamadi.");
      return;
    }
    
    // Priority-farkinda dongu: her adimda once Anlik Tara kuyrugunu bosalt (siranin onune gecer),
    // sonra sirasi gelen normal hatti isle. Boylece Anlik Tara PARALEL degil, kuyrukta oncelikli.
    const processedIds = new Set();
    let idx = 0;
    let firstTarget = true;
    while (idx < targets.length || obiletPriorityQueue.length > 0) {
      let target = null;
      if (obiletPriorityQueue.length > 0) {
        const pid = obiletPriorityQueue.shift();
        if (processedIds.has(pid)) continue;
        target = db.prepare("SELECT * FROM obilet_targets WHERE id = ? AND is_active = 1").get(pid);
        if (target) console.log(`[Takip Görevi] Öncelikli hat sıranın önüne alındı: ${target.origin} -> ${target.destination}`);
      } else {
        while (idx < targets.length && processedIds.has(targets[idx].id)) idx++;
        if (idx >= targets.length) break;
        target = targets[idx];
        idx++;
      }
      if (!target || processedIds.has(target.id)) continue;
      processedIds.add(target.id);

      // Cloudflare bot korumasını önlemek için hatlar arası 20 sn bekleme (ilk hat haric).
      if (!firstTarget) {
        console.log(`[Takip Görevi] Cloudflare koruması için 20 saniye bekleniyor...`);
        await new Promise(resolve => setTimeout(resolve, 20000));
      }
      firstTarget = false;
      await processObiletTarget(target);
    }

    console.log(`[Takip Görevi] Kontroller tamamlandı: ${nowStamp()}`);
  } finally {
    obiletTaskRunning = false;
  }
}

// API: Takip Listesini Getir
app.get("/api/obilet/targets", requireAuth, (req, res) => {
  try {
    const targets = db.prepare("SELECT * FROM obilet_targets ORDER BY id DESC").all();
    res.json({ ok: true, targets });
  } catch (error) {
    res.status(500).json({ message: error.message || "Liste alinamadi." });
  }
});

// API: Türkiye geneli firma listesi (secmeli alan icin)
app.get("/api/obilet/operators", requireAuth, (req, res) => {
  try {
    // Suffix-aware base slug: "Mersin Nur" ve "Mersin Nur Turizm" ayni anahtarda toplanir.
    // turizm/seyahat/tur/vip/luxury gibi yaygin sonekler eslestirmede dikkate alinmaz.
    const baseSlug = (name) => {
      const noiseWords = /\b(turizm|seyahat|tur|vip|luxury|ekspres|otobus|nakliyat|seferleri|gokbey)\b/gi;
      return slugTr(name)
        .replace(noiseWords, "")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/\s+/g, "-");
    };

    // slug -> { name, fromCatalog } eslemesi. Catalog'taki versiyon DAİMA korunur,
    // DB'den gelen ayni slug zaten varsa atilir (Mersin Nur Turizm dedup edilir).
    const bySlug = new Map();

    const tryAdd = (rawName, fromCatalog) => {
      const normalized = normalizeObiletOperatorName(rawName);
      if (!normalized) return;
      const key = baseSlug(normalized);
      if (!key) return;
      const existing = bySlug.get(key);
      // Catalog'tan gelen DB'den gelene üstün gelir; iki catalog girisi varsa ilki kalir.
      if (!existing || (fromCatalog && !existing.fromCatalog)) {
        bySlug.set(key, { name: normalized, fromCatalog });
      }
    };

    for (const name of OBILET_OPERATOR_CATALOG) tryAdd(name, true);

    const priceRows = db.prepare("SELECT DISTINCT operator FROM obilet_prices WHERE TRIM(operator) <> ''").all();
    for (const row of priceRows) tryAdd(row.operator, false);

    const targetRows = db.prepare("SELECT operators FROM obilet_targets WHERE TRIM(operators) <> ''").all();
    for (const row of targetRows) {
      for (const item of parseCsvList(row.operators)) tryAdd(item, false);
    }

    const operators = Array.from(bySlug.values())
      .map((entry) => entry.name)
      .sort((a, b) => a.localeCompare(b, "tr-TR"));
    res.json({ ok: true, operators });
  } catch (error) {
    res.status(500).json({ message: error.message || "Firma listesi alinamadi." });
  }
});

// API: Yeni Takip Ekle
app.post("/api/obilet/targets", requireAuth, (req, res) => {
  const origin = String(req.body.origin || "").trim();
  const destination = String(req.body.destination || "").trim();
  const date = String(req.body.date || "").trim(); // YYYY-MM-DD
  const endDate = String(req.body.endDate || "").trim(); // YYYY-MM-DD
  const departureStopFilter = String(req.body.departureStopFilter || "").trim();
  const operators = String(req.body.operators || "").trim();
  let emailNotifications = String(req.body.emailNotifications || "").trim();
  let routeId = String(req.body.routeId || req.body.route_id || "").trim();

  if (!origin || !destination || !date || !operators) {
    return res.status(400).json({ message: "Kalkis, Varis, Tarih ve Firmalar alanlari zorunludur." });
  }

  const normalizedEndDate = endDate || date;
  if (!isIsoDate(date) || !isIsoDate(normalizedEndDate)) {
    return res.status(400).json({ message: "Tarih formati gecersiz. YYYY-MM-DD kullanin." });
  }
  if (normalizedEndDate < date) {
    return res.status(400).json({ message: "Bitis tarihi baslangic tarihinden once olamaz." });
  }
  const rangeDays = buildIsoDateRange(date, normalizedEndDate).length;
  if (rangeDays > 45) {
    return res.status(400).json({ message: "Tarih araligi en fazla 45 gun olabilir." });
  }

  // route_id format validasyonu (varsa)
  if (routeId && !/^\d+-\d+$/.test(routeId)) {
    return res.status(400).json({ message: "oBilet Route ID formati gecersiz. Ornek: 595-356" });
  }

  // route_id verilmediyse statik tablodan otomatik bul
  if (!routeId) {
    const auto = buildObiletRouteIdLocal(origin, destination);
    if (auto) {
      routeId = auto;
      console.log(`[oBilet] route_id otomatik atandi: ${origin}->${destination} = ${routeId}`);
    }
  }

  // E-posta boşsa varsayılanı kullan
  if (!emailNotifications) {
    emailNotifications = String(process.env.DEFAULT_NOTIF_EMAIL || "").trim();
  }

  // Hatti ekleyen kullanici (audit icin)
  const createdBy = String(req.auth?.user?.username || "").trim();

  try {
    const result = db.prepare(
      "INSERT INTO obilet_targets (origin, destination, date, end_date, departure_stop_filter, operators, email_notifications, telegram_chat_ids, route_id, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).run(origin, destination, date, normalizedEndDate, departureStopFilter, operators, emailNotifications, "", routeId, createdBy, nowStamp());

    // Ekleme işleminden sonra fiyatları hemen çekmek için arka planda tetikle
    setTimeout(() => {
      refreshObiletPricesTask().catch(() => null);
    }, 1000);

    res.json({ ok: true, id: result.lastInsertRowid, routeId, createdBy });
  } catch (error) {
    res.status(500).json({ message: error.message || "Kayit basarisiz." });
  }
});

// API: TUM hatlarin tarih araligini toplu guncelle
app.post("/api/obilet/targets/bulk-dates", requireAuth, (req, res) => {
  const date = String(req.body.date || "").trim();
  const endDate = String(req.body.endDate || "").trim();
  if (!date) return res.status(400).json({ message: "Baslangic tarihi zorunlu." });
  const normalizedEndDate = endDate || date;
  if (!isIsoDate(date) || !isIsoDate(normalizedEndDate)) {
    return res.status(400).json({ message: "Tarih formati gecersiz. YYYY-MM-DD kullanin." });
  }
  if (normalizedEndDate < date) {
    return res.status(400).json({ message: "Bitis tarihi baslangic tarihinden once olamaz." });
  }
  if (buildIsoDateRange(date, normalizedEndDate).length > 45) {
    return res.status(400).json({ message: "Tarih araligi en fazla 45 gun olabilir." });
  }
  try {
    const info = db.prepare("UPDATE obilet_targets SET date = ?, end_date = ? WHERE is_active = 1").run(date, normalizedEndDate);
    setTimeout(() => { refreshObiletPricesTask().catch(() => null); }, 1000);
    res.json({ ok: true, updated: info.changes, date, endDate: normalizedEndDate });
  } catch (error) {
    res.status(500).json({ message: error.message || "Toplu guncelleme basarisiz." });
  }
});

// API: Takip Hattını Düzenle (tarih, firmalar, durak filtresi, e-posta, aktiflik, route_id)
app.patch("/api/obilet/targets/:id", requireAuth, (req, res) => {
  const id = parseInt(req.params.id, 10);
  const existing = db.prepare("SELECT * FROM obilet_targets WHERE id = ?").get(id);
  if (!existing) return res.status(404).json({ message: "Hat bulunamadi." });

  const pick = (v, fallback) => (v != null ? String(v).trim() : fallback);
  const origin = pick(req.body.origin, existing.origin);
  const destination = pick(req.body.destination, existing.destination);
  const date = pick(req.body.date, existing.date);
  const endDate = pick(req.body.endDate, existing.end_date || existing.date);
  const departureStopFilter = pick(req.body.departureStopFilter, existing.departure_stop_filter || "");
  const operators = pick(req.body.operators, existing.operators);
  let emailNotifications = pick(req.body.emailNotifications, existing.email_notifications || "");
  let routeId = pick(req.body.routeId != null ? req.body.routeId : req.body.route_id, existing.route_id || "");
  const isActive = req.body.isActive != null ? (req.body.isActive ? 1 : 0) : existing.is_active;

  if (!origin || !destination || !date || !operators) {
    return res.status(400).json({ message: "Kalkis, Varis, Tarih ve Firmalar alanlari zorunludur." });
  }
  const normalizedEndDate = endDate || date;
  if (!isIsoDate(date) || !isIsoDate(normalizedEndDate)) {
    return res.status(400).json({ message: "Tarih formati gecersiz. YYYY-MM-DD kullanin." });
  }
  if (normalizedEndDate < date) {
    return res.status(400).json({ message: "Bitis tarihi baslangic tarihinden once olamaz." });
  }
  if (buildIsoDateRange(date, normalizedEndDate).length > 45) {
    return res.status(400).json({ message: "Tarih araligi en fazla 45 gun olabilir." });
  }
  if (routeId && !/^\d+-\d+$/.test(routeId)) {
    return res.status(400).json({ message: "oBilet Route ID formati gecersiz. Ornek: 595-356" });
  }
  // Kalkis/varis degistiyse ve route_id elle verilmediyse otomatik yeniden bul.
  if ((origin !== existing.origin || destination !== existing.destination) && !req.body.routeId && !req.body.route_id) {
    const auto = buildObiletRouteIdLocal(origin, destination);
    routeId = auto || "";
  }

  try {
    db.prepare(
      "UPDATE obilet_targets SET origin = ?, destination = ?, date = ?, end_date = ?, departure_stop_filter = ?, operators = ?, email_notifications = ?, route_id = ?, is_active = ? WHERE id = ?"
    ).run(origin, destination, date, normalizedEndDate, departureStopFilter, operators, emailNotifications, routeId, isActive, id);

    setTimeout(() => { refreshObiletPricesTask().catch(() => null); }, 1000);
    res.json({ ok: true, id, routeId });
  } catch (error) {
    res.status(500).json({ message: error.message || "Guncelleme basarisiz." });
  }
});

// API: Takip Hattını Sil
app.delete("/api/obilet/targets/:id", requireAuth, (req, res) => {
  const id = Number(req.params.id);
  try {
    db.prepare("DELETE FROM obilet_targets WHERE id = ?").run(id);
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ message: error.message || "Silme basarisiz." });
  }
});

// API: Fiyat Detaylarını Getir
app.get("/api/obilet/prices/:targetId", requireAuth, (req, res) => {
  const targetId = Number(req.params.targetId);
  try {
    const prices = db.prepare("SELECT * FROM obilet_prices WHERE target_id = ? ORDER BY journey_date ASC, departure_time ASC").all(targetId);
    
    // Filter out past departure times (same journey_date, time < now)
    const futurePrices = prices.filter(p => isJourneyInFuture(p.journey_date, p.departure_time));
    
    const target = db
      .prepare("SELECT id, date, end_date, last_sync_status, last_sync_at FROM obilet_targets WHERE id = ?")
      .get(targetId);
    res.json({
      ok: true,
      prices: futurePrices,
      targetStatus: target
        ? {
            date: target.date || "",
            endDate: target.end_date || target.date || "",
            text: target.last_sync_status || "",
            at: target.last_sync_at || "",
          }
        : null,
    });
  } catch (error) {
    res.status(500).json({ message: error.message || "Fiyat detaylari alinamadi." });
  }
});

// API: Tüm hatlar için manuel yenileme (kuyruga al, duplicate engelle)
app.post("/api/obilet/refresh", requireAuth, async (req, res) => {
  try {
    if (obiletPendingFullRefresh) {
      return res.status(409).json({
        ok: false,
        queued: false,
        message: "Tum hatlar icin yenileme zaten kuyrukta. Lutfen tamamlanmasini bekleyin."
      });
    }

    obiletPendingFullRefresh = true;
    (async () => {
      try {
        const acquired = await acquireObiletLock(180000); // max 3 dk lock bekle
        if (!acquired) {
          console.warn("[Manuel Yenileme] Lock alinamadi (3 dk timeout). Iptal.");
          return;
        }
        try {
          // refreshObiletPricesTask kendi icinde de lock kontrolu yapıyor — direkt processObiletTarget'leri cagıralım
          const targets = db.prepare("SELECT * FROM obilet_targets WHERE is_active = 1").all();
          for (let i = 0; i < targets.length; i++) {
            await processObiletTarget(targets[i]);
            if (i < targets.length - 1) {
              await new Promise(r => setTimeout(r, 20000)); // Cloudflare cooldown
            }
          }
        } finally {
          releaseObiletLock();
        }
      } catch (error) {
        console.error("[Manuel Yenileme] Hata:", error.message);
      } finally {
        obiletPendingFullRefresh = false;
      }
    })();

    res.json({
      ok: true,
      queued: true,
      message: "İşleminiz sıraya alındı. Auto-task çalışıyorsa bitince işlenecek. Bütün hatlar 1-3 dakikada tamamlanır."
    });
  } catch (error) {
    res.status(500).json({ message: error.message || "Manuel yenileme tetiklenemedi." });
  }
});

// API: Tek Hat Manuel Güncelleme (kuyruga al, ayni hatta duplicate engelle)
app.post("/api/obilet/targets/:id/refresh", requireAuth, async (req, res) => {
  try {
    const targetId = parseInt(req.params.id, 10);
    if (!targetId) {
      return res.status(400).json({ message: "Gecersiz hat ID." });
    }

    const target = db.prepare("SELECT * FROM obilet_targets WHERE id = ?").get(targetId);
    if (!target) {
      return res.status(404).json({ message: "Hat bulunamadi." });
    }

    // Bu hat icin zaten kuyrukta bekleyen istek varsa, yenisini ekleme.
    if (obiletPendingManualTargets.has(targetId)) {
      return res.status(409).json({
        ok: false,
        queued: false,
        message: `${target.origin} - ${target.destination} hatti zaten kuyrukta. Lutfen tamamlanmasini bekleyin.`
      });
    }
    obiletPendingManualTargets.add(targetId);

    // Kullaniciya hemen "siraya alindi" cevabi don, arka planda lock al ve isle.
    (async () => {
      try {
        // Auto task veya baska bir tarama aktifse bitmesini bekle (max 3 dk).
        const acquired = await acquireObiletLock(180000);
        if (!acquired) {
          console.warn(`[Manuel Güncelleme] ${target.origin} -> ${target.destination}: lock 3 dk'da alinamadi, iptal.`);
          setObiletTargetSyncStatus(target.id, "Sira bekleme suresi doldu, lutfen tekrar deneyin.");
          return;
        }

        try {
          console.log(`[Manuel Güncelleme] ${target.origin} -> ${target.destination} basliyor...`);
          // 4 saniye nazik bekleme (Cloudflare burst tespitini onlemek icin)
          await new Promise(r => setTimeout(r, 4000));
          await processObiletTarget(target);
          console.log(`[Manuel Güncelleme] ${target.origin} -> ${target.destination} tamamlandı.`);
        } finally {
          releaseObiletLock();
        }
      } catch (error) {
        console.error(`[Manuel Güncelleme] Hata: ${error.message}`);
      } finally {
        obiletPendingManualTargets.delete(targetId);
      }
    })();

    const waitingInfo = obiletTaskRunning
      ? " Şu anda otomatik tarama çalışıyor, bitince işleyecek."
      : "";
    res.json({
      ok: true,
      queued: true,
      message: `İşleminiz sıraya alındı.${waitingInfo} Birkaç dakika içinde sonuç görünecek.`
    });
  } catch (error) {
    res.status(500).json({ message: error.message || "Manuel güncelleme tetiklenemedi." });
  }
});

// API: ADMIN ONCELIKLI TARAMA — sira/lock BEKLEMEZ, hemen bu hatti tarar (sadece admin).
// Mevcut kuyruk sistemine dokunmaz; bagimsiz, aninda calisir.
app.post("/api/obilet/targets/:id/priority-refresh", requireAuth, requireAdmin, async (req, res) => {
  try {
    const targetId = parseInt(req.params.id, 10);
    const target = db.prepare("SELECT * FROM obilet_targets WHERE id = ?").get(targetId);
    if (!target) return res.status(404).json({ message: "Hat bulunamadi." });

    // PARALEL DEGIL: hatti oncelik kuyruguna al. Tarama calisiyorsa siranin ONUNE gecer;
    // calismiyorsa hemen baslat. Boylece ayni anda iki tarama olmaz (Cloudflare'i tetiklemez).
    if (!obiletPriorityQueue.includes(target.id)) obiletPriorityQueue.push(target.id);

    let msg;
    if (obiletTaskRunning) {
      setObiletTargetSyncStatus(target.id, "Öncelikli sıraya alındı, çalışan taramanın önüne geçecek...");
      msg = `Öncelikli sıraya alındı: ${target.origin} - ${target.destination}. Çalışan tarama bu hattı hemen (bir sonraki adımda) işleyecek.`;
    } else {
      setObiletTargetSyncStatus(target.id, "Öncelikli tarama başlatılıyor...");
      setTimeout(() => { refreshObiletPricesTask().catch(() => null); }, 100);
      msg = `Öncelikli tarama başlatıldı: ${target.origin} - ${target.destination}. Birkaç dakikada sonuç görünecek.`;
    }
    console.log(`[Öncelikli Tarama] ${target.origin} -> ${target.destination} (admin: ${req.auth.user.username}) kuyruga alindi (calisan tarama: ${obiletTaskRunning}).`);
    res.json({ ok: true, message: msg });
  } catch (error) {
    res.status(500).json({ message: error.message || "Öncelikli tarama tetiklenemedi." });
  }
});

// Bir JSON icinde koltuk dizisini bul (seats / seat-list / data.seats vb.) — yapi bilinmedigi icin genel arama.
function findSeatArray(data) {
  const looksSeat = (arr) =>
    Array.isArray(arr) && arr.length >= 5 && typeof arr[0] === "object" && arr[0] &&
    /seat|koltuk|avail|number|no|status|gender|cinsiyet/i.test(Object.keys(arr[0]).join(","));
  const direct = [data?.data?.seats, data?.seats, data?.data?.["seat-list"], data?.["seat-list"],
    data?.data?.busSeats, data?.data?.["bus-seats"], Array.isArray(data?.data) ? data.data : null];
  for (const c of direct) if (looksSeat(c)) return c;
  let found = null;
  const walk = (o, depth) => {
    if (found || depth > 5 || !o || typeof o !== "object") return;
    for (const k of Object.keys(o)) {
      const v = o[k];
      if (looksSeat(v)) { found = v; return; }
      if (v && typeof v === "object") walk(v, depth + 1);
    }
  };
  walk(data, 0);
  return found;
}

// Bir koltugun DOLU olup olmadigini genel alan adlarindan cikar.
function seatIsSold(st) {
  const availRaw = st["is-available"] ?? st.available ?? st.availability ?? st["is-empty"] ?? st.isAvailable;
  const status = String(st.status ?? st.state ?? st.availability ?? "").toLowerCase();
  if (availRaw === true || availRaw === 1) return false;      // available -> bos
  if (availRaw === false || availRaw === 0) return true;      // not available -> dolu
  if (/avail|empty|bos|open|free|müsait|musait/.test(status)) return false;
  if (/sold|dolu|occupied|reserved|full|satil/.test(status)) return true;
  return null; // bilinmiyor
}

// GERCEK doluluk: /json/sefer/{id} yaniti { bus: "<svg ...>" } seklinde koltuk haritasini SVG olarak verir.
// GERCEK oBilet sinif semasi (canli teshis ile dogrulandi, 2026-07):
//   "available active [not-]single-seat" = BOS
//   "male [not-]single-seat"             = DOLU (erkek)
//   "female [not-]single-seat"           = DOLU (kadin)
//   "single-seats-warning warning ..."   = KOLTUK DEGIL (uyari ogesi) -> HARIC TUTULUR
// Onemli: hem "single-seat" hem "not-single-seat" GERCEK koltuktur (2+1'in tekli ve ikili tarafi).
// TOPLAM = dolu + bos (yalnizca gercek koltuklar; uyari/taninmayan ogeler sayilmaz -> yuzde dogru cikar).
// Kismi/eksik harita (is-partial-layout) veya anormal derecede az koltuk -> GUVENILMEZ, null doner
// (yanlis DUSUK doluluk yazip kullaniciyi yaniltmaktansa hic yazmamak dogru; onceki gercek deger korunur).
function countSeatsFromSeferJson(json) {
  const svg = (json && typeof json.bus === "string") ? json.bus
    : (json && json.data && typeof json.data.bus === "string" ? json.data.bus : "");
  if (!svg || !/single-seat/i.test(svg)) return null;
  let sold = 0, empty = 0, other = 0;
  for (const m of svg.matchAll(/class="([^"]*single-seat[^"]*)"/gi)) {
    const cls = m[1].toLowerCase();
    if (/\bavailable\b/.test(cls)) empty++;
    else if (/\b(male|female|erkek|kadin)\b/.test(cls)) sold++;
    else other++; // "single-seats-warning" vb. -> koltuk DEGIL, sayima katma
  }
  const total = sold + empty; // yalnizca gercek koltuklar
  if (total === 0) return null;

  const partial = json?.["is-partial-layout"] === true || json?.data?.["is-partial-layout"] === true;
  // Sehirlerarasi otobusler tipik 27-46 koltuk. total < 15 veya kismi duzen => eksik/bozuk harita.
  if (partial || total < 15) {
    if (DEBUG_OBILET_PRICE) console.log(`[Doluluk] Guvenilmez koltuk haritasi atlandi: dolu=${sold} bos=${empty} toplam=${total} partial=${partial}`);
    return null;
  }
  // Atanan otobus plakasi (varsa) — /json/sefer yanitinin ust seviyesinde "plate" alani. Kalkisa
  // yakin atanir; ileri tarihli seferlerde cogunlukla null gelir.
  const plateRaw = (json && json.plate != null) ? json.plate
    : (json && json.data && json.data.plate != null ? json.data.plate : null);
  const plate = plateRaw != null ? String(plateRaw).trim() : "";
  return { total, sold, empty, unknown: other, partial: false, plate };
}

// Bir seferin GERCEK koltuk sayimini /json/sefer/{id}'den al. oBilet bunu POST + json ister
// (GET HTML doner). Sayfa oturumunda (cf_bm cookie ile) fetch. Basarisizsa null.
// Cloudflare/gecici hataya karsi 2 deneme (arada bekleme).
async function fetchSeferRealSeats(page, seferId) {
  const doFetch = () => page.evaluate(async (sid) => {
    try {
      const r = await fetch(`/json/sefer/${sid}`, {
        method: "POST",
        headers: { "content-type": "application/json", "accept": "*/*", "x-requested-with": "XMLHttpRequest" },
        credentials: "include",
        body: "{}",
      });
      const ct = r.headers.get("content-type") || "";
      if (!ct.toLowerCase().includes("json")) return { __html: true };
      return await r.json();
    } catch (e) { return { __error: String(e && e.message || e) }; }
  }, seferId).catch((e) => ({ __error: String(e && e.message || e) }));

  // HIZ: 2 deneme + kisa backoff (700ms). Onceden 3 deneme + 1.5s/3s backoff idi -> tarama yavasliyordu.
  // Basarili cekim ZATEN ilk denemede doner (plaka + gercek doluluk gelir); yalnizca basarisizlikta 1 kez
  // daha (kisa) dener. Railway'de Cloudflare bir seferi KALICI engelliyorsa uc uc retry fayda vermiyordu,
  // sadece suru yiyordu -> kisaltildi. Boylece plakalar korunur ama tarama belirgin hizlanir.
  for (let attempt = 1; attempt <= 2; attempt++) {
    const json = await doFetch();
    if (json && !json.__error && !json.__html) {
      const c = countSeatsFromSeferJson(json);
      if (c) return c;
    }
    if (attempt < 2) await new Promise((r) => setTimeout(r, 700));
  }
  return null;
}

// Yakalanan GOVDE ile /json/sefer POST (tiklamasiz). Govde ilk seferin id'sini iceriyorsa
// hedef id ile degistirilir. Basarisizsa null.
async function fetchSeferSeatsWithBody(page, seferId, bodyTemplate, templateId) {
  const body = (templateId && bodyTemplate && String(bodyTemplate).includes(String(templateId)))
    ? String(bodyTemplate).split(String(templateId)).join(String(seferId))
    : (bodyTemplate || "{}");
  const doFetch = () => page.evaluate(async (sid, b) => {
    try {
      const r = await fetch(`/json/sefer/${sid}`, {
        method: "POST",
        headers: { "content-type": "application/json", "accept": "*/*", "x-requested-with": "XMLHttpRequest" },
        credentials: "include",
        body: b,
      });
      const ct = r.headers.get("content-type") || "";
      if (!ct.toLowerCase().includes("json")) return { __html: true };
      return await r.json();
    } catch (e) { return { __error: String(e && e.message || e) }; }
  }, seferId, body).catch(() => null);
  // Cloudflare throttle/challenge'a karsi 2 deneme (arada bekleme).
  for (let attempt = 1; attempt <= 2; attempt++) {
    const json = await doFetch();
    if (json && !json.__error && !json.__html) {
      const c = countSeatsFromSeferJson(json);
      if (c) return c;
    }
    if (attempt === 1) await new Promise((r) => setTimeout(r, 1800));
  }
  return null;
}

// Takip edilen operatorlerin seferlerine GERCEK doluluk ekle (koltuk haritasindan). journeys uzerine
// realSold/realTotal yazar. idsByKey: operator+saat -> tum listeleme id'leri (mukerrerde gecerli olani sec).
async function attachRealOccupancy(page, journeys, idsByKey, seatOperators, dateIso = null) {
  const REAL_SEATMAP_MAX_PER_CALL = 25; // tarih basi ust sinir (yuku/Cloudflare'i bounded tut)
  const allFirms = (seatOperators || []).includes("*"); // tum-firma hedefi: operator filtresi yok
  const wanted = allFirms ? [] : (seatOperators || []).map((o) => normalizeObiletOperatorName(o)).filter(Boolean);
  if (!allFirms && !wanted.length) return; // guvenlik: filtre yoksa cekme
  // YALNIZCA gelecek seferler — gecmis kalkanlar 25'lik cap slotunu bosa harcamasin (tarih verilmezse hepsi).
  const isFuture = (j) => !dateIso || isJourneyInFuture(dateIso, j.time);
  const targets = journeys
    .filter((j) => isFuture(j) && (allFirms || wanted.some((op) => isObiletOperatorMatch(op, j.operator))))
    .slice(0, REAL_SEATMAP_MAX_PER_CALL);
  let ok = 0;
  for (const j of targets) {
    const ids = idsByKey.get(j.matchKey) || [];
    let best = null;
    for (const id of ids) {
      const c = await fetchSeferRealSeats(page, id);
      // EN DOLU = EN AZ BOS (min available) — kullanicinin oBilet'te gordugu "Son X Koltuk" budur.
      // Kopya araclarda (ayni saatte >1 otobus) en dolu olani sec.
      if (c && (best == null || (c.total - c.sold) < (best.total - best.sold))) best = c;
      await new Promise((r) => setTimeout(r, 400)); // Cloudflare burst'u onlemek icin ara
    }
    if (best) {
      // KOPYA-REGRESYON KORUMASI: real yalnizca BASARILI cekilen id'ler arasindan sectigi icin, DOLU
      // kopyanin haritasi cekilemeyip (Cloudflare) BOS kopya cekilirse real bos araci gosterir ve dogru
      // (dolu) degeri ezer. Liste dolu araci biliyorsa (daha az bos) real'i YAZMA -> write liste-fullest'i
      // kullanir. YALNIZCA kopya seferlerde devreye girer; tekil seferde real her zaman gecerlidir.
      const isDup = ids.length > 1;
      const listReliable = j.seatInfoReliable === true && Number.isFinite(j.totalSeats) && Number.isFinite(j.availableSeats);
      const bestAvail = best.total - best.sold;
      if (isDup && listReliable && j.availableSeats < bestAvail) {
        if (DEBUG_OBILET_PRICE) console.log(`[Doluluk] ${j.operator} ${j.time}: KOPYA; real bos araci okudu (bos ${bestAvail}) > liste-fullest (bos ${j.availableSeats}) -> liste-fullest kullanilacak`);
      } else {
        j.realSold = best.sold;
        j.realTotal = best.total;
        j.realPlate = best.plate || ""; // atanan otobus plakasi (varsa) — occupancy ile birlikte yazilir
      }
      ok++;
      if (DEBUG_OBILET_PRICE) console.log(`[Doluluk] GERCEK ${j.operator} ${j.time}: ${best.sold}/${best.total} dolu (liste ${(j.totalSeats != null && j.availableSeats != null) ? j.totalSeats - j.availableSeats : "?"} diyordu)`);
    } else if (DEBUG_OBILET_PRICE) {
      console.log(`[Doluluk] ${j.operator} ${j.time}: GERCEK koltuk cekilemedi (2 deneme) -> "-" gosterilecek`);
    }
  }
  console.log(`[Doluluk] Gercek koltuk haritasi: ${ok}/${targets.length} sefer okundu.`);
}

// TEST/KESIF: Bir seferin GERCEK koltuk haritasini oBilet'ten cekip dolu/bos sayar; liste API'sindeki
// available-seats ile karsilastirir. Ilk calistirmada endpoint + koltuk yapisini KESFETMEK icin zengin loglar.
async function probeObiletSeatMap(routeId, dateIso, timeFilter = "", operatorFilter = "") {
  const browser = await getBrowserInstance();
  const page = await setupObiletPage(browser);
  const result = { apiUrls: [], seatEndpoints: [], listAvail: null, listTotal: null,
    soldReal: null, emptyReal: null, totalReal: null, clicked: "", matchedFirma: "", seferlerAtTime: [], note: "" };
  const seatResponses = [];
  const journeyItems = [];
  const opKey = String(operatorFilter || "").toLocaleLowerCase("tr-TR").replace(/\s+/g, " ").trim();

  // Bulunan endpoint'ler: /json/journeys/{route}/{date} (liste) ve /json/sefer/{id} (koltuk haritasi).
  page.on("response", async (response) => {
    try {
      const url = response.url();
      const ct = response.headers()["content-type"] || "";
      if (!/json/i.test(ct)) return;
      const short = url.split("?")[0];
      const text = await response.text().catch(() => "");
      if (!text || text.length < 2) return;
      let data; try { data = JSON.parse(text); } catch { return; }
      if (!result.apiUrls.includes(short)) result.apiUrls.push(short);

      // KOLTUK HARITASI: /json/sefer/{id}
      if (/\/json\/sefer\//i.test(url)) {
        const idm = url.match(/\/json\/sefer\/(\d+)/i);
        if (idm) result.clickedSeferId = idm[1];
        seatResponses.push({ url: short, data });
        console.log(`[SeatProbe] SEFER (koltuk) JSON yakalandi: ${short}`);
        console.log(`[SeatProbe] sefer JSON ust-anahtarlar: ${Object.keys(data || {}).join(", ")}`);
        if (data && data.data) console.log(`[SeatProbe] sefer.data ust-anahtarlar: ${Object.keys(data.data).join(", ")}`);
        console.log(`[SeatProbe] sefer JSON ornek (900 krkt): ${JSON.stringify(data).substring(0, 900)}`);
        return;
      }
      // LISTE: /json/journeys/...
      if (/\/json\/journeys\//i.test(url) || /"partner-name"|"available-seats"/.test(text.slice(0, 3000))) {
        const list = Array.isArray(data?.data) ? data.data
          : Array.isArray(data?.journeys) ? data.journeys
          : Array.isArray(data?.data?.journeys) ? data.data.journeys : [];
        if (list.length) {
          for (const it of list) journeyItems.push(it);
          console.log(`[SeatProbe] LISTE (journeys) JSON: ${list.length} sefer, ilk item anahtarlar: ${Object.keys(list[0] || {}).join(", ")}`);
        }
      }
    } catch {}
  });
  // Calisan /json/sefer isteginin method+header+GOVDE'sini logla (fetch'i birebir taklit icin).
  page.on("request", (req) => {
    try {
      if (/\/json\/sefer\//i.test(req.url())) {
        const body = req.postData();
        if (body != null) result.seferPostBody = body;
        console.log(`[SeatProbe] SEFER REQUEST ${req.method()} GOVDE: ${body != null ? String(body).substring(0, 400) : "(govde yok)"}`);
      }
    } catch {}
  });

  try {
    const pageUrl = `https://www.obilet.com/seferler/${routeId}/${dateIso}?t=${Date.now()}`;
    console.log(`[SeatProbe] Sayfa aciliyor: ${pageUrl} (saat filtresi: ${timeFilter || "ilk sefer"})`);
    await gotoAndHandleCloudflare(page, pageUrl);
    await new Promise(r => setTimeout(r, 4500)); // getbusjourneys ve render

    // Liste verisinden (journeyItems) bu saatteki TUM seferleri cikar: firma + id + koltuk.
    const itemInfo = (it) => {
      const dep = String(it?.journey?.departure || it?.departure || it?.["departure-time"] || it?.departureTime || it?.time || "");
      const tm = dep.match(/(\d{2}:\d{2})/);
      const g = (...ks) => { for (const k of ks) { const v = it?.[k] ?? it?.journey?.[k]; if (v != null) return v; } return null; };
      return {
        id: it?.id ?? it?.journey?.id,
        firma: String(g("partner-name", "partner_name", "partnerName", "company-name") || "").trim(),
        time: tm ? tm[1] : "",
        total: g("total-seats", "total_seats", "totalSeats"),
        avail: g("available-seats", "available_seats", "availableSeats"),
      };
    };
    const infos = journeyItems.map(itemInfo).filter((x) => x.time && x.id != null);
    const atTime = timeFilter ? infos.filter((x) => x.time === timeFilter) : infos;
    result.seferlerAtTime = atTime.map((x) => ({ firma: x.firma, time: x.time, avail: x.avail, total: x.total, id: x.id }));
    console.log(`[SeatProbe] ${timeFilter || "tum"} saatindeki seferler (${atTime.length}): ${atTime.map((x) => `${x.firma}[id=${x.id}, dolu=${(x.total != null && x.avail != null) ? x.total - x.avail : "?"}/${x.total}]`).join("  |  ") || "(liste bos — yapiyi asagidaki LISTE logundan gorecegiz)"}`);

    // Hedef seferi sec: saat + firma (varsa). Firma verilmediyse ilk sefer.
    let chosen = null;
    if (opKey) chosen = atTime.find((x) => x.firma.toLocaleLowerCase("tr-TR").includes(opKey) || opKey.includes(x.firma.toLocaleLowerCase("tr-TR")));
    if (!chosen) chosen = atTime[0] || infos[0] || null;
    if (chosen) {
      result.matchedFirma = chosen.firma;
      result.listTotal = chosen.total; result.listAvail = chosen.avail;
      console.log(`[SeatProbe] SECILEN firma: ${chosen.firma} ${chosen.time} — liste: ${(chosen.total != null && chosen.avail != null) ? chosen.total - chosen.avail : "?"}/${chosen.total}`);
    }

    // Koltuk haritasini TIKLAMA ile ac (fetch HTML donduruyor; tarayicinin kendi AJAX'i /json/sefer'i
    // getiriyor, response listener yakalar). Kart secimi: icinde KOLTUK/SEC butonu OLAN + saat iceren
    // + (firma verildiyse) firma eslesen kart. Filtre cubugu vb. elenir (koltuk butonu yok).
    result.clicked = await page.evaluate((t, firm) => {
      const hasSeatBtn = (el) => Array.from(el.querySelectorAll("button,a,span,div"))
        .some((b) => /koltuk|koltuğa|seç|^sec$|satın al|satin al/i.test((b.innerText || "").trim()));
      let cards = Array.from(document.querySelectorAll("div,li,article"))
        .filter((el) => (el.innerText || "").length < 700 && /\d{2}:\d{2}/.test(el.innerText || "") && hasSeatBtn(el));
      const matchFirm = (el) => {
        if (!firm) return true;
        const txt = (el.innerText || "").toLowerCase();
        if (txt.includes(firm)) return true;
        return Array.from(el.querySelectorAll("img")).some((im) =>
          ((im.getAttribute("alt") || "") + (im.getAttribute("title") || "")).toLowerCase().includes(firm));
      };
      const timed = t ? cards.filter((el) => (el.innerText || "").includes(t)) : cards;
      const target = (firm && timed.find(matchFirm)) || timed[0] || null;
      if (!target) return "uygun-kart-yok";
      // En kucuk (en ic) uygun karti sec — buyuk kapsayicilar yerine tek sefer karti.
      let best = target;
      const inner = cards.filter((c) => target.contains(c) && c !== target);
      for (const c of inner) if ((c.innerText || "").includes(t || "")) { best = c; break; }
      const btn = Array.from(best.querySelectorAll("button,a,span,div"))
        .find((b) => /koltuk|seç|^sec$|satın al|satin al/i.test((b.innerText || "").trim()));
      (btn || best).click();
      return (best.innerText || "").replace(/\s+/g, " ").trim().slice(0, 90);
    }, timeFilter, opKey).catch((e) => "click-hata:" + e.message);
    console.log(`[SeatProbe] Tiklanan kart: ${result.clicked}`);

    await new Promise(r => setTimeout(r, 7000)); // koltuk haritasi acilsin + /json/sefer AJAX gelsin

    // Yakalanan /json/sefer yanit(lar)ini SVG parser ile say.
    for (const sr of seatResponses) {
      const c = countSeatsFromSeferJson(sr.data);
      if (c) {
        console.log(`[SeatProbe] SVG say (${sr.url}): dolu=${c.sold} bos=${c.empty} toplam=${c.total} bilinmeyen=${c.unknown}`);
        if (result.soldRealSvg == null || c.sold > result.soldRealSvg) { result.soldRealSvg = c.sold; result.totalRealSvg = c.total; }
      }
    }
    if (result.soldRealSvg != null) {
      console.log(`[SeatProbe] >>> SVG SONUC: gercek ${result.soldRealSvg}/${result.totalRealSvg} dolu (liste bu firmada ${(result.listTotal != null && result.listAvail != null) ? result.listTotal - result.listAvail : "?"} diyordu)`);
    }

    // FETCH TESTI: yakalanan GOVDE ile POST fetch calisiyor mu? (bulk icin kritik)
    if (result.clickedSeferId) {
      const testBody = result.seferPostBody != null ? result.seferPostBody : "{}";
      const fetchTest = await page.evaluate(async (id, body) => {
        try {
          const r = await fetch(`/json/sefer/${id}`, {
            method: "POST",
            headers: { "content-type": "application/json", "accept": "*/*", "x-requested-with": "XMLHttpRequest" },
            credentials: "include",
            body: body,
          });
          const ct = r.headers.get("content-type") || "";
          const txt = await r.text();
          return { status: r.status, ct, isJson: ct.toLowerCase().includes("json"), sample: txt.substring(0, 120) };
        } catch (e) { return { err: String(e && e.message || e) }; }
      }, result.clickedSeferId, testBody).catch((e) => ({ err: String(e && e.message || e) }));
      console.log(`[SeatProbe] FETCH-TEST (govde ${result.seferPostBody != null ? "VAR:" + String(result.seferPostBody).substring(0, 80) : "YOK"}): status=${fetchTest.status} json=${fetchTest.isJson} ct=${fetchTest.ct} ornek=${fetchTest.sample || fetchTest.err}`);
    }

    // 1) ONCELIK: acilan koltuk haritasini DOM'dan oku. GERCEK koltuk hucreleri class'inda
    // "single-seat" ya da "not-single-seat" iceriyor (kesif logundan). Fiyat etiketleri/yapisal
    // elemanlar (dynamic-seat-price, s-seat, s-seat-n) HARIC. Dolu = male/female, Bos = available.
    const dom = await page.evaluate(() => {
      const all = Array.from(document.querySelectorAll('[class*="single-seat" i]'));
      let sold = 0, empty = 0, unknown = 0;
      const samples = [];
      for (const el of all) {
        const raw = el.className && el.className.baseVal !== undefined ? el.className.baseVal : el.className;
        const cls = String(raw || "").toLowerCase();
        const num = (el.textContent || "").trim();
        if (samples.length < 6) samples.push(`no=${num} cls="${cls}"`);
        if (/\b(male|female|erkek|kadin)\b/.test(cls)) sold++;      // cinsiyetli = DOLU
        else if (/\bavailable\b/.test(cls)) empty++;                 // available = BOS
        else unknown++;
      }
      return { count: all.length, sold, empty, unknown, samples };
    }).catch((e) => ({ error: e.message }));

    if (dom && dom.count) {
      result.totalReal = dom.sold + dom.empty; // gercek koltuk sayisi (bilinmeyenler haric)
      result.soldReal = dom.sold;
      result.emptyReal = dom.empty;
      console.log(`[SeatProbe] DOM koltuk (single-seat hucreleri): dolu=${dom.sold} bos=${dom.empty} bilinmeyen=${dom.unknown} (toplam eleman=${dom.count})`);
      console.log(`[SeatProbe] DOM ornek: ${(dom.samples || []).join("  |  ")}`);
    } else {
      console.log(`[SeatProbe] DOM'da single-seat hucresi bulunamadi (${dom && dom.error ? dom.error : "0"}).`);
    }

    // 2) YEDEK: koltuk-XHR yakalandiysa oradan da say (varsa).
    if (seatResponses.length) {
      const best = seatResponses[seatResponses.length - 1];
      const seats = findSeatArray(best.data);
      if (seats && seats.length) {
        let sold = 0, empty = 0, unknown = 0;
        for (const st of seats) { const v = seatIsSold(st); if (v === true) sold++; else if (v === false) empty++; else unknown++; }
        console.log(`[SeatProbe] XHR koltuk: toplam=${seats.length} dolu=${sold} bos=${empty} | ornek: ${JSON.stringify(seats[0]).substring(0, 200)}`);
        if (result.soldReal == null) { result.totalReal = seats.length; result.soldReal = sold; result.emptyReal = empty; }
      }
    }
    console.log(`[SeatProbe] Gorulen tum JSON endpoint'leri: ${result.apiUrls.join(" , ") || "(hicbiri)"}`);

    console.log(`[SeatProbe] === SONUC === Liste API: ${result.listAvail} bos / ${result.listTotal} toplam (=> ${result.listTotal != null && result.listAvail != null ? result.listTotal - result.listAvail : "?"} dolu)  |  GERCEK harita: ${result.soldReal} dolu, ${result.emptyReal} bos / ${result.totalReal} toplam`);
    console.log(`[SeatProbe] Gorulen API endpoint'leri: ${result.apiUrls.join(" , ")}`);
  } catch (e) {
    result.note = e.message;
    console.warn(`[SeatProbe] hata: ${e.message}`);
  } finally {
    try { await page.close(); } catch {}
  }
  return result;
}

// ADMIN TEST: bir hattin bir seferinin GERCEK koltuk haritasini cekip liste degeriyle karsilastir.
app.post("/api/obilet/targets/:id/seatmap-probe", requireAuth, requireAdmin, async (req, res) => {
  try {
    const target = db.prepare("SELECT * FROM obilet_targets WHERE id = ?").get(parseInt(req.params.id, 10));
    if (!target) return res.status(404).json({ message: "Hat bulunamadi." });
    const routeId = String(target.route_id || "").trim();
    if (!/^\d+-\d+$/.test(routeId)) return res.status(400).json({ message: "Bu hatta route_id yok, koltuk haritasi cekilemiyor." });
    const dateIso = String(req.query.date || target.date || "").trim();
    const time = String(req.query.time || "").trim();
    const operator = String(req.query.operator || "").trim();
    // NOT: obiletTaskRunning kilidine DOKUNMUYORUZ — calisan taramanin kilidini bozmamak icin.
    // Probe tek hafif sayfa acar (tek sefer), tam taramaya gore dusuk yuk; paralel calisabilir.
    const result = await probeObiletSeatMap(routeId, dateIso, time, operator);
    const listSold = (result.listTotal != null && result.listAvail != null) ? result.listTotal - result.listAvail : null;
    const gercek = result.soldRealSvg != null ? `${result.soldRealSvg} dolu / ${result.totalRealSvg}` : `${result.soldReal ?? "?"} dolu / ${result.totalReal ?? "?"} (DOM)`;
    res.json({
      ok: true,
      ozet: `Sefer: ${result.matchedFirma || operator || "?"} ${time || "(ilk)"}\nListe API: ${listSold ?? "?"} dolu / ${result.listTotal ?? "?"}  |  GERÇEK: ${gercek}`,
      seferler: result.seferlerAtTime || [],
      result,
    });
  } catch (e) {
    res.status(500).json({ message: e.message || "Koltuk testi basarisiz." });
  }
});

// Sefer Takip'te TEK SEFERIN gercek dolulugunu HEMEN cek + DB'ye yaz (source='gercek').
// Sefer Takip satirindaki butondan cagrilir (targetId + date + operator + time). TUM kullanicilar.
app.post("/api/obilet/seat-refresh", requireAuth, async (req, res) => {
  try {
    const targetId = parseInt(req.body?.targetId ?? req.query.targetId, 10);
    const target = db.prepare("SELECT * FROM obilet_targets WHERE id = ?").get(targetId);
    if (!target) return res.status(404).json({ message: "Hat bulunamadi." });
    const routeId = String(target.route_id || "").trim();
    if (!/^\d+-\d+$/.test(routeId)) return res.status(400).json({ message: "Bu hatta route_id yok, koltuk cekilemiyor." });
    const dateIso = String(req.body?.date ?? req.query.date ?? "").trim();
    const operator = String(req.body?.operator ?? req.query.operator ?? "").trim();
    const time = String(req.body?.time ?? req.query.time ?? "").trim();
    if (!dateIso || !time) return res.status(400).json({ message: "Tarih ve saat gerekli." });
    const ops = parseCsvList(target.operators).map(normalizeObiletOperatorName).filter(Boolean);
    const opKey = operator.toLocaleLowerCase("tr-TR").trim();
    const written = await occupancyForHatDate(target, routeId, ops, dateIso, opKey || null, time);
    const m = (written || []).find((w) => w.time === time) || (written || [])[0];
    if (m) {
      res.json({ ok: true, sold: m.sold, total: m.total, occ: m.occ, operator: m.operator, time: m.time });
    } else {
      res.json({ ok: false, message: "Bu seferin koltuk haritasi su an alinamadi, birazdan tekrar dene." });
    }
  } catch (e) {
    res.status(500).json({ message: e.message || "Kontrol basarisiz." });
  }
});

// API: oBilet sehir kodlari — listele (seed + ogrenilmiş hepsi tek listede)
app.get("/api/obilet/station-ids", requireAuth, (req, res) => {
  try {
    const learned = db.prepare("SELECT city_key, city_name, station_id, hits, last_seen FROM obilet_station_ids ORDER BY city_name ASC").all();
    const learnedKeys = new Set(learned.map(r => r.city_key));
    const seed = Object.entries(OBILET_STATION_IDS_SEED)
      .filter(([k]) => !learnedKeys.has(k))
      .map(([k, id]) => ({
        city_key: k,
        city_name: k.charAt(0).toUpperCase() + k.slice(1),
        station_id: id,
        hits: 0,
        last_seen: "",
        source: "seed",
      }));
    const learnedFmt = learned.map(r => ({ ...r, source: "ogrenilmis" }));
    const list = [...learnedFmt, ...seed].sort((a, b) => a.city_name.localeCompare(b.city_name, "tr-TR"));
    res.json({ ok: true, list });
  } catch (error) {
    res.status(500).json({ message: error.message || "Liste alinamadi." });
  }
});

// API: oBilet sehir kodu ekle veya guncelle
app.post("/api/obilet/station-ids", requireAuth, (req, res) => {
  const cityName = String(req.body.cityName || "").trim();
  const stationId = parseInt(req.body.stationId, 10);
  if (!cityName || !Number.isFinite(stationId) || stationId <= 0) {
    return res.status(400).json({ message: "Sehir adi ve gecerli bir station ID girin." });
  }
  try {
    learnObiletStationId(cityName, stationId);
    res.json({ ok: true, cityKey: cityKey(cityName), cityName, stationId });
  } catch (error) {
    res.status(500).json({ message: error.message || "Kayit basarisiz." });
  }
});

// API: oBilet sehir kodunu sil
app.delete("/api/obilet/station-ids/:cityKey", requireAuth, (req, res) => {
  const key = String(req.params.cityKey || "").trim().toLowerCase();
  if (!key) return res.status(400).json({ message: "Gecersiz sehir anahtari." });
  try {
    const result = db.prepare("DELETE FROM obilet_station_ids WHERE city_key = ?").run(key);
    res.json({ ok: true, deleted: result.changes });
  } catch (error) {
    res.status(500).json({ message: error.message || "Silme basarisiz." });
  }
});

// API: Bir hattin tum kayitli fiyatlarini sil (kayitli hat duruyor, sadece fiyatlar temizlenir)
// Eski yanlis fiyatlar DB'de kalmissa, bunu cagirip bir sonraki taramada temiz baslarsiniz.
// API: Fiyat degisiklik gecmisi (Raporlama sayfasi icin).
// Filtreler: hat (targetId), tarih araligi (from/to), arama (firma/sehir), limit.
// DOLULUK TAKIP — bir hattin (veya tum hatlarin) gelecek seferlerinin anlik doluluk durumu.
app.get("/api/obilet/occupancy", requireAuth, (req, res) => {
  try {
    const targetId = parseInt(req.query.targetId || "0", 10);
    const operator = String(req.query.operator || "").trim();
    const today = todayIsoInIstanbul();
    // 3 gunden eski doluluk kayitlarini temizle (yakin gecmis kalsin — Sefer Takip'te kalkis dolulugu).
    try { db.prepare("DELETE FROM obilet_occupancy WHERE journey_date < ?").run(shiftIsoDate(today, -10)); } catch (e) {}
    // Ayni sefer icin ESKI kopya doluluk satirlarini sil (durak yazimi degisince olusan mukerrer kayitlar);
    // her sefer icin sadece EN GUNCEL satir kalsin.
    try {
      db.exec(`
        DELETE FROM obilet_occupancy WHERE id IN (
          SELECT o1.id FROM obilet_occupancy o1 WHERE EXISTS (
            SELECT 1 FROM obilet_occupancy o2
             WHERE o2.target_id = o1.target_id AND o2.journey_date = o1.journey_date
               AND o2.operator = o1.operator AND o2.departure_time = o1.departure_time AND o2.id <> o1.id
               AND (substr(o2.last_updated,7,4)||substr(o2.last_updated,4,2)||substr(o2.last_updated,1,2)||substr(o2.last_updated,12,8))
                 > (substr(o1.last_updated,7,4)||substr(o1.last_updated,4,2)||substr(o1.last_updated,1,2)||substr(o1.last_updated,12,8))
          )
        )
      `);
    } catch (e) { /* yok say */ }

    // Firma dropdown'i icin: doluluk verisi olan tum firmalar (filtreden bagimsiz).
    const operators = db.prepare(
      "SELECT DISTINCT operator FROM obilet_occupancy WHERE journey_date >= ? AND operator != '' ORDER BY operator"
    ).all(today).map((r) => r.operator);

    const conditions = ["o.journey_date >= ?"];
    const params = [today];
    if (targetId) { conditions.push("o.target_id = ?"); params.push(targetId); }
    if (operator) { conditions.push("o.operator = ?"); params.push(operator); }
    const rows = db.prepare(`
      SELECT o.target_id, t.origin, t.destination, o.journey_date, o.operator, o.departure_time,
             o.departure_stop, o.total_seats, o.available_seats, o.occupancy_percent, o.plate, o.last_updated
        FROM obilet_occupancy o
        LEFT JOIN obilet_targets t ON t.id = o.target_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY o.occupancy_percent DESC, o.journey_date, o.departure_time
    `).all(...params);

    const count = rows.length;
    const avgOccupancy = count ? Math.round(rows.reduce((s, r) => s + (r.occupancy_percent || 0), 0) / count) : 0;
    const totalSeats = rows.reduce((s, r) => s + (r.total_seats || 0), 0);
    const soldSeats = rows.reduce((s, r) => s + ((r.total_seats || 0) - (r.available_seats || 0)), 0);
    const fullest = count ? rows[0] : null;
    const emptiest = count ? rows[rows.length - 1] : null;

    res.json({ ok: true, count, avgOccupancy, totalSeats, soldSeats, fullest, emptiest, operators, rows });
  } catch (error) {
    res.status(500).json({ message: error.message || "Doluluk alınamadı." });
  }
});

// ===================== PKM GUZERGAH FORMU EXPORT (Pkm Form Sure Hesaplama menusunden) =====================
// Kullanicinin gonderdigi PKM Talep Formu sablonunu (assets/pkm_form_template.xlsx) yukleyip duraklari (C),
// sureleri (E: seyahat+peron) ve kalkis saatini (G11) doldurur. Varis/kalkis saatleri, sira ve durak kodu
// SABLONUN KENDI FORMULLERI ile Excel acilinca otomatik hesaplanir. Fiyat/doluluk mantigindan bagimsiz.
app.post("/api/pkm-export", requireAuth, async (req, res) => {
  try {
    const ExcelJS = require("exceljs");
    const departureMin = Math.max(0, Math.min(1439, parseInt(req.body && req.body.departureMin, 10) || 0));
    const stops = Array.isArray(req.body && req.body.stops) ? req.body.stops : [];
    if (stops.length < 2) { res.status(400).json({ message: "En az 2 durak gerekli." }); return; }
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.readFile(path.join(__dirname, "assets", "pkm_form_template.xlsx"));
    try { wb.calcProperties = wb.calcProperties || {}; wb.calcProperties.fullCalcOnLoad = true; } catch (e) {}
    // Excel "onarim" uyarisini onle: exceljs, Tablo + otomatik-filtre tanimlarini round-trip'te
    // bozuyor -> kaldir. (Durak lookup'i VLOOKUP $C:$D araligini kullanir, tabloya bagimli degil;
    // hucre verisi aynen korunur, sadece tablo/filtre stilleri gider.)
    wb.eachSheet((sheet) => {
      try { if (sheet.tables) Object.keys(sheet.tables).forEach((tn) => { try { sheet.removeTable(tn); } catch (e) {} }); } catch (e) {}
      try { sheet.autoFilter = null; } catch (e) {}
    });
    const ws = wb.getWorksheet("GÜZERGAH TALEP");
    ws.getCell("C3").value = "Hasan HAZER"; // TALEP EDEN (AD & SOYAD) her zaman Hasan HAZER
    const LAST = 69;
    // Ornek veriyi temizle (GIDIS C/E, DONUS K/M) + formullerin BAYAT sonucunu dusur (Excel yeniden hesaplasin).
    for (let r = 11; r <= LAST; r++) {
      for (const col of ["C", "E", "K", "M"]) ws.getCell(`${col}${r}`).value = null;
      for (const col of ["A", "B", "G", "I", "J", "O"]) {
        const v = ws.getCell(`${col}${r}`).value;
        if (v && typeof v === "object" && v.formula) ws.getCell(`${col}${r}`).value = { formula: v.formula };
      }
    }
    ws.getCell("O11").value = null; ws.getCell("O12").value = null;
    ws.getCell("G11").value = departureMin / 1440; // kalkis saati (Excel serial = dk/1440)
    ws.getCell("G12").value = departureMin / 1440;
    stops.forEach((s, idx) => {
      const k = idx + 1;
      const name = String((s && s.name) || "").trim();
      if (k === 1) { ws.getCell("C11").value = name; ws.getCell("C12").value = name; return; }
      const rs = 9 + 2 * k, rp = 10 + 2 * k; // seyahat satiri / peron satiri
      if (rp > LAST) return; // sablon 69 satir -> ~29 duraga kadar
      ws.getCell(`C${rs}`).value = name; ws.getCell(`C${rp}`).value = name;
      ws.getCell(`E${rs}`).value = Math.max(0, Number(s && s.sey) || 0) / 1440; // seyahat suresi (dk->serial)
      ws.getCell(`E${rp}`).value = Math.max(0, Number(s && s.per) || 0) / 1440; // peron suresi
    });
    const buf = await wb.xlsx.writeBuffer();
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", 'attachment; filename="pkm-guzergah-plani.xlsx"');
    res.send(Buffer.from(buf));
  } catch (error) {
    res.status(500).json({ message: error.message || "Excel oluşturulamadı." });
  }
});

// ---- PKM kayitli guzergahlar (PAYLASIMLI: herkes gorur/acar) ----
app.get("/api/pkm-routes", requireAuth, (req, res) => {
  try {
    const rows = db.prepare("SELECT id, name, plan_json, created_by, created_at FROM pkm_routes ORDER BY id DESC").all();
    const routes = rows.map((r) => {
      let plan = {}; try { plan = JSON.parse(r.plan_json || "{}"); } catch (e) {}
      const stops = Array.isArray(plan.stops) ? plan.stops : [];
      return { id: r.id, name: r.name, created_by: r.created_by, created_at: r.created_at, stopCount: stops.length, first: stops[0] || "", last: stops[stops.length - 1] || "" };
    });
    res.json({ routes });
  } catch (error) { res.status(500).json({ message: error.message }); }
});
app.get("/api/pkm-routes/:id", requireAuth, (req, res) => {
  try {
    const row = db.prepare("SELECT id, name, plan_json, created_by, created_at FROM pkm_routes WHERE id = ?").get(req.params.id);
    if (!row) { res.status(404).json({ message: "Güzergah bulunamadı." }); return; }
    let plan = {}; try { plan = JSON.parse(row.plan_json || "{}"); } catch (e) {}
    res.json({ id: row.id, name: row.name, created_by: row.created_by, created_at: row.created_at, plan });
  } catch (error) { res.status(500).json({ message: error.message }); }
});
app.post("/api/pkm-routes", requireAuth, (req, res) => {
  try {
    const name = String((req.body && req.body.name) || "").trim().slice(0, 120);
    const plan = req.body && req.body.plan;
    if (!name) { res.status(400).json({ message: "Güzergah adı gerekli." }); return; }
    if (!plan || !Array.isArray(plan.stops) || plan.stops.length < 2) { res.status(400).json({ message: "Geçerli bir plan gerekli (en az 2 durak)." }); return; }
    const clean = {
      stops: plan.stops.map((s) => String(s).slice(0, 80)),
      legMin: (Array.isArray(plan.legMin) ? plan.legMin : []).map((n) => Math.max(0, Number(n) || 0)),
      peronMin: (Array.isArray(plan.peronMin) ? plan.peronMin : []).map((n) => Math.max(0, Number(n) || 0)),
      depMin: Math.max(0, Math.min(1439, Number(plan.depMin) || 0)),
    };
    const info = db.prepare("INSERT INTO pkm_routes (name, plan_json, created_by, created_at) VALUES (?, ?, ?, ?)")
      .run(name, JSON.stringify(clean), (req.auth.user && req.auth.user.username) || "", nowStamp());
    res.json({ ok: true, id: info.lastInsertRowid });
  } catch (error) { res.status(500).json({ message: error.message }); }
});
app.delete("/api/pkm-routes/:id", requireAuth, (req, res) => {
  try {
    const row = db.prepare("SELECT created_by FROM pkm_routes WHERE id = ?").get(req.params.id);
    if (!row) { res.status(404).json({ message: "Bulunamadı." }); return; }
    const isOwner = req.auth.user && req.auth.user.username === row.created_by;
    if (!isOwner && !(req.auth.user && req.auth.user.isAdmin)) { res.status(403).json({ message: "Sadece oluşturan veya admin silebilir." }); return; }
    db.prepare("DELETE FROM pkm_routes WHERE id = ?").run(req.params.id);
    res.json({ ok: true });
  } catch (error) { res.status(500).json({ message: error.message }); }
});

// ===================== TALEP RADARI API'LERI (otonom yogunluk analizi) =====================
const WEEKDAY_TR = ["Pazar", "Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi"];
function isoWeekdayTr(dateIso) {
  const m = String(dateIso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return { idx: -1, name: "" };
  const idx = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], 12, 0, 0)).getUTCDay();
  return { idx, name: WEEKDAY_TR[idx] };
}

// Ana analiz: gecmis olcumlerden her seferin EN GUNCEL dolulugunu alip yogun/bos gunleri cikarir.
app.get("/api/analysis/demand", requireAuth, requirePermission("demand"), (req, res) => {
  try {
    const today = todayIsoInIstanbul();
    let days = parseInt(req.query.days || String(ANALYSIS_HORIZON_DAYS), 10);
    if (!Number.isFinite(days) || days < 1) days = ANALYSIS_HORIZON_DAYS;
    days = Math.min(days, 60);
    const endIso = shiftIsoDate(today, days - 1);
    const routeRef = String(req.query.routeRef || "").trim();
    let high = parseInt(req.query.high || "80", 10); if (!Number.isFinite(high)) high = 80;
    let low = parseInt(req.query.low || "50", 10); if (!Number.isFinite(low)) low = 50;

    const cond = ["journey_date >= ?", "journey_date <= ?"];
    const params = [today, endIso];
    if (routeRef) { cond.push("route_ref = ?"); params.push(routeRef); }
    const raw = db.prepare(`
      SELECT route_ref, origin, destination, journey_date, operator, departure_time,
             total_seats, available_seats, occupancy_percent, measured_at, measured_at_iso
        FROM analysis_occupancy_history
       WHERE ${cond.join(" AND ")}
    `).all(...params);

    // Her sefer kimligi (route_ref|date|operator|time) icin EN GUNCEL olcumu al (append-only tablo).
    const latest = new Map();
    for (const r of raw) {
      const key = `${r.route_ref}|${r.journey_date}|${r.operator}|${r.departure_time}`;
      const prev = latest.get(key);
      if (!prev || String(r.measured_at_iso) > String(prev.measured_at_iso)) latest.set(key, r);
    }
    const seferler = [...latest.values()];

    const count = seferler.length;
    const withCap = seferler.filter((s) => (s.total_seats || 0) > 0);
    const avgOccupancy = count ? Math.round(seferler.reduce((a, s) => a + (s.occupancy_percent || 0), 0) / count) : 0;
    const highCount = seferler.filter((s) => (s.occupancy_percent || 0) >= high).length;
    const lowCount = withCap.filter((s) => (s.occupancy_percent || 0) < low).length;
    let lastMeasuredAt = "";
    for (const s of seferler) if (String(s.measured_at_iso) > lastMeasuredAt) lastMeasuredAt = String(s.measured_at_iso);

    // Rota bazli skor + her rota icin gun kirilimlari (hangi gun yogun/bos).
    const byRouteMap = new Map();
    for (const s of seferler) {
      if (!byRouteMap.has(s.route_ref)) byRouteMap.set(s.route_ref, { route_ref: s.route_ref, origin: s.origin, destination: s.destination, list: [] });
      byRouteMap.get(s.route_ref).list.push(s);
    }
    const routes = [...byRouteMap.values()].map((r) => {
      const c = r.list.length;
      const avg = c ? Math.round(r.list.reduce((a, s) => a + (s.occupancy_percent || 0), 0) / c) : 0;
      const dm = new Map();
      for (const s of r.list) {
        if (!dm.has(s.journey_date)) dm.set(s.journey_date, []);
        dm.get(s.journey_date).push(s.occupancy_percent || 0);
      }
      const perDate = [...dm.entries()].map(([date, arr]) => ({
        date, weekday: isoWeekdayTr(date).name,
        avg: Math.round(arr.reduce((a, v) => a + v, 0) / arr.length), seferler: arr.length,
      })).sort((a, b) => a.date.localeCompare(b.date));
      const byAvg = [...perDate].sort((a, b) => b.avg - a.avg);
      return {
        route_ref: r.route_ref, origin: r.origin, destination: r.destination,
        seferCount: c, avgOccupancy: avg,
        busiestDate: byAvg[0] || null, quietestDate: byAvg[byAvg.length - 1] || null,
        perDate,
      };
    }).sort((a, b) => b.avgOccupancy - a.avgOccupancy);

    // Genel: gelecek tarihlerin ortalama dolulugu (hangi gunler yogun).
    const dateMap = new Map();
    for (const s of seferler) {
      if (!dateMap.has(s.journey_date)) dateMap.set(s.journey_date, []);
      dateMap.get(s.journey_date).push(s.occupancy_percent || 0);
    }
    const byDate = [...dateMap.entries()].map(([date, arr]) => ({
      date, weekday: isoWeekdayTr(date).name,
      avg: Math.round(arr.reduce((a, v) => a + v, 0) / arr.length), seferler: arr.length,
    })).sort((a, b) => a.date.localeCompare(b.date));

    // Haftanin gunu deseni (Pazartesi-basli).
    const wdMap = new Map();
    for (const s of seferler) {
      const wd = isoWeekdayTr(s.journey_date);
      if (wd.idx < 0) continue;
      if (!wdMap.has(wd.idx)) wdMap.set(wd.idx, []);
      wdMap.get(wd.idx).push(s.occupancy_percent || 0);
    }
    const byWeekday = [1, 2, 3, 4, 5, 6, 0].map((i) => {
      const arr = wdMap.get(i) || [];
      return { idx: i, name: WEEKDAY_TR[i], avg: arr.length ? Math.round(arr.reduce((a, v) => a + v, 0) / arr.length) : null, seferler: arr.length };
    });

    // En yogun / en bos seferler.
    const enriched = seferler.map((s) => ({
      route_ref: s.route_ref, origin: s.origin, destination: s.destination,
      journey_date: s.journey_date, weekday: isoWeekdayTr(s.journey_date).name,
      operator: s.operator, departure_time: s.departure_time,
      total_seats: s.total_seats || 0, sold: (s.total_seats || 0) - (s.available_seats || 0),
      occupancy_percent: s.occupancy_percent || 0, measured_at: s.measured_at,
    }));
    const topFull = [...enriched].sort((a, b) => b.occupancy_percent - a.occupancy_percent).slice(0, 25);
    const topEmpty = [...enriched].filter((s) => s.total_seats > 0).sort((a, b) => a.occupancy_percent - b.occupancy_percent).slice(0, 25);

    res.json({
      ok: true, today, days, high, low,
      summary: { count, avgOccupancy, highCount, lowCount, routeCount: routes.length, lastMeasuredAt },
      routes, byDate, byWeekday, topFull, topEmpty,
      workerRunning: analysisWorkerRunning,
    });
  } catch (error) {
    res.status(500).json({ message: error.message || "Analiz alınamadı." });
  }
});

// Rota havuzu listesi + kapsam (her rota kac sefer olculdu, en son ne zaman).
app.get("/api/analysis/routes", requireAuth, requirePermission("demand"), (req, res) => {
  try {
    const today = todayIsoInIstanbul();
    const rows = db.prepare("SELECT * FROM analysis_routes ORDER BY origin, destination").all();
    const covStmt = db.prepare(`
      SELECT COUNT(DISTINCT journey_date || '|' || operator || '|' || departure_time) AS seferler,
             MAX(measured_at_iso) AS last_measured
        FROM analysis_occupancy_history
       WHERE route_ref = ? AND journey_date >= ?
    `);
    const out = rows.map((r) => {
      const cov = covStmt.get(r.route_id, today) || {};
      return { ...r, coverage: cov.seferler || 0, lastMeasured: cov.last_measured || "" };
    });
    res.json({ ok: true, routes: out, workerRunning: analysisWorkerRunning });
  } catch (error) {
    res.status(500).json({ message: error.message || "Rotalar alınamadı." });
  }
});

// Havuza yeni rota ekle (sehir cifti; route_id otomatik cozulur).
app.post("/api/analysis/routes", requireAuth, requirePermission("demand"), (req, res) => {
  try {
    const origin = String(req.body?.origin || "").trim();
    const destination = String(req.body?.destination || "").trim();
    if (!origin || !destination) return res.status(400).json({ message: "Kalkis ve varis zorunlu." });
    const rid = buildObiletRouteIdLocal(origin, destination) || "";
    db.prepare(`
      INSERT INTO analysis_routes (origin, destination, route_id, is_active, is_seed, created_at)
      VALUES (?, ?, ?, 1, 0, ?)
      ON CONFLICT(origin, destination) DO UPDATE SET is_active = 1, route_id = CASE WHEN TRIM(analysis_routes.route_id) = '' THEN excluded.route_id ELSE analysis_routes.route_id END
    `).run(origin, destination, rid, nowStamp());
    res.json({ ok: true, route_id: rid, resolved: Boolean(rid) });
  } catch (error) {
    res.status(500).json({ message: error.message || "Rota eklenemedi." });
  }
});

// Rota aktif/pasif.
app.patch("/api/analysis/routes/:id", requireAuth, requirePermission("demand"), (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const isActive = req.body?.isActive ? 1 : 0;
    db.prepare("UPDATE analysis_routes SET is_active = ? WHERE id = ?").run(isActive, id);
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ message: error.message || "Guncellenemedi." });
  }
});

// Rota sil.
app.delete("/api/analysis/routes/:id", requireAuth, requirePermission("demand"), (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    db.prepare("DELETE FROM analysis_routes WHERE id = ?").run(id);
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ message: error.message || "Silinemedi." });
  }
});

// Motor durumu (worker calisiyor mu, kac olcum var).
app.get("/api/analysis/status", requireAuth, requirePermission("demand"), (req, res) => {
  try {
    const today = todayIsoInIstanbul();
    const routeCount = db.prepare("SELECT COUNT(*) c FROM analysis_routes WHERE is_active = 1").get().c;
    const measurements = db.prepare("SELECT COUNT(*) c FROM analysis_occupancy_history WHERE journey_date >= ?").get(today).c;
    const last = db.prepare("SELECT MAX(measured_at_iso) m FROM analysis_occupancy_history").get().m || "";
    res.json({ ok: true, workerRunning: analysisWorkerRunning, routeCount, measurements, lastMeasured: last, horizonDays: ANALYSIS_HORIZON_DAYS });
  } catch (error) {
    res.status(500).json({ message: error.message || "Durum alınamadı." });
  }
});

// Manuel tarama tetikle (admin) — arka planda calisir.
app.post("/api/analysis/scan", requireAuth, requireAdmin, (req, res) => {
  if (analysisWorkerRunning) return res.status(409).json({ message: "Tarama zaten calisiyor." });
  runAnalysisWorker().catch(() => null);
  res.json({ ok: true, message: "Tarama baslatildi (arka planda calisiyor)." });
});

// Sefer bazli fiyat degisiklik gecmisi + doluluk hesaplar. Hem API hem bot kullanir.
function computeJourneyTracking({ date, origin, destination, operator } = {}) {
  const norm = (s) => String(s || "").toLocaleLowerCase("tr-TR").trim();
  const originQ = norm(origin);
  const destQ = norm(destination);
  const op = String(operator || "").trim();

  // Firma listesi: TUM takip edilen firmalar (guncel seferlerden).
  const operators = db.prepare(
    "SELECT DISTINCT operator FROM obilet_prices WHERE operator != '' ORDER BY operator"
  ).all().map((r) => r.operator);

  // 1) ANA LISTE = guncel seferler (obilet_prices) — oBilet Takip'in gosterdigi seferlerin aynisi.
  const conds = ["p.price > 0"];
  const params = [];
  if (date && /^\d{4}-\d{2}-\d{2}$/.test(date)) { conds.push("p.journey_date = ?"); params.push(date); }
  if (op) { conds.push("p.operator = ?"); params.push(op); }
  const priceRows = db.prepare(`
    SELECT p.target_id, t.origin, t.destination, p.journey_date, p.operator, p.departure_time, p.departure_stop, p.price
      FROM obilet_prices p JOIN obilet_targets t ON t.id = p.target_id
     WHERE ${conds.join(" AND ")}
  `).all(...params);
  const currentSefers = priceRows.filter((r) =>
    (!originQ || norm(r.origin).includes(originQ)) &&
    (!destQ || norm(r.destination).includes(destQ))
  );

  // 2) Fiyat degisiklik gecmisi haritasi (son 3 gun) — sefere ekle.
  const histMap = new Map();
  try {
    const histRows = db.prepare(
      "SELECT id, target_id, journey_date, operator, departure_time, departure_stop, old_price, new_price, changed_at FROM obilet_price_history ORDER BY id ASC"
    ).all();
    for (const h of histRows) {
      const key = `${h.target_id}|${h.journey_date}|${h.operator}|${h.departure_time}|${h.departure_stop}`;
      if (!histMap.has(key)) histMap.set(key, []);
      histMap.get(key).push(h);
    }
  } catch (e) { /* yok say */ }

  // 3) Doluluk haritasi. ONEMLI: ayni sefer icin (durak yazimi degisince) birden fazla
  // doluluk satiri olabilir; HER ZAMAN EN GUNCEL (last_updated) satiri al — eski satir cekilmesin.
  // ANA SEFER: serviceId -> gercek kalkis sehri/saati/bitisi (feeder taramasindan kesfedilir).
  const serviceRouteMap = new Map();
  try {
    for (const r of db.prepare("SELECT service_id, origin_city, origin_time, dest_city FROM obilet_service_routes").all()) {
      if (r.service_id) serviceRouteMap.set(String(r.service_id), r);
    }
  } catch (e) { /* tablo yoksa yok say */ }

  const occMap = new Map();
  try {
    const occRows = db.prepare(
      "SELECT target_id, journey_date, operator, departure_time, total_seats, available_seats, plate, route_from, route_to, service_id, last_updated FROM obilet_occupancy"
    ).all();
    const tsKey = (s) => {
      const m = String(s || "").match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);
      return m ? `${m[3]}${m[2]}${m[1]}${m[4]}${m[5]}${m[6]}` : "0";
    };
    for (const o of occRows) {
      const key = `${o.target_id}|${o.journey_date}|${o.operator}|${o.departure_time}`;
      const prev = occMap.get(key);
      if (!prev || tsKey(o.last_updated) >= tsKey(prev.last_updated)) occMap.set(key, o);
    }
  } catch (e) { /* yok say */ }

  const journeys = currentSefers.map((s) => {
    const key = `${s.target_id}|${s.journey_date}|${s.operator}|${s.departure_time}|${s.departure_stop}`;
    const changes = histMap.get(key) || [];
    let prices, changeCount, lastChangedAt;
    if (changes.length) {
      prices = [changes[0].old_price, ...changes.map((c) => c.new_price)];
      changeCount = changes.length;
      lastChangedAt = changes[changes.length - 1].changed_at;
    } else {
      prices = [s.price];       // degismemis: sadece guncel fiyat
      changeCount = 0;
      lastChangedAt = "";
    }
    const occ = occMap.get(`${s.target_id}|${s.journey_date}|${s.operator}|${s.departure_time}`);
    const totalSeats = occ && occ.total_seats != null ? occ.total_seats : null;
    const availableSeats = occ && occ.available_seats != null ? occ.available_seats : null;
    const yolcu = (totalSeats != null && availableSeats != null) ? (totalSeats - availableSeats) : null;
    const plate = occ && occ.plate ? String(occ.plate) : "";
    const routeFrom = occ && occ.route_from ? String(occ.route_from) : "";
    const routeTo = occ && occ.route_to ? String(occ.route_to) : "";
    // ANA SEFER: bu segmentin serviceId'si feeder taramasinda bulunduysa gercek kalkis sehri/saati gelir.
    const svc = occ && occ.service_id ? serviceRouteMap.get(String(occ.service_id)) : null;
    const parentOrigin = svc && svc.origin_city ? String(svc.origin_city) : "";
    const parentOriginTime = svc && svc.origin_time ? String(svc.origin_time) : "";
    const parentDest = svc && svc.dest_city ? String(svc.dest_city) : "";
    return {
      target_id: s.target_id,
      journey_date: s.journey_date, origin: s.origin, destination: s.destination,
      operator: s.operator, departure_time: s.departure_time, departure_stop: s.departure_stop,
      changeCount, prices, currentPrice: s.price,
      totalSeats, availableSeats, yolcu, plate, routeFrom, routeTo, lastChangedAt,
      parentOrigin, parentOriginTime, parentDest,
    };
  }).sort((a, b) =>
    String(a.journey_date).localeCompare(String(b.journey_date)) ||
    String(a.departure_time).localeCompare(String(b.departure_time)) ||
    String(a.operator).localeCompare(String(b.operator), "tr")
  );

  return { operators, journeys };
}

// SEFER TAKIP — bir seferin son 3 gundeki fiyat degisiklik gecmisi (kac kez, eski->guncel).
app.get("/api/obilet/journey-tracking", requireAuth, (req, res) => {
  try {
    const { operators, journeys } = computeJourneyTracking({
      date: String(req.query.date || "").trim(),
      origin: req.query.origin, destination: req.query.destination, operator: req.query.operator,
    });
    res.json({ ok: true, operators, count: journeys.length, journeys });
  } catch (error) {
    res.status(500).json({ message: error.message || "Sefer takip alınamadı." });
  }
});

// Pazar payi verisini hesaplar (hat + firma bazinda). date bos ise tum gelecek seferler.
function computeMarketShare(date) {
  const today = todayIsoInIstanbul();
  try { db.prepare("DELETE FROM obilet_occupancy WHERE journey_date < ?").run(shiftIsoDate(today, -10)); } catch (e) {}
  const availableDates = db.prepare(
    "SELECT DISTINCT journey_date FROM obilet_occupancy WHERE journey_date >= ? ORDER BY journey_date"
  ).all(today).map((r) => r.journey_date);

  const dateOk = date && /^\d{4}-\d{2}-\d{2}$/.test(date);
  const occWhere = dateOk ? "AND o.journey_date = ?" : "AND o.journey_date >= ?";
  const occParam = dateOk ? date : today;
  const occ = db.prepare(`
    SELECT o.target_id, t.origin, t.destination, o.operator,
           COUNT(*) AS sefer,
           SUM(o.total_seats) AS kapasite,
           SUM(o.total_seats - o.available_seats) AS yolcu
      FROM obilet_occupancy o
      JOIN obilet_targets t ON t.id = o.target_id
     WHERE o.total_seats > 0 ${occWhere}
     GROUP BY o.target_id, o.operator
  `).all(occParam);

  const prWhere = dateOk ? "AND journey_date = ?" : "AND journey_date >= ?";
  const prices = db.prepare(`
    SELECT target_id, operator, MIN(price) AS fmin, MAX(price) AS fmax
      FROM obilet_prices WHERE price > 0 ${prWhere}
     GROUP BY target_id, operator
  `).all(occParam);
  const priceMap = new Map();
  for (const p of prices) priceMap.set(`${p.target_id}|${p.operator}`, p);

  const routes = new Map();
  for (const r of occ) {
    if (!routes.has(r.target_id)) {
      routes.set(r.target_id, {
        route: `${(r.origin || "").toUpperCase()}-${(r.destination || "").toUpperCase()}`,
        firms: [],
      });
    }
    const pm = priceMap.get(`${r.target_id}|${r.operator}`) || {};
    const doluluk = r.kapasite ? Math.round((r.yolcu / r.kapasite) * 10000) / 100 : 0;
    routes.get(r.target_id).firms.push({
      operator: r.operator, sefer: r.sefer, kapasite: r.kapasite || 0, yolcu: r.yolcu || 0,
      doluluk, fiyatMin: pm.fmin != null ? pm.fmin : null, fiyatMax: pm.fmax != null ? pm.fmax : null,
    });
  }
  const result = [...routes.values()]
    .map((rt) => ({ ...rt, firms: rt.firms.sort((a, b) => b.sefer - a.sefer) }))
    .sort((a, b) => a.route.localeCompare(b.route, "tr"));
  return { date: dateOk ? date : null, availableDates, routes: result };
}

// PAZAR PAYI ANALIZI (JSON) — dropdown tarihleri + veri.
app.get("/api/obilet/market-share", requireAuth, (req, res) => {
  try {
    const { date, availableDates, routes } = computeMarketShare(String(req.query.date || "").trim());
    res.json({ ok: true, date, availableDates, routes });
  } catch (error) {
    res.status(500).json({ message: error.message || "Analiz alınamadı." });
  }
});

// PAZAR PAYI ANALIZI (stilli Excel) — yesil basliklar + doluluk barlari.
app.get("/api/obilet/market-share.xlsx", requireAuth, async (req, res) => {
  try {
    const ExcelJS = require("exceljs");
    const date = String(req.query.date || "").trim();
    const { date: resolvedDate, routes } = computeMarketShare(date);
    const dot = (iso) => { const m = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/); return m ? `${m[3]}.${m[2]}.${m[1]}` : iso; };

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("Pazar Payı");
    ws.columns = [{ width: 28 }, { width: 12 }, { width: 11 }, { width: 13 }, { width: 15 }, { width: 13 }, { width: 13 }];
    const thin = { top: { style: "thin" }, left: { style: "thin" }, bottom: { style: "thin" }, right: { style: "thin" } };
    const headers = ["Firma", "Sefer Sayısı", "Kapasite", "Yolcu Sayısı", "Doluluk Oranı%", "Fiyat Minimum", "Fiyat Maximum"];

    ws.mergeCells(1, 1, 1, 7);
    const tcell = ws.getCell(1, 1);
    tcell.value = resolvedDate ? `${dot(resolvedDate)} Pazar Payı Analizi` : "Pazar Payı Analizi (Tüm Dönem)";
    tcell.font = { bold: true, size: 14 };

    let row = 3;
    for (const rt of routes) {
      const rc = ws.getCell(row, 1); rc.value = rt.route; rc.font = { bold: true, size: 12 }; row++;
      headers.forEach((h, i) => {
        const c = ws.getCell(row, i + 1);
        c.value = h; c.font = { bold: true };
        c.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFC6E0B4" } };
        c.border = thin; c.alignment = { horizontal: i === 0 ? "left" : "center", wrapText: true };
      });
      row++;
      const dolStart = row;
      for (const f of rt.firms) {
        const vals = [f.operator, f.sefer, f.kapasite, f.yolcu, (f.doluluk != null ? f.doluluk / 100 : null), f.fiyatMin, f.fiyatMax];
        vals.forEach((v, i) => {
          const c = ws.getCell(row, i + 1);
          c.value = v; c.border = thin;
          if (i > 0) c.alignment = { horizontal: "center" };
          if (i === 4 && v != null) c.numFmt = "0.00%";
        });
        row++;
      }
      const dolEnd = row - 1;
      if (dolEnd >= dolStart) {
        ws.addConditionalFormatting({
          ref: `E${dolStart}:E${dolEnd}`,
          rules: [{ type: "dataBar", cfvo: [{ type: "num", value: 0 }, { type: "num", value: 1 }], color: { argb: "FFFFC000" } }],
        });
      }
      row++; // hatlar arasi bos satir
    }

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="pazar-payi${resolvedDate ? "-" + resolvedDate : "-tum-donem"}.xlsx"`);
    await wb.xlsx.write(res);
    res.end();
  } catch (error) {
    res.status(500).json({ message: error.message || "Excel oluşturulamadı." });
  }
});

app.get("/api/obilet/price-history", requireAuth, (req, res) => {
  try {
    const targetId = parseInt(req.query.targetId || "0", 10);
    const fromDate = String(req.query.from || "").trim();
    const toDate = String(req.query.to || "").trim();
    const search = String(req.query.search || "").trim().toLowerCase();
    const limit = Math.min(parseInt(req.query.limit || "1000", 10) || 1000, 2000);
    const offset = Math.max(parseInt(req.query.offset || "0", 10) || 0, 0);

    const conditions = [];
    const params = [];

    if (targetId) {
      conditions.push("target_id = ?");
      params.push(targetId);
    }
    if (fromDate && /^\d{4}-\d{2}-\d{2}$/.test(fromDate)) {
      conditions.push("substr(changed_at, 7, 4) || '-' || substr(changed_at, 4, 2) || '-' || substr(changed_at, 1, 2) >= ?");
      params.push(fromDate);
    }
    if (toDate && /^\d{4}-\d{2}-\d{2}$/.test(toDate)) {
      conditions.push("substr(changed_at, 7, 4) || '-' || substr(changed_at, 4, 2) || '-' || substr(changed_at, 1, 2) <= ?");
      params.push(toDate);
    }

    const whereSql = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const total = db.prepare(`SELECT COUNT(*) AS c FROM obilet_price_history ${whereSql}`).get(...params).c;
    const sql = `
      SELECT id, target_id, origin, destination, journey_date, operator,
             departure_time, departure_stop, old_price, new_price, changed_at, detected_by
        FROM obilet_price_history
        ${whereSql}
       ORDER BY id DESC
       LIMIT ? OFFSET ?
    `;
    let rows = db.prepare(sql).all(...params, limit, offset);

    // Search'u SQL'de yapmadık (LIKE turkce karakterlerle zayif). JS tarafinda filtrele.
    if (search) {
      rows = rows.filter(r =>
        String(r.operator || "").toLowerCase().includes(search) ||
        String(r.origin || "").toLowerCase().includes(search) ||
        String(r.destination || "").toLowerCase().includes(search) ||
        String(r.departure_stop || "").toLowerCase().includes(search)
      );
    }

    res.json({ ok: true, count: rows.length, total, offset, limit, history: rows });
  } catch (error) {
    res.status(500).json({ message: error.message || "Geçmiş alınamadı." });
  }
});

// API: Genel Panel dashboard ozeti — tek call'da tum kartlar icin veri.
app.get("/api/obilet/dashboard-summary", requireAuth, (req, res) => {
  try {
    // DD.MM.YYYY HH:mm:ss formatindaki changed_at'i ISO formatina ceviren SQL ifadesi.
    // SQLite'da substr ile parse — Istanbul timezone tutarli.
    const changedAtToISO = `
      (substr(changed_at, 7, 4) || '-' ||
       substr(changed_at, 4, 2) || '-' ||
       substr(changed_at, 1, 2) || 'T' ||
       substr(changed_at, 12, 8))
    `;

    // Aktif hat sayisi
    const activeTargets = db.prepare(
      "SELECT COUNT(*) AS c FROM obilet_targets WHERE is_active = 1"
    ).get();

    // Bugunku degisiklikler (Istanbul local "DD.MM.YYYY" prefix ile)
    const today = todayIsoInIstanbul(); // "2026-06-17"
    const todayDot = today.split("-").reverse().join(".");
    const todayChanges = db.prepare(
      "SELECT COUNT(*) AS c FROM obilet_price_history WHERE substr(changed_at, 1, 10) = ?"
    ).get(todayDot);

    // Son 24 saat degisiklik sayisi (Istanbul'da "now - 24h" ISO uretip karsilastir)
    const last24Cutoff = new Date(Date.now() - 24 * 3600 * 1000)
      .toLocaleString("sv-SE", { timeZone: "Europe/Istanbul" })
      .replace(" ", "T");
    const last24Changes = db.prepare(
      `SELECT COUNT(*) AS c FROM obilet_price_history WHERE ${changedAtToISO} >= ?`
    ).get(last24Cutoff);

    // Bugunun en buyuk fiyat degisikligi
    const biggestToday = db.prepare(
      `SELECT origin, destination, operator, departure_time, journey_date,
              old_price, new_price, changed_at
         FROM obilet_price_history
        WHERE substr(changed_at, 1, 10) = ?
        ORDER BY ABS(new_price - old_price) DESC
        LIMIT 1`
    ).get(todayDot);

    // En aktif rakip firma (son 30 gun)
    const cutoff30 = new Date(Date.now() - 30 * 24 * 3600 * 1000)
      .toLocaleString("sv-SE", { timeZone: "Europe/Istanbul" })
      .replace(" ", "T");
    const topOperator = db.prepare(
      `SELECT operator, COUNT(*) AS c
         FROM obilet_price_history
        WHERE ${changedAtToISO} >= ?
        GROUP BY operator
        ORDER BY c DESC
        LIMIT 1`
    ).get(cutoff30);

    // Son 7 gunun gunluk degisim sayisi (sparkline icin)
    const sevenDays = [];
    for (let i = 6; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      const isoDate = d.toLocaleDateString("en-CA", { timeZone: "Europe/Istanbul" });
      const dotDate = isoDate.split("-").reverse().join(".");
      const row = db.prepare(
        "SELECT COUNT(*) AS c FROM obilet_price_history WHERE substr(changed_at, 1, 10) = ?"
      ).get(dotDate);
      sevenDays.push({ date: isoDate, count: row.c });
    }

    // Toplam izlenen sefer sayisi (anlik snapshot)
    const totalJourneys = db.prepare(
      "SELECT COUNT(*) AS c FROM obilet_prices"
    ).get();

    // Son 5 bildirim
    const recentNotifs = db.prepare(
      `SELECT id, username, message, created_at, is_read
         FROM pricing_notifications
        ORDER BY id DESC
        LIMIT 5`
    ).all();

    res.json({
      ok: true,
      summary: {
        activeTargets: activeTargets.c,
        todayChanges: todayChanges.c,
        last24hChanges: last24Changes.c,
        totalJourneys: totalJourneys.c,
        biggestToday,
        topOperator,
        sevenDays,
        recentNotifs,
        generatedAt: nowStamp(),
      },
    });
  } catch (error) {
    res.status(500).json({ message: error.message || "Dashboard verisi alinamadi." });
  }
});

// API: Tek bir seferin (operator + saat + hat) tum fiyat gecmisini getir + ozet stats.
// Drilldown sidebar icin kullanilir.
app.get("/api/obilet/price-history/journey-detail", requireAuth, (req, res) => {
  try {
    const targetId = parseInt(req.query.targetId || "0", 10);
    const operator = String(req.query.operator || "").trim();
    const time = String(req.query.time || "").trim();
    if (!targetId || !operator || !time) {
      return res.status(400).json({ message: "targetId, operator ve time zorunlu." });
    }
    // Bu spesifik seferin tum degisiklikleri (son 50, kronolojik artan)
    const history = db.prepare(
      `SELECT id, journey_date, departure_stop, old_price, new_price, changed_at
         FROM obilet_price_history
        WHERE target_id = ? AND operator = ? AND departure_time = ?
        ORDER BY id DESC
        LIMIT 100`
    ).all(targetId, operator, time);

    // Ayni hatta + ayni saatte calisan diger firmalarla karsilastirma (son fiyat).
    // obilet_prices guncel anlik fiyat, oradan cekiyoruz.
    const peers = db.prepare(
      `SELECT operator, departure_stop, price, last_updated
         FROM obilet_prices
        WHERE target_id = ? AND departure_time = ? AND operator != ?
        ORDER BY price ASC
        LIMIT 20`
    ).all(targetId, time, operator);

    // Anlik fiyat (bu sefer)
    const currentRow = db.prepare(
      `SELECT price, last_updated, departure_stop, arrival_stop
         FROM obilet_prices
        WHERE target_id = ? AND operator = ? AND departure_time = ?
        ORDER BY last_updated DESC LIMIT 1`
    ).get(targetId, operator, time);

    // Stats (history uzerinden)
    const prices = [];
    history.forEach(h => { prices.push(h.old_price, h.new_price); });
    if (currentRow?.price) prices.push(currentRow.price);
    const stats = prices.length ? {
      min: Math.min(...prices),
      max: Math.max(...prices),
      avg: Math.round(prices.reduce((a, b) => a + b, 0) / prices.length),
      changes: history.length,
    } : null;

    res.json({ ok: true, history, peers, current: currentRow, stats });
  } catch (error) {
    res.status(500).json({ message: error.message || "Detay alınamadı." });
  }
});

// API: Tek satir fiyat gecmisi silme (admin yardimi icin)
app.delete("/api/obilet/price-history/:id", requireAuth, (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ message: "Gecersiz ID." });
    db.prepare("DELETE FROM obilet_price_history WHERE id = ?").run(id);
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ message: error.message || "Silme basarisiz." });
  }
});

app.post("/api/obilet/targets/:id/clear-prices", requireAuth, (req, res) => {
  try {
    const targetId = parseInt(req.params.id, 10);
    if (!targetId) return res.status(400).json({ message: "Gecersiz hat ID." });
    const result = db.prepare("DELETE FROM obilet_prices WHERE target_id = ?").run(targetId);
    console.log(`[Temizlik] target ${targetId}: ${result.changes} fiyat kaydi silindi.`);
    res.json({ ok: true, deleted: result.changes });
  } catch (error) {
    res.status(500).json({ message: error.message || "Temizlik basarisiz." });
  }
});

// API: Hat için tüm fiyat geçmişini getir (Excel indirme için)
app.get("/api/obilet/targets/:id/prices", requireAuth, (req, res) => {
  try {
    const targetId = parseInt(req.params.id, 10);
    if (!targetId) {
      return res.status(400).json({ message: "Geçersiz hat ID." });
    }

    const target = db.prepare("SELECT * FROM obilet_targets WHERE id = ?").get(targetId);
    if (!target) {
      return res.status(404).json({ message: "Hat bulunamadı." });
    }

    // Bu hat için tüm fiyatları çek (tarih, firma, saat, fiyat)
    const prices = db.prepare(`
      SELECT 
        journey_date,
        operator,
        departure_time,
        departure_stop,
        arrival_stop,
        price,
        last_updated
      FROM obilet_prices
      WHERE target_id = ?
      ORDER BY journey_date ASC, departure_time ASC, operator ASC
    `).all(targetId);

    res.json(prices);
  } catch (error) {
    res.status(500).json({ message: error.message || "Fiyat geçmişi alınamadı." });
  }
});

// API: Debug - Fiyat Kayıtlarını Görüntüle
app.get("/api/obilet/debug-prices/:targetId", requireAuth, (req, res) => {
  try {
    const targetId = parseInt(req.params.targetId, 10);
    const prices = db.prepare(
      "SELECT * FROM obilet_prices WHERE target_id = ? ORDER BY journey_date, departure_time, last_updated DESC"
    ).all(targetId);
    res.json({ prices });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

// ===================== ANA SEFER KESFI (feeder tarama) =====================
// Enver gibi ara-segmentini gizleyen firmalarin GERCEK kalkis sehri oBilet'te yalnizca aracin gercek
// kalktigi sehirden sorgulaninca gorunur (orn. Sanliurfa->Ankara sorgusunun stops dizisinde Adana 00:30
// + serviceId cikar). Bu is feeder sehirleri tarar, her servisin serviceId'sini gercek kalkis/saat/bitisiyle
// eslestirip obilet_service_routes'a yazar. Sefer Takip segmentin serviceId'siyle JOIN yapip ana seferi gosterir.
// FIYAT/DEGISIKLIK/TELEGRAM dongusune HIC DOKUNMAZ — tamamen ayri tablo, ayri zamanlayici, dusuk frekans.
const ROUTE_DISCOVERY_ENABLED = true;
// CIFT YONLU feeder hub'lari: takip edilen hattin HANGI yonde oldugu onceden bilinmez. Bir segmentin
// (orn. Ankara->Adana) gercek kalkisi, aracin gercek kalktigi sehirden sorgulaninca gorunur:
//  - Ankara->Adana (guneye giden) araclar KUZEYBATI'dan gelir: Istanbul, Bursa, Eskisehir, Bolu, Izmir...
//    (canli kanit: Istanbul->Adana, 14 Ankara->Adana Enver seferinin 12'sini yakaladi.)
//  - Adana->Ankara (kuzeye giden) araclar GUNEYDOGU'dan gelir: Sanliurfa, Gaziantep, Diyarbakir...
// Bu yuzden her iki bolgenin hub'larini tarariz; feeder->hedef rotasi tracked-Origin'den gecmiyorsa
// serviceId eslesmez (zararsiz). En yuksek verimli hub'lar basta (tarama limiti onlara oncelik verir).
const ROUTE_DISCOVERY_FEEDERS = [
  "İstanbul", "Şanlıurfa", "Gaziantep", "Bursa", "Konya", "Kayseri", "Eskişehir",
  "Bolu", "İzmir", "Diyarbakır", "Adıyaman", "Osmaniye", "Kadirli", "Hatay", "Balıkesir", "Mersin"
];
const ROUTE_DISCOVERY_MAX_SCANS = 40;     // tek turda en fazla feeder-tarama (Cloudflare yuku sinirli)
const ROUTE_DISCOVERY_MAX_DATES = 1;      // hedef basina 1 tarih yeter — serviceId tarih-bagimsiz (schedule id)
const ROUTE_DISCOVERY_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6 saat (rotalar sabit, sik taramaya gerek yok)
let routeDiscoveryRunning = false;

async function discoverParentRoutes() {
  if (routeDiscoveryRunning) return { ok: false, reason: "already-running" };
  routeDiscoveryRunning = true;
  let scanned = 0, upserts = 0, combos = 0;
  try {
    const today = todayIsoInIstanbul();
    // Aktif hedeflerin (varis, tarih) kombinasyonlari — feeder'lardan bu varislere ayni tarihte tarariz.
    const destDates = new Map(); // destination -> Set(dateIso)
    for (const t of db.prepare("SELECT destination, date, end_date FROM obilet_targets WHERE is_active = 1").all()) {
      const dest = String(t.destination || "").trim();
      if (!dest) continue;
      const dates = buildIsoDateRange(t.date, t.end_date || t.date)
        .filter((d) => d >= today)
        .slice(0, ROUTE_DISCOVERY_MAX_DATES);
      if (!destDates.has(dest)) destDates.set(dest, new Set());
      dates.forEach((d) => destDates.get(dest).add(d));
    }
    if (!destDates.size) { destDates.set("Ankara", new Set([today])); }

    const stmt = db.prepare(`INSERT INTO obilet_service_routes (service_id, operator, origin_city, origin_time, dest_city, stops_json, updated_at)
      VALUES (?, ?, ?, ?, ?, '', ?)
      ON CONFLICT(service_id) DO UPDATE SET operator=excluded.operator, origin_city=excluded.origin_city, origin_time=excluded.origin_time, dest_city=excluded.dest_city, updated_at=excluded.updated_at`);
    const now = nowStamp();
    let lockFails = 0; // fiyat taramasi surekli mesgulse bosuna beklememek icin

    outer:
    for (const feeder of ROUTE_DISCOVERY_FEEDERS) {
      for (const [dest, dateSet] of destDates) {
        if (slugTr(feeder) === slugTr(dest)) continue;
        const routeId = buildObiletRouteIdLocal(feeder, dest);
        if (!routeId) continue;
        for (const dateIso of dateSet) {
          if (scanned >= ROUTE_DISCOVERY_MAX_SCANS) break outer;
          combos++;
          // FIYAT TARAMASINI BLOKLAMA: kilidi HER tarama icin ayri al/birak — aralarda fiyat turu girebilsin.
          // Fiyat taramasi mesgulse (kilit alinamaz) bu feeder-taramayi atla, 6 saat sonra tekrar denenir.
          const locked = await acquireObiletLock(60000);
          if (!locked) {
            // Fiyat taramasi surekli mesgul: 3 kez ust uste alinamazsa bu turu birak (6 saat sonra tekrar).
            if (++lockFails >= 3) { console.log("[Ana Sefer Kesfi] fiyat taramasi mesgul, tur ertelendi."); break outer; }
            continue;
          }
          lockFails = 0;
          let journeys = [];
          try {
            const browser = await getBrowserInstance();
            journeys = await scrapeObiletSeferlerPage(browser, routeId, dateIso, null);
          } catch (e) { journeys = []; }
          finally { releaseObiletLock(); }
          scanned++;
          for (const j of journeys) {
            const sid = String(j.serviceId || "").trim();
            if (!sid) continue;
            const origin = String(j.routeFrom || feeder).trim();
            const originTime = String(j.originStopTime || j.time || "").trim();
            const dcity = String(j.routeTo || dest).trim();
            try { stmt.run(sid, String(j.operator || ""), origin, originTime, dcity, now); upserts++; } catch (e) {}
          }
          await new Promise((r) => setTimeout(r, 2500)); // Cloudflare burst tespitini onle (kilit disinda)
        }
      }
    }
  } catch (e) {
    console.warn("[Ana Sefer Kesfi] hata:", e.message);
  } finally {
    routeDiscoveryRunning = false;
  }
  console.log(`[Ana Sefer Kesfi] ${scanned}/${combos} feeder-tarama, ${upserts} servis eslestirildi.`);
  return { ok: true, scanned, combos, upserts };
}

// Manuel tetikleme (admin) — kanit icin "hemen kesfet".
app.post("/api/obilet/discover-routes", requireAuth, requireAdmin, (req, res) => {
  try {
    const before = db.prepare("SELECT COUNT(*) AS c FROM obilet_service_routes").get().c;
    if (routeDiscoveryRunning) return res.json({ ok: true, started: false, running: true, totalServices: before });
    // Uzun surebilir (dakikalar) — arka planda calistir, HTTP'yi bekletme.
    discoverParentRoutes().catch((e) => console.warn("[Ana Sefer Kesfi] manuel tetik hatasi:", e.message));
    res.json({ ok: true, started: true, totalServices: before, note: "Kesif arka planda basladi; birkac dakika sonra Sefer Takip'te gorunur." });
  } catch (error) {
    res.status(500).json({ message: error.message || "Kesif basarisiz." });
  }
});

// AG & KAPASITE DIFF: son yapisal degisiklikler (rapor/liste icin).
app.get("/api/obilet/structure-changes/recent", requireAuth, (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || "50", 10) || 50, 200);
    const rows = db.prepare(
      "SELECT id, target_id, journey_date, route_label, change_type, operator, message, detected_at FROM obilet_structure_changes ORDER BY id DESC LIMIT ?"
    ).all(limit);
    res.json({ ok: true, changes: rows });
  } catch (error) {
    res.status(500).json({ message: error.message || "Değişiklikler alınamadı." });
  }
});

// KULLANICI-BAZLI BILDIRIM FEED'i: bu kullanicinin HENUZ GORMEDIGI (id > last_seen) fiyat + yapi degisimleri.
// Sag-ust sticky bildirim bunu 60 sn'de bir + giriste yoklar. Kullanici bir toast'i kapatinca /toast-seen ile
// o kind'in pointer'i ilerler; kapatana kadar (reload dahil) tekrar gorunur. Fiyat mantigina dokunmaz (salt okur).
function getUserLastSeen(userId, kind) {
  try {
    const r = db.prepare("SELECT last_seen_id FROM notification_reads WHERE user_id=? AND kind=?").get(userId, kind);
    return r ? Number(r.last_seen_id) || 0 : 0;
  } catch (e) { return 0; }
}
app.get("/api/obilet/toast-feed", requireAuth, (req, res) => {
  try {
    const uid = req.auth.user.id;
    const FEED_LIMIT = 6; // ekrandaki toast ust siniriyla ayni -> donen her toast gerceklen gosterilir (sessiz elenme yok)
    const priceSeen = getUserLastSeen(uid, "price");
    const structSeen = getUserLastSeen(uid, "structure");
    // ASCENDING (eski->yeni) dondur ki toast'lar dogru sirayla dizilsin.
    const price = db.prepare(
      "SELECT id, origin, destination, operator, departure_time, journey_date, old_price, new_price FROM obilet_price_history WHERE id > ? ORDER BY id DESC LIMIT ?"
    ).all(priceSeen, FEED_LIMIT).reverse();
    const structure = db.prepare(
      "SELECT id, route_label, change_type, message FROM obilet_structure_changes WHERE id > ? ORDER BY id DESC LIMIT ?"
    ).all(structSeen, FEED_LIMIT).reverse();
    res.json({ ok: true, price, structure });
  } catch (error) {
    res.status(500).json({ message: error.message || "Bildirim feed alınamadı." });
  }
});
// Bir toast kapatilinca: o kind'in pointer'ini ileri al (id'ye kadar gorüldü say).
app.post("/api/obilet/toast-seen", requireAuth, (req, res) => {
  try {
    const uid = req.auth.user.id;
    const kind = String(req.body?.kind || "").trim();
    const id = parseInt(req.body?.id, 10);
    if (kind !== "price" && kind !== "structure") return res.status(400).json({ message: "Geçersiz tür." });
    if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ message: "Geçersiz id." });
    db.prepare(`INSERT INTO notification_reads (user_id, kind, last_seen_id, updated_at) VALUES (?,?,?,?)
      ON CONFLICT(user_id, kind) DO UPDATE SET last_seen_id = MAX(last_seen_id, excluded.last_seen_id), updated_at = excluded.updated_at`)
      .run(uid, kind, id, nowStamp());
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ message: error.message || "İşaretlenemedi." });
  }
});

if (ROUTE_DISCOVERY_ENABLED) {
  setTimeout(() => { discoverParentRoutes().catch(() => null); }, 90 * 1000); // boot'tan 90 sn sonra (ilk fiyat taramasindan sonra)
  setInterval(() => { discoverParentRoutes().catch(() => null); }, ROUTE_DISCOVERY_INTERVAL_MS);
}

// Sunucu Başladığında ve Her X Dakikada Bir Otomatik Kontrol Zamanlayıcı
setTimeout(() => {
  refreshObiletPricesTask().catch(err => console.error("[Otomatik Kontrol] Ilk baslangic hatasi:", err.message));
}, 5 * 1000); // Server başladıktan 5 saniye sonra ilk kontrol

setInterval(() => {
  refreshObiletPricesTask().catch(err => console.error("[Otomatik Kontrol] Zamanlayici hatasi:", err.message));
}, OBILET_CHECK_INTERVAL_MS);

// ===================== DOLULUK İŞÇİSİ (fiyat taramasindan TAMAMEN AYRI) =====================
// Fiyat çekme koduna DOKUNMAZ. Kendi sayfasini acar, takip edilen seferlerin GERCEK koltuk
// haritasini (/json/sefer, tiklama ile) okur, obilet_occupancy'yi 'gercek' kaynakli gunceller.
// ASLA silmez (okuyamadigi seferin mevcut degeri kalir). Fiyat taramasi calisiyorsa BEKLER.
let occupancyWorkerRunning = false;
// KAPALI: Bu ayri "Doluluk İşçisi" koltuk haritasini TIKLAMA ile aciyordu ve bazen YANLIS otobusu
// acip eksik/yanlis sayiyordu (or. 6/42). Artik doluluk TEK dogru kaynaktan gelir: fiyat taramasinin
// listedeki available-seats degeri (gercek koltuk haritasiyla birebir ayni oldugu dogrulandi).
// Iki tarama cakismasin + oBilet yuku azalsin diye bu isci kapatildi.
const OCCUPANCY_WORKER_ENABLED = false;
const OCCUPANCY_NEAR_DAYS = 7;          // 1 hafta (bugun + 6 gun)
const OCCUPANCY_WORKER_INTERVAL_MS = 15 * 60 * 1000; // 15 dk
const OCCUPANCY_MAX_SEFER_PER_DATE = 20;

// filterOpKey/filterTime verilirse SADECE o seferi isler (admin "hemen kontrol" butonu icin).
// Doner: [{operator,time,sold,total,occ}] yazilan seferler.
async function occupancyForHatDate(target, routeId, ops, dateIso, filterOpKey = null, filterTime = null) {
  const browser = await getBrowserInstance();
  const page = await setupObiletPage(browser);
  const journeyItems = [];
  const seatById = new Map(); // seferId -> {sold,total,...}
  page.on("response", async (resp) => {
    try {
      const url = resp.url();
      const ct = resp.headers()["content-type"] || "";
      if (!/json/i.test(ct)) return;
      const txt = await resp.text().catch(() => "");
      if (!txt) return;
      let data; try { data = JSON.parse(txt); } catch { return; }
      if (/\/json\/sefer\//i.test(url)) {
        const m = url.match(/\/json\/sefer\/(\d+)/i);
        const c = countSeatsFromSeferJson(data);
        if (m && c) seatById.set(m[1], c);
        return;
      }
      if (/\/json\/journeys\//i.test(url)) {
        const list = Array.isArray(data?.data) ? data.data : Array.isArray(data?.journeys) ? data.journeys : [];
        for (const it of list) journeyItems.push(it);
      }
    } catch {}
  });
  // Ilk tiklamada tarayicinin /json/sefer POST GOVDE'sini yakala -> kalan seferleri FETCH ile cek.
  let capturedBody = null, capturedBodyId = null;
  page.on("request", (req) => {
    try {
      const m = req.url().match(/\/json\/sefer\/(\d+)/i);
      if (m && req.postData()) { capturedBody = req.postData(); capturedBodyId = m[1]; }
    } catch {}
  });
  try {
    await gotoAndHandleCloudflare(page, "https://www.obilet.com/");
    await new Promise(r => setTimeout(r, 1200));
    await gotoAndHandleCloudflare(page, `https://www.obilet.com/seferler/${routeId}/${dateIso}?t=${Date.now()}`);
    // Sefer listesi (journeys XHR) SABIT sure yerine GELENE KADAR beklenir (soguk/Cloudflare
    // sayfalari yavas yukleniyor). Max ~30s, 2s'de bir kontrol; gelince hemen devam.
    for (let i = 0; i < 16 && !journeyItems.length; i++) {
      await new Promise(r => setTimeout(r, 2000));
    }
    if (!journeyItems.length) {
      // Bir kez daha dene (sayfayi yeniden yukle) — bazen ilk yukleme Cloudflare'e takiliyor.
      await gotoAndHandleCloudflare(page, `https://www.obilet.com/seferler/${routeId}/${dateIso}?t=${Date.now()}`);
      for (let i = 0; i < 12 && !journeyItems.length; i++) {
        await new Promise(r => setTimeout(r, 2000));
      }
    }

    // Takip edilen operatorlerin seferleri (id + firma + saat)
    const infos = journeyItems.map((it) => {
      const dep = String(it?.journey?.departure || it?.departure || "");
      const tm = dep.match(/(\d{2}:\d{2})/);
      return { id: it?.id != null ? String(it.id) : null, firma: String(it?.["partner-name"] || "").trim(), time: tm ? tm[1] : "" };
    }).filter((x) => x.id && x.time && ops.some((op) => isObiletOperatorMatch(op, x.firma)))
      .filter((x) => (!filterTime || x.time === filterTime) &&
        (!filterOpKey || x.firma.toLocaleLowerCase("tr-TR").includes(filterOpKey)))
      .slice(0, OCCUPANCY_MAX_SEFER_PER_DATE);

    if (!infos.length) { console.log(`[Doluluk İşçisi] ${target.origin}->${target.destination} ${dateIso}: takip edilen sefer yok.`); return []; }

    // Seferleri oku: once yakalanmis GOVDE ile FETCH dene (tiklamasiz, guvenilir).
    // GOVDE yoksa TIKLA (ilk seferde tarayicinin POST'u calisir, govdeyi yakalar; sonrakiler fetch).
    let viaFetch = 0, viaClick = 0;
    for (const info of infos) {
      if (seatById.has(info.id)) continue;
      // 1) FETCH (govde varsa) — art arda burst yapmamak icin arada bekle (Cloudflare throttle).
      if (capturedBody) {
        const c = await fetchSeferSeatsWithBody(page, info.id, capturedBody, capturedBodyId);
        await new Promise((r) => setTimeout(r, 600));
        if (c) { seatById.set(info.id, c); viaFetch++; continue; }
      }
      // 2) TIKLA (bootstrap / yedek) — tutmazsa 2 kez daha dene (tek-sefer butonu icin kritik).
      const doClick = () => page.evaluate((t, firm) => {
        const hasSeatBtn = (el) => Array.from(el.querySelectorAll("button,a,span,div"))
          .some((b) => /koltuk|seç|^sec$|satın al|satin al/i.test((b.innerText || "").trim()));
        let cards = Array.from(document.querySelectorAll("div,li,article"))
          .filter((el) => (el.innerText || "").length < 700 && /\d{2}:\d{2}/.test(el.innerText || "") && hasSeatBtn(el));
        const matchFirm = (el) => {
          const txt = (el.innerText || "").toLowerCase();
          if (txt.includes(firm)) return true;
          return Array.from(el.querySelectorAll("img")).some((im) =>
            ((im.getAttribute("alt") || "") + (im.getAttribute("title") || "")).toLowerCase().includes(firm));
        };
        const timed = cards.filter((el) => (el.innerText || "").includes(t));
        const target = timed.find(matchFirm) || timed[0];
        if (!target) return false;
        let best = target;
        for (const c of cards) if (target.contains(c) && c !== target && (c.innerText || "").includes(t)) { best = c; break; }
        const btn = Array.from(best.querySelectorAll("button,a,span,div"))
          .find((b) => /koltuk|seç|^sec$|satın al|satin al/i.test((b.innerText || "").trim()));
        (btn || best).click();
        return true;
      }, info.time, info.firma.toLocaleLowerCase("tr-TR")).catch(() => false);

      for (let attempt = 1; attempt <= 3 && !seatById.has(info.id); attempt++) {
        await doClick();
        await new Promise((r) => setTimeout(r, 2600)); // koltuk haritasi + POST gelsin
      }
      if (seatById.has(info.id)) viaClick++;
      // Acik koltuk haritasini KAPAT (sonraki tiklama/DOM temiz kalsin)
      await page.evaluate(() => {
        const k = Array.from(document.querySelectorAll("button,a,span,div"))
          .find((b) => { const t = (b.innerText || "").trim(); return t.length < 12 && /^kapat$/i.test(t); });
        if (k) k.click();
      }).catch(() => {});
    }
    await new Promise((r) => setTimeout(r, 1200));
    if (DEBUG_OBILET_PRICE && capturedBody) console.log(`[Doluluk İşçisi] GOVDE ornek: ${String(capturedBody).substring(0, 160)}`);

    // Ayni operator+saat icin birden cok listeleme olabilir -> en dolu (gercek otobus) alinir.
    const bestByKey = new Map(); // normOp|time -> {sold,total}
    for (const info of infos) {
      const c = seatById.get(info.id);
      if (!c) continue;
      const occOp = normalizeObiletOperatorName(info.firma) || info.firma;
      const key = `${occOp}|${info.time}`;
      const prev = bestByKey.get(key);
      if (!prev || c.sold > prev.sold) bestByKey.set(key, { occOp, time: info.time, sold: c.sold, total: c.total });
    }

    const now = nowStamp();
    let ok = 0;
    const written = [];
    for (const v of bestByKey.values()) {
      try {
        const occ = v.total > 0 ? Math.max(0, Math.min(100, Math.round((v.sold / v.total) * 100))) : 0;
        db.prepare(`
          INSERT INTO obilet_occupancy (target_id, journey_date, operator, departure_time, departure_stop, total_seats, available_seats, occupancy_percent, last_updated, source)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'gercek')
          ON CONFLICT(target_id, journey_date, operator, departure_time)
          DO UPDATE SET total_seats = excluded.total_seats, available_seats = excluded.available_seats, occupancy_percent = excluded.occupancy_percent, last_updated = excluded.last_updated, source = 'gercek'
        `).run(target.id, dateIso, v.occOp, v.time, "", v.total, Math.max(0, v.total - v.sold), occ, now);
        written.push({ operator: v.occOp, time: v.time, sold: v.sold, total: v.total, occ });
        ok++;
      } catch (e) { /* tek sefer hatasi digerlerini bozmasin */ }
    }
    // Sefer-sefer detay: hangisi geldi (saat dolu/toplam), hangisi EKSIK kaldi.
    const okList = [], eksikList = [];
    const seenT = new Set();
    for (const info of infos) {
      if (seenT.has(info.time)) continue; seenT.add(info.time);
      const c = seatById.get(info.id) || [...infos].filter(x => x.time === info.time).map(x => seatById.get(x.id)).find(Boolean);
      if (c) okList.push(`${info.time}=${c.sold}/${c.total}`);
      else eksikList.push(info.time);
    }
    console.log(`[Doluluk İşçisi]   ${dateIso}: ${ok}/${seenT.size} sefer (fetch=${viaFetch},tik=${viaClick})`);
    console.log(`[Doluluk İşçisi]     ${okList.join("  ") || "(yok)"}`);
    if (eksikList.length) console.log(`[Doluluk İşçisi]     EKSIK: ${eksikList.join(", ")}`);
    return written;
  } finally {
    try { await page.close(); } catch {}
  }
}

async function runOccupancyWorker() {
  if (!OCCUPANCY_WORKER_ENABLED || occupancyWorkerRunning) return;
  // NOT: Fiyat taramasi SUREKLI calisiyor; "bos iken calis" dersek isci hic calismaz.
  // Bu yuzden AYNI ANDA calisir (kendi sayfasi, ayni tarayici/oturum). NAZIK tempo ile
  // (hatlar arasi bekleme) fiyat taramasini bozmadan arka planda ilerler.
  occupancyWorkerRunning = true;
  const t0 = Date.now();
  let doneHats = 0;
  try {
    const today = todayIsoInIstanbul();
    const targets = db.prepare("SELECT * FROM obilet_targets WHERE is_active = 1").all();
    for (const target of targets) {
      const routeId = String(target.route_id || "").trim();
      if (!/^\d+-\d+$/.test(routeId)) continue;
      const ops = parseCsvList(target.operators).map(normalizeObiletOperatorName).filter(Boolean);
      if (!ops.length || ops.includes("*")) continue; // tum-firma cok agir -> atla (liste degeri kalir)
      const dates = buildIsoDateRange(target.date, target.end_date || target.date)
        .filter((d) => d >= today).slice(0, OCCUPANCY_NEAR_DAYS);
      const hat = `${target.origin}->${target.destination}`;
      console.log(`[Doluluk İşçisi] ===== HAT BASLADI: ${hat} (${ops.join(",")}) — ${dates.length} gun =====`);
      let hatOk = 0;
      for (const dateIso of dates) {
        try {
          const w = await occupancyForHatDate(target, routeId, ops, dateIso);
          hatOk += (w || []).length;
        } catch (e) { console.warn(`[Doluluk İşçisi]   ${dateIso} hata: ${e.message}`); }
        await new Promise((r) => setTimeout(r, 4000)); // tarihler arasi nazik bekleme
      }
      console.log(`[Doluluk İşçisi] ===== HAT BITTI: ${hat} — toplam ${hatOk} sefer GERCEK yazildi =====`);
      doneHats++;
    }
  } catch (e) {
    console.warn(`[Doluluk İşçisi] genel hata: ${e.message}`);
  } finally {
    occupancyWorkerRunning = false;
    console.log(`[Doluluk İşçisi] Tur bitti: ${doneHats} hat, ${Math.round((Date.now() - t0) / 1000)}s.`);
  }
}

// Ilk tur: server acildiktan 2 dk sonra (fiyat taramasinin ilk turuna cakismasin), sonra her 15 dk.
setTimeout(() => { runOccupancyWorker().catch(() => null); }, 2 * 60 * 1000);
setInterval(() => { runOccupancyWorker().catch(() => null); }, OCCUPANCY_WORKER_INTERVAL_MS);

// ===================== TALEP RADARI MOTORU (otonom yogunluk analizi) =====================
// oBilet Takip'ten (obilet_targets / obilet_occupancy) BAGIMSIZ. Hazir populer rota havuzunu
// (analysis_routes) arka planda tarar, her seferin GERCEK dolulugunu (koltuk haritasi) TUM
// firmalar icin okur ve analysis_occupancy_history'ye APPEND eder. Mevcut fiyat/doluluk
// iscilerine DOKUNMAZ; kendi reentrancy kilidi + nazik tempo ile calisir.
let analysisWorkerRunning = false;
// Talep Radari GECICI OLARAK KAPALI (kullanici istegi). Arka planda populer hatlari tarayan analiz
// iscisi calismaz -> ek tarama yuku de olmaz. Tekrar acmak icin: false -> true (+ script.js'de menu).
const ANALYSIS_WORKER_ENABLED = false;
const ANALYSIS_HORIZON_DAYS = 15;                        // gelecek 15 gun
const ANALYSIS_MAX_SEFER_PER_DATE = 30;                  // tarih basina en fazla sefer (tum firmalar)
const ANALYSIS_WORKER_INTERVAL_MS = 3 * 60 * 60 * 1000;  // 3 saat
const ANALYSIS_HISTORY_RETENTION_DAYS = 120;             // gecmis olcumleri bu kadar gun sakla

// Hazir populer rota havuzu. Sehirler OBILET_STATION_IDS_SEED'de tanimli olmali (route_id otomatik cikar).
const ANALYSIS_SEED_ROUTES = [
  ["Adana", "Ankara"], ["Adana", "İstanbul"], ["Adana", "Gaziantep"],
  ["Gaziantep", "İstanbul"], ["Gaziantep", "Ankara"],
  ["Diyarbakır", "İstanbul"], ["Diyarbakır", "Ankara"],
  ["Şanlıurfa", "İstanbul"], ["Van", "İstanbul"], ["Van", "Ankara"],
  ["Mersin", "Ankara"], ["Mersin", "İstanbul"],
  ["Kayseri", "İstanbul"], ["Hatay", "İstanbul"], ["Adıyaman", "Ankara"],
];

const insertAnalysisHistStmt = db.prepare(`
  INSERT INTO analysis_occupancy_history
    (route_ref, origin, destination, journey_date, operator, departure_time,
     total_seats, available_seats, occupancy_percent, source, measured_at, measured_at_iso)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'gercek', ?, ?)
`);

// Seed rotalarini DB'ye ekle (varsa dokunma) + route_id backfill dene.
function seedAnalysisRoutes() {
  try {
    const now = nowStamp();
    const insert = db.prepare(`
      INSERT OR IGNORE INTO analysis_routes (origin, destination, route_id, is_active, is_seed, created_at)
      VALUES (?, ?, ?, 1, 1, ?)
    `);
    for (const [o, d] of ANALYSIS_SEED_ROUTES) {
      const rid = buildObiletRouteIdLocal(o, d) || "";
      insert.run(o, d, rid, now);
    }
    // route_id'si bos olanlari doldurmayi dene (sehir sonradan ogrenilmis olabilir).
    const empties = db.prepare("SELECT id, origin, destination FROM analysis_routes WHERE TRIM(route_id) = ''").all();
    const upd = db.prepare("UPDATE analysis_routes SET route_id = ? WHERE id = ?");
    let filled = 0;
    for (const r of empties) {
      const rid = buildObiletRouteIdLocal(r.origin, r.destination);
      if (rid) { upd.run(rid, r.id); filled++; }
    }
    const total = db.prepare("SELECT COUNT(*) c FROM analysis_routes").get().c;
    console.log(`[Talep Radari] Rota havuzu hazir: ${total} rota (${filled} route_id backfill).`);
  } catch (e) {
    console.warn("[Talep Radari] Seed hatasi:", e.message);
  }
}

// Tek rota × tek gun tarama — TUM firmalar. Sadece analysis_occupancy_history'ye APPEND yazar.
// occupancyForHatDate'in okuma cekirdegini temel alir ama operator filtresi yok ve baska tabloya yazar.
async function scanAnalysisRouteDate(route, dateIso) {
  const routeId = String(route.route_id || "").trim();
  if (!/^\d+-\d+$/.test(routeId)) return [];
  const browser = await getBrowserInstance();
  const page = await setupObiletPage(browser);
  const journeyItems = [];
  const seatById = new Map();
  page.on("response", async (resp) => {
    try {
      const url = resp.url();
      const ct = resp.headers()["content-type"] || "";
      if (!/json/i.test(ct)) return;
      const txt = await resp.text().catch(() => "");
      if (!txt) return;
      let data; try { data = JSON.parse(txt); } catch { return; }
      if (/\/json\/sefer\//i.test(url)) {
        const m = url.match(/\/json\/sefer\/(\d+)/i);
        const c = countSeatsFromSeferJson(data);
        if (m && c) seatById.set(m[1], c);
        return;
      }
      if (/\/json\/journeys\//i.test(url)) {
        const list = Array.isArray(data?.data) ? data.data : Array.isArray(data?.journeys) ? data.journeys : [];
        for (const it of list) journeyItems.push(it);
      }
    } catch {}
  });
  let capturedBody = null, capturedBodyId = null;
  page.on("request", (req) => {
    try {
      const m = req.url().match(/\/json\/sefer\/(\d+)/i);
      if (m && req.postData()) { capturedBody = req.postData(); capturedBodyId = m[1]; }
    } catch {}
  });
  try {
    await gotoAndHandleCloudflare(page, "https://www.obilet.com/");
    await new Promise(r => setTimeout(r, 1200));
    await gotoAndHandleCloudflare(page, `https://www.obilet.com/seferler/${routeId}/${dateIso}?t=${Date.now()}`);
    for (let i = 0; i < 16 && !journeyItems.length; i++) await new Promise(r => setTimeout(r, 2000));
    if (!journeyItems.length) {
      await gotoAndHandleCloudflare(page, `https://www.obilet.com/seferler/${routeId}/${dateIso}?t=${Date.now()}`);
      for (let i = 0; i < 12 && !journeyItems.length; i++) await new Promise(r => setTimeout(r, 2000));
    }

    // TUM firmalarin seferleri (operator filtresi YOK — analizde her firma dahil).
    const infos = journeyItems.map((it) => {
      const dep = String(it?.journey?.departure || it?.departure || "");
      const tm = dep.match(/(\d{2}:\d{2})/);
      return { id: it?.id != null ? String(it.id) : null, firma: String(it?.["partner-name"] || "").trim(), time: tm ? tm[1] : "" };
    }).filter((x) => x.id && x.time && x.firma)
      .slice(0, ANALYSIS_MAX_SEFER_PER_DATE);

    if (!infos.length) { console.log(`[Talep Radari]   ${route.origin}->${route.destination} ${dateIso}: sefer yok.`); return []; }

    let viaFetch = 0, viaClick = 0;
    for (const info of infos) {
      if (seatById.has(info.id)) continue;
      if (capturedBody) {
        const c = await fetchSeferSeatsWithBody(page, info.id, capturedBody, capturedBodyId);
        await new Promise((r) => setTimeout(r, 600));
        if (c) { seatById.set(info.id, c); viaFetch++; continue; }
      }
      const doClick = () => page.evaluate((t, firm) => {
        const hasSeatBtn = (el) => Array.from(el.querySelectorAll("button,a,span,div"))
          .some((b) => /koltuk|seç|^sec$|satın al|satin al/i.test((b.innerText || "").trim()));
        let cards = Array.from(document.querySelectorAll("div,li,article"))
          .filter((el) => (el.innerText || "").length < 700 && /\d{2}:\d{2}/.test(el.innerText || "") && hasSeatBtn(el));
        const matchFirm = (el) => {
          const txt = (el.innerText || "").toLowerCase();
          if (txt.includes(firm)) return true;
          return Array.from(el.querySelectorAll("img")).some((im) =>
            ((im.getAttribute("alt") || "") + (im.getAttribute("title") || "")).toLowerCase().includes(firm));
        };
        const timed = cards.filter((el) => (el.innerText || "").includes(t));
        const target = timed.find(matchFirm) || timed[0];
        if (!target) return false;
        let best = target;
        for (const c of cards) if (target.contains(c) && c !== target && (c.innerText || "").includes(t)) { best = c; break; }
        const btn = Array.from(best.querySelectorAll("button,a,span,div"))
          .find((b) => /koltuk|seç|^sec$|satın al|satin al/i.test((b.innerText || "").trim()));
        (btn || best).click();
        return true;
      }, info.time, info.firma.toLocaleLowerCase("tr-TR")).catch(() => false);

      for (let attempt = 1; attempt <= 3 && !seatById.has(info.id); attempt++) {
        await doClick();
        await new Promise((r) => setTimeout(r, 2600));
      }
      if (seatById.has(info.id)) viaClick++;
      await page.evaluate(() => {
        const k = Array.from(document.querySelectorAll("button,a,span,div"))
          .find((b) => { const t = (b.innerText || "").trim(); return t.length < 12 && /^kapat$/i.test(t); });
        if (k) k.click();
      }).catch(() => {});
    }
    await new Promise((r) => setTimeout(r, 1200));

    // Ayni operator+saat icin birden cok listeleme olabilir -> en dolu (gercek otobus) alinir.
    const bestByKey = new Map();
    for (const info of infos) {
      const c = seatById.get(info.id);
      if (!c) continue;
      const op = normalizeObiletOperatorName(info.firma) || info.firma;
      const key = `${op}|${info.time}`;
      const prev = bestByKey.get(key);
      if (!prev || c.sold > prev.sold) bestByKey.set(key, { op, time: info.time, sold: c.sold, total: c.total });
    }

    const now = nowStamp();
    const nowIso = new Date().toISOString();
    const written = [];
    const rows = [...bestByKey.values()];
    const tx = db.transaction(() => {
      for (const v of rows) {
        const occ = v.total > 0 ? Math.max(0, Math.min(100, Math.round((v.sold / v.total) * 100))) : 0;
        insertAnalysisHistStmt.run(routeId, route.origin, route.destination, dateIso, v.op, v.time,
          v.total, Math.max(0, v.total - v.sold), occ, now, nowIso);
        written.push({ operator: v.op, time: v.time, sold: v.sold, total: v.total, occ });
      }
    });
    tx();
    console.log(`[Talep Radari]   ${route.origin}->${route.destination} ${dateIso}: ${written.length} sefer yazildi (fetch=${viaFetch},tik=${viaClick}).`);
    return written;
  } finally {
    try { await page.close(); } catch {}
  }
}

async function runAnalysisWorker() {
  if (!ANALYSIS_WORKER_ENABLED || analysisWorkerRunning) return;
  analysisWorkerRunning = true;
  const t0 = Date.now();
  let doneRoutes = 0, totalWritten = 0;
  try {
    // Retention: cok eski olcumleri temizle (append-only tablo sinirsiz buyumesin).
    try {
      db.prepare("DELETE FROM analysis_occupancy_history WHERE journey_date < ?")
        .run(shiftIsoDate(todayIsoInIstanbul(), -ANALYSIS_HISTORY_RETENTION_DAYS));
    } catch (e) {}

    const today = todayIsoInIstanbul();
    const horizonEnd = shiftIsoDate(today, ANALYSIS_HORIZON_DAYS - 1);
    const dates = buildIsoDateRange(today, horizonEnd);
    const routes = db.prepare("SELECT * FROM analysis_routes WHERE is_active = 1 AND TRIM(route_id) <> ''").all();
    console.log(`[Talep Radari] ===== TUR BASLADI: ${routes.length} rota × ${dates.length} gun =====`);
    for (const route of routes) {
      let routeWritten = 0;
      for (const dateIso of dates) {
        try {
          const w = await scanAnalysisRouteDate(route, dateIso);
          routeWritten += (w || []).length;
        } catch (e) { console.warn(`[Talep Radari]   ${route.origin}->${route.destination} ${dateIso} hata: ${e.message}`); }
        await new Promise((r) => setTimeout(r, 4000)); // tarihler arasi nazik bekleme
      }
      totalWritten += routeWritten;
      doneRoutes++;
      console.log(`[Talep Radari] Rota bitti: ${route.origin}->${route.destination} — ${routeWritten} olcum.`);
      await new Promise((r) => setTimeout(r, 20000)); // rotalar arasi nazik bekleme (Cloudflare burst'u onle)
    }
  } catch (e) {
    console.warn(`[Talep Radari] genel hata: ${e.message}`);
  } finally {
    analysisWorkerRunning = false;
    console.log(`[Talep Radari] ===== TUR BITTI: ${doneRoutes} rota, ${totalWritten} olcum, ${Math.round((Date.now() - t0) / 1000)}s. =====`);
  }
}

// Boot: rota havuzunu seed'le, ilk turu occupancy'den (2dk) farkli offset'te (6dk) baslat, sonra periyodik.
seedAnalysisRoutes();
setTimeout(() => { runAnalysisWorker().catch(() => null); }, 6 * 60 * 1000);
setInterval(() => { runAnalysisWorker().catch(() => null); }, ANALYSIS_WORKER_INTERVAL_MS);

// Eski pending kayıtları temizle - buildJourneyIdentityKey değişikliğinden sonra gerekli
try {
  db.prepare("UPDATE obilet_prices SET pending_price = NULL, pending_seen_count = 0 WHERE pending_seen_count > 0").run();
  console.log("[oBilet] Eski pending fiyat kayıtları temizlendi.");
} catch (e) {
  console.warn("[oBilet] Pending temizleme atlandı:", e.message);
}

// Boot-time backfill: route_id'si bos olan hatlar icin statik tablodan otomatik doldur.
try {
  const emptyTargets = db.prepare("SELECT id, origin, destination FROM obilet_targets WHERE TRIM(route_id) = ''").all();
  const updateStmt = db.prepare("UPDATE obilet_targets SET route_id = ? WHERE id = ?");
  let filled = 0;
  for (const t of emptyTargets) {
    const rid = buildObiletRouteIdLocal(t.origin, t.destination);
    if (rid) {
      updateStmt.run(rid, t.id);
      console.log(`[oBilet] Hat #${t.id} (${t.origin}->${t.destination}) icin route_id atandi: ${rid}`);
      filled++;
    }
  }
  if (filled > 0) console.log(`[oBilet] ${filled} hat icin route_id otomatik dolduruldu.`);
} catch (e) {
  console.warn("[oBilet] Route ID backfill atlandı:", e.message);
}

// Graceful shutdown: Browser instance'ı temizle
process.on("SIGTERM", async () => {
  console.log("[Process] SIGTERM sinyali alındı, temizlik yapılıyor...");
  await closeBrowserInstance();
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("[Process] SIGINT sinyali alındı, temizlik yapılıyor...");
  await closeBrowserInstance();
  process.exit(0);
});

// Telegram test endpoint'i — panelden veya tarayicidan tetiklenir, ornek bildirim gonderir.
app.post("/api/telegram/test", requireAuth, async (req, res) => {
  if (!TELEGRAM_ENABLED) {
    return res.status(400).json({ message: "Telegram botu kapali (TELEGRAM_BOT_TOKEN tanimli degil)." });
  }
  if (TELEGRAM_DEFAULT_CHAT_IDS.length === 0) {
    return res.status(400).json({ message: "Sohbet ID tanimli degil (TELEGRAM_CHAT_ID)." });
  }
  const text =
    "<b>Test bildirimi</b>\n" +
    "KK oBilet fiyat takip botu çalışıyor. Fiyat değişikliklerinde bildirim buraya gelecek. ";
  let sent = 0;
  for (const chatId of TELEGRAM_DEFAULT_CHAT_IDS) {
    if (await sendTelegramMessage(chatId, text)) sent++;
  }
  res.json({ ok: true, sent, total: TELEGRAM_DEFAULT_CHAT_IDS.length });
});

app.listen(PORT, () => {
  console.log(`Sunucu calisiyor: http://localhost:${PORT}`);
  if (TELEGRAM_ENABLED) {
    console.log(`[Telegram] Bildirim botu AKTIF (${TELEGRAM_DEFAULT_CHAT_IDS.length} varsayilan sohbet).`);
    // Komut dinleyiciyi baslat (long polling, arka planda surekli calisir).
    startTelegramPolling().catch((e) => console.error(`[Telegram] Polling baslatma hatasi: ${e.message}`));
  } else {
    console.log("[Telegram] Bildirim botu KAPALI (TELEGRAM_BOT_TOKEN tanimli degil).");
  }
});
