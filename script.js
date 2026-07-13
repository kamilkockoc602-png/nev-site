const TOKEN_KEY = "bus_auth_token_v2";
const THEME_KEY = "bus_theme_v1";

// Eski kalıcı oturumları temizle (isteğe bağlı güvenlik önlemi)
if (localStorage.getItem(TOKEN_KEY)) {
  localStorage.removeItem(TOKEN_KEY);
}

const MENUS = [
  { key: "dashboard", label: "Genel Panel" },
  { key: "routes", label: "Bakanlik Fiyati" },
  { key: "pricing", label: "Fiyat Yukleme" },
  { key: "reports", label: "Tek Yon Fiyatlar" },
  { key: "reporting", label: "Raporlama" },
  { key: "oneops", label: "Hatali Islemler" },
  { key: "obilet_tracker", label: "oBilet Takip" },
  { key: "occupancy", label: "Doluluk Takip" },
  { key: "sefer_takip", label: "Sefer Takip" },
  { key: "sure_hesap", label: "Pkm Form Süre Hesaplama" },
  // { key: "demand", label: "Talep Radarı" },  // GECICI KAPALI (kullanici istegi) — geri acmak icin bu satiri ac + server.js ANALYSIS_WORKER_ENABLED=true
  { key: "ocr", label: "Foto Tarama" },
  { key: "permissions", label: "Yetki Menusu" },
  { key: "logs", label: "Giris Kayitlari" },
];

const ADMIN_ONLY_MENUS = new Set(["permissions", "logs"]);

const dom = {
  loginView: document.getElementById("loginView"),
  portalView: document.getElementById("portalView"),
  loginForm: document.getElementById("loginForm"),
  loginUsername: document.getElementById("loginUsername"),
  loginPassword: document.getElementById("loginPassword"),
  loginRemember: document.getElementById("loginRemember"),
  loginPasswordToggle: document.getElementById("loginPasswordToggle"),
  loginMessage: document.getElementById("loginMessage"),
  menuList: document.getElementById("menuList"),
  contentCard: document.querySelector(".content"),
  activeTitle: document.getElementById("activeTitle"),
  logoutBtn: document.getElementById("logoutBtn"),
  currentUserLabel: document.getElementById("currentUserLabel"),
  sidebarUserAvatar: document.getElementById("sidebarUserAvatar"),
  sidebarUserName: document.getElementById("sidebarUserName"),
  sidebarUserRole: document.getElementById("sidebarUserRole"),
  sidebarUserSessionText: document.getElementById("sidebarUserSessionText"),
  activeSubtitle: document.getElementById("activeSubtitle"),
  contentHeadStat: document.getElementById("contentHeadStat"),
  dashGreeting: document.getElementById("dashGreeting"),
  dashTitle: document.getElementById("dashTitle"),
  dashGeneratedAt: document.getElementById("dashGeneratedAt"),
  dashActiveTargets: document.getElementById("dashActiveTargets"),
  dashTotalJourneys: document.getElementById("dashTotalJourneys"),
  dashTodayChanges: document.getElementById("dashTodayChanges"),
  dash24hChanges: document.getElementById("dash24hChanges"),
  dashBiggestAmount: document.getElementById("dashBiggestAmount"),
  dashBiggestDetail: document.getElementById("dashBiggestDetail"),
  dashTopOperator: document.getElementById("dashTopOperator"),
  dashTopOperatorCount: document.getElementById("dashTopOperatorCount"),
  dashTrendSummary: document.getElementById("dashTrendSummary"),
  dashTrendChart: document.getElementById("dashTrendChart"),
  dashNotifList: document.getElementById("dashNotifList"),
  dashAllNotifsLink: document.getElementById("dashAllNotifsLink"),
  permissionAdminArea: document.getElementById("permissionAdminArea"),
  loginLogsArea: document.getElementById("loginLogsArea"),
  templateRow: document.getElementById("userRowTemplate"),
  loadingOverlay: document.getElementById("loadingOverlay"),
  routeTableBody: document.getElementById("routeTableBody"),
  tariffSearchInput: document.getElementById("tariffSearchInput"),
  tariffResetBtn: document.getElementById("tariffResetBtn"),
  tariffLoadMoreBtn: document.getElementById("tariffLoadMoreBtn"),
  tariffSummary: document.getElementById("tariffSummary"),
  reportSearchInput: document.getElementById("reportSearchInput"),
  reportSearchResetBtn: document.getElementById("reportSearchResetBtn"),
  reportSummary: document.getElementById("reportSummary"),
  reportTableBody: document.getElementById("reportTableBody"),
  reportingDateInput: document.getElementById("reportingDateInput"),
  reportingOriginInput: document.getElementById("reportingOriginInput"),
  reportingSyncBtn: document.getElementById("reportingSyncBtn"),
  reportingRefreshBtn: document.getElementById("reportingRefreshBtn"),
  reportingFilterInput: document.getElementById("reportingFilterInput"),
  reportingRideUuidInput: document.getElementById("reportingRideUuidInput"),
  reportingPlateDebugBtn: document.getElementById("reportingPlateDebugBtn"),
  reportingPlateDebugMsg: document.getElementById("reportingPlateDebugMsg"),
  reportingRevenueTryTotal: document.getElementById("reportingRevenueTryTotal"),
  reportingRevenueEurTotal: document.getElementById("reportingRevenueEurTotal"),
  reportingRevenueCount: document.getElementById("reportingRevenueCount"),
  reportingSummary: document.getElementById("reportingSummary"),
  reportingTableBody: document.getElementById("reportingTableBody"),
  errorReportForm: document.getElementById("errorReportForm"),
  errorDescInput: document.getElementById("errorDescInput"),
  errorUserInput: document.getElementById("errorUserInput"),
  errorPhotosInput: document.getElementById("errorPhotosInput"),
  errorUploadMsg: document.getElementById("errorUploadMsg"),
  errorListArea: document.getElementById("errorListArea"),
  pricingUploadForm: document.getElementById("pricingUploadForm"),
  pricingDirectionType: document.getElementById("pricingDirectionType"),
  pricingValidFrom: document.getElementById("pricingValidFrom"),
  pricingValidTo: document.getElementById("pricingValidTo"),
  pricingExcelFile: document.getElementById("pricingExcelFile"),
  pricingDescriptionInput: document.getElementById("pricingDescriptionInput"),
  pricingUploadBtn: document.getElementById("pricingUploadBtn"),
  pricingUploadMsg: document.getElementById("pricingUploadMsg"),
  pricingRejectedWrap: document.getElementById("pricingRejectedWrap"),
  pricingRejectedList: document.getElementById("pricingRejectedList"),
  pricingUploadsList: document.getElementById("pricingUploadsList"),
  avgFare: document.getElementById("avgFare"),
  totalRoutes: document.getElementById("totalRoutes"),
  updateCount: document.getElementById("updateCount"),
  lastPriceUpdate: document.getElementById("lastPriceUpdate"),
  expiredUploadsCount: document.getElementById("expiredUploadsCount"),
  expiredUploadsEmpty: document.getElementById("expiredUploadsEmpty"),
  expiredUploadsList: document.getElementById("expiredUploadsList"),
  notifBtn: document.getElementById("notifBtn"),
  notifBadge: document.getElementById("notifBadge"),
  notifPanel: document.getElementById("notifPanel"),
  notifList: document.getElementById("notifList"),
  notifEmpty: document.getElementById("notifEmpty"),
  notifReadAllBtn: document.getElementById("notifReadAllBtn"),
  notifSoundBtn: document.getElementById("notifSoundBtn"),
  themeToggleBtn: document.getElementById("themeToggleBtn"),
  themeLabel: document.getElementById("themeLabel"),
  ocrOriginInput: document.getElementById("ocrOriginInput"),
  ocrImageInput: document.getElementById("ocrImageInput"),
  ocrScanBtn: document.getElementById("ocrScanBtn"),
  ocrCopyBtn: document.getElementById("ocrCopyBtn"),
  ocrStatus: document.getElementById("ocrStatus"),
  ocrOutput: document.getElementById("ocrOutput"),
  ocrReviewPanel: document.getElementById("ocrReviewPanel"),
  ocrReviewBody: document.getElementById("ocrReviewBody"),
  ocrApplyReviewBtn: document.getElementById("ocrApplyReviewBtn"),
  errorPriorityInput: document.getElementById("errorPriorityInput"),
  errorSearchInput: document.getElementById("errorSearchInput"),
  // Fiyat Değişiklik Raporu (Raporlama menusu yeni)
  phFromDate: document.getElementById("phFromDate"),
  phToDate: document.getElementById("phToDate"),
  phTargetSelect: document.getElementById("phTargetSelect"),
  phSearchInput: document.getElementById("phSearchInput"),
  phApplyBtn: document.getElementById("phApplyBtn"),
  phResetBtn: document.getElementById("phResetBtn"),
  phExportBtn: document.getElementById("phExportBtn"),
  phStatTotal: document.getElementById("phStatTotal"),
  phStatIncrease: document.getElementById("phStatIncrease"),
  phStatDecrease: document.getElementById("phStatDecrease"),
  phStatRoutes: document.getElementById("phStatRoutes"),
  phStatusMsg: document.getElementById("phStatusMsg"),
  phTableBody: document.getElementById("phTableBody"),
  phChipsRow: document.getElementById("phChipsRow"),
  phRouteCards: document.getElementById("phRouteCards"),
  phDetailBackdrop: document.getElementById("phDetailBackdrop"),
  phDetailPanel: document.getElementById("phDetailPanel"),
  phDetailTitle: document.getElementById("phDetailTitle"),
  phDetailSubtitle: document.getElementById("phDetailSubtitle"),
  phDetailClose: document.getElementById("phDetailClose"),
  phDetailStats: document.getElementById("phDetailStats"),
  phDetailChart: document.getElementById("phDetailChart"),
  phDetailHistory: document.getElementById("phDetailHistory"),
  phDetailPeers: document.getElementById("phDetailPeers"),
};

const state = {
  token: sessionStorage.getItem(TOKEN_KEY) || "",
  currentUser: null,
  usersCache: [],
  prices: [],
  tariffCatalog: [],
  tariffRows: [],
  tariffTotal: 0,
  tariffOffset: 0,
  tariffQuery: "",
  reportQuery: "",
  reportingDate: "",
  reportingOrigin: "Siirt",
  reportingQuery: "",
  reportingRows: [],
  controlConfig: null,
  tariffPageSize: 50,
  tariffSearchTimer: null,
  pricingUploads: [],
  notifications: [],
  notifPollTimer: null,
  // Bu oturumda ekranda GOSTERILMIS bildirim id'leri (dupe engeller); "gorüldü" kalici takip sunucuda.
  shownToastIds: { price: new Set(), structure: new Set() },
  theme: localStorage.getItem(THEME_KEY) || "light",
  lastOcrPairs: [],
  lastOcrOrigin: "",
  lastOcrDestinationPool: [],
  lastSuspiciousRows: [],
  panelSwitchSeq: 0,
  activeMenu: "",
  reportingAutoTimer: null,
};

function applyTheme(theme) {
  state.theme = theme === "dark" ? "dark" : "light";
  document.body.setAttribute("data-theme", state.theme);
  dom.themeLabel.textContent = state.theme === "dark" ? "Koyu" : "Acik";
  localStorage.setItem(THEME_KEY, state.theme);
}

function setOcrStatus(message, isError = false) {
  if (!dom.ocrStatus) {
    return;
  }
  dom.ocrStatus.style.color = isError ? "#d64545" : "var(--muted)";
  dom.ocrStatus.textContent = message;
}

function normalizePrice(raw) {
  if (!raw) {
    return "";
  }

  const digits = String(raw).replace(/[^0-9]/g, "");
  return digits;
}

function normalizeSearchText(value) {
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

function todayIsoDate() {
  const date = new Date();
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

const KNOWN_DESTINATIONS = [
  "Erzurum", "Batman", "Refahiye", "Zara", "Sivas", "Akdağmadeni", "Sorgun", "Yozgat",
  "Kırıkkale", "Ankara", "Eskişehir", "Bozüyük", "İnegöl", "Bursa", "Biga", "Bandırma",
  "Lapseki", "Çanakkale", "Kayseri", "Nevşehir", "Aksaray", "Konya", "Akşehir", "Çay",
  "Dinar", "Denizli", "Sarayköy", "Nazilli", "Aydın", "Çine", "Yatağan", "Milas", "Marmaris",
  "Bayburt", "Gümüşhane", "Trabzon", "Akçaabat", "Espiye", "Görele", "Tirebolu", "Giresun",
  "Ordu", "Fatsa", "Çarşamba", "Perşembe", "Samsun", "Sinop", "Gaziantep", "Osmaniye",
  "Adana", "Ereğli", "Ilgın", "Afyon", "Uşak", "Kula", "Salihli", "Turgutlu", "Manisa",
  "İzmir", "Didim", "Bodrum", "Polatlı", "Balıkesir", "Edremit", "Burhaniye", "Ayvalık",
  "Bolu", "Düzce", "Sakarya", "Kocaeli", "Gebze", "Alibeyköy", "Dudullu", "Esenler",
  "Pınarbaşı", "Yıldızeli", "Tokat", "Amasya", "Terme", "Keşap", "Fındıklı", "Pazar", "Of",
  "Boğazlıyan", "Sarıkaya", "Alaca", "Çorum", "Merzifon", "Hekimhan", "Kangal", "Ulaş",
  "Hafik", "İmranlı", "Erzincan", "Kelkit", "Torul", "Malatya", "Elazığ", "Bingöl", "Genç",
  "Karlıova", "Çat", "Aşkale", "Araklı", "Sürmene", "Horasan", "Pasinler", "Selim",
  "Sarıkamış", "Kars", "Ardahan"
];

const KNOWN_DESTINATION_SLUGS = new Set(KNOWN_DESTINATIONS.map((city) => slugTr(city)));

function slugTr(value) {
  return String(value || "")
    .toLocaleLowerCase("tr-TR")
    .replace(/ı/g, "i")
    .replace(/ğ/g, "g")
    .replace(/ü/g, "u")
    .replace(/ş/g, "s")
    .replace(/ö/g, "o")
    .replace(/ç/g, "c")
    .replace(/[^a-z\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function levenshtein(a, b) {
  const s = slugTr(a);
  const t = slugTr(b);
  const m = s.length;
  const n = t.length;
  if (!m) return n;
  if (!n) return m;

  const dp = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));
  for (let i = 0; i <= m; i += 1) dp[i][0] = i;
  for (let j = 0; j <= n; j += 1) dp[0][j] = j;

  for (let i = 1; i <= m; i += 1) {
    for (let j = 1; j <= n; j += 1) {
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost
      );
    }
  }

  return dp[m][n];
}

function correctDestinationName(name) {
  return correctDestinationNameWithPool(name, KNOWN_DESTINATIONS);
}

function correctDestinationNameWithPool(
  name,
  destinationPool = KNOWN_DESTINATIONS,
  forcePoolMatch = false
) {
  const raw = String(name || "").trim();
  if (!raw) {
    return raw;
  }

  const pool = Array.isArray(destinationPool) && destinationPool.length
    ? destinationPool
    : KNOWN_DESTINATIONS;

  const exact = pool.find((city) => slugTr(city) === slugTr(raw));
  if (exact) {
    return toTurkishTitleCase(exact);
  }

  let best = null;
  let bestDistance = Infinity;
  for (const city of pool) {
    const d = levenshtein(raw, city);
    if (d < bestDistance) {
      bestDistance = d;
      best = city;
    }
  }

  const rawLen = slugTr(raw).length;
  const maxAllowed = rawLen <= 5 ? 1 : 2;
  if (best && bestDistance <= maxAllowed) {
    return toTurkishTitleCase(best);
  }

  if (forcePoolMatch && best) {
    return toTurkishTitleCase(best);
  }

  return toTurkishTitleCase(raw);
}

function toTurkishTitleCase(text) {
  return String(text)
    .split(" ")
    .filter(Boolean)
    .map((word) => {
      const first = word.charAt(0).toLocaleUpperCase("tr-TR");
      const rest = word.slice(1).toLocaleLowerCase("tr-TR");
      return `${first}${rest}`;
    })
    .join(" ");
}

function normalizeCity(raw) {
  if (!raw) {
    return "";
  }

  const cleaned = String(raw)
    .replace(/[0-9]/g, "")
    .replace(/[=:\-]/g, " ")
    .replace(/[^A-Za-z\u00C7\u011E\u0130\u00D6\u015E\u00DC\u00E7\u011F\u0131\u00F6\u015F\u00FC\s'.]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return toTurkishTitleCase(cleaned);
}

function isLikelyRow(destination, price) {
  if (!destination || destination.length < 3) {
    return false;
  }

  const priceNum = Number(price);
  if (!Number.isFinite(priceNum) || priceNum < 100 || priceNum > 20000) {
    return false;
  }

  return true;
}

function normalizeBusinessPrice(price) {
  let n = Number(price);
  if (!Number.isFinite(n)) {
    return "";
  }

  // OCR bazen sondan sifir ekler: 900 -> 9000, 1800 -> 18000
  while (n > 3500 && n % 10 === 0) {
    n = n / 10;
  }

  // OCR bazen sifir yutar: 16 -> 160, 90 -> 900
  while (n > 0 && n < 100) {
    n = n * 10;
  }

  if (n < 100 || n > 5000) {
    return "";
  }

  return String(Math.round(n));
}

function parseOcrPriceToken(rawToken) {
  let token = String(rawToken || "").trim();
  if (!token) {
    return "";
  }

  token = token.replace(/[^0-9,.-]/g, "");
  if (!token) {
    return "";
  }

  let normalized = token;
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
    const parts = normalized.split(",");
    if (parts.length === 2 && parts[1].length === 3) {
      normalized = parts.join("");
    } else {
      normalized = normalized.replace(/,/g, ".");
    }
  } else if (hasDot) {
    const parts = normalized.split(".");
    if (parts.length === 2 && parts[1].length === 3) {
      normalized = parts.join("");
    }
  }

  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) {
    return "";
  }

  return normalizeBusinessPrice(parsed);
}

function buildDestinationSlugSet(destinationPool = KNOWN_DESTINATIONS) {
  const pool = Array.isArray(destinationPool) && destinationPool.length
    ? destinationPool
    : KNOWN_DESTINATIONS;
  return new Set(pool.map((city) => slugTr(city)).filter(Boolean));
}

function buildTariffDestinationPool(origin) {
  const originSlug = slugTr(origin);
  if (!originSlug || !Array.isArray(state.tariffCatalog) || !state.tariffCatalog.length) {
    return KNOWN_DESTINATIONS;
  }

  const result = [];
  const seen = new Set();

  for (const row of state.tariffCatalog) {
    const route = String(row?.route || "");
    const parts = route.split(" - ").map((part) => String(part || "").trim()).filter(Boolean);
    if (parts.length < 2) {
      continue;
    }

    const from = parts[0];
    const to = parts.slice(1).join(" - ");
    const fromSlug = slugTr(from);
    const toSlug = slugTr(to);

    let candidate = "";
    if (fromSlug === originSlug) {
      candidate = to;
    } else if (toSlug === originSlug) {
      candidate = from;
    }

    const key = slugTr(candidate);
    if (!key || seen.has(key)) {
      continue;
    }

    seen.add(key);
    result.push(toTurkishTitleCase(candidate));
  }

  return result.length ? result : KNOWN_DESTINATIONS;
}

function findDestinationInLine(line, destinationPool = KNOWN_DESTINATIONS) {
  const lineSlug = slugTr(line);
  if (!lineSlug) {
    return "";
  }

  const pool = Array.isArray(destinationPool) && destinationPool.length
    ? destinationPool
    : KNOWN_DESTINATIONS;

  let best = "";
  let bestLen = 0;

  for (const city of pool) {
    const citySlug = slugTr(city);
    if (citySlug && lineSlug.includes(citySlug) && citySlug.length > bestLen) {
      best = city;
      bestLen = citySlug.length;
    }
  }

  if (best) {
    return toTurkishTitleCase(best);
  }

  const beforeDigit = String(line).split(/\d/)[0] || "";
  return correctDestinationNameWithPool(normalizeCity(beforeDigit), pool, false);
}

function findKnownDestinationInLine(line, destinationPool = KNOWN_DESTINATIONS) {
  const lineSlug = slugTr(line);
  if (!lineSlug) {
    return "";
  }

  const pool = Array.isArray(destinationPool) && destinationPool.length
    ? destinationPool
    : KNOWN_DESTINATIONS;

  let best = "";
  let bestLen = 0;

  for (const city of pool) {
    const citySlug = slugTr(city);
    if (citySlug && lineSlug.includes(citySlug) && citySlug.length > bestLen) {
      best = city;
      bestLen = citySlug.length;
    }
  }

  return best ? toTurkishTitleCase(best) : "";
}

function isKnownDestinationName(name, destinationSlugSet = KNOWN_DESTINATION_SLUGS) {
  return destinationSlugSet.has(slugTr(name));
}

function extractPriceCandidates(line) {
  const matches = String(line).match(/(?:\d{1,3}(?:[.,]\d{3})+|\d+)(?:[.,]\d{2})?/g) || [];
  const out = [];
  for (const m of matches) {
    const price = parseOcrPriceToken(m);
    if (price) {
      out.push(price);
    }
  }
  return out;
}

function parseLineToPair(line, destinationPool = KNOWN_DESTINATIONS) {
  const cleaned = String(line || "").replace(/[|]/g, " ").replace(/\s+/g, " ").trim();
  if (!cleaned) {
    return null;
  }

  const destination = findDestinationInLine(cleaned, destinationPool);
  const destinationSlugSet = buildDestinationSlugSet(destinationPool);
  if (!isKnownDestinationName(destination, destinationSlugSet)) {
    return null;
  }

  const priceCandidates = extractPriceCandidates(cleaned);
  const price = priceCandidates[priceCandidates.length - 1] || "";

  if (!isLikelyRow(destination, price)) {
    return null;
  }

  return { destination, price };
}

function parseDestinationsAndPrices(text, destinationPool = KNOWN_DESTINATIONS) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const pairs = [];

  for (const line of lines) {
    const pair = parseLineToPair(line, destinationPool);
    if (pair) {
      pairs.push(pair);
    }
  }

  const seen = new Set();
  return pairs.filter((item) => {
    const key = item.destination.toLocaleLowerCase("tr-TR");
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function dedupePairs(items) {
  const out = [];
  const seen = new Set();
  for (const item of items) {
    const key = slugTr(item.destination);
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(item);
  }
  return out;
}

function aggregateReliablePairs(
  items,
  destinationSlugSet = KNOWN_DESTINATION_SLUGS,
  destinationPool = KNOWN_DESTINATIONS
) {
  const byDestination = new Map();

  for (const item of items) {
    if (!item || !item.destination || !item.price) {
      continue;
    }

    const key = slugTr(item.destination);
    if (!key) {
      continue;
    }

    if (!byDestination.has(key)) {
      byDestination.set(key, {
        destination: item.destination,
        total: 0,
        isKnown: isKnownDestinationName(item.destination, destinationSlugSet),
        priceCounts: new Map(),
      });
    }

    const bucket = byDestination.get(key);
    bucket.total += 1;
    if (isKnownDestinationName(item.destination, destinationSlugSet)) {
      bucket.destination =
        findKnownDestinationInLine(item.destination, destinationPool) || item.destination;
      bucket.isKnown = true;
    }

    const priceKey = String(item.price);
    bucket.priceCounts.set(priceKey, (bucket.priceCounts.get(priceKey) || 0) + 1);
  }

  const results = [];
  for (const bucket of byDestination.values()) {
    const priceEntries = Array.from(bucket.priceCounts.entries()).sort((a, b) => {
      if (b[1] !== a[1]) {
        return b[1] - a[1];
      }
      return Number(a[0]) - Number(b[0]);
    });

    const bestPrice = priceEntries[0] ? priceEntries[0][0] : "";
    if (!bestPrice) {
      continue;
    }

    // Bilinmeyen ve tek sefer gorulmus satirlar genelde OCR copu olur.
    if (!bucket.isKnown && bucket.total < 2) {
      continue;
    }

    results.push({
      destination: bucket.destination,
      price: bestPrice,
    });
  }

  return dedupePairs(results);
}

function buildSuspiciousRows(allPairs, acceptedPairs, destinationSlugSet = KNOWN_DESTINATION_SLUGS) {
  const acceptedMap = new Map(
    acceptedPairs.map((item) => [slugTr(item.destination), String(item.price)])
  );
  const freq = new Map();

  for (const item of allPairs) {
    const key = `${slugTr(item.destination)}|${item.price}`;
    freq.set(key, (freq.get(key) || 0) + 1);
  }

  const rows = [];
  const seen = new Set();
  for (const item of allPairs) {
    const destKey = slugTr(item.destination);
    const key = `${destKey}|${item.price}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);

    const known = isKnownDestinationName(item.destination, destinationSlugSet);
    const isAcceptedPrice = acceptedMap.get(destKey) === String(item.price);
    const confidence = freq.get(key) || 0;

    const isSuspicious = !known || !isAcceptedPrice || confidence < 2;
    if (!isSuspicious) {
      continue;
    }

    rows.push({
      destination: item.destination,
      price: String(item.price),
      confidence,
      include: false,
    });
  }

  rows.sort((a, b) => a.destination.localeCompare(b.destination, "tr"));
  return rows.slice(0, 80);
}

function renderSuspiciousRows(rows) {
  if (!dom.ocrReviewPanel || !dom.ocrReviewBody) {
    return;
  }

  if (!rows.length) {
    dom.ocrReviewBody.innerHTML = "";
    dom.ocrReviewPanel.classList.add("hidden");
    return;
  }

  dom.ocrReviewPanel.classList.remove("hidden");
  dom.ocrReviewBody.innerHTML = "";

  for (const item of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="checkbox" class="ocr-include" ${item.include ? "checked" : ""} /></td>
      <td><input type="text" class="ocr-destination" value="${item.destination}" /></td>
      <td><input type="number" class="ocr-price" value="${item.price}" step="100" min="100" /></td>
    `;
    dom.ocrReviewBody.appendChild(tr);
  }
}

function applyReviewedRowsToOutput() {
  if (!dom.ocrReviewBody || !dom.ocrOutput) {
    return;
  }

  const baseMap = new Map();
  for (const item of state.lastOcrPairs || []) {
    baseMap.set(slugTr(item.destination), {
      destination: item.destination,
      price: String(item.price),
    });
  }

  const rows = dom.ocrReviewBody.querySelectorAll("tr");
  const destinationPool =
    Array.isArray(state.lastOcrDestinationPool) && state.lastOcrDestinationPool.length
      ? state.lastOcrDestinationPool
      : KNOWN_DESTINATIONS;
  rows.forEach((tr) => {
    const include = tr.querySelector(".ocr-include")?.checked;
    if (!include) {
      return;
    }

    const rawDestination = tr.querySelector(".ocr-destination")?.value || "";
    const rawPrice = tr.querySelector(".ocr-price")?.value || "";
    const destination = correctDestinationNameWithPool(
      normalizeCity(rawDestination),
      destinationPool,
      true
    );
    const price = normalizeBusinessPrice(normalizePrice(rawPrice));

    if (!isLikelyRow(destination, price)) {
      return;
    }

    baseMap.set(slugTr(destination), { destination, price: String(price) });
  });

  const finalPairs = Array.from(baseMap.values()).sort((a, b) =>
    a.destination.localeCompare(b.destination, "tr")
  );
  state.lastOcrPairs = finalPairs;
  dom.ocrOutput.value = toTsv(state.lastOcrOrigin || dom.ocrOriginInput?.value || "Patnos", finalPairs);
  setOcrStatus(`${finalPairs.length} satir guncellendi. TSV hazir.`);
}

function groupWordsByRow(words) {
  if (!Array.isArray(words) || !words.length) {
    return [];
  }

  const filtered = words
    .filter((w) => w && w.text && w.bbox)
    .map((w) => ({
      text: String(w.text).trim(),
      x0: Number(w.bbox.x0),
      x1: Number(w.bbox.x1),
      y0: Number(w.bbox.y0),
      y1: Number(w.bbox.y1),
    }))
    .filter((w) => w.text && Number.isFinite(w.x0) && Number.isFinite(w.y0));

  if (!filtered.length) {
    return [];
  }

  const avgHeight =
    filtered.reduce((acc, w) => acc + Math.max(1, w.y1 - w.y0), 0) / filtered.length;
  const tolerance = Math.max(8, Math.round(avgHeight * 0.7));

  filtered.sort((a, b) => ((a.y0 + a.y1) / 2) - ((b.y0 + b.y1) / 2));

  const rows = [];
  for (const word of filtered) {
    const yCenter = (word.y0 + word.y1) / 2;
    let bucket = null;
    for (const row of rows) {
      if (Math.abs(row.y - yCenter) <= tolerance) {
        bucket = row;
        break;
      }
    }

    if (!bucket) {
      bucket = { y: yCenter, words: [] };
      rows.push(bucket);
    }

    bucket.words.push(word);
    bucket.y = (bucket.y * (bucket.words.length - 1) + yCenter) / bucket.words.length;
  }

  rows.forEach((row) => row.words.sort((a, b) => a.x0 - b.x0));
  rows.sort((a, b) => a.y - b.y);
  return rows;
}

function parseA4SegmentWords(segmentWords, destinationPool = KNOWN_DESTINATIONS) {
  if (!segmentWords || segmentWords.length < 2) {
    return null;
  }

  const text = segmentWords.map((w) => w.text).join(" ");
  const destination =
    findKnownDestinationInLine(text, destinationPool) ||
    correctDestinationNameWithPool(
      normalizeCity(String(text).split(/\d/)[0] || ""),
      destinationPool,
      false
    );
  if (!destination) {
    return null;
  }

  const destinationSlugSet = buildDestinationSlugSet(destinationPool);
  if (!isKnownDestinationName(destination, destinationSlugSet)) {
    return null;
  }

  const priceCandidates = extractPriceCandidates(text);
  const price = priceCandidates[priceCandidates.length - 1] || "";
  if (!isLikelyRow(destination, price)) {
    return null;
  }

  return { destination, price };
}

function parseA4FromOcrData(ocrData, destinationPool = KNOWN_DESTINATIONS) {
  const rows = groupWordsByRow(ocrData?.words || []);
  if (!rows.length) {
    return [];
  }

  const centers = [];
  rows.forEach((row) => {
    row.words.forEach((w) => {
      centers.push((w.x0 + w.x1) / 2);
    });
  });

  if (!centers.length) {
    return [];
  }

  centers.sort((a, b) => a - b);
  const splitX = centers[Math.floor(centers.length / 2)];
  const margin = 12;
  const pairs = [];

  for (const row of rows) {
    const left = row.words.filter((w) => (w.x0 + w.x1) / 2 < splitX - margin);
    const right = row.words.filter((w) => (w.x0 + w.x1) / 2 >= splitX + margin);

    const leftPair = parseA4SegmentWords(left, destinationPool);
    const rightPair = parseA4SegmentWords(right, destinationPool);

    if (leftPair) {
      pairs.push(leftPair);
    }
    if (rightPair) {
      pairs.push(rightPair);
    }
  }

  return dedupePairs(pairs);
}

function parseStructuredRowsFromOcrData(ocrData, destinationPool = KNOWN_DESTINATIONS) {
  const rows = groupWordsByRow(ocrData?.words || []);
  if (!rows.length) {
    return [];
  }

  const pairs = [];
  const gapThreshold = 48;

  for (const row of rows) {
    if (!row.words || !row.words.length) {
      continue;
    }

    // Ayni satirdaki buyuk yatay bosluklar farkli kolonlar oldugunu gosterir.
    const segments = [];
    let current = [row.words[0]];

    for (let i = 1; i < row.words.length; i += 1) {
      const prev = row.words[i - 1];
      const next = row.words[i];
      const gap = next.x0 - prev.x1;
      if (gap > gapThreshold) {
        segments.push(current);
        current = [next];
      } else {
        current.push(next);
      }
    }
    segments.push(current);

    for (const segment of segments) {
      const text = segment.map((w) => w.text).join(" ");
      const pair = parseLineToPair(text, destinationPool);
      if (pair) {
        pairs.push(pair);
      }
    }

    const wholeRowText = row.words.map((w) => w.text).join(" ");
    const rowPair = parseLineToPair(wholeRowText, destinationPool);
    if (rowPair) {
      pairs.push(rowPair);
    }
  }

  return dedupePairs(pairs);
}

function parseTabularRowsFromOcrData(ocrData, destinationPool = KNOWN_DESTINATIONS) {
  const rows = groupWordsByRow(ocrData?.words || []);
  if (!rows.length) {
    return [];
  }

  const pairs = [];
  const destinationSlugSet = buildDestinationSlugSet(destinationPool);
  const headerHints = ["gidecegi", "yer", "fiyat", "tarife"];

  for (const row of rows) {
    const words = (row.words || []).map((w) => String(w.text || "").trim()).filter(Boolean);
    if (!words.length) {
      continue;
    }

    const rowText = words.join(" ").toLocaleLowerCase("tr-TR");
    if (headerHints.some((hint) => rowText.includes(hint))) {
      continue;
    }

    // Tek satirda birden fazla "Sehir + Fiyat" ciftini yakalar.
    const segments = [];
    let currentWords = [];

    for (const token of words) {
      const parsedPrice = parseOcrPriceToken(token);
      if (parsedPrice) {
        if (currentWords.length) {
          segments.push({
            destinationRaw: currentWords.join(" "),
            price: parsedPrice,
          });
          currentWords = [];
        }
      } else {
        currentWords.push(token);
      }
    }

    for (const segment of segments) {
      const destination = correctDestinationNameWithPool(
        normalizeCity(segment.destinationRaw),
        destinationPool,
        false
      );

      if (!isKnownDestinationName(destination, destinationSlugSet)) {
        continue;
      }

      if (!isLikelyRow(destination, segment.price)) {
        continue;
      }

      pairs.push({
        destination,
        price: String(segment.price),
      });
    }
  }

  return dedupePairs(pairs);
}

function collectTextBlocks(ocrData) {
  const blocks = [];
  if (ocrData?.text) {
    blocks.push(String(ocrData.text));
  }
  if (Array.isArray(ocrData?.lines) && ocrData.lines.length) {
    blocks.push(ocrData.lines.map((line) => line.text).join("\n"));
  }
  return blocks;
}

function loadImageFromFile(file) {
  return new Promise((resolve, reject) => {
    const imageUrl = URL.createObjectURL(file);
    const img = new Image();
    img.onload = () => {
      URL.revokeObjectURL(imageUrl);
      resolve(img);
    };
    img.onerror = () => {
      URL.revokeObjectURL(imageUrl);
      reject(new Error("Gorsel yuklenemedi."));
    };
    img.src = imageUrl;
  });
}

async function preprocessImageForOcr(file) {
  const img = await loadImageFromFile(file);
  const scale = 2;
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.floor(img.width * scale));
  canvas.height = Math.max(1, Math.floor(img.height * scale));

  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  if (!ctx) {
    throw new Error("Canvas olusturulamadi.");
  }

  ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
  const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
  const data = imageData.data;

  // El yazisini daha net okumak icin gri ton + kontrast + esikleme uygular.
  let sum = 0;
  for (let i = 0; i < data.length; i += 4) {
    const gray = Math.round(data[i] * 0.299 + data[i + 1] * 0.587 + data[i + 2] * 0.114);
    const contrasted = Math.max(0, Math.min(255, (gray - 128) * 1.6 + 128));
    data[i] = contrasted;
    data[i + 1] = contrasted;
    data[i + 2] = contrasted;
    sum += contrasted;
  }

  const avg = sum / (data.length / 4);
  const threshold = Math.max(95, Math.min(190, avg * 0.95));

  for (let i = 0; i < data.length; i += 4) {
    const val = data[i] >= threshold ? 255 : 0;
    data[i] = val;
    data[i + 1] = val;
    data[i + 2] = val;
    data[i + 3] = 255;
  }

  ctx.putImageData(imageData, 0, 0);
  return canvas.toDataURL("image/png");
}

function toTsv(origin, pairs) {
  const rows = ["Nereden\tNereye\tFiyat"];
  for (const item of pairs) {
    rows.push(`${origin}\t${item.destination}\t${item.price}`);
  }
  return rows.join("\n");
}

async function runOcrAndBuildTable() {
  if (!dom.ocrImageInput || !dom.ocrOutput || !dom.ocrOriginInput) {
    return;
  }

  const file = dom.ocrImageInput.files && dom.ocrImageInput.files[0];
  const origin = dom.ocrOriginInput.value.trim() || "Patnos";

  if (!file) {
    setOcrStatus("Lutfen once bir fotograf sec.", true);
    return;
  }

  if (!window.Tesseract) {
    setOcrStatus("OCR kutuphanesi yuklenemedi. Internet baglantisini kontrol et.", true);
    return;
  }

  dom.ocrOutput.value = "";
  renderSuspiciousRows([]);
  setOcrStatus("Tarama basladi...");

  try {
    await refreshTariffCatalogData();
    const destinationPool = buildTariffDestinationPool(origin);
    const destinationSlugSet = buildDestinationSlugSet(destinationPool);

    const processedImage = await preprocessImageForOcr(file);

    const runPass = async (imageSource, passName, psm) => {
      const worker = await window.Tesseract.createWorker("tur+eng", 1, {
        logger: (m) => {
          if (m && m.status) {
            const ratio = typeof m.progress === "number" ? ` %${Math.round(m.progress * 100)}` : "";
            setOcrStatus(`${passName}: ${m.status}${ratio}`);
          }
        },
      });

      await worker.setParameters({
        tessedit_pageseg_mode: String(psm),
        preserve_interword_spaces: "1",
        tessedit_char_whitelist:
          "ABCÇDEFGĞHIİJKLMNOÖPRSŞTUÜVYZabcçdefgğhıijklmnoöprsştuüvyz0123456789=:-.' ",
      });

      const result = await worker.recognize(imageSource);
      await worker.terminate();

      const blocks = collectTextBlocks(result?.data);
      const parsed = [];
      for (const block of blocks) {
        parsed.push(...parseDestinationsAndPrices(block, destinationPool));
      }

      const unique = [];
      const seen = new Set();
      for (const item of parsed) {
        const key = slugTr(item.destination);
        if (!seen.has(key)) {
          seen.add(key);
          unique.push(item);
        }
      }

      return {
        pairs: unique,
        ocrData: result?.data || {},
      };
    };

    const pass1 = await runPass(processedImage, "Tarama 1/3", 6);
    const pass2 = await runPass(processedImage, "Tarama 2/3", 11);
    const pass3 = await runPass(file, "Tarama 3/3", 6);

    const mergedPairs = [
      ...parseTabularRowsFromOcrData(pass1.ocrData, destinationPool),
      ...parseTabularRowsFromOcrData(pass2.ocrData, destinationPool),
      ...parseTabularRowsFromOcrData(pass3.ocrData, destinationPool),
      ...parseStructuredRowsFromOcrData(pass1.ocrData, destinationPool),
      ...parseStructuredRowsFromOcrData(pass2.ocrData, destinationPool),
      ...parseStructuredRowsFromOcrData(pass3.ocrData, destinationPool),
      ...parseA4FromOcrData(pass1.ocrData, destinationPool),
      ...parseA4FromOcrData(pass2.ocrData, destinationPool),
      ...parseA4FromOcrData(pass3.ocrData, destinationPool),
      ...pass1.pairs,
      ...pass2.pairs,
      ...pass3.pairs,
    ];
    const pairs = aggregateReliablePairs(mergedPairs, destinationSlugSet, destinationPool);

    if (!pairs.length) {
      setOcrStatus("Satirlar ayrisamadi. Daha net ve dik cekim bir fotograf deneyin.", true);
      return;
    }

    state.lastOcrOrigin = origin;
    state.lastOcrDestinationPool = destinationPool;
    state.lastOcrPairs = pairs;
    state.lastSuspiciousRows = [];
    dom.ocrOutput.value = toTsv(origin, pairs);
    renderSuspiciousRows([]);
    setOcrStatus(`${pairs.length} satir otomatik duzeltildi ve TSV olusturuldu.`);
  } catch {
    setOcrStatus("Tarama sirasinda hata olustu.", true);
  }
}

async function copyOcrOutput() {
  if (!dom.ocrOutput || !dom.ocrOutput.value.trim()) {
    setOcrStatus("Kopyalanacak bir sonuc yok.", true);
    return;
  }

  try {
    await navigator.clipboard.writeText(dom.ocrOutput.value);
    setOcrStatus("TSV panoya kopyalandi.");
  } catch {
    setOcrStatus("Kopyalama basarisiz. Metni manuel kopyalayabilirsin.", true);
  }
}

function toggleTheme() {
  applyTheme(state.theme === "dark" ? "light" : "dark");
}

function setToken(token) {
  state.token = token || "";
  if (state.token) {
    sessionStorage.setItem(TOKEN_KEY, state.token);
  } else {
    sessionStorage.removeItem(TOKEN_KEY);
  }
}

async function apiFetch(url, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }

  let res;
  try {
    res = await fetch(url, { ...options, headers });
  } catch {
    throw new Error("Sunucuya ulasilamadi. Uygulamayi http://localhost:3000 adresinden ac.");
  }

  let data = {};
  const contentType = res.headers.get("content-type") || "";
  try {
    if (contentType.includes("application/json")) {
      data = await res.json();
    }
  } catch {
    data = {};
  }

  if (!res.ok) {
    if (!contentType.includes("application/json")) {
      throw new Error("API yaniti alinamadi. Sayfayi http://localhost:3000 adresinden ac.");
    }
    const err = new Error(data.message || "Islem basarisiz.");
    err.payload = data;
    throw err;
  }

  return data;
}

function showMessage(message, isError = true) {
  dom.loginMessage.style.color = isError ? "#d64545" : "#1f7a1f";
  dom.loginMessage.textContent = message;
}

function showOverlay(show) {
  dom.loadingOverlay.classList.toggle("hidden", !show);
}

function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function availableMenus(user) {
  if (!user) {
    return [];
  }

  if (user.isAdmin) {
    return MENUS;
  }

  return MENUS.filter(
    (menu) => !ADMIN_ONLY_MENUS.has(menu.key) && Boolean(user.permissions?.[menu.key])
  );
}

function renderMenu(user) {
  const items = availableMenus(user);
  dom.menuList.innerHTML = "";

  items.forEach((item, index) => {
    const li = document.createElement("li");
    li.className = "menu-item";
    li.dataset.menu = item.key;
    li.textContent = item.label;

    if (index === 0) {
      li.classList.add("active");
    }

    li.addEventListener("click", () => {
      document.querySelectorAll(".menu-item").forEach((el) => el.classList.remove("active"));
      li.classList.add("active");
      activatePanel(item.key);
    });

    dom.menuList.appendChild(li);
  });
}

function render() {
  if (!state.currentUser) {
    dom.portalView.classList.remove("active");
    dom.loginView.classList.add("active");
    return;
  }

  dom.loginView.classList.remove("active");
  dom.portalView.classList.add("active");

  // Eski currentUserLabel (varsa hala bos durur)
  if (dom.currentUserLabel) {
    dom.currentUserLabel.textContent = "";
  }

  renderSidebarUserCard();
  renderMenu(state.currentUser);
  const first = availableMenus(state.currentUser)[0]?.key || "dashboard";
  activatePanel(first);
}

// ===== Sidebar user card =====
function renderSidebarUserCard() {
  if (!state.currentUser) return;
  const u = state.currentUser;
  const fullName = u.fullName || u.username || "Kullanıcı";

  if (dom.sidebarUserName) dom.sidebarUserName.textContent = fullName;
  if (dom.sidebarUserRole) {
    // Ünvan/rol gösterimi kaldırıldı — yalnızca Admin rozeti kalsın, o da yoksa satır gizlenir.
    const label = u.isAdmin ? "Admin" : "";
    dom.sidebarUserRole.textContent = label;
    dom.sidebarUserRole.style.display = label ? "" : "none";
  }

  // Initials avatar: ad ve soyaddan al
  if (dom.sidebarUserAvatar) {
    const parts = String(fullName).replace(/\./g, " ").split(/\s+/).filter(Boolean);
    let initials = "?";
    if (parts.length >= 2) initials = (parts[0][0] + parts[1][0]).toUpperCase();
    else if (parts.length === 1) initials = parts[0].slice(0, 2).toUpperCase();
    dom.sidebarUserAvatar.textContent = initials;
  }

  if (dom.sidebarUserSessionText) {
    dom.sidebarUserSessionText.textContent = `Oturum: şimdi`;
  }
}

// ===== Content header (alt başlık + sağ chip) =====
function setContentHeader(opts) {
  const { title, subtitle, stat } = opts || {};
  if (title && dom.activeTitle) dom.activeTitle.textContent = title;
  if (dom.activeSubtitle) dom.activeSubtitle.textContent = subtitle || "";
  if (dom.contentHeadStat) {
    if (stat) {
      dom.contentHeadStat.textContent = stat;
      dom.contentHeadStat.classList.remove("hidden");
    } else {
      dom.contentHeadStat.classList.add("hidden");
      dom.contentHeadStat.textContent = "";
    }
  }
}

// ===== Göreli zaman ("2dk önce") =====
function relativeTime(input) {
  // input: "DD.MM.YYYY HH:mm:ss" Istanbul-local
  const m = String(input || "").match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return input || "";
  const past = new Date(`${m[3]}-${m[2]}-${m[1]}T${m[4]}:${m[5]}:${m[6]}`).getTime();
  const diffSec = Math.floor((Date.now() - past) / 1000);
  if (diffSec < 60) return "az önce";
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)} dk önce`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)} saat önce`;
  const days = Math.floor(diffSec / 86400);
  if (days < 7) return `${days} gün önce`;
  return input;
}

// ===== Saat-bazlı selamlama =====
function timeBasedGreeting() {
  const h = new Date().getHours();
  if (h >= 6 && h < 12) return "Günaydın ";
  if (h >= 12 && h < 18) return "İyi günler ";
  if (h >= 18 && h < 24) return "İyi akşamlar ";
  return "İyi geceler ";
}

// ===== Dashboard: backend'den çek ve render et =====
async function loadDashboardSummary() {
  try {
    const data = await apiFetch("/api/obilet/dashboard-summary");
    renderDashboardSummary(data?.summary || null);
  } catch (err) {
    if (dom.dashNotifList) {
      dom.dashNotifList.innerHTML = `<p class="subtle">Veri alınamadı: ${err.message}</p>`;
    }
  }
}

function renderDashboardSummary(s) {
  if (!s) return;

  // Hero
  if (dom.dashGreeting) {
    const greet = timeBasedGreeting();
    const name = state.currentUser?.fullName || state.currentUser?.username || "";
    dom.dashGreeting.textContent = name ? `${greet}, ${name}` : greet;
  }
  if (dom.dashGeneratedAt) {
    dom.dashGeneratedAt.textContent = `Son güncelleme: ${s.generatedAt || "-"}`;
  }

  // KPI 1 — Aktif hat
  if (dom.dashActiveTargets) dom.dashActiveTargets.textContent = s.activeTargets ?? 0;
  if (dom.dashTotalJourneys) {
    dom.dashTotalJourneys.textContent = `${s.totalJourneys || 0} sefer izleniyor`;
  }

  // KPI 2 — Bugün ve son 24 saat
  if (dom.dashTodayChanges) dom.dashTodayChanges.textContent = s.todayChanges ?? 0;
  if (dom.dash24hChanges) {
    dom.dash24hChanges.textContent = `Son 24 saatte ${s.last24hChanges ?? 0} değişiklik`;
  }

  // KPI 3 — En büyük değişim
  if (s.biggestToday) {
    const b = s.biggestToday;
    const diff = b.new_price - b.old_price;
    const arrow = diff > 0 ? "+" : diff < 0 ? "-" : "";
    const color = diff > 0 ? "#27ae60" : diff < 0 ? "#d32f2f" : "#999";
    if (dom.dashBiggestAmount) {
      dom.dashBiggestAmount.textContent = `${arrow} ${Math.abs(diff)} TL`;
      dom.dashBiggestAmount.style.color = color;
    }
    if (dom.dashBiggestDetail) {
      const route = `${(b.origin || "").toUpperCase()} - ${(b.destination || "").toUpperCase()}`;
      dom.dashBiggestDetail.textContent = `${b.operator} ${b.departure_time} · ${route}`;
    }
  } else {
    if (dom.dashBiggestAmount) {
      dom.dashBiggestAmount.textContent = "—";
      dom.dashBiggestAmount.style.color = "";
    }
    if (dom.dashBiggestDetail) dom.dashBiggestDetail.textContent = "Bugün değişiklik yok";
  }

  // KPI 4 — En aktif rakip
  if (s.topOperator) {
    if (dom.dashTopOperator) dom.dashTopOperator.textContent = s.topOperator.operator || "—";
    if (dom.dashTopOperatorCount) {
      dom.dashTopOperatorCount.textContent = `${s.topOperator.c || 0} değişiklik`;
    }
  } else {
    if (dom.dashTopOperator) dom.dashTopOperator.textContent = "—";
    if (dom.dashTopOperatorCount) dom.dashTopOperatorCount.textContent = "Veri yok";
  }

  // Trend chart (SVG line)
  if (dom.dashTrendChart) {
    const days = Array.isArray(s.sevenDays) ? s.sevenDays : [];
    dom.dashTrendChart.innerHTML = renderDashTrendSVG(days);
    if (dom.dashTrendSummary) {
      const total = days.reduce((a, d) => a + (d.count || 0), 0);
      dom.dashTrendSummary.textContent = `Toplam ${total} değişiklik`;
    }
  }

  // Notifikasyonlar
  if (dom.dashNotifList) {
    const notifs = Array.isArray(s.recentNotifs) ? s.recentNotifs : [];
    if (!notifs.length) {
      dom.dashNotifList.innerHTML = `<p class="subtle">Henüz bildirim yok.</p>`;
    } else {
      const escape = (s) => String(s || "").replace(/[&<>"']/g, c => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      })[c]);
      dom.dashNotifList.innerHTML = notifs.map(n => `
        <div class="dash-notif-item ${n.is_read ? "" : "unread"}">
          <p class="dash-notif-msg">${escape(n.message)}</p>
          <p class="dash-notif-time">${relativeTime(n.created_at)}</p>
        </div>
      `).join("");
    }
  }
}

// Mini SVG line chart — 7 gün trend
function renderDashTrendSVG(days) {
  if (!days.length) return `<p class="subtle">Yeterli veri yok.</p>`;
  const W = 540, H = 130, P = 24;
  const counts = days.map(d => d.count || 0);
  const maxC = Math.max(1, ...counts);
  const xStep = (W - 2 * P) / Math.max(1, days.length - 1);
  const yScale = (c) => H - P - (c / maxC) * (H - 2 * P);
  const pts = days.map((d, i) => `${P + i * xStep},${yScale(d.count || 0)}`);
  const polyline = pts.join(" ");
  const areaPath = `${P},${H - P} ${polyline} ${P + (days.length - 1) * xStep},${H - P}`;
  const circles = days.map((d, i) => {
    const cx = P + i * xStep;
    const cy = yScale(d.count || 0);
    const dayLabel = d.date ? d.date.slice(8) : "";
    return `<g>
      <circle cx="${cx}" cy="${cy}" r="3.5" fill="#3dc2b2" />
      <text x="${cx}" y="${H - 6}" text-anchor="middle" font-size="10" fill="currentColor" opacity="0.6">${dayLabel}</text>
      <text x="${cx}" y="${cy - 8}" text-anchor="middle" font-size="10" fill="currentColor" opacity="0.85" font-weight="600">${d.count || 0}</text>
    </g>`;
  }).join("");
  return `
    <svg viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:auto;">
      <polygon points="${areaPath}" fill="rgba(61, 194, 178, 0.15)" stroke="none" />
      <polyline points="${polyline}" fill="none" stroke="#3dc2b2" stroke-width="2" />
      ${circles}
    </svg>
  `;
}

function renderPrices() {
  // Dashboard ozetleri fiyat yukleme kayitlarindan uretiliyor.
}

function renderPricingDashboardSummary() {
  const uploads = Array.isArray(state.pricingUploads) ? state.pricingUploads : [];

  let oneWayCount = 0;
  let roundTripCount = 0;
  let totalCount = 0;

  for (const upload of uploads) {
    const items = Array.isArray(upload?.items) ? upload.items : [];
    totalCount += items.length;

    if (!items.length) {
      continue;
    }

    for (const item of items) {
      const rowDirection = String(item?.directionLabel || "")
        .toLocaleLowerCase("tr-TR")
        .trim();

      if (rowDirection === "gidis" || rowDirection === "donus") {
        oneWayCount += 1;
      } else if (rowDirection === "gidis-donus") {
        roundTripCount += 1;
      } else {
        // Legacy kayitlar icin upload seviyesi yon bilgisini kullan.
        const uploadDirection = String(upload?.directionType || "")
          .toLocaleLowerCase("tr-TR")
          .trim();
        if (uploadDirection === "tek-yon") {
          oneWayCount += 1;
        } else if (uploadDirection === "gidis-donus") {
          roundTripCount += 1;
        }
      }
    }
  }

  if (dom.avgFare) {
    dom.avgFare.textContent = `${oneWayCount} satir`;
  }
  if (dom.totalRoutes) {
    dom.totalRoutes.textContent = `${roundTripCount} satir`;
  }
  if (dom.updateCount) {
    dom.updateCount.textContent = `${totalCount} satir`;
  }

  if (dom.lastPriceUpdate) {
    const latest = uploads[0];
    if (!latest) {
      dom.lastPriceUpdate.textContent = "Henuz fiyat yuklemesi yok.";
    } else {
      const latestStamp = latest.createdAt || "-";
      dom.lastPriceUpdate.textContent = `Son fiyat yukleme: ${latestStamp} (${uploads.length} yukleme kaydi)`;
    }
  }

  renderExpiredUploadsWarning(uploads);
}

function isUploadExpired(upload) {
  const end = new Date(String(upload?.validTo || "").trim());
  return Number.isFinite(end.getTime()) && end.getTime() < Date.now();
}

function renderExpiredUploadsWarning(uploads) {
  if (!dom.expiredUploadsList || !dom.expiredUploadsEmpty || !dom.expiredUploadsCount) {
    return;
  }

  const expired = (Array.isArray(uploads) ? uploads : [])
    .filter((upload) => upload && isUploadExpired(upload))
    .sort((a, b) => String(a.validTo || "").localeCompare(String(b.validTo || ""), "tr"));

  dom.expiredUploadsCount.textContent = String(expired.length);
  dom.expiredUploadsList.innerHTML = "";

  if (!expired.length) {
    dom.expiredUploadsEmpty.classList.remove("hidden");
    return;
  }

  dom.expiredUploadsEmpty.classList.add("hidden");

  expired.forEach((upload) => {
    const li = document.createElement("li");
    const desc = String(upload.description || "").trim();
    li.className = "dashboard-expired-item";
    li.innerHTML = `
      <div class="dashboard-expired-item-main">
        <strong>${escapeHtml(upload.uploadedBy || "-")}</strong>
        <span>#${Number(upload.id) || "-"}</span>
        <span>${formatUploadDate(upload.validFrom)} - ${formatUploadDate(upload.validTo)}</span>
      </div>
      ${desc ? `<div class="dashboard-expired-item-desc">Aciklama: ${escapeHtml(desc)}</div>` : ""}
      <div class="dashboard-expired-item-actions">
        <button class="btn btn-small btn-danger expiredDeleteBtn" type="button" data-upload-id="${Number(upload.id) || 0}">Kaldir</button>
      </div>
    `;

    li.querySelector(".expiredDeleteBtn")?.addEventListener("click", async () => {
      const id = Number(upload.id || 0);
      if (!Number.isInteger(id) || id <= 0) {
        return;
      }

      const ok = window.confirm(`Kayit #${id} tarihi gecmis. Kaldirmak istiyor musun?`);
      if (!ok) {
        return;
      }

      try {
        await apiFetch(`/api/pricing-uploads/${id}`, { method: "DELETE" });
        await refreshPricingUploadsData();
        await refreshNotificationsData();
      } catch (error) {
        if (dom.lastPriceUpdate) {
          dom.lastPriceUpdate.textContent = error.message || "Kayit kaldirilamadi.";
        }
      }
    });

    dom.expiredUploadsList.appendChild(li);
  });
}

function renderTariffRows(append = false) {
  if (!append) {
    dom.routeTableBody.innerHTML = "";
  }

  if (!state.tariffRows.length) {
    dom.routeTableBody.innerHTML = '<tr><td colspan="3">Sonuc bulunamadi.</td></tr>';
    if (dom.tariffSummary) {
      dom.tariffSummary.textContent = "Aramana uygun tarife kaydi bulunamadi.";
    }
    if (dom.tariffLoadMoreBtn) {
      dom.tariffLoadMoreBtn.classList.add("hidden");
    }
    return;
  }

  state.tariffRows.forEach((item) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${item.route}</td>
      <td>${item.tariffPrice} TL</td>
      <td>${item.discountedPrice} TL</td>
    `;
    dom.routeTableBody.appendChild(tr);
  });

  if (dom.tariffSummary) {
    const q = state.tariffQuery;
    dom.tariffSummary.textContent = q
      ? `Akilli arama: "${q}" icin ${state.tariffOffset} / ${state.tariffTotal} sonuc gosteriliyor.`
      : `Toplam ${state.tariffOffset} / ${state.tariffTotal} bakanlik tarife satiri gosteriliyor.`;
  }

  if (dom.tariffLoadMoreBtn) {
    dom.tariffLoadMoreBtn.classList.toggle("hidden", state.tariffOffset >= state.tariffTotal);
  }
}

function buildReportRows() {
  const uploads = Array.isArray(state.pricingUploads) ? state.pricingUploads : [];
  const rows = [];

  for (const upload of uploads) {
    if (!upload) {
      continue;
    }

    const period = `${formatUploadDate(upload.validFrom)} - ${formatUploadDate(upload.validTo)}`;
    for (const item of upload.items || []) {
      rows.push({
        route: item.route,
        demandPrice: Number(item.demandPrice) || 0,
        directionLabel: item.directionLabel || formatUploadDirection(upload.directionType),
        period,
        searchText: normalizeSearchText(`${item.route} ${item.directionLabel || ""} ${period}`),
      });
    }
  }

  rows.sort((a, b) => a.route.localeCompare(b.route, "tr"));
  return rows;
}

function renderReportsPanel() {
  if (!dom.reportTableBody || !dom.reportSummary) {
    return;
  }

  const allRows = buildReportRows();
  const query = String(state.reportQuery || "").trim();
  const queryNorm = normalizeSearchText(query);

  const rows = queryNorm
    ? allRows.filter((row) => row.searchText.includes(queryNorm))
    : allRows;

  dom.reportTableBody.innerHTML = "";
  if (!rows.length) {
    dom.reportTableBody.innerHTML = '<tr><td colspan="4">Sonuc bulunamadi.</td></tr>';
    dom.reportSummary.textContent = query
      ? `Aradigin guzergah icin yuklenmis fiyat yok: "${query}"`
      : "Yuklenmis fiyat kaydi bulunamadi.";
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.route}</td>
      <td>${row.demandPrice} TL</td>
      <td>${row.directionLabel}</td>
      <td>${row.period}</td>
    `;
    dom.reportTableBody.appendChild(tr);
  });

  dom.reportSummary.textContent = query
    ? `"${query}" icin ${rows.length} yuklenmis fiyat bulundu.`
    : `Toplam ${rows.length} yuklenmis fiyat listeleniyor.`;
}

function renderReportingPanel() {
  if (!dom.reportingTableBody || !dom.reportingSummary) {
    return;
  }

  const formatMoney = (value, currency) => {
    if (value === null || value === undefined || value === "") {
      return "-";
    }
    const n = Number(value);
    if (!Number.isFinite(n)) {
      return "-";
    }
    return new Intl.NumberFormat("tr-TR", {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(n);
  };

  const query = normalizeSearchText(state.reportingQuery || "");
  const allRows = Array.isArray(state.reportingRows) ? state.reportingRows : [];
  const rows = query
    ? allRows.filter((row) => normalizeSearchText(`${row.routeLabel} ${row.vehiclePlate || ""}`).includes(query))
    : allRows;

  dom.reportingTableBody.innerHTML = "";
  if (!rows.length) {
    dom.reportingTableBody.innerHTML = '<tr><td colspan="10">Bu tarihte rapor kaydi yok. Gunluk raporu cek butonuna bas.</td></tr>';
    dom.reportingSummary.textContent = `${state.reportingDate || "-"} icin kayit bulunamadi.`;
    if (dom.reportingRevenueTryTotal) dom.reportingRevenueTryTotal.textContent = "0 TL";
    if (dom.reportingRevenueEurTotal) dom.reportingRevenueEurTotal.textContent = "0 EUR";
    if (dom.reportingRevenueCount) dom.reportingRevenueCount.textContent = "0";
    return;
  }

  const delayedCount = rows.filter((row) => Number(row.delayMinutes) !== 0).length;
  dom.reportingSummary.textContent = `${state.reportingDate} icin ${rows.length} sefer listelendi. Rotarli sefer: ${delayedCount}.`;

  const totalTry = rows.reduce((acc, row) => acc + (Number.isFinite(Number(row.revenueTry)) ? Number(row.revenueTry) : 0), 0);
  const totalEur = rows.reduce((acc, row) => acc + (Number.isFinite(Number(row.revenueEur)) ? Number(row.revenueEur) : 0), 0);
  const revenueCount = rows.filter((row) => Number.isFinite(Number(row.revenueTry)) || Number.isFinite(Number(row.revenueEur))).length;

  if (dom.reportingRevenueTryTotal) {
    dom.reportingRevenueTryTotal.textContent = new Intl.NumberFormat("tr-TR").format(Math.round(totalTry * 100) / 100) + " TL";
  }
  if (dom.reportingRevenueEurTotal) {
    dom.reportingRevenueEurTotal.textContent = new Intl.NumberFormat("tr-TR").format(Math.round(totalEur * 100) / 100) + " EUR";
  }
  if (dom.reportingRevenueCount) {
    dom.reportingRevenueCount.textContent = String(revenueCount);
  }

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const hasOccupancyPercent = row.occupancyPercent !== null && row.occupancyPercent !== undefined && row.occupancyPercent !== "";
    const hasSeatCount = row.seatsAvailable !== null && row.seatsAvailable !== undefined && row.seatsAvailable !== "";
    const occupancyText = hasOccupancyPercent
      ? `%${Math.max(0, Math.min(100, Number(row.occupancyPercent)))}`
      : (row.occupancyLevel === "veri-yok" ? "Veri yok" : (row.occupancyLevel || "-"));
    const delayMinutes = Number(row.delayMinutes) || 0;
    const delayBadgeText = delayMinutes > 0
      ? `+${delayMinutes} dk`
      : (delayMinutes < 0 ? `${delayMinutes} dk` : "R");
    const delayBadgeClass = delayMinutes !== 0 ? "delay-badge-late" : "delay-badge-on-time";

    tr.innerHTML = `
      <td>
        <div>${escapeHtml(row.routeLabel || "-")}</div>
        <small class="subtle">uuid: ${escapeHtml(row.rideUuid || "-")}</small>
      </td>
      <td>${escapeHtml(row.departureTime || "-")}</td>
      <td>${escapeHtml(row.arrivalTime || "-")}</td>
      <td>
        <div class="report-delay-wrap">
          <span class="report-delay-badge ${delayBadgeClass}">${delayBadgeText}</span>
          <input class="report-delay-input" type="number" min="-720" max="720" step="1" value="${delayMinutes}" />
        </div>
      </td>
      <td>
        <input class="report-plate-input" type="text" maxlength="32" value="${escapeHtml(row.vehiclePlate || "")}" placeholder="34 ABC 123" />
      </td>
      <td>
        <div>${escapeHtml(row.vehicleModel || "-")}</div>
        <small class="subtle">${escapeHtml(row.operatorName || "-")}</small>
      </td>
      <td>${occupancyText}${hasSeatCount ? ` (Bos: ${Number(row.seatsAvailable)})` : ""}</td>
      <td>${formatMoney(row.revenueTry, "TRY")} / ${formatMoney(row.revenueEur, "EUR")}</td>
      <td>
        <input class="report-note-input" type="text" maxlength="400" value="${escapeHtml(row.note || "")}" placeholder="Not" />
      </td>
      <td>
        <button class="btn btn-small btn-ghost report-save-btn" type="button">Kaydet</button>
        <button class="btn btn-small btn-ghost report-debug-btn" type="button">UUID Test</button>
      </td>
    `;

    tr.querySelector(".report-save-btn")?.addEventListener("click", async () => {
      const delayInput = tr.querySelector(".report-delay-input");
      const plateInput = tr.querySelector(".report-plate-input");
      const noteInput = tr.querySelector(".report-note-input");

      const payload = {
        delayMinutes: Number(delayInput?.value || 0),
        vehiclePlate: String(plateInput?.value || ""),
        note: String(noteInput?.value || ""),
      };

      await apiFetch(`/api/operations-reports/${row.id}`, {
        method: "PATCH",
        body: JSON.stringify(payload),
      });

      await refreshReportingData();
    });

    tr.querySelector(".report-debug-btn")?.addEventListener("click", async () => {
      if (dom.reportingRideUuidInput) {
        dom.reportingRideUuidInput.value = String(row.rideUuid || "");
      }
      await debugPlateByRideUuid();
    });

    dom.reportingTableBody.appendChild(tr);
  });
}

async function debugPlateByRideUuid() {
  const rideUuid = String(dom.reportingRideUuidInput?.value || "").trim();
  if (!rideUuid) {
    if (dom.reportingPlateDebugMsg) {
      dom.reportingPlateDebugMsg.style.color = "#d64545";
      dom.reportingPlateDebugMsg.textContent = "Lutfen bir ride_uuid gir.";
    }
    return;
  }

  if (dom.reportingPlateDebugMsg) {
    dom.reportingPlateDebugMsg.style.color = "var(--muted)";
    dom.reportingPlateDebugMsg.textContent = "ride_uuid icin plaka probe calisiyor...";
  }

  const result = await apiFetch("/api/operations-reports/plate-debug", {
    method: "POST",
    body: JSON.stringify({ rideUuid }),
  });

  const extracted = String(result.extractedPlate || "").trim();
  const extractedModel = String(result.extractedVehicleModel || "").trim();
  const extractedOperator = String(result.extractedOperatorName || "").trim();
  const extractedTry = Number(result.extractedRevenueTry);
  const extractedEur = Number(result.extractedRevenueEur);
  const current = String(result.currentRow?.vehiclePlate || "").trim();
  const probes = Array.isArray(result.probes) ? result.probes : [];
  const successProbe = probes.find((p) => String(p?.matchedBy || "").trim()) || null;
  const firstProbe = probes.length ? probes[0] : null;
  const detail = successProbe
    ? ` | eslesen endpoint: HTTP ${successProbe.status || "-"}, match=${successProbe.matchedBy}`
    : (firstProbe
      ? ` | ilk endpoint: HTTP ${firstProbe.status || "-"}${firstProbe.note ? `, not=${firstProbe.note}` : ""}`
      : "");

  if (dom.reportingPlateDebugMsg) {
    if (extracted) {
      dom.reportingPlateDebugMsg.style.color = "#1f7a1f";
      const revenueText = `${Number.isFinite(extractedTry) ? extractedTry.toFixed(2) : "-"} TRY / ${Number.isFinite(extractedEur) ? extractedEur.toFixed(2) : "-"} EUR`;
      dom.reportingPlateDebugMsg.textContent = `Bulunan plaka: ${extracted} | Model: ${extractedModel || "-"} | Operator: ${extractedOperator || "-"} | Gelir: ${revenueText} | DB'deki mevcut: ${current || "-"}${detail}`;
    } else {
      dom.reportingPlateDebugMsg.style.color = "#d64545";
      dom.reportingPlateDebugMsg.textContent = `Bu ride_uuid icin plaka bulunamadi.${detail}`;
    }
  }
}

async function refreshReportingData() {
  const date = String(state.reportingDate || todayIsoDate()).trim();
  state.reportingDate = date;
  state.reportingOrigin = String(dom.reportingOriginInput?.value || state.reportingOrigin || "Siirt").trim() || "Siirt";

  const result = await apiFetch(`/api/operations-reports?date=${encodeURIComponent(date)}&origin=${encodeURIComponent(state.reportingOrigin)}`);
  state.reportingRows = result.rows || [];
  renderReportingPanel();
}

function stopReportingAutoRefresh() {
  if (state.reportingAutoTimer) {
    clearInterval(state.reportingAutoTimer);
    state.reportingAutoTimer = null;
  }
}

function ensureReportingAutoRefresh() {
  stopReportingAutoRefresh();
  state.reportingAutoTimer = setInterval(() => {
    if (state.activeMenu === "reporting" && state.currentUser) {
      refreshReportingData().catch(() => null);
    }
  }, 60000);
}

async function syncReportingData() {
  const date = String(state.reportingDate || todayIsoDate()).trim();
  state.reportingDate = date;
  state.reportingOrigin = String(dom.reportingOriginInput?.value || state.reportingOrigin || "Siirt").trim() || "Siirt";

  if (dom.reportingSummary) {
    dom.reportingSummary.textContent = "Gunluk rapor cekiliyor...";
  }

  await apiFetch("/api/operations-reports/sync", {
    method: "POST",
    body: JSON.stringify({ date, origin: state.reportingOrigin }),
  });

  await refreshReportingData();
  await refreshNotificationsData();
}

async function refreshControlIntegrationData() {
  const result = await apiFetch("/api/control-integration");
  state.controlConfig = result || null;
  return result;
}

async function renderControlIntegrationPanel() {
  if (typeof renderErrorList === "function") {
    renderErrorList();
  }
}

async function saveControlIntegrationData() {
  const payload = {
    baseUrl: dom.controlBaseUrlInput?.value || "",
    loginUrl: dom.controlLoginUrlInput?.value || "",
    cookieHeader: dom.controlCookieInput?.value || "",
    csrfToken: dom.controlCsrfInput?.value || "",
    sessionTestUrl: dom.controlSessionTestUrlInput?.value || "",
  };

  await apiFetch("/api/control-integration", {
    method: "PATCH",
    body: JSON.stringify(payload),
  });

  if (dom.controlStatusMsg) {
    dom.controlStatusMsg.style.color = "#1f7a1f";
    dom.controlStatusMsg.textContent = "OneOps baglanti bilgileri kaydedildi.";
  }

  await renderControlIntegrationPanel();
}

async function testControlIntegrationSession() {
  const testUrl = String(dom.controlSessionTestUrlInput?.value || "").trim();
  const result = await apiFetch("/api/control-integration/test", {
    method: "POST",
    body: JSON.stringify({ testUrl }),
  });

  if (!dom.controlStatusMsg) {
    return;
  }

  if (result.authenticated) {
    dom.controlStatusMsg.style.color = "#1f7a1f";
    dom.controlStatusMsg.textContent = `${result.message} (HTTP ${result.statusCode})`;
    return;
  }

  dom.controlStatusMsg.style.color = "#d64545";
  dom.controlStatusMsg.textContent = `${result.message} (HTTP ${result.statusCode})`;
}

async function refreshTariffData(query = "", append = false) {
  const q = String(query || "").trim();
  const nextOffset = append ? state.tariffOffset : 0;
  const url = `/api/tariff-prices?q=${encodeURIComponent(q)}&limit=${state.tariffPageSize}&offset=${nextOffset}`;
  const result = await apiFetch(url);
  const incoming = result.rows || [];
  state.tariffRows = incoming;
  state.tariffTotal = Number(result.total || 0);
  state.tariffOffset = nextOffset + incoming.length;
  state.tariffQuery = q;
  renderTariffRows(append);
}

async function refreshTariffCatalogData(force = false) {
  if (!force && Array.isArray(state.tariffCatalog) && state.tariffCatalog.length) {
    return;
  }

  const all = [];
  let offset = 0;
  let total = Infinity;
  const pageSize = 2000;

  while (offset < total) {
    const result = await apiFetch(`/api/tariff-prices?limit=${pageSize}&offset=${offset}`);
    const rows = Array.isArray(result?.rows) ? result.rows : [];
    total = Number(result?.total || 0);

    if (!rows.length) {
      break;
    }

    all.push(...rows);
    offset += rows.length;

    if (rows.length < pageSize) {
      break;
    }
  }

  state.tariffCatalog = all;
}

function normalizeUploadHeader(value) {
  return String(value || "")
    .toLocaleLowerCase("tr-TR")
    .replace(/ı/g, "i")
    .replace(/ğ/g, "g")
    .replace(/ü/g, "u")
    .replace(/ş/g, "s")
    .replace(/ö/g, "o")
    .replace(/ç/g, "c")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function findUploadHeaderIndex(headers, aliases) {
  const normalized = headers.map((header) => normalizeUploadHeader(header));
  const normalizedAliases = aliases.map((alias) => normalizeUploadHeader(alias));
  return normalized.findIndex((header) =>
    normalizedAliases.some((alias) => header.includes(alias))
  );
}

function formatUploadDirection(value) {
  const key = String(value || "").trim().toLocaleLowerCase("tr-TR");
  if (key === "tek-yon") {
    return "Tek Yon";
  }
  if (key === "karma") {
    return "Tek Yon";
  }
  if (key === "gidis-donus") {
    return "Gidis-Donus";
  }
  return toTurkishTitleCase(String(value || ""));
}

function formatUploadDate(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "-";
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return raw.replace("T", " ");
  }

  const hasTime = /[T\s]\d{2}:\d{2}/.test(raw);
  const options = hasTime
    ? {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      }
    : {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
      };

  return new Intl.DateTimeFormat("tr-TR", options).format(parsed);
}

function routeToParts(routeText) {
  const text = String(routeText || "");
  const parts = text.split(" - ");
  if (parts.length < 2) {
    return { origin: text.trim(), destination: "" };
  }

  return {
    origin: String(parts.shift() || "").trim(),
    destination: String(parts.join(" - ") || "").trim(),
  };
}

function safeFilePart(value) {
  return String(value || "")
    .toLocaleLowerCase("tr-TR")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .trim();
}

function downloadPricingUploadAsExcel(upload) {
  if (!window.XLSX) {
    throw new Error("Excel kutuphanesi yuklenemedi.");
  }

  const rows = (upload.items || []).map((item) => {
    const routeParts = routeToParts(item.route);
    const origin = String(item.origin || routeParts.origin || "").trim();
    const destination = String(item.destination || routeParts.destination || "").trim();

    return {
      Nereden: origin,
      Nereye: destination,
      "Talep Fiyati": Number(item.demandPrice) || 0,
    };
  });

  const wb = window.XLSX.utils.book_new();
  const ws = window.XLSX.utils.json_to_sheet(rows, {
    header: ["Nereden", "Nereye", "Talep Fiyati"],
  });
  window.XLSX.utils.book_append_sheet(wb, ws, "Talep Fiyatlari");

  const direction = safeFilePart(formatUploadDirection(upload.directionType));
  const validFrom = safeFilePart(upload.validFrom);
  const validTo = safeFilePart(upload.validTo);
  const fileName = `talep-fiyatlari-${direction || "kayit"}-${validFrom || "baslangic"}-${validTo || "bitis"}.xlsx`;

  window.XLSX.writeFile(wb, fileName);
}

function parseClientPrice(raw) {
  const text = String(raw ?? "").trim();
  if (!text) {
    return NaN;
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
    // 1,000 (thousands) and 1000,50 (decimal) cases
    const commaParts = normalized.split(",");
    if (commaParts.length === 2 && commaParts[1].length === 3 && commaParts[0].length >= 1) {
      normalized = commaParts.join("");
    } else {
      normalized = normalized.replace(/,/g, ".");
    }
  } else if (hasDot) {
    // 1.000 (thousands) and 1000.50 (decimal) cases
    const dotParts = normalized.split(".");
    if (dotParts.length === 2 && dotParts[1].length === 3 && dotParts[0].length >= 1) {
      normalized = dotParts.join("");
    }
  }

  return Number(normalized);
}

function parseRouteCell(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    return { origin: "", destination: "" };
  }

  const parts = text
    .split(/\s*[-–—]\s*/)
    .map((item) => String(item || "").trim())
    .filter(Boolean);

  if (parts.length < 2) {
    return { origin: "", destination: "" };
  }

  return {
    origin: parts.shift() || "",
    destination: parts.join(" - "),
  };
}

function parseDirectionTypeFromCell(rawDirection) {
  const text = normalizeUploadHeader(rawDirection);
  if (!text) {
    return "gidis-donus";
  }

  if (text.includes("gidis") || text.includes("donus")) {
    return "tek-yon";
  }

  return "tek-yon";
}

function parseDirectionLabelFromCell(rawDirection) {
  const text = normalizeUploadHeader(rawDirection);
  if (!text) {
    return "Gidis-Donus";
  }

  if (text.includes("gidis") && text.includes("donus")) {
    return "Gidis-Donus";
  }
  if (text.includes("gidis")) {
    return "Gidis";
  }
  if (text.includes("donus")) {
    return "Donus";
  }

  return "Gidis-Donus";
}

async function readPricingRowsFromFile(file) {
  const name = String(file?.name || "").toLowerCase();

  if (name.endsWith(".csv")) {
    const text = await file.text();
    const lines = text
      .replace(/^\uFEFF/, "")
      .split(/\r?\n/)
      .map((line) => line.split(/[;,\t]/));

    return lines.map((cells, index) => ({
      cells,
      rowNumber: index + 1,
    }));
  }

  if (!window.XLSX) {
    throw new Error("Excel kutuphanesi yuklenemedi.");
  }

  const buffer = await file.arrayBuffer();
  const wb = window.XLSX.read(buffer, { type: "array" });
  const first = wb.SheetNames[0];
  if (!first) {
    throw new Error("Excel sayfasi bulunamadi.");
  }

  const ws = wb.Sheets[first];
  const ref = ws["!ref"];
  if (!ref) {
    return [];
  }

  const range = window.XLSX.utils.decode_range(ref);
  const out = [];

  for (let r = range.s.r; r <= range.e.r; r += 1) {
    if (ws["!rows"]?.[r]?.hidden) {
      continue;
    }

    const cells = [];
    for (let c = range.s.c; c <= range.e.c; c += 1) {
      const addr = window.XLSX.utils.encode_cell({ r, c });
      const cell = ws[addr];
      const raw = cell ? (cell.w ?? cell.v ?? "") : "";
      cells.push(String(raw).trim());
    }

    out.push({
      cells,
      rowNumber: r + 1,
    });
  }

  return out;
}

async function parsePricingUploadRows(file) {
  const matrix = await readPricingRowsFromFile(file);
  const rows = matrix
    .map((entry) => ({
      cells: Array.isArray(entry?.cells)
        ? entry.cells.map((cell) => String(cell || "").trim())
        : [],
      rowNumber: Number(entry?.rowNumber) || 0,
    }))
    .filter((entry) => entry.cells.some((cell) => cell));

  if (rows.length < 1) {
    throw new Error("Excel satirlari okunamadi.");
  }

  let headerRowIndex = -1;
  let originIndex = 0;
  let destinationIndex = 1;
  let demandIndex = 2;
  let routeIndex = -1;
  let directionIndex = -1;

  // 1) Header varsa kolonlari header'dan bul.
  for (let i = 0; i < Math.min(rows.length, 30); i += 1) {
    const headers = rows[i].cells;
    const route = findUploadHeaderIndex(headers, ["atob", "guzergah", "rota", "kalkis-varis", "kalkis varis"]);
    const demandNew = findUploadHeaderIndex(headers, ["talep edilen fiyat", "talep fiyati", "fiyat"]);
    const dirNew = findUploadHeaderIndex(headers, ["yon"]);

    if (route >= 0 && demandNew >= 0) {
      headerRowIndex = i;
      routeIndex = route;
      demandIndex = demandNew;
      directionIndex = dirNew;
      break;
    }

    const o = findUploadHeaderIndex(headers, ["kalkis", "nereden", "origin"]);
    const d = findUploadHeaderIndex(headers, ["varis", "nereye", "destination"]);
    const p = findUploadHeaderIndex(headers, ["guncel bilet fiyati", "fiyat", "talep"]);
    if (o >= 0 && d >= 0 && p >= 0) {
      headerRowIndex = i;
      originIndex = o;
      destinationIndex = d;
      demandIndex = p;
      break;
    }
  }

  // 2) Header yoksa yeni formati algila: A-B-C veya B-C-D (Kalkis;Varis;Fiyat)
  if (headerRowIndex < 0) {
    const sample = rows.slice(0, Math.min(rows.length, 80));
    const scoreMapping = (start) => {
      let score = 0;
      for (const entry of sample) {
        const origin = String(entry.cells[start] || "").trim();
        const destination = String(entry.cells[start + 1] || "").trim();
        const price = parseClientPrice(entry.cells[start + 2]);
        if (origin && destination && Number.isFinite(price)) {
          score += 1;
        }
      }
      return score;
    };

    const scoreABC = scoreMapping(0);
    const scoreBCD = scoreMapping(1);
    const start = scoreBCD > scoreABC ? 1 : 0;
    originIndex = start;
    destinationIndex = start + 1;
    demandIndex = start + 2;
  }

  const dataStart = headerRowIndex >= 0 ? headerRowIndex + 1 : 0;

  const parsed = [];
  for (let i = dataStart; i < rows.length; i += 1) {
    const row = rows[i].cells;
    let origin = String(row[originIndex] || "").trim();
    let destination = String(row[destinationIndex] || "").trim();
    const demandRaw = row[demandIndex];
    const directionRaw = directionIndex >= 0 ? row[directionIndex] : "";

    if (routeIndex >= 0) {
      const parsedRoute = parseRouteCell(row[routeIndex]);
      origin = parsedRoute.origin;
      destination = parsedRoute.destination;
    }

    if (!origin && !destination && !String(demandRaw || "").trim()) {
      continue;
    }

    parsed.push({
      rowNumber: rows[i].rowNumber || i + 1,
      origin,
      destination,
      demandPrice: parseClientPrice(demandRaw),
      directionType: parseDirectionTypeFromCell(directionRaw),
      directionLabel: parseDirectionLabelFromCell(directionRaw),
    });
  }

  return parsed;
}

function renderPricingRejectedRows(rejected) {
  if (!dom.pricingRejectedWrap || !dom.pricingRejectedList) {
    return;
  }

  const list = Array.isArray(rejected) ? rejected : [];
  dom.pricingRejectedList.innerHTML = "";

  if (!list.length) {
    dom.pricingRejectedWrap.classList.add("hidden");
    dom.pricingRejectedWrap.removeAttribute("open");
    return;
  }

  list.forEach((item) => {
    const li = document.createElement("li");
    li.className = "pricing-rejected-item";

    const text = String(item?.reason || "");
    const overlapMatch = text.match(
      /^(.*?) icin zaten aktif fiyat var: ([\d.,]+) TL \((.*?), (.*?) - (.*?), kayit #(\d+)\)\.?$/
    );

    if (overlapMatch) {
      const route = overlapMatch[1];
      const price = overlapMatch[2];
      const direction = overlapMatch[3];
      const validFrom = formatUploadDate(overlapMatch[4]);
      const validTo = formatUploadDate(overlapMatch[5]);
      const uploadId = overlapMatch[6];

      li.innerHTML = `
        <div class="pricing-rejected-head">
          <strong>Satir ${item.rowNumber}</strong>
          <span class="pricing-rejected-chip">Cakisiyor</span>
        </div>
        <div class="pricing-rejected-route">${route}</div>
        <div class="pricing-rejected-meta">Mevcut: ${price} TL | Yon: ${direction} | Donem: ${validFrom} - ${validTo} | Kayit #${uploadId}</div>
      `;
    } else {
      li.innerHTML = `
        <div class="pricing-rejected-head">
          <strong>Satir ${item.rowNumber}</strong>
          <span class="pricing-rejected-chip">Uyari</span>
        </div>
        <div class="pricing-rejected-meta">${text}</div>
      `;
    }

    dom.pricingRejectedList.appendChild(li);
  });

  dom.pricingRejectedWrap.classList.remove("hidden");
  dom.pricingRejectedWrap.setAttribute("open", "open");
}

function renderPricingUploads() {
  if (!dom.pricingUploadsList) {
    return;
  }

  dom.pricingUploadsList.innerHTML = "";
  if (!state.pricingUploads.length) {
    dom.pricingUploadsList.innerHTML = '<p class="subtle">Henuz fiyat yuklemesi yok.</p>';
    return;
  }

  state.pricingUploads.forEach((upload) => {
    const directionLabel = formatUploadDirection(upload.directionType);
    const currentDescription = String(upload.description || "").trim();
    const escapedDescription = escapeHtml(currentDescription);
    const rowCountLabel = `${upload.items.length} SATIR`;
    const validFromText = formatUploadDate(upload.validFrom);
    const validToText = formatUploadDate(upload.validTo);
    const createdAtText = formatUploadDate(upload.createdAt);

    const detailsRows = (upload.items || [])
      .slice(0, 200)
      .map(
        (item) =>
          `<tr>
            <td>${item.route}</td>
            <td>${item.demandPrice} TL</td>
            <td>${item.directionLabel || formatUploadDirection(upload.directionType)}</td>
            <td><button class="btn btn-small btn-danger deletePriceItemBtn" type="button" data-item-id="${item.id}">Sil</button></td>
          </tr>`
      )
      .join("");

    const card = document.createElement("details");
    card.className = "pricing-upload-card";
    if (upload.isOpen) {
      card.setAttribute("open", "open");
    }

    card.innerHTML = `
      <summary>
        <div class="pricing-upload-top">
          <strong>${upload.uploadedBy}</strong>
          <span class="pricing-upload-chip pricing-upload-chip-primary">${directionLabel}</span>
          <span class="pricing-upload-chip pricing-upload-chip-count">${rowCountLabel}</span>
          ${escapedDescription ? `<span class="pricing-upload-inline-desc">Aciklama: ${escapedDescription}</span>` : ""}
        </div>
        <div class="pricing-upload-range">
          <span class="pricing-upload-range-label">Gecerlilik:</span>
          <span class="pricing-upload-range-value">${validFromText} - ${validToText}</span>
        </div>
        <span class="pricing-upload-meta">Yukleme: ${createdAtText}</span>
      </summary>
      <div class="pricing-upload-body">
        <div class="actions" style="margin-bottom:.5rem;">
          <button class="btn btn-small btn-ghost toggleUploadBtn" type="button">${upload.isOpen ? "Kapat" : "Ac"}</button>
            <button class="btn btn-small btn-success downloadUploadExcelBtn" type="button"><span class="excel-mini-icon" aria-hidden="true">XLS</span> Excel ile indir</button>
          <button class="btn btn-small btn-danger deleteUploadBtn" type="button">Sil</button>
        </div>
        ${escapedDescription ? `<div class="pricing-upload-desc-inline">Aciklama: ${escapedDescription}</div>` : ""}
        <div class="pricing-upload-desc-editor">
          <input type="text" class="uploadDescriptionInput" maxlength="500" placeholder="Aciklama ekle veya guncelle" value="${escapedDescription}" />
          <button class="btn btn-small btn-ghost saveUploadDescriptionBtn" type="button">Aciklamayi Kaydet</button>
        </div>
        <div class="pricing-upload-table-wrap">
          <table class="data-table">
            <thead><tr><th>Rota</th><th>Talep</th><th>Yon</th><th>Islem</th></tr></thead>
            <tbody>${detailsRows || '<tr><td colspan="4">Detay yok.</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    `;

    card.querySelector(".toggleUploadBtn").addEventListener("click", async () => {
      await apiFetch(`/api/pricing-uploads/${upload.id}/toggle`, {
        method: "PATCH",
        body: JSON.stringify({ isOpen: !upload.isOpen }),
      });
      await refreshPricingUploadsData();
    });

    card.querySelector(".deleteUploadBtn").addEventListener("click", async () => {
      const ok = window.confirm("Bu fiyat yuklemesini silmek istiyor musun?");
      if (!ok) {
        return;
      }

      await apiFetch(`/api/pricing-uploads/${upload.id}`, {
        method: "DELETE",
      });
      await refreshPricingUploadsData();
      await refreshNotificationsData();
    });

    card.querySelectorAll(".deletePriceItemBtn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const itemId = Number(btn.getAttribute("data-item-id") || 0);
        if (!Number.isInteger(itemId) || itemId <= 0) {
          return;
        }

        const ok = window.confirm("Bu fiyat satirini silmek istiyor musun?");
        if (!ok) {
          return;
        }

        await apiFetch(`/api/pricing-upload-items/${itemId}`, {
          method: "DELETE",
        });

        await refreshPricingUploadsData();
        await refreshNotificationsData();
      });
    });

    card.querySelector(".saveUploadDescriptionBtn")?.addEventListener("click", async () => {
      const input = card.querySelector(".uploadDescriptionInput");
      if (!input) {
        return;
      }

      const description = String(input.value || "").trim();
      await apiFetch(`/api/pricing-uploads/${upload.id}/description`, {
        method: "PATCH",
        body: JSON.stringify({ description }),
      });
      if (dom.pricingUploadMsg) {
        dom.pricingUploadMsg.style.color = "#1f7a1f";
        dom.pricingUploadMsg.textContent = "Aciklama kaydedildi.";
      }
      await refreshPricingUploadsData();
      await refreshNotificationsData();
    });

    card.querySelector(".downloadUploadExcelBtn").addEventListener("click", () => {
      try {
        downloadPricingUploadAsExcel(upload);
      } catch (error) {
        if (dom.pricingUploadMsg) {
          dom.pricingUploadMsg.style.color = "#d64545";
          dom.pricingUploadMsg.textContent = error.message || "Excel indirilemedi.";
        }
      }
    });

    dom.pricingUploadsList.appendChild(card);
  });
}

async function refreshPricingUploadsData() {
  const result = await apiFetch("/api/pricing-uploads");
  state.pricingUploads = result.uploads || [];
  renderPricingDashboardSummary();
  renderPricingUploads();
  renderReportsPanel();
}

async function submitPricingUpload() {
  if (!dom.pricingUploadForm || !dom.pricingExcelFile?.files?.length) {
    throw new Error("Excel dosyasi secmelisin.");
  }

  const file = dom.pricingExcelFile.files[0];
  let rows = await parsePricingUploadRows(file);

  if (!rows.length) {
    throw new Error("Excel icinde gecerli satir yok.");
  }

  const rowDirectionTypes = new Set(
    rows.map((item) => String(item.directionType || "").trim()).filter(Boolean)
  );

  let normalizedDirectionType = dom.pricingDirectionType?.value || "tek-yon";
  if (rowDirectionTypes.size === 1) {
    normalizedDirectionType = Array.from(rowDirectionTypes)[0];
  } else if (rowDirectionTypes.size > 1) {
    normalizedDirectionType = "tek-yon";
  }

  const payload = {
    directionType: normalizedDirectionType,
    validFrom: dom.pricingValidFrom?.value || "",
    validTo: dom.pricingValidTo?.value || "",
    description: dom.pricingDescriptionInput?.value || "",
    sourceFileName: file.name,
    rows,
  };

  const result = await apiFetch("/api/pricing-uploads", {
    method: "POST",
    body: JSON.stringify(payload),
  });

  renderPricingRejectedRows(result.rejected || []);
  if (dom.pricingUploadMsg) {
    dom.pricingUploadMsg.style.color = "#1f7a1f";
    dom.pricingUploadMsg.textContent = `${result.acceptedCount || 0} satir yuklendi ve uygulandi.`;
  }

  await refreshPricingUploadsData();
  await refreshNotificationsData();
}

function renderNotifications() {
  const unread = state.notifications.filter((n) => !n.isRead).length;
  dom.notifBadge.textContent = String(unread);
  dom.notifBadge.classList.toggle("hidden", unread === 0);

  dom.notifList.innerHTML = "";
  if (!state.notifications.length) {
    dom.notifEmpty.classList.remove("hidden");
    return;
  }

  dom.notifEmpty.classList.add("hidden");
  state.notifications.forEach((item) => {
    const message = String(item.message || "").trim();
    const actor = String(item.route || "").trim();

    let typeClass = "notif-type-info";
    let typeLabel = "Bilgi";
    if (message.includes("fiyat yukledi")) {
      typeClass = "notif-type-upload";
      typeLabel = "Yukleme";
    } else if (message.includes("sildi")) {
      typeClass = "notif-type-delete";
      typeLabel = "Silme";
    }

    const title = actor ? `${actor} islemi` : "Bildirim";
    const li = document.createElement("li");
    li.className = "notif-item";
    li.innerHTML = `
      <div class="notif-item-head">
        <strong class="notif-item-title">${title}</strong>
        <span class="notif-item-type ${typeClass}">${typeLabel}</span>
      </div>
      <div class="notif-item-body">${message}</div>
      <div class="notif-item-meta">${item.time || "-"}</div>
    `;
    dom.notifList.appendChild(li);
  });
}

async function refreshPricesData() {
  const result = await apiFetch("/api/prices");
  state.prices = result.prices || [];
  state.lastUpdated = result.lastUpdated || "-";
}

async function refreshNotificationsData() {
  const result = await apiFetch("/api/notifications");
  state.notifications = result.notifications || [];
  renderNotifications();
}

// ===== Fiyat degisim toast bildirimleri (sag ust kose) =====
// oBilet fiyat degisiklikleri obilet_price_history tablosuna yaziliyor (taramada).
// Burada SADECE okuyoruz — tarama/tespit/Telegram mantigina dokunmuyoruz.
// ===== Bildirim sesi (MP3 — /assets/notif.mp3) =====
let notifAudioEl = null;
function getNotifAudio() {
  if (!notifAudioEl) {
    notifAudioEl = new Audio("/assets/notif.mp3");
    notifAudioEl.preload = "auto";
    notifAudioEl.volume = 0.6;
  }
  return notifAudioEl;
}
// Tarayıcı, kullanıcı etkileşimi olmadan ses çalmaya izin vermez -> ilk tık/tuşta sesi önceden yükle.
["pointerdown", "keydown"].forEach((ev) =>
  window.addEventListener(ev, () => { try { getNotifAudio().load(); } catch {} }, { once: true })
);
function isNotifMuted() { return localStorage.getItem("notifSoundMuted") === "1"; }
function playNotifSound() {
  if (isNotifMuted()) return;
  try {
    const a = getNotifAudio();
    a.currentTime = 0;
    const p = a.play();
    if (p && p.catch) p.catch(() => {}); // autoplay engellenirse (etkileşim yoksa) sessizce geç
  } catch {}
}

// sticky=true (varsayilan): OTOMATIK KAYBOLMAZ — kullanici × ile / uzerine tiklayarak kapatir.
// Kullanici tercihi: bildirim sagda kalsin, kacirilmasin, "geliyor mu" test edilebilsin.
function showToast({ head, route, body, oldPrice, newPrice, kind, sticky = true, ttl = 9000, onDismiss }) {
  const host = document.getElementById("toastHost");
  if (!host) return;
  const el = document.createElement("div");
  el.className = "toast" + (kind ? " " + kind : "");
  const arrow = kind === "up" ? "▲" : kind === "down" ? "▼" : "→";
  const priceHtml = (oldPrice != null && newPrice != null)
    ? `<div class="toast-price"><span class="old">${escapeHtml(oldPrice)}₺</span><span class="arw">${arrow}</span><span class="new">${escapeHtml(newPrice)}₺</span></div>`
    : "";
  el.innerHTML = `
    <div class="toast-head"><span>${escapeHtml(head || "Bildirim")}</span><span class="toast-x">×</span></div>
    ${route ? `<div class="toast-route">${route}</div>` : ""}
    ${body ? `<div class="toast-body">${body}</div>` : ""}
    ${priceHtml}
    ${sticky ? "" : `<div class="toast-bar" style="animation-duration:${ttl}ms"></div>`}
  `;
  const dismiss = (userClosed) => {
    if (el.dataset.leaving) return;
    el.dataset.leaving = "1";
    el.classList.add("leaving");
    setTimeout(() => el.remove(), 260);
    // Kullanici kapatinca "gorüldü" bildir (sunucuya). Otomatik kaybolmada (sticky degil) tetiklenmez.
    if (userClosed && typeof onDismiss === "function") { try { onDismiss(); } catch {} }
  };
  el.addEventListener("click", () => dismiss(true));
  host.appendChild(el);
  // En fazla 6 bildirim ekranda dursun (en eskiyi at — kapatilmamis olsa da; sunucu pointer'i ilerlemedigi
  // icin bir sonraki acilista tekrar gelir).
  while (host.children.length > 6) host.firstElementChild.remove();
  if (!sticky) setTimeout(() => dismiss(false), ttl);
}

// ===== KULLANICI-BAZLI BİLDİRİM FEED'i (sağ-üst sticky) =====
// Sunucu, BU kullanıcının henüz görmediği (id > kendi pointer'ı) fiyat + yapısal değişimleri döndürür.
// Kullanıcı bir toast'ı kapatınca markToastSeen o kind'in pointer'ını ilerletir (kalıcı, sunucuda, kullanıcı
// bazlı). Kapatılmayanlar (reload/yeni oturum dahil) tekrar gelir. Giriş yapmamış kullanıcı, giriş yapınca
// biriken değişimleri görür. shownToastIds = bu oturumda gösterilenler (aynı toast'ı tekrar basmamak için).

function stStructHead(type) {
  switch (type) {
    case "new_operator": return "Yeni rakip";
    case "removed_operator": return "Rakip çekildi";
    case "new_departure": return "Yeni sefer";
    case "removed_departure": return "Sefer kaldırıldı";
    case "capacity_up": return "Kapasite arttı";
    case "capacity_down": return "Kapasite azaldı";
    case "extra_bus": return "Ek araç";
    case "time_shift": return "Saat kaydırma";
    default: return "Ağ değişikliği";
  }
}

async function markToastSeen(kind, id) {
  try { await apiFetch("/api/obilet/toast-seen", { method: "POST", body: JSON.stringify({ kind, id }) }); }
  catch { /* sessiz — pointer ilerlemezse bir sonraki turda tekrar gelir */ }
}

async function checkToastFeed() {
  if (!state.currentUser) return;
  let data;
  try { data = await apiFetch("/api/obilet/toast-feed"); } catch { return; }
  if (!data || !data.ok) return;
  let newCount = 0; // bu turda yeni gösterilen bildirim sayısı (>0 ise TEK ses çal)

  // Fiyat değişimleri (eski -> yeni sırayla)
  for (const r of (Array.isArray(data.price) ? data.price : [])) {
    const id = Number(r.id) || 0;
    if (!id || state.shownToastIds.price.has(id)) continue;
    state.shownToastIds.price.add(id);
    newCount++;
    const oldP = Math.round(Number(r.old_price) || 0);
    const newP = Math.round(Number(r.new_price) || 0);
    const kind = newP > oldP ? "up" : newP < oldP ? "down" : "info";
    const op = escapeHtml(String(r.operator || "").trim() || "-");
    const from = escapeHtml(String(r.origin || "").trim());
    const to = escapeHtml(String(r.destination || "").trim());
    const when = escapeHtml(String(r.departure_time || "").trim());
    const day = escapeHtml(String(r.journey_date || "").trim());
    showToast({
      head: "Fiyat değişti",
      route: `<b>${op}</b> · ${from} → ${to}<br>${day} ${when} kalkış`,
      oldPrice: oldP, newPrice: newP, kind, sticky: true,
      onDismiss: () => markToastSeen("price", id),
    });
  }

  // Yapısal değişimler (eski -> yeni sırayla)
  for (const r of (Array.isArray(data.structure) ? data.structure : [])) {
    const id = Number(r.id) || 0;
    if (!id || state.shownToastIds.structure.has(id)) continue;
    state.shownToastIds.structure.add(id);
    newCount++;
    const type = String(r.change_type || "");
    // Tehdit (yeni rakip / ek araç / saat kaydırma / kapasite-sefer artışı) kırmızı; fırsat (çekildi/kaldırıldı) yeşil.
    const kind = /new_operator|extra_bus|time_shift|capacity_up|new_departure/.test(type) ? "up"
      : /removed_operator|removed_departure|capacity_down/.test(type) ? "down" : "info";
    showToast({
      head: stStructHead(type),
      body: escapeHtml(String(r.message || "")),
      kind, sticky: true,
      onDismiss: () => markToastSeen("structure", id),
    });
  }

  if (newCount > 0) playNotifSound(); // patlama olsa da tek "ding"
}

function startNotificationPolling() {
  stopNotificationPolling();
  state.notifPollTimer = setInterval(() => {
    if (state.currentUser) {
      refreshNotificationsData().catch(() => null);
      refreshPricingUploadsData().catch(() => null);
      refreshPricesData().catch(() => null);
      checkToastFeed().catch(() => null);
    }
  }, 60000);
}

function stopNotificationPolling() {
  if (state.notifPollTimer) {
    clearInterval(state.notifPollTimer);
    state.notifPollTimer = null;
  }
}

async function activatePanel(menuKey) {
  state.activeMenu = String(menuKey || "");
  const switchSeq = ++state.panelSwitchSeq;
  if (dom.contentCard) {
    dom.contentCard.classList.add("panel-switching");
  }

  // Menu gecisinde spinner gorunsun diye kisa bekleme (yaklasik yarim tur donus).
  await pause(420);
  if (switchSeq !== state.panelSwitchSeq) {
    return;
  }

  document.querySelectorAll(".panel-block").forEach((el) => {
    el.classList.remove("panel-enter");
    el.classList.toggle("hidden", el.dataset.menu !== menuKey);
  });

  const activePanel = document.querySelector(`.panel-block[data-menu="${menuKey}"]`);
  if (activePanel) {
    activePanel.classList.add("panel-enter");
  }

  if (dom.contentCard) {
    dom.contentCard.classList.remove("panel-switching");
  }

  const current = MENUS.find((m) => m.key === menuKey);
  const title = current ? current.label : "Panel";

  // Her menu icin alt baslik tanimla (sayfa kapsami bir bakista anlasilsin)
  const SUBTITLES = {
    dashboard: "Bugünün özetini ve son hareketleri tek bakışta görün",
    routes: "Bakanlık tarife verisi - hat ve fiyat sorgulama",
    pricing: "Tek yön / gidiş-dönüş fiyat dosyalarını yükleyin",
    oneway: "Yüklenen tek yön fiyatlarını görüntüleyin",
    reporting: "oBilet üzerinde tespit edilen fiyat değişikliklerinin geçmişi",
    oneops: "Hatalı işlem bildirimleri ve fotoğraf kayıtları",
    obilet: "Rakip otobüs firmalarının fiyatlarını otomatik takip edin",
    ocr: "Fotoğraftan otomatik tablo çıkarımı",
    demand: "Sistem popüler hatları otomatik tarar; yoğun ve boş günleri gösterir",
    permissions: "Kullanıcı yetki yönetimi",
    logs: "Sistem giriş ve oturum kayıtları",
    sure_hesap: "İki yer arası karayolu süresi + mesafe (yeni güzergah planlarken durak/il arası süreyi bulmak için)",
  };
  setContentHeader({ title, subtitle: SUBTITLES[menuKey] || "" });

  // Dashboard verisi yukle
  if (menuKey === "dashboard") {
    loadDashboardSummary().catch(() => null);
  }

  if (menuKey === "permissions") {
    renderAdminPermissions();
  }

  if (menuKey === "logs") {
    renderLoginLogs();
  }

  if (menuKey === "pricing") {
    refreshPricingUploadsData().catch((error) => {
      if (dom.pricingUploadsList) {
        dom.pricingUploadsList.innerHTML = `<p class="subtle">${error.message}</p>`;
      }
    });
  }

  if (menuKey === "routes") {
    const query = dom.tariffSearchInput ? dom.tariffSearchInput.value : state.tariffQuery;
    refreshTariffData(query).catch((error) => {
      dom.routeTableBody.innerHTML = `<tr><td colspan="3">${error.message}</td></tr>`;
      if (dom.tariffSummary) {
        dom.tariffSummary.textContent = "Tarife verisi yuklenemedi.";
      }
    });
  }

  if (menuKey === "reports") {
    renderReportsPanel();
  }

  if (menuKey === "reporting") {
    // Yeni Raporlama sayfasi: oBilet fiyat degisiklik gecmisi.
    initPriceHistoryPanel().catch((error) => {
      if (dom.phStatusMsg) {
        dom.phStatusMsg.textContent = error.message || "Veri yuklenemedi.";
      }
    });
  }

  if (menuKey === "oneops") {
    renderControlIntegrationPanel().catch((error) => {
      if (dom.controlStatusMsg) {
        dom.controlStatusMsg.style.color = "#d64545";
        dom.controlStatusMsg.textContent = error.message || "OneOps bilgileri yuklenemedi.";
      }
    });
  }

  if (menuKey === "obilet_tracker") {
    initObiletPanel().catch((error) => {
      const statusEl = document.getElementById("obiletActionStatus");
      if (statusEl) {
        statusEl.style.color = "#d64545";
        statusEl.textContent = error.message || "oBilet paneli yuklenemedi.";
      }
    });
  }

  if (menuKey === "occupancy") {
    setupOccupancyPanel();
  }

  if (menuKey === "sefer_takip") {
    setupSeferTakipPanel();
  }

  if (menuKey === "demand") {
    setupDemandPanel();
  }

  if (menuKey === "sure_hesap") {
    setupSureHesap();
  }
}

// ============ SURE HESAPLAMA (iki yer arasi karayolu suresi) ============
// Ucretsiz: Nominatim (yer->koordinat) + OSRM (koordinat->sure). API anahtari GEREKMEZ. Tamamen
// client-side (tarayicidan); backend/egress bagimliligi yok. Yeni guzergah planlarken il/durak arasi
// sureyi Google'a bakmadan bulmak icin.
function shFmtDur(sec) {
  const m = Math.round(sec / 60);
  const h = Math.floor(m / 60);
  const mm = m % 60;
  return h ? `${h} sa ${mm} dk` : `${mm} dk`;
}
// Adi merkez sehirden FARKLI olan iller (il poligon merkezi uzak kaliyor -> merkez sehre cevir).
// Cogu ilde ad = merkez sehir (Ankara, Adana, Bursa...) zaten dogru cikar; bunlar istisna.
const SH_IL_MERKEZ = {
  "hatay": "Antakya", "kocaeli": "İzmit", "sakarya": "Adapazarı",
  "afyon": "Afyonkarahisar", "içel": "Mersin", "icel": "Mersin",
};
function shNormPlace(q) {
  const k = String(q || "").toLocaleLowerCase("tr").trim();
  return SH_IL_MERKEZ[k] || q;
}
async function shGeoNominatim(q) {
  const r = await fetch(`https://nominatim.openstreetmap.org/search?format=json&limit=1&countrycodes=tr&q=${encodeURIComponent(q + ", Türkiye")}`, { headers: { "Accept-Language": "tr" } });
  if (!r.ok) throw new Error("nominatim");
  const a = await r.json();
  if (!a || !a.length) throw new Error(`"${q}" bulunamadı`);
  return { lat: +a[0].lat, lon: +a[0].lon, name: String(a[0].display_name || q).split(",")[0] };
}
async function shGeoPhoton(q) {
  const r = await fetch(`https://photon.komoot.io/api/?q=${encodeURIComponent(q + ", Türkiye")}&limit=1`);
  if (!r.ok) throw new Error("photon");
  const d = await r.json();
  const f = d && d.features && d.features[0];
  if (!f) throw new Error(`"${q}" bulunamadı`);
  return { lat: f.geometry.coordinates[1], lon: f.geometry.coordinates[0], name: f.properties.name || q };
}
async function shGeocode(q) {
  const query = shNormPlace(q);
  // Once Nominatim (iyi Turkce eslesme, tarayici gercek UA gonderir). Engellenir/bos donerse Photon yedek.
  try { return await shGeoNominatim(query); }
  catch (e) { return await shGeoPhoton(query); }
}
async function shRoute(a, b) {
  const url = `https://router.project-osrm.org/route/v1/driving/${a.lon},${a.lat};${b.lon},${b.lat}?overview=false`;
  const r = await fetch(url);
  if (!r.ok) throw new Error("Rota servisi yanıt vermedi");
  const d = await r.json();
  if (!d.routes || !d.routes.length) throw new Error("İki nokta arası rota bulunamadı");
  return { seconds: d.routes[0].duration, meters: d.routes[0].distance };
}
// ---- COK-DURAKLI GUZERGAH PLANLAYICI (PKM Talep Formu otomasyonu) ----
// Kullanici siralı duraklar + kalkis saati girer; her leg suresi (OSRM+Nominatim, otobus tahmini)
// otomatik cekilir, peron sureleriyle kumulatif varis/kalkis saatleri hesaplanir, Excel indirilir.
const shState = { stops: [], geo: [], legMin: [], peronMin: [], depMin: 840 };
function shHMtoMin(hm) { const m = String(hm || "").match(/^(\d{1,2}):(\d{2})/); return m ? (+m[1]) * 60 + (+m[2]) : 0; }
function shMinToHM(total) {
  const day = Math.floor(total / 1440);
  const t = ((total % 1440) + 1440) % 1440;
  const hm = `${String(Math.floor(t / 60)).padStart(2, "0")}:${String(t % 60).padStart(2, "0")}`;
  return day > 0 ? `${hm} (+${day}g)` : hm;
}
function shMinToDur(min) { const h = Math.floor(min / 60), m = Math.round(min % 60); return h ? `${h} sa ${m} dk` : `${m} dk`; }
function shComputeTimes() {
  const n = shState.stops.length;
  const arr = new Array(n), dep = new Array(n);
  dep[0] = shState.depMin; arr[0] = null;
  for (let i = 1; i < n; i++) {
    arr[i] = dep[i - 1] + (shState.legMin[i] || 0);
    dep[i] = arr[i] + (shState.peronMin[i] || 0);
  }
  return { arr, dep };
}
function shUpdateTimes() {
  const { arr, dep } = shComputeTimes();
  const n = shState.stops.length;
  for (let i = 0; i < n; i++) {
    const v = document.querySelector(`[data-shvaris="${i}"]`);
    const k = document.querySelector(`[data-shkalkis="${i}"]`);
    if (v) v.innerHTML = i === 0 ? `<span class="subtle">—</span>` : `<b>${shMinToHM(arr[i])}</b>`;
    if (k) k.innerHTML = `<b style="color:var(--accent);">${shMinToHM(dep[i])}</b>`;
  }
  const tot = document.getElementById("shTotal");
  if (tot) { const last = arr[n - 1] != null ? arr[n - 1] : dep[n - 1]; tot.innerHTML = `Toplam seyahat: <b>${shMinToDur(last - shState.depMin)}</b> · Son varış: <b>${shMinToHM(arr[n - 1])}</b>`; }
}
function shRenderPlan() {
  const el = document.getElementById("shPlanResult");
  if (!el) return;
  const { arr, dep } = shComputeTimes();
  const rows = shState.stops.map((s, i) => {
    const seyahat = i === 0 ? `<span class="subtle">ana durak</span>`
      : `<input type="number" min="0" value="${shState.legMin[i] || 0}" data-shleg="${i}" style="width:64px;" /> dk <span class="subtle" style="font-size:0.72rem;">(${shMinToDur(shState.legMin[i] || 0)})</span>`;
    const peron = i === 0 ? `<span class="subtle">—</span>`
      : `<input type="number" min="0" value="${shState.peronMin[i] || 0}" data-shperon="${i}" style="width:54px;" /> dk`;
    return `<tr><td>${i + 1}</td><td><b>${occEsc(s)}</b></td><td>${seyahat}</td>` +
      `<td data-shvaris="${i}">${i === 0 ? `<span class="subtle">—</span>` : `<b>${shMinToHM(arr[i])}</b>`}</td>` +
      `<td>${peron}</td><td data-shkalkis="${i}"><b style="color:var(--accent);">${shMinToHM(dep[i])}</b></td></tr>`;
  }).join("");
  const n = shState.stops.length;
  const last = arr[n - 1] != null ? arr[n - 1] : dep[n - 1];
  el.innerHTML =
    `<table class="obilet-prices-table" style="width:100%; min-width:520px; border-collapse:collapse;">` +
    `<thead><tr><th>Sıra</th><th>Durak</th><th>Seyahat</th><th>Varış</th><th>Peron</th><th>Kalkış</th></tr></thead>` +
    `<tbody>${rows}</tbody></table>` +
    `<div id="shTotal" class="subtle" style="margin-top:0.7rem;">Toplam seyahat: <b>${shMinToDur(last - shState.depMin)}</b> · Son varış: <b>${shMinToHM(arr[n - 1])}</b></div>`;
  el.querySelectorAll("input[data-shleg]").forEach((inp) => inp.addEventListener("input", () => { shState.legMin[+inp.dataset.shleg] = Math.max(0, parseInt(inp.value || "0", 10) || 0); shUpdateTimes(); }));
  el.querySelectorAll("input[data-shperon]").forEach((inp) => inp.addEventListener("input", () => { shState.peronMin[+inp.dataset.shperon] = Math.max(0, parseInt(inp.value || "0", 10) || 0); shUpdateTimes(); }));
}
async function shPlan() {
  const stops = (document.getElementById("shStops").value || "").split("\n").map((s) => s.trim()).filter(Boolean);
  const msg = document.getElementById("shPlanMsg");
  const resEl = document.getElementById("shPlanResult");
  const excelBtn = document.getElementById("shExcel");
  const planBtn = document.getElementById("shPlan");
  if (stops.length < 2) { msg.innerHTML = `<span style="color:#d64545;">En az 2 durak yazın (her satıra bir yer).</span>`; return; }
  shState.depMin = shHMtoMin(document.getElementById("shDep").value);
  const peronDef = Math.max(0, parseInt(document.getElementById("shPeronDef").value || "5", 10) || 0);
  planBtn.disabled = true;
  if (excelBtn) excelBtn.style.display = "none";
  resEl.innerHTML = `<div class="loader-ring" style="margin:1rem 0;"></div>`;
  try {
    const geo = [];
    for (let i = 0; i < stops.length; i++) {
      msg.innerHTML = `<span class="subtle">Konumlar bulunuyor... ${i + 1}/${stops.length} (${occEsc(stops[i])})</span>`;
      geo.push(await shGeocode(stops[i]));
      if (i < stops.length - 1) await pause(1100); // Nominatim 1 istek/sn
    }
    const legMin = [0];
    for (let i = 1; i < stops.length; i++) {
      msg.innerHTML = `<span class="subtle">Süreler hesaplanıyor... ${i}/${stops.length - 1}</span>`;
      const rt = await shRoute(geo[i - 1], geo[i]);
      legMin.push(Math.round((rt.seconds * 1.15) / 60)); // otobus tahmini (~%15), dakika
      await pause(150);
    }
    shState.stops = stops; shState.geo = geo; shState.legMin = legMin;
    shState.peronMin = stops.map((_, i) => (i === 0 ? 0 : peronDef));
    msg.innerHTML = `<span class="subtle">Hesaplandı. Seyahat/peron sürelerini tabloda düzenleyebilirsin — saatler anında güncellenir.</span>`;
    shRenderPlan();
    if (excelBtn) excelBtn.style.display = "";
    const saveBtn = document.getElementById("shSave"); if (saveBtn) saveBtn.style.display = "";
  } catch (e) {
    resEl.innerHTML = "";
    msg.innerHTML = `<span style="color:#d64545;">Hata: ${occEsc(e.message || "hesaplanamadı")}</span>`;
  } finally {
    planBtn.disabled = false;
  }
}
async function shExportExcel() {
  if (!shState.stops.length) { alert("Önce Hesapla'ya basın."); return; }
  const btn = document.getElementById("shExcel");
  const orig = btn ? btn.textContent : "";
  // Backend, senin gonderdigin PKM Talep Formu SABLONUNU doldurur (duraklar + sureler + kalkis saati);
  // saatleri sablonun kendi formulleri Excel'de hesaplar.
  const stops = shState.stops.map((name, i) => ({ name, sey: i === 0 ? 0 : (shState.legMin[i] || 0), per: i === 0 ? 0 : (shState.peronMin[i] || 0) }));
  if (btn) { btn.disabled = true; btn.textContent = "İndiriliyor..."; }
  try {
    const r = await fetch("/api/pkm-export", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...(state.token ? { Authorization: `Bearer ${state.token}` } : {}) },
      body: JSON.stringify({ departureMin: shState.depMin, stops }),
    });
    if (!r.ok) { let m = "İndirilemedi"; try { m = (await r.json()).message || m; } catch (e) {} throw new Error(m); }
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = "pkm-guzergah-plani.xlsx";
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  } catch (e) {
    alert("Excel indirilemedi: " + (e.message || e));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = orig; }
  }
}
function setupSureHesap() {
  shLoadRoutesList(); // her acilista tazele (baskalari yeni guzergah eklemis olabilir)
  const planBtn = document.getElementById("shPlan");
  if (!planBtn || planBtn.dataset.wired) return;
  planBtn.dataset.wired = "1";
  planBtn.addEventListener("click", shPlan);
  const excelBtn = document.getElementById("shExcel");
  if (excelBtn) excelBtn.addEventListener("click", shExportExcel);
  const saveBtn = document.getElementById("shSave");
  if (saveBtn) saveBtn.addEventListener("click", shSaveRoute);
  const depEl = document.getElementById("shDep");
  if (depEl) depEl.addEventListener("change", () => { shState.depMin = shHMtoMin(depEl.value); if (shState.stops.length) shUpdateTimes(); });
}
// ---- Kayitli guzergahlar (paylasimli) ----
async function shLoadRoutesList() {
  const el = document.getElementById("shSavedList");
  if (!el) return;
  try { const d = await apiFetch("/api/pkm-routes"); shRenderSavedList(d.routes || []); }
  catch (e) { el.innerHTML = `<span style="color:#d64545;">${occEsc(e.message || "yüklenemedi")}</span>`; }
}
function shRenderSavedList(routes) {
  const el = document.getElementById("shSavedList");
  if (!el) return;
  if (!routes.length) { el.innerHTML = `Henüz kayıtlı güzergah yok. Bir plan hesaplayıp <b>Güzergahı Kaydet</b> ile ekleyebilirsin.`; return; }
  el.innerHTML = routes.map((r) =>
    `<div class="sh-saved-row" data-id="${r.id}" style="display:flex; justify-content:space-between; align-items:center; gap:0.8rem; padding:0.55rem 0.7rem; border:1px solid var(--stroke); border-radius:8px; margin-bottom:0.5rem; cursor:pointer;">` +
    `<div><b>${occEsc(r.name)}</b> <span class="subtle" style="font-size:0.78rem;">· ${occEsc(r.first)} → ${occEsc(r.last)} · ${r.stopCount} durak</span>` +
    `<div class="subtle" style="font-size:0.72rem;">${occEsc(r.created_by || "")}${r.created_at ? " · " + occEsc(r.created_at) : ""}</div></div>` +
    `<button class="sh-del btn btn-sm btn-danger" data-id="${r.id}" type="button" title="Sil">Sil</button></div>`
  ).join("");
  el.querySelectorAll(".sh-saved-row").forEach((row) => row.addEventListener("click", (e) => { if (e.target.closest(".sh-del")) return; shLoadRoute(row.dataset.id); }));
  el.querySelectorAll(".sh-del").forEach((b) => b.addEventListener("click", (e) => { e.stopPropagation(); shDeleteRoute(b.dataset.id); }));
}
async function shSaveRoute() {
  if (!shState.stops.length) { alert("Önce Hesapla'ya basın."); return; }
  const def = `${shState.stops[0]} → ${shState.stops[shState.stops.length - 1]}`;
  const name = prompt("Güzergah adı:", def);
  if (name == null) return;
  const nm = String(name).trim(); if (!nm) return;
  try {
    await apiFetch("/api/pkm-routes", { method: "POST", body: JSON.stringify({ name: nm, plan: { stops: shState.stops, legMin: shState.legMin, peronMin: shState.peronMin, depMin: shState.depMin } }) });
    const msg = document.getElementById("shPlanMsg"); if (msg) msg.innerHTML = `<span style="color:#1f7a1f;">Kaydedildi: ${occEsc(nm)}</span>`;
    shLoadRoutesList();
  } catch (e) { alert("Kaydedilemedi: " + (e.message || e)); }
}
async function shLoadRoute(id) {
  try {
    const d = await apiFetch(`/api/pkm-routes/${id}`);
    const p = d.plan || {};
    if (!Array.isArray(p.stops) || p.stops.length < 2) throw new Error("Geçersiz plan");
    shState.stops = p.stops; shState.legMin = Array.isArray(p.legMin) ? p.legMin : []; shState.peronMin = Array.isArray(p.peronMin) ? p.peronMin : []; shState.depMin = Number(p.depMin) || 840;
    const st = document.getElementById("shStops"); if (st) st.value = p.stops.join("\n");
    const dep = document.getElementById("shDep"); if (dep) dep.value = shMinToHM(shState.depMin);
    const ex = document.getElementById("shExcel"); if (ex) ex.style.display = "";
    const sv = document.getElementById("shSave"); if (sv) sv.style.display = "";
    shRenderPlan();
    const msg = document.getElementById("shPlanMsg"); if (msg) msg.innerHTML = `<span class="subtle">Açıldı: <b>${occEsc(d.name || "")}</b> — düzenleyip yeniden kaydedebilir ya da Excel indirebilirsin.</span>`;
  } catch (e) { alert("Açılamadı: " + (e.message || e)); }
}
async function shDeleteRoute(id) {
  if (!confirm("Bu güzergah silinsin mi?")) return;
  try { await apiFetch(`/api/pkm-routes/${id}`, { method: "DELETE" }); shLoadRoutesList(); }
  catch (e) { alert("Silinemedi: " + (e.message || e)); }
}

async function handleLogin(username, password) {
  try {
    const result = await apiFetch("/api/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });

    setToken(result.token);
    state.currentUser = result.user;
    showMessage("Signed in successfully.", false);

    showOverlay(true);
    await pause(1100);
    showOverlay(false);

    dom.loginForm.reset();
    showMessage("", false);
    render();
    await refreshPricesData();
    await refreshPricingUploadsData();
    await refreshNotificationsData();
    await renderControlIntegrationPanel().catch(() => null);
    startNotificationPolling();
    // Kaçırılan (giriş yapmadığın sırada biriken) değişimleri 60 sn beklemeden HEMEN göster.
    checkToastFeed().catch(() => null);
  } catch (error) {
    showMessage(error.message || "Sign-in failed.");
  }
}

async function verifySession() {
  if (!state.token) {
    render();
    return;
  }

  try {
    const result = await apiFetch("/api/me");
    state.currentUser = result.user;
    await refreshPricesData();
    await refreshPricingUploadsData();
    await refreshNotificationsData();
    await renderControlIntegrationPanel().catch(() => null);
    startNotificationPolling();
    // Kaçırılan (giriş yapmadığın sırada biriken) değişimleri 60 sn beklemeden HEMEN göster.
    checkToastFeed().catch(() => null);
  } catch {
    setToken("");
    state.currentUser = null;
    stopReportingAutoRefresh();
    stopNotificationPolling();
  }

  render();
}

async function handleLogout() {
  try {
    await apiFetch("/api/logout", { method: "POST" });
  } catch {
    // Oturum zaten dusmus olabilir; lokal temizlemek yeterli.
  }

  setToken("");
  state.currentUser = null;
  stopReportingAutoRefresh();
  stopNotificationPolling();
  render();
}

function buildPermissions(fullAccess) {
  const permissions = {};
  MENUS.forEach((menu) => {
    if (ADMIN_ONLY_MENUS.has(menu.key)) {
      permissions[menu.key] = false;
      return;
    }
    permissions[menu.key] = Boolean(fullAccess) || menu.key === "dashboard";
  });
  return permissions;
}

// "DD.MM.YYYY HH:MM:SS" (Istanbul) damgasini Date'e cevir (tarayici da Istanbul saatinde).
function parseTrStamp(s) {
  const m = String(s || "").match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return null;
  return new Date(+m[3], +m[2] - 1, +m[1], +m[4], +m[5], +m[6]);
}

// Son aktifligi insan-okur metne cevir + "su an aktif" durumu (90 sn esik).
function activityStatus(lastSeen) {
  const d = parseTrStamp(lastSeen);
  if (!d) return { text: "Hiç giriş yapmadı", online: false, muted: true };
  let diff = Math.floor((Date.now() - d.getTime()) / 1000);
  if (diff < 0) diff = 0;
  if (diff < 90) return { text: "Şu an aktif", online: true };
  if (diff < 3600) return { text: `${Math.floor(diff / 60)} dakika önce`, online: false };
  if (diff < 86400) {
    const h = Math.floor(diff / 3600);
    const mm = Math.floor((diff % 3600) / 60);
    return { text: mm ? `${h} saat ${mm} dk önce` : `${h} saat önce`, online: false };
  }
  return { text: `${Math.floor(diff / 86400)} gün önce`, online: false };
}

// Bir aktiflik hucresini doldur (yesil nokta = su an aktif).
function renderActivityCell(cell, lastSeen) {
  const st = activityStatus(lastSeen);
  cell.title = lastSeen || "Kayit yok";
  if (st.online) {
    cell.innerHTML = '<span class="status-badge status-active">Şu an aktif</span>';
  } else {
    cell.innerHTML = `<span class="subtle"${st.muted ? "" : ' style="color:inherit"'}>${st.text}</span>`;
  }
}

// Admin panelindeki aktiflik hucrelerini periyodik tazele (tam re-render yapmadan,
// acik editorleri bozmadan). Tablo DOM'da degilse interval'i durdur.
async function refreshUserActivityCells() {
  const rowsEl = document.getElementById("userRows");
  if (!rowsEl) { if (state.adminUsersTimer) { clearInterval(state.adminUsersTimer); state.adminUsersTimer = null; } return; }
  try {
    const result = await apiFetch("/api/admin/users");
    const map = new Map(result.users.map((u) => [String(u.id), u.lastSeen]));
    rowsEl.querySelectorAll("tr[data-user-id]").forEach((tr) => {
      const cell = tr.querySelector(".lastseen");
      if (cell && map.has(tr.dataset.userId)) renderActivityCell(cell, map.get(tr.dataset.userId));
    });
  } catch (e) { /* sessiz — arka plan tazeleme istegi bozmasin */ }
}

async function renderAdminPermissions() {
  if (state.adminUsersTimer) { clearInterval(state.adminUsersTimer); state.adminUsersTimer = null; }
  if (!state.currentUser?.isAdmin) {
    dom.permissionAdminArea.innerHTML = '<p class="subtle">Bu bolum sadece admin icin aciktir.</p>';
    return;
  }

  try {
    const result = await apiFetch("/api/admin/users");
    state.usersCache = result.users.filter((u) => !u.isAdmin);

    dom.permissionAdminArea.innerHTML = `
      <div class="admin-grid">
        <section class="admin-card">
          <h5>Yeni Kullanici Olustur</h5>
          <form id="newUserForm" class="inline-form" autocomplete="off">
            <label><span>Kullanici Adi</span><input id="newUsername" required /></label>
            <label><span>Sifre</span><input id="newPassword" type="password" required /></label>
            <label class="checkbox-row"><input id="newFullAccess" type="checkbox" checked /> Tum menulere tam yetki ver</label>
            <button class="btn btn-primary" type="submit">Kullanici Ekle</button>
          </form>
          <p id="newUserMsg" class="message"></p>
        </section>

        <section class="admin-card">
          <h5>Kullanicilar ve Yetkiler</h5>
          <table class="data-table">
            <thead>
              <tr><th>Kullanici</th><th>Durum</th><th>Son Aktiflik</th><th>Olusturma</th><th>Islem</th></tr>
            </thead>
            <tbody id="userRows"></tbody>
          </table>
        </section>
      </div>
    `;

    const rows = document.getElementById("userRows");

    state.usersCache.forEach((user) => {
      const row = dom.templateRow.content.cloneNode(true);
      const tr = row.querySelector("tr");
      if (tr) tr.dataset.userId = user.id;
      row.querySelector(".username").textContent = user.username;
      row.querySelector(".created").textContent = user.createdAt;

      const statusCell = row.querySelector(".status");
      statusCell.innerHTML = user.isActive
        ? '<span class="status-badge status-active">Aktif</span>'
        : '<span class="status-badge status-passive">Pasif</span>';

      const seenCell = row.querySelector(".lastseen");
      if (seenCell) renderActivityCell(seenCell, user.lastSeen);

      const toggleBtn = row.querySelector(".toggleActiveBtn");
      toggleBtn.textContent = user.isActive ? "Pasif Et" : "Aktif Et";
      toggleBtn.classList.add(user.isActive ? "btn-warn" : "btn-success");
      toggleBtn.addEventListener("click", async () => {
        await updateUser(user.id, { isActive: !user.isActive });
      });

      row.querySelector(".updateBtn").addEventListener("click", () => {
        openUpdateEditor(user.id);
      });

      row.querySelector(".editPermBtn").addEventListener("click", () => {
        openPermissionEditor(user.id);
      });

      row.querySelector(".deleteBtn").addEventListener("click", async () => {
        try {
          await apiFetch(`/api/admin/users/${user.id}`, { method: "DELETE" });
          await renderAdminPermissions();
        } catch (error) {
          alert(error.message || "Kullanici silinemedi.");
        }
      });

      rows.appendChild(row);
    });

    // Aktiflik hucrelerini her 20 sn'de bir tazele ("su an aktif / X dk once" canli kalsin).
    state.adminUsersTimer = setInterval(refreshUserActivityCells, 20000);

    const newUserForm = document.getElementById("newUserForm");
    const newUserMsg = document.getElementById("newUserMsg");

    newUserForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      const username = document.getElementById("newUsername").value.trim();
      const password = document.getElementById("newPassword").value.trim();
      const fullAccess = document.getElementById("newFullAccess").checked;

      if (!username || !password) {
        newUserMsg.textContent = "Kullanici adi ve sifre zorunlu.";
        return;
      }

      try {
        await apiFetch("/api/admin/users", {
          method: "POST",
          body: JSON.stringify({ username, password, fullAccess }),
        });
        newUserMsg.style.color = "#1f7a1f";
        newUserMsg.textContent = "Kullanici olusturuldu.";
        newUserForm.reset();
        await renderAdminPermissions();
      } catch (error) {
        newUserMsg.style.color = "#d64545";
        newUserMsg.textContent = error.message || "Kullanici olusturulamadi.";
      }
    });
  } catch (error) {
    dom.permissionAdminArea.innerHTML = `<p class="subtle">${error.message}</p>`;
  }
}

async function updateUser(userId, payload) {
  try {
    await apiFetch(`/api/admin/users/${userId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    await renderAdminPermissions();
  } catch (error) {
    alert(error.message || "Kullanici guncellenemedi.");
  }
}

function openUpdateEditor(userId) {
  const user = state.usersCache.find((u) => u.id === userId);
  if (!user) {
    return;
  }

  const wrap = document.createElement("div");
  wrap.className = "admin-card";
  wrap.innerHTML = `
    <h5>${user.username} - Kullanici Guncelle</h5>
    <div class="inline-form" style="grid-template-columns: 1fr 1fr; gap: 0.7rem;">
      <label>
        <span>Kullanici Adi</span>
        <input id="editUsername" type="text" value="${escapeHtml(user.username)}" />
      </label>
      <label>
        <span>Yeni Sifre (opsiyonel)</span>
        <input id="editPassword" type="password" placeholder="Bos birakirsan degismez" />
      </label>
      <label>
        <span>Ad Soyad</span>
        <input id="editFullName" type="text" value="${escapeHtml(user.fullName || "")}" placeholder="Ornek: Ersan Bildirici" />
      </label>
      <label>
        <span>Unvan / Rol (sidebar'da gorunur)</span>
        <input id="editRole" type="text" value="${escapeHtml(user.role || "")}" placeholder="Ornek: Bölge Operasyon Uzmanı" />
      </label>
      <label>
        <span>Gorevi (opsiyonel ek bilgi)</span>
        <input id="editTitle" type="text" value="${escapeHtml(user.title || "")}" placeholder="Ornek: Dogu Anadolu" />
      </label>
      <label class="checkbox-row" style="align-self:center;">
        <input id="editActive" type="checkbox" ${user.isActive ? "checked" : ""} /> Hesap aktif
      </label>
    </div>
    <p class="field-hint">Sifre alanini bos birakirsan mevcut sifre korunur.</p>
    <div class="actions" style="margin-top:.6rem;">
      <button class="btn btn-primary" id="saveUserBtn">Kaydet</button>
      <button class="btn btn-ghost" id="cancelUserBtn">Kapat</button>
    </div>
  `;

  const area = dom.permissionAdminArea.querySelector(".admin-grid");
  area.prepend(wrap);

  wrap.querySelector("#cancelUserBtn").addEventListener("click", () => wrap.remove());
  wrap.querySelector("#saveUserBtn").addEventListener("click", async () => {
    const username = wrap.querySelector("#editUsername").value.trim();
    const password = wrap.querySelector("#editPassword").value.trim();
    const fullName = wrap.querySelector("#editFullName").value.trim();
    const role = wrap.querySelector("#editRole").value.trim();
    const title = wrap.querySelector("#editTitle").value.trim();
    const isActive = wrap.querySelector("#editActive").checked;
    await updateUser(userId, { username, password, fullName, role, title, isActive });
    wrap.remove();
  });
}

function openPermissionEditor(userId) {
  const user = state.usersCache.find((u) => u.id === userId);
  if (!user) {
    return;
  }

  const options = MENUS.filter((m) => !ADMIN_ONLY_MENUS.has(m.key))
    .map((m) => {
      const checked = user.permissions?.[m.key] ? "checked" : "";
      return `<label class="checkbox-row"><input type="checkbox" data-key="${m.key}" ${checked} /> ${m.label}</label>`;
    })
    .join("");

  const wrap = document.createElement("div");
  wrap.className = "admin-card";
  wrap.innerHTML = `
    <h5>${user.username} - Yetki Duzenle</h5>
    <div style="display:grid;gap:.45rem;margin-bottom:.7rem;">${options}</div>
    <div class="actions">
      <button class="btn btn-primary" id="savePermBtn">Kaydet</button>
      <button class="btn btn-ghost" id="cancelPermBtn">Vazgec</button>
    </div>
  `;

  const area = dom.permissionAdminArea.querySelector(".admin-grid");
  area.prepend(wrap);

  wrap.querySelector("#cancelPermBtn").addEventListener("click", () => wrap.remove());
  wrap.querySelector("#savePermBtn").addEventListener("click", async () => {
    const next = buildPermissions(false);

    wrap.querySelectorAll("input[type='checkbox']").forEach((cb) => {
      next[cb.dataset.key] = cb.checked;
    });

    const hasAny = Object.entries(next).some(
      ([menuKey, allowed]) => !ADMIN_ONLY_MENUS.has(menuKey) && Boolean(allowed)
    );

    if (!hasAny) {
      alert("En az bir menu yetkisi verilmeli.");
      return;
    }

    await updateUser(userId, { permissions: next });
    wrap.remove();
  });
}

// Bir kullanicinin aktiflik durumunu HTML rozetine cevir (Giris Kayitlari ozeti icin).
function activityBadgeHtml(lastSeen) {
  const st = activityStatus(lastSeen);
  if (st.online) return '<span class="status-badge status-active">Şu an aktif</span>';
  if (st.muted) return '<span class="subtle">Hiç giriş yapmadı</span>';
  return `<span class="status-badge status-passive">${st.text}</span>`;
}

async function renderLoginLogs() {
  if (state.loginLogsTimer) { clearInterval(state.loginLogsTimer); state.loginLogsTimer = null; }
  if (!state.currentUser?.isAdmin) {
    dom.loginLogsArea.innerHTML = '<p class="subtle">Bu bolum sadece admin tarafindan gorulebilir.</p>';
    return;
  }

  try {
    // Aktiflik ozeti + giris kayitlarini birlikte cek.
    const [usersRes, logsRes] = await Promise.all([
      apiFetch("/api/admin/users").catch(() => ({ users: [] })),
      apiFetch("/api/admin/logs").catch(() => ({ logs: [] })),
    ]);

    // KULLANICI AKTIFLIGI — TUM kullanicilar (admin dahil), en son aktif olan ustte.
    const users = (usersRes.users || []).slice().sort((a, b) => {
      const da = parseTrStamp(a.lastSeen); const db2 = parseTrStamp(b.lastSeen);
      const ta = da ? da.getTime() : -1; const tb = db2 ? db2.getTime() : -1;
      return tb - ta;
    });
    const activityRows = users.map((u) => {
      const self = u.id === state.currentUser?.id ? ' <span class="subtle">(sen)</span>' : "";
      return `<tr data-activity-id="${u.id}">
        <td>${escapeHtml(u.username)}${self}</td>
        <td class="activity-badge">${activityBadgeHtml(u.lastSeen)}</td>
        <td class="subtle">${u.lastSeen ? escapeHtml(u.lastSeen) : "—"}</td>
      </tr>`;
    }).join("");
    const activityCard = users.length ? `
      <div class="admin-card" style="margin-bottom:1rem;">
        <h5>Kullanıcı Aktifliği <span class="subtle" style="font-weight:400;">(kim şu an aktif / en son ne zaman)</span></h5>
        <table class="data-table">
          <thead><tr><th>Kullanıcı</th><th>Durum</th><th>Son Görülme</th></tr></thead>
          <tbody>${activityRows}</tbody>
        </table>
      </div>` : "";

    // GIRIS KAYITLARI
    const logs = logsRes.logs || [];
    const logRows = logs
      .map((log) => {
        const ok = log.status === "ok";
        const statusText = ok ? "Basarili" : "Basarisiz";
        const statusClass = ok ? "log-ok" : "log-fail";
        const reason = log.reason ? ` (${log.reason})` : "";
        return `<tr><td>${log.time}</td><td>${log.username}</td><td class="${statusClass}">${statusText}${reason}</td><td>${log.ip || "-"}</td></tr>`;
      })
      .join("");
    const logsTable = logs.length ? `
      <table class="data-table">
        <thead><tr><th>Zaman</th><th>Kullanici</th><th>Durum</th><th>IP</th></tr></thead>
        <tbody>${logRows}</tbody>
      </table>` : '<p class="subtle">Henuz giris kaydi yok.</p>';

    dom.loginLogsArea.innerHTML = activityCard + logsTable;

    // Aktiflik durumunu 20 sn'de bir canli tut (panel gorunurken).
    state.loginLogsTimer = setInterval(refreshLoginActivity, 20000);
  } catch (error) {
    dom.loginLogsArea.innerHTML = `<p class="subtle">${error.message}</p>`;
  }
}

// Aktiflik rozetlerini arka planda tazele (tam re-render yapmadan). Panel gizliyse durdur.
async function refreshLoginActivity() {
  const panel = document.getElementById("panelLogs");
  if (!panel || panel.classList.contains("hidden")) {
    if (state.loginLogsTimer) { clearInterval(state.loginLogsTimer); state.loginLogsTimer = null; }
    return;
  }
  try {
    const res = await apiFetch("/api/admin/users");
    const map = new Map((res.users || []).map((u) => [String(u.id), u.lastSeen]));
    dom.loginLogsArea.querySelectorAll("tr[data-activity-id]").forEach((tr) => {
      const badge = tr.querySelector(".activity-badge");
      if (badge && map.has(tr.dataset.activityId)) badge.innerHTML = activityBadgeHtml(map.get(tr.dataset.activityId));
    });
  } catch (e) { /* sessiz */ }
}

// "Beni hatirla" — kullanici adi local storage'da saklanir (sifre ASLA saklanmaz).
const REMEMBERED_USER_KEY = "bus_remembered_username";

// Sayfa acilinca eski kullanici adini doldur (varsa)
(() => {
  try {
    const remembered = localStorage.getItem(REMEMBERED_USER_KEY);
    if (remembered && dom.loginUsername && !dom.loginUsername.value) {
      dom.loginUsername.value = remembered;
      if (dom.loginRemember) dom.loginRemember.checked = true;
      // Imleci sifre alanina otomatik tasi — UX
      if (dom.loginPassword) setTimeout(() => dom.loginPassword.focus(), 100);
    }
  } catch { /* localStorage devre disi olabilir */ }
})();

dom.loginForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const username = dom.loginUsername.value.trim();
  const password = dom.loginPassword.value.trim();
  // Beni hatirla isaretliyse kullanici adini sakla; isaretli degilse sil.
  try {
    if (dom.loginRemember?.checked) {
      localStorage.setItem(REMEMBERED_USER_KEY, username);
    } else {
      localStorage.removeItem(REMEMBERED_USER_KEY);
    }
  } catch { /* sessiz gec */ }
  await handleLogin(username, password);
});

// Parola goster/gizle toggle
if (dom.loginPasswordToggle && dom.loginPassword) {
  dom.loginPasswordToggle.addEventListener("click", () => {
    const isPwd = dom.loginPassword.type === "password";
    dom.loginPassword.type = isPwd ? "text" : "password";
    dom.loginPasswordToggle.setAttribute(
      "aria-label",
      isPwd ? "Hide password" : "Show password"
    );
    const eyeOn = dom.loginPasswordToggle.querySelector(".pw-eye-on");
    const eyeOff = dom.loginPasswordToggle.querySelector(".pw-eye-off");
    if (eyeOn && eyeOff) {
      eyeOn.hidden = isPwd;
      eyeOff.hidden = !isPwd;
    }
  });
}

dom.logoutBtn.addEventListener("click", async () => {
  await handleLogout();
});

// ===== Hesap Ayarlari modali (kullanici kendi kullanici adi + sifresini degistirir) =====
function setupAccountModal() {
  const btn = document.getElementById("accountSettingsBtn");
  const backdrop = document.getElementById("accountModalBackdrop");
  const form = document.getElementById("accountForm");
  if (!btn || !backdrop || !form) return;
  const closeBtn = document.getElementById("accountModalClose");
  const cancelBtn = document.getElementById("accountCancelBtn");
  const msg = document.getElementById("accountMsg");
  const newU = document.getElementById("accountNewUsername");
  const newP = document.getElementById("accountNewPassword");
  const newP2 = document.getElementById("accountNewPassword2");
  const curP = document.getElementById("accountCurrentPassword");

  const open = () => {
    form.reset();
    if (msg) { msg.textContent = ""; msg.style.color = ""; }
    if (newU) newU.placeholder = state.currentUser?.username ? `Mevcut: ${state.currentUser.username}` : "Boş bırakırsan değişmez";
    backdrop.classList.remove("hidden");
    setTimeout(() => curP?.focus(), 50);
  };
  const close = () => backdrop.classList.add("hidden");

  btn.addEventListener("click", open);
  if (closeBtn) closeBtn.addEventListener("click", close);
  if (cancelBtn) cancelBtn.addEventListener("click", close);
  backdrop.addEventListener("click", (e) => { if (e.target === backdrop) close(); });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && !backdrop.classList.contains("hidden")) close();
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const currentPassword = curP ? curP.value : "";
    const newUsername = (newU ? newU.value : "").trim();
    const newPassword = newP ? newP.value : "";
    const confirmPw = newP2 ? newP2.value : "";
    const setErr = (t) => { if (msg) { msg.style.color = "#d64545"; msg.textContent = t; } };

    if (!newUsername && !newPassword) return setErr("Yeni kullanıcı adı veya şifre girin.");
    if (newPassword && newPassword !== confirmPw) return setErr("Yeni şifreler eşleşmiyor.");
    if (!currentPassword) return setErr("Mevcut şifrenizi girin.");

    const submitBtn = form.querySelector('button[type="submit"]');
    const oldLabel = submitBtn ? submitBtn.textContent : "";
    if (submitBtn) { submitBtn.disabled = true; submitBtn.textContent = "Kaydediliyor..."; }
    try {
      const payload = { currentPassword };
      if (newUsername) payload.newUsername = newUsername;
      if (newPassword) payload.newPassword = newPassword;
      const r = await apiFetch("/api/account", { method: "PATCH", body: JSON.stringify(payload) });
      if (r.user) { state.currentUser = r.user; renderSidebarUserCard(); }
      if (msg) {
        msg.style.color = "#1f7a1f";
        const parts = [];
        if (r.usernameChanged) parts.push("Kullanıcı adı");
        if (r.passwordChanged) parts.push("şifre");
        msg.textContent = `${parts.join(" ve ")} güncellendi.`;
      }
      setTimeout(close, 1300);
    } catch (err) {
      setErr(err.message || "Güncelleme başarısız.");
    } finally {
      if (submitBtn) { submitBtn.disabled = false; submitBtn.textContent = oldLabel || "Kaydet"; }
    }
  });
}
setupAccountModal();

// Dashboard "Tümünü Gör" linki - bildirim panelini aç
if (dom.dashAllNotifsLink) {
  dom.dashAllNotifsLink.addEventListener("click", (e) => {
    e.preventDefault();
    if (dom.notifBtn) dom.notifBtn.click();
  });
}

dom.notifBtn.addEventListener("click", async () => {
  const willOpen = dom.notifPanel.classList.contains("hidden");
  dom.notifPanel.classList.toggle("hidden", !willOpen);

  if (willOpen) {
    try {
      await apiFetch("/api/notifications/read-all", { method: "POST" });
      await refreshNotificationsData();
    } catch {
      // Bildirim okunduya alma basarisiz olursa panel yine acik kalir.
    }
  }
});

dom.notifReadAllBtn.addEventListener("click", async () => {
  try {
    await apiFetch("/api/notifications/read-all", { method: "POST" });
    await refreshNotificationsData();
  } catch {
    // Sessiz gec.
  }
});

// Bildirim sesi aç/kapat (tercih localStorage'da kalıcı).
function updateNotifSoundBtn() {
  if (!dom.notifSoundBtn) return;
  dom.notifSoundBtn.textContent = isNotifMuted() ? "Ses: Kapalı" : "Ses: Açık";
}
if (dom.notifSoundBtn) {
  updateNotifSoundBtn();
  dom.notifSoundBtn.addEventListener("click", () => {
    const muted = isNotifMuted();
    localStorage.setItem("notifSoundMuted", muted ? "0" : "1");
    updateNotifSoundBtn();
    // Az önce sessizden açıldıysa kısa bir örnek ses çal (butona tıklamak zaten etkileşim sayılır).
    if (muted) playNotifSound();
  });
}

dom.themeToggleBtn.addEventListener("click", () => {
  toggleTheme();
});

function scheduleTariffSearch() {
  if (!dom.tariffSearchInput) {
    return;
  }

  if (state.tariffSearchTimer) {
    clearTimeout(state.tariffSearchTimer);
  }

  state.tariffSearchTimer = setTimeout(() => {
    refreshTariffData(dom.tariffSearchInput.value || "", false).catch(() => null);
  }, 220);
}

if (dom.tariffSearchInput) {
  dom.tariffSearchInput.addEventListener("input", () => {
    scheduleTariffSearch();
  });

  dom.tariffSearchInput.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter") {
      return;
    }
    event.preventDefault();
    if (state.tariffSearchTimer) {
      clearTimeout(state.tariffSearchTimer);
    }
    await refreshTariffData(dom.tariffSearchInput.value || "", false);
  });
}

if (dom.tariffResetBtn) {
  dom.tariffResetBtn.addEventListener("click", async () => {
    if (dom.tariffSearchInput) {
      dom.tariffSearchInput.value = "";
    }
    await refreshTariffData("", false);
  });
}

if (dom.tariffLoadMoreBtn) {
  dom.tariffLoadMoreBtn.addEventListener("click", async () => {
    if (state.tariffOffset >= state.tariffTotal) {
      return;
    }
    await refreshTariffData(state.tariffQuery, true);
  });
}

if (dom.reportSearchInput) {
  dom.reportSearchInput.addEventListener("input", () => {
    state.reportQuery = dom.reportSearchInput.value || "";
    renderReportsPanel();
  });
}

if (dom.reportSearchResetBtn) {
  dom.reportSearchResetBtn.addEventListener("click", () => {
    state.reportQuery = "";
    if (dom.reportSearchInput) {
      dom.reportSearchInput.value = "";
    }
    renderReportsPanel();
  });
}

if (dom.reportingDateInput) {
  state.reportingDate = dom.reportingDateInput.value || todayIsoDate();
  dom.reportingDateInput.value = state.reportingDate;

  dom.reportingDateInput.addEventListener("change", () => {
    state.reportingDate = dom.reportingDateInput.value || todayIsoDate();
    refreshReportingData().catch(() => null);
  });
}

if (dom.reportingOriginInput) {
  state.reportingOrigin = dom.reportingOriginInput.value || "Siirt";
  dom.reportingOriginInput.addEventListener("change", () => {
    state.reportingOrigin = String(dom.reportingOriginInput.value || "Siirt").trim() || "Siirt";
    refreshReportingData().catch((error) => {
      if (dom.reportingSummary) {
        dom.reportingSummary.textContent = error.message || "Kalkis sehri icin veri yuklenemedi.";
      }
    });
  });
}

if (dom.reportingFilterInput) {
  dom.reportingFilterInput.addEventListener("input", () => {
    state.reportingQuery = dom.reportingFilterInput.value || "";
    renderReportingPanel();
  });
}

if (dom.reportingRefreshBtn) {
  dom.reportingRefreshBtn.addEventListener("click", () => {
    refreshReportingData().catch(() => null);
  });
}

if (dom.reportingSyncBtn) {
  dom.reportingSyncBtn.addEventListener("click", () => {
    syncReportingData().catch((error) => {
      if (dom.reportingSummary) {
        dom.reportingSummary.textContent = error.message || "Rapor senkronu basarisiz.";
      }
    });
  });
}

if (dom.reportingPlateDebugBtn) {
  dom.reportingPlateDebugBtn.addEventListener("click", () => {
    debugPlateByRideUuid().catch((error) => {
      if (dom.reportingPlateDebugMsg) {
        dom.reportingPlateDebugMsg.style.color = "#d64545";
        dom.reportingPlateDebugMsg.textContent = error.message || "UUID plaka testi basarisiz.";
      }
    });
  });
}

if (dom.controlOpenLoginBtn) {
  dom.controlOpenLoginBtn.addEventListener("click", () => {
    const url = (dom.controlLoginUrlInput?.value || "https://app.oneops.flixbus.com/users/login").trim();
    window.open(url, "_blank", "noopener");
  });
}

if (dom.controlOpenOpsBtn) {
  dom.controlOpenOpsBtn.addEventListener("click", () => {
    window.open("https://app.oneops.flixbus.com/ops-portal/", "_blank", "noopener");
  });
}

if (dom.controlSaveBtn) {
  dom.controlSaveBtn.addEventListener("click", () => {
    saveControlIntegrationData().catch((error) => {
      if (dom.controlStatusMsg) {
        dom.controlStatusMsg.style.color = "#d64545";
        dom.controlStatusMsg.textContent = error.message || "OneOps bilgileri kaydedilemedi.";
      }
    });
  });
}

if (dom.controlTestBtn) {
  dom.controlTestBtn.addEventListener("click", () => {
    if (dom.controlStatusMsg) {
      dom.controlStatusMsg.style.color = "var(--muted)";
      dom.controlStatusMsg.textContent = "OneOps oturumu test ediliyor...";
    }
    testControlIntegrationSession().catch((error) => {
      if (dom.controlStatusMsg) {
        dom.controlStatusMsg.style.color = "#d64545";
        dom.controlStatusMsg.textContent = error.message || "OneOps oturum testi basarisiz.";
      }
    });
  });
}

if (dom.controlReloadBtn) {
  dom.controlReloadBtn.addEventListener("click", () => {
    renderControlIntegrationPanel()
      .then(() => testControlIntegrationSession())
      .catch(() => null);
  });
}

if (dom.pricingUploadForm) {
  dom.pricingUploadForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (dom.pricingUploadMsg) {
      dom.pricingUploadMsg.style.color = "var(--muted)";
      dom.pricingUploadMsg.textContent = "Excel okunuyor ve kontrol ediliyor...";
    }

    try {
      await submitPricingUpload();
      dom.pricingUploadForm.reset();
    } catch (error) {
      const rejected = error?.payload?.rejected || [];
      renderPricingRejectedRows(rejected);
      if (dom.pricingUploadMsg) {
        dom.pricingUploadMsg.style.color = "#d64545";
        dom.pricingUploadMsg.textContent = error.message || "Fiyat yukleme basarisiz.";
      }
    }
  });
}

if (dom.ocrScanBtn) {
  dom.ocrScanBtn.addEventListener("click", () => {
    runOcrAndBuildTable();
  });
}

if (dom.ocrCopyBtn) {
  dom.ocrCopyBtn.addEventListener("click", () => {
    copyOcrOutput();
  });
}

if (dom.ocrApplyReviewBtn) {
  dom.ocrApplyReviewBtn.addEventListener("click", () => {
    applyReviewedRowsToOutput();
  });
}

applyTheme(state.theme);
verifySession();

window.deleteError = async function(id) {
  if (!confirm("Bu hatali islemi silmek istediginize emin misiniz?")) return;
  try {
    await apiFetch(`/api/error-reports/${id}`, { method: "DELETE" });
    renderErrorList();
  } catch (err) {
    alert("Silinemedi: " + err.message);
  }
};

window.updateErrorStatus = async function(id, status) {
  try {
    await apiFetch(`/api/error-reports/${id}`, {
      method: "PATCH",
      body: JSON.stringify({ status })
    });
    renderErrorList();
  } catch (err) {
    alert("Durum guncellenemedi: " + err.message);
  }
};

async function renderErrorList() {
  if (!dom.errorListArea) return;
  try {
    const data = await apiFetch("/api/error-reports");
    let errors = data.errors || [];
    
    const query = dom.errorSearchInput?.value.trim().toLowerCase() || "";
    if (query) {
      errors = errors.filter(err => 
        err.desc.toLowerCase().includes(query) || 
        err.user.toLowerCase().includes(query)
      );
    }

    if (!errors.length) {
      dom.errorListArea.innerHTML = '<p class="subtle">Henuz yuklenmis veya kriterlere uygun bir hatali islem bulunmuyor.</p>';
      return;
    }
    
    dom.errorListArea.innerHTML = errors.map(err => {
      const photosHtml = Array.isArray(err.photos) 
        ? err.photos.map(p => `<div class="error-photo-wrap"><img src="${p}" alt="Foto" onclick="window.open().document.write('<img src=\\'${p}\\' style=\\'max-width:100%\\'>')" /></div>`).join("")
        : "";
      
      const priorityClass = `priority-${(err.priority || "Orta").toLowerCase()}`;
      const statusClass = `status-${(err.status || "Yeni").toLowerCase().replace(/\s/g, '-')}`;

      return `
        <div class="error-card fade-up">
          <div class="error-card-header">
            <div class="error-card-user-info">
              <div class="user-avatar">${(err.user || "B").charAt(0).toUpperCase()}</div>
              <div>
                <span class="error-card-title">${err.user || "Bilinmiyor"}</span>
                <span class="error-card-meta">${new Date(err.date).toLocaleString("tr-TR")}</span>
              </div>
            </div>
            <div class="error-card-badges">
              <button type="button" class="btn btn-small btn-ghost" style="color:#d64545; border:none;" onclick="window.deleteError(${err.id})">Sil</button>
            </div>
          </div>
          <div class="error-card-desc">${err.desc}</div>
          <div class="error-photos">${photosHtml}</div>
        </div>
      `;
    }).join("");
  } catch (err) {
    dom.errorListArea.innerHTML = '<p class="subtle" style="color:#d64545;">Hatalar yuklenemedi.</p>';
  }
}

if (dom.errorSearchInput) {
  dom.errorSearchInput.addEventListener("input", () => {
    renderErrorList();
  });
}

if (dom.errorReportForm) {
  dom.errorReportForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const desc = dom.errorDescInput?.value.trim() || "";
    const user = dom.errorUserInput?.value.trim() || "";
    const files = dom.errorPhotosInput?.files || [];
    
    if (!desc || !user || !files.length) return;
    
    if (dom.errorUploadMsg) {
      dom.errorUploadMsg.style.color = "var(--muted)";
      dom.errorUploadMsg.textContent = "Fotograflar isleniyor, lutfen bekleyin...";
    }
    
    try {
      const photos = [];
      for (const file of files) {
        const base64 = await new Promise((resolve, reject) => {
          const reader = new FileReader();
          reader.onload = () => resolve(reader.result);
          reader.onerror = reject;
          reader.readAsDataURL(file);
        });
        photos.push(base64);
      }
      
      await apiFetch("/api/error-reports", {
        method: "POST",
        body: JSON.stringify({
          desc,
          user,
          priority: dom.errorPriorityInput?.value || "Orta",
          photos
        })
      });
      
      dom.errorReportForm.reset();
      if (dom.errorUploadMsg) {
        dom.errorUploadMsg.style.color = "#1f7a1f";
        dom.errorUploadMsg.textContent = "Gonderim basarili!";
      }
      
      renderErrorList();
      setTimeout(() => {
        if (dom.errorUploadMsg && dom.errorUploadMsg.textContent === "Gonderim basarili!") {
          dom.errorUploadMsg.textContent = "";
        }
      }, 3000);
    } catch(err) {
      if (dom.errorUploadMsg) {
        dom.errorUploadMsg.style.color = "#d64545";
        dom.errorUploadMsg.textContent = "Yukleme sirasinda hata olustu.";
      }
    }
  });
}

// ============================================
// oBILET TAKİP PANELİ - Frontend Fonksiyonları
// ============================================

const obiletState = {
  targets: [],
  expandedTargetId: null,
  operatorCatalog: [],
};

function parseObiletOperatorsCsv(raw) {
  return String(raw || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeObiletOperatorName(raw) {
  return toTurkishTitleCase(String(raw || "").replace(/\s+/g, " ").trim());
}


async function loadObiletOperatorCatalog() {
  if (obiletState.operatorCatalog.length) {
    return obiletState.operatorCatalog;
  }

  const result = await apiFetch("/api/obilet/operators");
  obiletState.operatorCatalog = (result.operators || [])
    .map(normalizeObiletOperatorName)
    .filter(Boolean);
  return obiletState.operatorCatalog;
}

// Firma logosu / avatar yardimcilari
// Dosya adi normalize: "Kamil Koç" -> "kamil-koc"
function firmaLogoSlug(name) {
  return String(name || "")
    .toLocaleLowerCase("tr-TR")
    .replace(/ı/g, "i").replace(/ğ/g, "g").replace(/ü/g, "u")
    .replace(/ş/g, "s").replace(/ö/g, "o").replace(/ç/g, "c")
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function firmaInitials(name) {
  const words = String(name || "")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(w => w && !/^(turizm|seyahat|otobus|nakliyat|vip|tur)$/i.test(w));
  if (!words.length) return "?";
  if (words.length === 1) return words[0].slice(0, 2).toLocaleUpperCase("tr-TR");
  return (words[0][0] + words[1][0]).toLocaleUpperCase("tr-TR");
}

function firmaAvatarColor(name) {
  // Firma adindan hash uret, sabit bir renk paletinden sec.
  // Renkler kasten karanlik tonlarda secildi (panel temasiyla uyumlu).
  const palette = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#3b5998", "#c0392b", "#16a085", "#8e44ad", "#d35400",
  ];
  let hash = 0;
  for (let i = 0; i < (name || "").length; i++) {
    hash = (hash * 31 + name.charCodeAt(i)) | 0;
  }
  return palette[Math.abs(hash) % palette.length];
}

function setupObiletOperatorPicker(root) {
  const hiddenInput = root.querySelector(".obilet-operators-hidden");
  const searchInput = root.querySelector(".obilet-operator-search");
  const optionsEl = root.querySelector(".obilet-operator-options");
  const selectedEl = root.querySelector(".obilet-operator-selected");
  const selectAllBtn = root.querySelector(".obilet-operator-select-all");
  const clearBtn = root.querySelector(".obilet-operator-clear");

  if (!hiddenInput || !searchInput || !optionsEl || !selectedEl || !selectAllBtn || !clearBtn) {
    return { clear: () => null };
  }

  const selected = new Set(
    parseObiletOperatorsCsv(hiddenInput.value).map(normalizeObiletOperatorName).filter(Boolean)
  );
  let allOperators = [];

  const syncHiddenInput = () => {
    hiddenInput.value = Array.from(selected).join(", ");
  };

  const renderSelected = () => {
    selectedEl.innerHTML = "";
    if (!selected.size) {
      selectedEl.textContent = "Firma seçiniz...";
      selectedEl.classList.add("is-placeholder");
      return;
    }
    selectedEl.classList.remove("is-placeholder");

    Array.from(selected)
      .sort((a, b) => a.localeCompare(b, "tr-TR"))
      .forEach((name) => {
        const chip = document.createElement("span");
        chip.className = "obilet-operator-chip";
        const label = document.createElement("span");
        label.textContent = name;
        chip.appendChild(label);
        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "obilet-operator-chip-remove";
        remove.setAttribute("aria-label", `${name} firmasını kaldır`);
        remove.textContent = "×";
        remove.addEventListener("click", (e) => {
          e.stopPropagation();
          selected.delete(name);
          syncHiddenInput();
          renderSelected();
          renderOptions(searchInput.value);
        });
        chip.appendChild(remove);
        selectedEl.appendChild(chip);
      });
  };

  const renderOptions = (query = "") => {
    const normalizedQuery = normalizeSearchText(query || "");
    const filtered = allOperators.filter((name) =>
      normalizeSearchText(name).includes(normalizedQuery)
    );

    optionsEl.innerHTML = "";
    if (!filtered.length) {
      const empty = document.createElement("div");
      empty.className = "obilet-empty-sm";
      empty.textContent = "Aramaya uygun firma bulunamadi.";
      optionsEl.appendChild(empty);
      return;
    }

    filtered.forEach((name) => {
      const label = document.createElement("label");
      label.className = "obilet-operator-option";

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = selected.has(name);
      checkbox.addEventListener("change", () => {
        if (checkbox.checked) {
          selected.add(name);
        } else {
          selected.delete(name);
        }
        syncHiddenInput();
        renderSelected();
      });

      // Logo / amblem alani — once gercek logo dene; tum yaygın uzantilari sirayla dene
      // (.png, .jpg, .jpeg, .webp, .svg). Hicbiri bulamazsa firma adinin ilk
      // harflerinden renkli avatar fallback.
      const avatar = document.createElement("span");
      avatar.className = "firma-avatar";
      const slug = firmaLogoSlug(name);
      const initials = firmaInitials(name);
      const bg = firmaAvatarColor(name);
      avatar.style.background = bg;

      const extensions = ["png", "jpg", "jpeg", "webp", "svg"];
      let extIdx = 0;
      const img = document.createElement("img");
      img.src = `/firma-logolari/${slug}.${extensions[extIdx]}`;
      img.alt = "";
      img.loading = "lazy";
      img.addEventListener("error", () => {
        extIdx++;
        if (extIdx < extensions.length) {
          img.src = `/firma-logolari/${slug}.${extensions[extIdx]}`;
        } else {
          img.remove();
          avatar.textContent = initials;
          avatar.classList.add("firma-avatar-fallback");
        }
      });
      avatar.appendChild(img);

      const text = document.createElement("span");
      text.className = "firma-option-name";
      text.textContent = name;

      label.appendChild(checkbox);
      label.appendChild(avatar);
      label.appendChild(text);
      optionsEl.appendChild(label);
    });
  };

  searchInput.addEventListener("input", () => {
    renderOptions(searchInput.value);
  });

  selectAllBtn.addEventListener("click", () => {
    const normalizedQuery = normalizeSearchText(searchInput.value || "");
    allOperators.forEach((name) => {
      if (normalizeSearchText(name).includes(normalizedQuery)) {
        selected.add(name);
      }
    });
    syncHiddenInput();
    renderSelected();
    renderOptions(searchInput.value);
  });

  clearBtn.addEventListener("click", () => {
    selected.clear();
    syncHiddenInput();
    renderSelected();
    renderOptions(searchInput.value);
  });

  // Acilir/kapanir davranis (varsayilan KAPALI). Trigger -> ac/kapa; disari tikla/Escape -> kapat.
  const pickerEl = root.querySelector(".obilet-operator-picker");
  const triggerEl = root.querySelector(".obilet-operator-trigger");
  if (pickerEl && triggerEl) {
    const closePicker = () => {
      pickerEl.classList.remove("open");
      triggerEl.setAttribute("aria-expanded", "false");
    };
    const openPicker = () => {
      pickerEl.classList.add("open");
      triggerEl.setAttribute("aria-expanded", "true");
      setTimeout(() => searchInput.focus(), 0);
    };
    triggerEl.addEventListener("click", (e) => {
      if (e.target.closest(".obilet-operator-chip-remove")) return; // chip silme, toggle etmesin
      if (pickerEl.classList.contains("open")) closePicker();
      else openPicker();
    });
    triggerEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); triggerEl.click(); }
      else if (e.key === "Escape") closePicker();
    });
    document.addEventListener("click", (e) => {
      if (!pickerEl.contains(e.target)) closePicker();
    });
  }

  const boot = async () => {
    try {
      allOperators = await loadObiletOperatorCatalog();
      renderSelected();
      renderOptions("");
      syncHiddenInput();
    } catch (error) {
      optionsEl.innerHTML = `<div class="obilet-empty-sm">Firma listesi alinamadi: ${error.message}</div>`;
    }
  };

  boot().catch(() => null);

  return {
    clear: () => {
      selected.clear();
      syncHiddenInput();
      renderSelected();
      renderOptions(searchInput.value);
    },
  };
}

async function initObiletPanel() {
  await renderObiletTargets();
  setupObiletForm();
  const bulkBtn = document.getElementById("obiletBulkDatesBtn");
  if (bulkBtn && !bulkBtn.dataset.wired) {
    bulkBtn.dataset.wired = "1";
    bulkBtn.addEventListener("click", openBulkDatesModal);
  }
}

function openBulkDatesModal() {
  document.getElementById("obiletBulkBackdrop")?.remove();
  const targets = obiletState.targets || [];
  // Varsayilan: ilk hattin mevcut tarihleri, yoksa bugun.
  const first = targets[0] || {};
  const today = new Date().toISOString().slice(0, 10);
  const defStart = first.date || today;
  const defEnd = first.end_date || first.date || today;
  const backdrop = document.createElement("div");
  backdrop.id = "obiletBulkBackdrop";
  backdrop.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;z-index:9999;padding:1rem;";
  backdrop.innerHTML = `
    <div style="background:#1c2530;border:1px solid rgba(255,255,255,0.12);border-radius:14px;max-width:460px;width:100%;padding:1.4rem;">
      <h4 style="margin:0 0 0.6rem;">Toplu Tarih Güncelle</h4>
      <p class="subtle" style="margin:0 0 1rem;">Aşağıdaki tarih aralığı <b>tüm aktif hatlara</b> uygulanacak (${targets.length} hat).</p>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.8rem;">
        <label style="display:flex;flex-direction:column;gap:0.3rem;"><span>Başlangıç</span><input id="obDateStart" type="date" value="${defStart}" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;"><span>Bitiş</span><input id="obDateEnd" type="date" value="${defEnd}" /></label>
      </div>
      <p id="obBulkMsg" style="min-height:1.2em;margin:0.7rem 0;"></p>
      <div style="display:flex;gap:0.6rem;justify-content:flex-end;">
        <button id="obBulkCancel" class="btn btn-ghost" type="button">İptal</button>
        <button id="obBulkSave" class="btn btn-primary" type="button">Tümüne Uygula</button>
      </div>
    </div>`;
  document.body.appendChild(backdrop);
  const close = () => backdrop.remove();
  backdrop.addEventListener("click", (e) => { if (e.target === backdrop) close(); });
  document.getElementById("obBulkCancel").addEventListener("click", close);
  document.getElementById("obBulkSave").addEventListener("click", async () => {
    const date = document.getElementById("obDateStart").value;
    const endDate = document.getElementById("obDateEnd").value;
    const msg = document.getElementById("obBulkMsg");
    if (!date || !endDate) { msg.style.color = "#e0796f"; msg.textContent = "Her iki tarihi de gir."; return; }
    if (!confirm(`Tüm hatların tarihi ${date} - ${endDate} olarak güncellenecek. Onaylıyor musun?`)) return;
    msg.style.color = "#e0796f";
    msg.textContent = "Güncelleniyor...";
    try {
      const r = await apiFetch("/api/obilet/targets/bulk-dates", { method: "POST", body: JSON.stringify({ date, endDate }) });
      msg.style.color = "#27ae60";
      msg.textContent = `${r.updated} hat güncellendi. Fiyatlar arka planda çekiliyor...`;
      setTimeout(async () => { close(); await renderObiletTargets(); }, 800);
    } catch (err) {
      msg.style.color = "#e0796f";
      msg.textContent = "Hata: " + err.message;
    }
  });
}

// CSV formatına çevir (Excel'de açılabilir - Türkiye için noktalı virgül delimiter)
function generateCSV(prices, target) {
  const headers = ["Tarih", "Firma", "Kalkış Saati", "Kalkış Durağı", "Varış Durağı", "Fiyat (TL)", "Son Güncelleme"];
  const rows = prices.map(p => {
    const journeyDate = p.journey_date || "-";
    const operator = p.operator || "-";
    const departureTime = p.departure_time || "-";
    const departureStop = p.departure_stop || "-";
    const arrivalStop = p.arrival_stop || "-";
    const price = p.price || "-";
    const lastUpdated = p.last_updated ? new Date(p.last_updated).toLocaleString("tr-TR") : "-";
    
    // Türkiye'de Excel noktalı virgül (;) delimiter kullanır
    return [journeyDate, operator, departureTime, departureStop, arrivalStop, price, lastUpdated]
      .map(field => String(field).replace(/;/g, ",")) // Noktalı virgülleri virgüle çevir
      .join(";");
  });
  
  return [headers.join(";"), ...rows].join("\n");
}

// ==========================================
// FIYAT DEGISIKLIK RAPORU (Raporlama menusu)
// ==========================================

const priceHistoryState = {
  rows: [],
  lastQuery: null,
  sortKey: "changed_at",
  sortDir: "desc",
  targets: [],
  targetsById: {},
  total: 0,
  offset: 0,
  limit: 1000,
};

async function loadPriceHistory(append = false) {
  if (!dom.phTableBody) return;
  if (dom.phStatusMsg) dom.phStatusMsg.textContent = append ? "Daha fazla yükleniyor..." : "Veri yükleniyor...";

  if (!append) {
    priceHistoryState.offset = 0;
    priceHistoryState.rows = [];
  }

  const params = new URLSearchParams();
  const targetId = dom.phTargetSelect?.value || "";
  const from = dom.phFromDate?.value || "";
  const to = dom.phToDate?.value || "";
  const search = dom.phSearchInput?.value.trim() || "";
  if (targetId) params.set("targetId", targetId);
  if (from) params.set("from", from);
  if (to) params.set("to", to);
  if (search) params.set("search", search);
  params.set("limit", String(priceHistoryState.limit));
  params.set("offset", String(priceHistoryState.offset));

  try {
    const data = await apiFetch(`/api/obilet/price-history?${params.toString()}`);
    const incoming = Array.isArray(data?.history) ? data.history : [];
    priceHistoryState.rows = append ? [...priceHistoryState.rows, ...incoming] : incoming;
    priceHistoryState.total = data?.total ?? priceHistoryState.rows.length;
    priceHistoryState.offset += incoming.length;
    renderPriceHistory();
    if (dom.phStatusMsg) {
      if (priceHistoryState.rows.length === 0) {
        dom.phStatusMsg.textContent = "Filtreye uygun kayıt yok.";
      } else {
        const remaining = priceHistoryState.total - priceHistoryState.rows.length;
        dom.phStatusMsg.textContent = `${priceHistoryState.rows.length} / ${priceHistoryState.total} kayıt gösteriliyor.${remaining > 0 ? ` (${remaining} kayıt daha var)` : ""}`;
      }
    }
    renderPhLoadMoreBtn();
  } catch (err) {
    if (dom.phStatusMsg) dom.phStatusMsg.textContent = `Hata: ${err.message}`;
  }
}

function renderPhLoadMoreBtn() {
  const existing = document.getElementById("phLoadMoreBtn");
  const hasMore = priceHistoryState.rows.length < priceHistoryState.total;
  if (existing) existing.remove();
  if (!hasMore || !dom.phTableBody) return;
  const wrap = document.createElement("div");
  wrap.style.cssText = "margin-top:.75rem;display:flex;gap:.5rem;align-items:center;";
  const remaining = priceHistoryState.total - priceHistoryState.rows.length;
  wrap.innerHTML = `<button id="phLoadMoreBtn" class="btn btn-ghost" type="button">+ ${Math.min(remaining, priceHistoryState.limit)} kayıt daha yükle</button><span class="subtle" style="font-size:.85rem;">${remaining} kayıt kaldı</span>`;
  dom.phTableBody.closest("table").after(wrap);
  wrap.querySelector("#phLoadMoreBtn").addEventListener("click", () => loadPriceHistory(true));
}

function phEscape(s) {
  return String(s || "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  })[c]);
}

function phToDot(iso) {
  const m = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[3]}.${m[2]}.${m[1]}` : iso || "-";
}

// DD.MM.YYYY HH:mm:ss - sortable timestamp (yaklasik). Backend Istanbul timezone ile yaziyor.
function phChangedAtSortKey(s) {
  const m = String(s || "").match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return 0;
  return new Date(`${m[3]}-${m[2]}-${m[1]}T${m[4]}:${m[5]}:${m[6]}`).getTime();
}

function phSortedRows() {
  const { rows, sortKey, sortDir } = priceHistoryState;
  const dir = sortDir === "asc" ? 1 : -1;
  const arr = rows.slice();
  arr.sort((a, b) => {
    let va, vb;
    switch (sortKey) {
      case "changed_at":
        va = phChangedAtSortKey(a.changed_at); vb = phChangedAtSortKey(b.changed_at); break;
      case "journey_date":
        va = a.journey_date || ""; vb = b.journey_date || ""; break;
      case "departure_time":
        va = a.departure_time || ""; vb = b.departure_time || ""; break;
      case "operator":
        va = (a.operator || "").toLocaleLowerCase("tr-TR"); vb = (b.operator || "").toLocaleLowerCase("tr-TR"); break;
      case "route":
        va = `${a.origin}-${a.destination}`.toLocaleLowerCase("tr-TR");
        vb = `${b.origin}-${b.destination}`.toLocaleLowerCase("tr-TR"); break;
      case "old_price":
        va = Number(a.old_price); vb = Number(b.old_price); break;
      case "new_price":
        va = Number(a.new_price); vb = Number(b.new_price); break;
      case "diff": {
        const da = Math.abs(a.new_price - a.old_price);
        const db = Math.abs(b.new_price - b.old_price);
        va = da; vb = db; break;
      }
      default:
        va = a[sortKey]; vb = b[sortKey];
    }
    if (va < vb) return -1 * dir;
    if (va > vb) return 1 * dir;
    return 0;
  });
  return arr;
}

function renderPriceHistory() {
  if (!dom.phTableBody) return;
  const rows = priceHistoryState.rows;

  // İstatistikler
  let increase = 0, decrease = 0;
  const routeSet = new Set();
  for (const r of rows) {
    if (r.new_price > r.old_price) increase++;
    else if (r.new_price < r.old_price) decrease++;
    routeSet.add(`${r.origin}->${r.destination}`);
  }
  if (dom.phStatTotal) dom.phStatTotal.textContent = rows.length;
  if (dom.phStatIncrease) dom.phStatIncrease.textContent = increase;
  if (dom.phStatDecrease) dom.phStatDecrease.textContent = decrease;
  if (dom.phStatRoutes) dom.phStatRoutes.textContent = routeSet.size;

  renderPriceHistoryRouteCards();
  renderPriceHistorySortIcons();

  if (!rows.length) {
    dom.phTableBody.innerHTML = `<tr><td colspan="10" style="text-align:center; padding:2rem; opacity:0.6;">Filtreye uygun kayıt yok.</td></tr>`;
    return;
  }

  const sorted = phSortedRows();

  dom.phTableBody.innerHTML = sorted.map((r, idx) => {
    const diff = r.new_price - r.old_price;
    const isIncrease = diff > 0;
    const isDecrease = diff < 0;
    // Firma perspektifi: rakibin artisi = YESIL (iyi haber), rakibin dususu = KIRMIZI (rekabet baskisi)
    const color = isIncrease ? "#27ae60" : isDecrease ? "#d32f2f" : "#666";
    const arrow = isIncrease ? "+" : isDecrease ? "-" : "";
    const pct = r.old_price > 0 ? ((diff / r.old_price) * 100).toFixed(1) : "0.0";
    const pctStr = (diff >= 0 ? "+" : "") + pct + "%";
    const badge = isIncrease
      ? `<span style="background:#e8f5e9;color:#1b5e20;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;">ARTIŞ</span>`
      : isDecrease
      ? `<span style="background:#ffe5e5;color:#c0392b;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;">DÜŞÜŞ</span>`
      : `<span style="background:#f0f0f0;color:#666;padding:2px 8px;border-radius:4px;font-size:11px;">—</span>`;

    return `
      <tr class="ph-row" data-idx="${idx}">
        <td style="white-space:nowrap;">${phEscape(r.changed_at)}</td>
        <td><strong>${phEscape((r.origin || "").toUpperCase())}</strong> - <strong>${phEscape((r.destination || "").toUpperCase())}</strong></td>
        <td>${phToDot(r.journey_date)}</td>
        <td style="text-align:center;font-family:monospace;">${phEscape(r.departure_time)}</td>
        <td>${phEscape(r.operator)}</td>
        <td style="opacity:0.85;">${phEscape(r.departure_stop || "-")}</td>
        <td style="text-align:right;text-decoration:line-through;opacity:0.7;">${r.old_price} TL</td>
        <td style="text-align:right;color:${color};font-weight:700;">${r.new_price} TL</td>
        <td style="text-align:right;color:${color};font-weight:600;white-space:nowrap;">
          ${arrow} ${Math.abs(diff)} TL <span style="opacity:0.75;font-size:11px;">(${pctStr})</span>
        </td>
        <td>${badge}</td>
      </tr>
    `;
  }).join("");

  // Satira tiklama -> drilldown panel
  dom.phTableBody.querySelectorAll(".ph-row").forEach(tr => {
    tr.addEventListener("click", () => {
      const idx = parseInt(tr.dataset.idx, 10);
      const row = sorted[idx];
      if (row) openPriceHistoryDetail(row);
    });
  });
}

function renderPriceHistorySortIcons() {
  document.querySelectorAll(".ph-sortable").forEach(th => {
    const icon = th.querySelector(".ph-sort-icon");
    if (!icon) return;
    if (th.dataset.sort === priceHistoryState.sortKey) {
      icon.textContent = priceHistoryState.sortDir === "asc" ? "+" : "-";
      th.classList.add("ph-sort-active");
    } else {
      icon.textContent = "";
      th.classList.remove("ph-sort-active");
    }
  });
}

function renderPriceHistoryRouteCards() {
  if (!dom.phRouteCards) return;
  const rows = priceHistoryState.rows;
  if (!rows.length) { dom.phRouteCards.innerHTML = ""; return; }

  // Hat bazinda grupla
  const byRoute = new Map();
  for (const r of rows) {
    const key = `${r.target_id}|${r.origin}->${r.destination}`;
    if (!byRoute.has(key)) {
      byRoute.set(key, {
        target_id: r.target_id,
        origin: r.origin,
        destination: r.destination,
        total: 0, up: 0, down: 0,
        diffs: [],
        operatorCounts: new Map(),
      });
    }
    const grp = byRoute.get(key);
    grp.total++;
    const diff = r.new_price - r.old_price;
    grp.diffs.push(diff);
    if (diff > 0) grp.up++;
    else if (diff < 0) grp.down++;
    grp.operatorCounts.set(r.operator, (grp.operatorCounts.get(r.operator) || 0) + 1);
  }

  // Toplam degisiklik sayisina gore sirala (desc)
  const groups = Array.from(byRoute.values()).sort((a, b) => b.total - a.total);

  dom.phRouteCards.innerHTML = groups.map(g => {
    const avgDiff = Math.round(g.diffs.reduce((a, b) => a + b, 0) / g.diffs.length);
    const topOp = Array.from(g.operatorCounts.entries()).sort((a, b) => b[1] - a[1])[0];
    const avgColor = avgDiff > 0 ? "#27ae60" : avgDiff < 0 ? "#d32f2f" : "#888";
    const avgSign = avgDiff > 0 ? "+" : "";
    return `
      <article class="ph-route-card" data-target-id="${g.target_id}" tabindex="0" role="button">
        <header class="ph-route-card-head">
          <span class="ph-route-icon"></span>
          <h5>${phEscape((g.origin || "").toUpperCase())} - ${phEscape((g.destination || "").toUpperCase())}</h5>
          <button class="btn btn-small btn-ghost ph-route-excel-btn" type="button"
            data-target-id="${g.target_id}"
            data-origin="${phEscape(g.origin || "")}"
            data-destination="${phEscape(g.destination || "")}"
            title="Bu hattı Excel'e aktar"></button>
        </header>
        <div class="ph-route-card-grid">
          <div><span class="ph-rc-label">Değişiklik</span><span class="ph-rc-value">${g.total}</span></div>
          <div><span class="ph-rc-label">Artış</span><span class="ph-rc-value" style="color:#27ae60;">${g.up}</span></div>
          <div><span class="ph-rc-label">Düşüş</span><span class="ph-rc-value" style="color:#d32f2f;">${g.down}</span></div>
          <div><span class="ph-rc-label">Ort. fark</span><span class="ph-rc-value" style="color:${avgColor};">${avgSign}${avgDiff} TL</span></div>
        </div>
        ${topOp ? `<footer class="ph-route-card-foot">En aktif: <strong>${phEscape(topOp[0])}</strong> · ${topOp[1]} değişim</footer>` : ""}
      </article>
    `;
  }).join("");

  dom.phRouteCards.querySelectorAll(".ph-route-card").forEach(card => {
    card.addEventListener("click", (e) => {
      if (e.target.closest(".ph-route-excel-btn")) return;
      const tid = card.dataset.targetId;
      if (dom.phTargetSelect && tid) {
        dom.phTargetSelect.value = tid;
        loadPriceHistory();
      }
    });
  });

  dom.phRouteCards.querySelectorAll(".ph-route-excel-btn").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      exportRouteToExcel(btn.dataset.targetId, btn.dataset.origin, btn.dataset.destination);
    });
  });
}

function exportRouteToExcel(targetId, origin, destination) {
  const all = priceHistoryState.rows;
  const rows = targetId
    ? all.filter(r => String(r.target_id) === String(targetId))
    : all;

  if (!rows.length) {
    alert("Bu hat için aktarılacak kayıt yok.");
    return;
  }

  const toDot = (iso) => {
    const m = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
    return m ? `${m[3]}.${m[2]}.${m[1]}` : iso || "";
  };

  const sheetData = [
    ["Tespit Zamanı", "Kalkış", "Varış", "Sefer Tarihi", "Saat", "Firma", "Kalkış Durağı", "Eski Fiyat (TL)", "Yeni Fiyat (TL)", "Fark (TL)", "Durum"],
    ...rows.map(r => {
      const diff = r.new_price - r.old_price;
      return [
        r.changed_at || "",
        r.origin || "",
        r.destination || "",
        toDot(r.journey_date),
        r.departure_time || "",
        r.operator || "",
        r.departure_stop || "",
        Number(r.old_price) || 0,
        Number(r.new_price) || 0,
        diff,
        diff > 0 ? "ARTIŞ" : diff < 0 ? "DÜŞÜŞ" : "DEĞİŞİM YOK",
      ];
    }),
  ];

  const XLSX = window.XLSX;
  if (!XLSX) { alert("Excel kütüphanesi yüklenemedi."); return; }

  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.aoa_to_sheet(sheetData);

  // Sütun genişlikleri
  ws["!cols"] = [
    { wch: 20 }, { wch: 14 }, { wch: 14 }, { wch: 14 }, { wch: 8 },
    { wch: 28 }, { wch: 20 }, { wch: 14 }, { wch: 14 }, { wch: 10 }, { wch: 12 },
  ];

  XLSX.utils.book_append_sheet(wb, ws, "Fiyat Degisiklikleri");

  const routeSlug = `${(origin || "").replace(/\s+/g, "-")}-${(destination || "").replace(/\s+/g, "-")}`.toLocaleLowerCase("tr-TR");
  const dateStr = new Date().toISOString().slice(0, 10);
  XLSX.writeFile(wb, `fiyat-degisiklik_${routeSlug}_${dateStr}.xlsx`);
}

// =====================
// Drilldown side panel
// =====================
async function openPriceHistoryDetail(row) {
  if (!dom.phDetailPanel) return;
  dom.phDetailPanel.classList.remove("hidden");
  dom.phDetailPanel.setAttribute("aria-hidden", "false");
  if (dom.phDetailBackdrop) dom.phDetailBackdrop.classList.remove("hidden");

  dom.phDetailTitle.textContent = `${(row.origin || "").toUpperCase()} - ${(row.destination || "").toUpperCase()}`;
  dom.phDetailSubtitle.textContent = `${row.operator} · ${row.departure_time} · ${row.departure_stop || "-"}`;
  dom.phDetailStats.innerHTML = `<p class="subtle">Yükleniyor...</p>`;
  dom.phDetailChart.innerHTML = "";
  dom.phDetailHistory.innerHTML = "";
  dom.phDetailPeers.innerHTML = "";

  try {
    const params = new URLSearchParams({
      targetId: String(row.target_id || ""),
      operator: row.operator || "",
      time: row.departure_time || "",
    });
    const data = await apiFetch(`/api/obilet/price-history/journey-detail?${params.toString()}`);

    // Stats
    const s = data.stats || {};
    dom.phDetailStats.innerHTML = `
      <div class="ph-detail-stat-grid">
        <div><span>Değişim</span><strong>${s.changes ?? 0}</strong></div>
        <div><span>Min</span><strong style="color:#d32f2f;">${s.min ?? "-"} TL</strong></div>
        <div><span>Ortalama</span><strong>${s.avg ?? "-"} TL</strong></div>
        <div><span>Max</span><strong style="color:#27ae60;">${s.max ?? "-"} TL</strong></div>
      </div>
    `;

    // Chart (SVG line) — history kronolojik ASC olmasi lazim
    const history = Array.isArray(data.history) ? data.history.slice().reverse() : [];
    dom.phDetailChart.innerHTML = renderPriceTrendSVG(history, data.current);

    // History list
    if (history.length) {
      dom.phDetailHistory.innerHTML = history.slice().reverse().map(h => {
        const diff = h.new_price - h.old_price;
        const color = diff > 0 ? "#27ae60" : diff < 0 ? "#d32f2f" : "#888";
        const arrow = diff > 0 ? "+" : diff < 0 ? "-" : "";
        return `<div class="ph-detail-history-row">
          <span style="opacity:0.75;font-size:12px;">${phEscape(h.changed_at)}</span>
          <span style="text-decoration:line-through;opacity:0.7;">${h.old_price} TL</span>
          <span>-</span>
          <strong style="color:${color};">${h.new_price} TL</strong>
          <span style="color:${color};">${arrow} ${Math.abs(diff)} TL</span>
        </div>`;
      }).join("");
    } else {
      dom.phDetailHistory.innerHTML = `<p class="subtle">Bu sefer icin gecmis kayit yok (bu satir ilk degisim olabilir).</p>`;
    }

    // Peers
    const peers = Array.isArray(data.peers) ? data.peers : [];
    if (peers.length) {
      dom.phDetailPeers.innerHTML = peers.map(p => `
        <div class="ph-detail-peer-row">
          <span>${phEscape(p.operator)}</span>
          <span style="opacity:0.7;font-size:12px;">${phEscape(p.departure_stop || "-")}</span>
          <strong>${p.price} TL</strong>
        </div>
      `).join("");
    } else {
      dom.phDetailPeers.innerHTML = `<p class="subtle">Bu saatte ${phEscape(row.operator)} disinda kayitli firma yok.</p>`;
    }
  } catch (err) {
    dom.phDetailStats.innerHTML = `<p class="subtle" style="color:#d32f2f;">Detay alinamadi: ${phEscape(err.message)}</p>`;
  }
}

function closePriceHistoryDetail() {
  if (!dom.phDetailPanel) return;
  dom.phDetailPanel.classList.add("hidden");
  dom.phDetailPanel.setAttribute("aria-hidden", "true");
  if (dom.phDetailBackdrop) dom.phDetailBackdrop.classList.add("hidden");
}

// Basit SVG line chart — son fiyatlar zaman ekseninde
function renderPriceTrendSVG(history, current) {
  // Veri noktalari: her history kaydinin old_price + new_price + suanki anlik fiyat
  const points = [];
  for (const h of history) {
    points.push({ y: h.old_price, label: h.changed_at + " (önce)" });
    points.push({ y: h.new_price, label: h.changed_at + " (sonra)" });
  }
  if (current?.price) points.push({ y: current.price, label: "Şu an" });
  if (points.length < 2) {
    return `<p class="subtle">Yeterli veri yok — fiyat değişikliği oldukça trend çizilecek.</p>`;
  }
  const W = 460, H = 140, P = 24;
  const ys = points.map(p => p.y);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const rangeY = Math.max(1, maxY - minY);
  const xStep = (W - 2 * P) / Math.max(1, points.length - 1);
  const yScale = (y) => H - P - ((y - minY) / rangeY) * (H - 2 * P);
  const pts = points.map((p, i) => `${P + i * xStep},${yScale(p.y)}`);
  const last = points[points.length - 1];
  const first = points[0];
  const trendUp = last.y >= first.y;
  const lineColor = trendUp ? "#27ae60" : "#d32f2f";
  const areaColor = trendUp ? "rgba(39,174,96,0.12)" : "rgba(211,47,47,0.12)";
  const polyline = pts.join(" ");
  const areaPath = `${P},${H - P} ${polyline} ${P + (points.length - 1) * xStep},${H - P}`;
  const circles = points.map((p, i) => {
    const cx = P + i * xStep;
    const cy = yScale(p.y);
    return `<circle cx="${cx}" cy="${cy}" r="3" fill="${lineColor}" />`;
  }).join("");
  return `
    <svg viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:auto;">
      <polygon points="${areaPath}" fill="${areaColor}" stroke="none" />
      <polyline points="${polyline}" fill="none" stroke="${lineColor}" stroke-width="2" />
      ${circles}
      <text x="${P}" y="${P - 6}" font-size="10" fill="currentColor" opacity="0.65">${maxY} TL</text>
      <text x="${P}" y="${H - 6}" font-size="10" fill="currentColor" opacity="0.65">${minY} TL</text>
    </svg>
  `;
}

// =====================
// Hizli tarih chip'leri
// =====================
function applyDateRange(rangeKey) {
  const today = new Date();
  let from = new Date(today), to = new Date(today);
  switch (rangeKey) {
    case "today":
      break;
    case "yesterday":
      from.setDate(from.getDate() - 1);
      to.setDate(to.getDate() - 1);
      break;
    case "last7":
      from.setDate(from.getDate() - 6);
      break;
    case "last30":
      from.setDate(from.getDate() - 29);
      break;
    case "thismonth":
      from = new Date(today.getFullYear(), today.getMonth(), 1);
      break;
    case "thisyear":
      from = new Date(today.getFullYear(), 0, 1);
      break;
    case "all":
      from = null;
      break;
  }
  if (dom.phFromDate) dom.phFromDate.value = from ? from.toISOString().slice(0, 10) : "";
  if (dom.phToDate) dom.phToDate.value = to.toISOString().slice(0, 10);
  // Active chip işareti
  if (dom.phChipsRow) {
    dom.phChipsRow.querySelectorAll(".ph-chip").forEach(c => {
      c.classList.toggle("ph-chip-active", c.dataset.range === rangeKey);
    });
  }
  loadPriceHistory();
}

function exportPriceHistoryCsv() {
  const rows = priceHistoryState.rows;
  if (!rows.length) {
    alert("Aktarılacak kayıt yok.");
    return;
  }
  const toDot = (iso) => {
    const m = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
    return m ? `${m[3]}.${m[2]}.${m[1]}` : iso || "";
  };
  const headers = ["Tespit Zamani", "Kalkis", "Varis", "Sefer Tarihi", "Saat", "Firma", "Kalkis Duragi", "Eski Fiyat", "Yeni Fiyat", "Fark"];
  const csvRows = rows.map(r => {
    const diff = r.new_price - r.old_price;
    return [
      r.changed_at || "",
      r.origin || "",
      r.destination || "",
      toDot(r.journey_date),
      r.departure_time || "",
      r.operator || "",
      r.departure_stop || "",
      r.old_price,
      r.new_price,
      diff
    ].map(v => String(v).replace(/;/g, ",")).join(";");
  });
  const csv = "﻿" + [headers.join(";"), ...csvRows].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `fiyat-degisiklikleri-${new Date().toISOString().slice(0,10)}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

async function populatePriceHistoryTargetSelect() {
  if (!dom.phTargetSelect) return;
  try {
    const data = await apiFetch("/api/obilet/targets");
    const targets = Array.isArray(data?.targets) ? data.targets : [];
    const currentVal = dom.phTargetSelect.value;
    dom.phTargetSelect.innerHTML = `<option value="">Tüm hatlar</option>` +
      targets.map(t => `<option value="${t.id}">${(t.origin || "").toUpperCase()} - ${(t.destination || "").toUpperCase()}</option>`).join("");
    if (currentVal) dom.phTargetSelect.value = currentVal;
  } catch (e) { /* sessiz */ }
}

function setupPriceHistoryPanel() {
  if (!dom.phApplyBtn) return;
  // Varsayilan tarih araligi: son 30 gun
  if (dom.phFromDate && !dom.phFromDate.value) {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    dom.phFromDate.value = d.toISOString().slice(0, 10);
  }
  if (dom.phToDate && !dom.phToDate.value) {
    dom.phToDate.value = new Date().toISOString().slice(0, 10);
  }

  dom.phApplyBtn.addEventListener("click", () => loadPriceHistory());
  if (dom.phResetBtn) {
    dom.phResetBtn.addEventListener("click", () => {
      if (dom.phTargetSelect) dom.phTargetSelect.value = "";
      if (dom.phSearchInput) dom.phSearchInput.value = "";
      const d = new Date(); d.setDate(d.getDate() - 30);
      if (dom.phFromDate) dom.phFromDate.value = d.toISOString().slice(0, 10);
      if (dom.phToDate) dom.phToDate.value = new Date().toISOString().slice(0, 10);
      // Active chip = last30
      if (dom.phChipsRow) {
        dom.phChipsRow.querySelectorAll(".ph-chip").forEach(c =>
          c.classList.toggle("ph-chip-active", c.dataset.range === "last30")
        );
      }
      loadPriceHistory();
    });
  }
  if (dom.phExportBtn) dom.phExportBtn.addEventListener("click", exportPriceHistoryCsv);
  if (dom.phSearchInput) {
    dom.phSearchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") loadPriceHistory();
    });
  }

  // Hızlı tarih chip'leri
  if (dom.phChipsRow) {
    dom.phChipsRow.querySelectorAll(".ph-chip").forEach(chip => {
      chip.addEventListener("click", () => applyDateRange(chip.dataset.range));
    });
  }

  // Sutun sıralama
  document.querySelectorAll(".ph-sortable").forEach(th => {
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (priceHistoryState.sortKey === key) {
        priceHistoryState.sortDir = priceHistoryState.sortDir === "asc" ? "desc" : "asc";
      } else {
        priceHistoryState.sortKey = key;
        priceHistoryState.sortDir = "desc";
      }
      renderPriceHistory();
    });
  });

  // Drilldown panel close
  if (dom.phDetailClose) dom.phDetailClose.addEventListener("click", closePriceHistoryDetail);
  if (dom.phDetailBackdrop) dom.phDetailBackdrop.addEventListener("click", closePriceHistoryDetail);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && dom.phDetailPanel && !dom.phDetailPanel.classList.contains("hidden")) {
      closePriceHistoryDetail();
    }
  });
}

async function initPriceHistoryPanel() {
  setupPriceHistoryPanel();
  await populatePriceHistoryTargetSelect();
  await loadPriceHistory();
  setupMarketShare();
}

// ============ PAZAR PAYI ANALIZI (Excel) ============
async function setupMarketShare() {
  const btn = document.getElementById("msExportBtn");
  const sel = document.getElementById("msDateSelect");
  if (!btn) return;
  try {
    const data = await apiFetch("/api/obilet/market-share");
    if (sel && Array.isArray(data.availableDates)) {
      sel.innerHTML = `<option value="">Tüm dönem (toplam)</option>` +
        data.availableDates.map((d) => `<option value="${d}">${occToDot(d)}</option>`).join("");
    }
  } catch (e) { /* sessiz */ }
  if (!btn.dataset.wired) {
    btn.dataset.wired = "1";
    btn.addEventListener("click", exportMarketShareExcel);
  }
}

async function exportMarketShareExcel() {
  const date = document.getElementById("msDateSelect")?.value || "";
  const btn = document.getElementById("msExportBtn");
  const oldText = btn.textContent;
  btn.textContent = "Hazırlanıyor...";
  btn.disabled = true;
  try {
    const params = new URLSearchParams();
    if (date) params.set("date", date);
    // Stilli Excel sunucuda uretiliyor (ExcelJS) — blob olarak indir.
    const res = await fetch(`/api/obilet/market-share.xlsx?${params.toString()}`, {
      headers: state.token ? { Authorization: `Bearer ${state.token}` } : {},
    });
    if (!res.ok) {
      let m = "Analiz oluşturulamadı.";
      try { const j = await res.json(); m = j.message || m; } catch { /* */ }
      throw new Error(m);
    }
    const blob = await res.blob();
    if (blob.size < 200) {
      alert("Analiz için veri yok. Doluluk taraması bir tur dönünce (~10 dk) dolar.");
      return;
    }
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = date ? `pazar-payi-${date}.xlsx` : "pazar-payi-tum-donem.xlsx";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch (err) {
    alert(err.message);
  } finally {
    btn.textContent = oldText;
    btn.disabled = false;
  }
}

// ============================================================
// DOLULUK TAKIP (oBilet Takip'ten bagimsiz)
// ============================================================
const occupancyState = { wired: false, rows: [] };

function occEsc(s) {
  return String(s == null ? "" : s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
function occToDot(iso) {
  const m = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[3]}.${m[2]}.${m[1]}` : iso || "";
}
function occColor(p) {
  if (p >= 80) return "#e74c3c";
  if (p >= 50) return "#e67e22";
  return "#27ae60";
}

function setupOccupancyPanel() {
  const refreshBtn = document.getElementById("occRefreshBtn");
  if (!refreshBtn) return;
  if (!occupancyState.wired) {
    occupancyState.wired = true;
    refreshBtn.addEventListener("click", () => loadOccupancy());
    const exportBtn = document.getElementById("occExportBtn");
    if (exportBtn) exportBtn.addEventListener("click", exportOccupancyCsv);
    const sel = document.getElementById("occTargetSelect");
    if (sel) sel.addEventListener("change", () => loadOccupancy());
    const opSel = document.getElementById("occOperatorSelect");
    if (opSel) opSel.addEventListener("change", () => loadOccupancy());
  }
  populateOccupancyTargetSelect().then(() => loadOccupancy());
}

function populateOccupancyOperatorSelect(operators) {
  const sel = document.getElementById("occOperatorSelect");
  if (!sel || !Array.isArray(operators)) return;
  const cur = sel.value;
  sel.innerHTML = `<option value="">Tüm firmalar</option>` +
    operators.map((op) => `<option value="${occEsc(op)}">${occEsc(op)}</option>`).join("");
  // Onceki secim hala listede varsa koru.
  if (cur && operators.includes(cur)) sel.value = cur;
}

async function populateOccupancyTargetSelect() {
  const sel = document.getElementById("occTargetSelect");
  if (!sel) return;
  try {
    const data = await apiFetch("/api/obilet/targets");
    const targets = Array.isArray(data?.targets) ? data.targets : [];
    const cur = sel.value;
    sel.innerHTML = `<option value="">Tüm hatlar</option>` +
      targets.map((t) => `<option value="${t.id}">${occEsc((t.origin || "").toUpperCase())} - ${occEsc((t.destination || "").toUpperCase())}</option>`).join("");
    if (cur) sel.value = cur;
  } catch (e) { /* sessiz */ }
}

async function loadOccupancy() {
  const body = document.getElementById("occTableBody");
  const statusEl = document.getElementById("occStatusMsg");
  if (!body) return;
  const targetId = document.getElementById("occTargetSelect")?.value || "";
  const operator = document.getElementById("occOperatorSelect")?.value || "";
  if (statusEl) statusEl.textContent = "Doluluk yükleniyor...";
  try {
    const params = new URLSearchParams();
    if (targetId) params.set("targetId", targetId);
    if (operator) params.set("operator", operator);
    const data = await apiFetch(`/api/obilet/occupancy?${params.toString()}`);
    occupancyState.rows = Array.isArray(data?.rows) ? data.rows : [];
    populateOccupancyOperatorSelect(data.operators);
    renderOccupancy(data);
    if (statusEl) {
      statusEl.textContent = occupancyState.rows.length
        ? `${occupancyState.rows.length} sefer · ortalama doluluk %${data.avgOccupancy}`
        : "Henüz doluluk verisi yok. Tarama bir tur dönünce (~10 dk) dolacak.";
    }
  } catch (err) {
    if (statusEl) statusEl.textContent = `Hata: ${err.message}`;
  }
}

function renderOccupancy(data) {
  const summary = document.getElementById("occSummary");
  const body = document.getElementById("occTableBody");
  if (!body) return;

  if (summary) {
    if (!data || !data.count) {
      summary.innerHTML = "";
    } else {
      const card = (label, value, sub, color) =>
        `<div style="flex:1;min-width:150px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:0.8rem 1rem;">
          <div style="font-size:0.75rem;opacity:0.7;">${label}</div>
          <div style="font-size:1.5rem;font-weight:700;${color ? `color:${color};` : ""}">${value}</div>
          ${sub ? `<div style="font-size:0.75rem;opacity:0.6;">${sub}</div>` : ""}
        </div>`;
      const f = data.fullest || {};
      summary.style.display = "flex";
      summary.style.flexWrap = "wrap";
      summary.style.gap = "0.7rem";
      summary.innerHTML =
        card("Sefer", data.count) +
        card("Ortalama Doluluk", `%${data.avgOccupancy}`, null, occColor(data.avgOccupancy)) +
        card("Satılan / Toplam", `${data.soldSeats} / ${data.totalSeats}`) +
        card("En Dolu Sefer", `%${f.occupancy_percent || 0}`, occEsc(f.operator || ""), occColor(f.occupancy_percent || 0));
    }
  }

  const rows = occupancyState.rows;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" style="text-align:center;color:#888;">Kayıt yok</td></tr>`;
    occupancyState.shown = 0;
    return;
  }
  // Kademeli yukleme: ilk 50 + "+50 daha goster" (Sefer Takip ile ayni ST_PAGE).
  const end = Math.min(ST_PAGE, rows.length);
  body.innerHTML = rows.slice(0, end).map(occRowHtml).join("") + occLoadMoreRowHtml(rows.length, end);
  occupancyState.shown = end;
  occBindLoadMore(rows);
}

// Tek bir doluluk satirinin HTML'i.
// "DD.MM.YYYY HH:mm:ss" (bazen "...,..." virgullu) -> dakika cinsinden ne kadar eski (Istanbul saatiyle
// karsilastirma icin basit bir yaklasik hesap; sunucudaki isOccStale ile ayni mantik, client tarafi).
function occMinutesAgo(ts) {
  const m = String(ts || "").match(/(\d{2})\.(\d{2})\.(\d{4})[\s,]+(\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  const then = new Date(Date.UTC(+m[3], +m[2] - 1, +m[1], +m[4], +m[5], +m[6]));
  const now = new Date(new Date().toLocaleString("en-US", { timeZone: "Europe/Istanbul" }));
  return Math.round((now - then) / 60000);
}
// Doluluk kaynagi rozeti: source==='liste' ise "~ tahmini" (koltuk haritasindan degil, bos-koltuk
// listesinden tahmin edilmis); "gercek" ama last_updated 90 dk'dan eskiyse "bayat olabilir" uyarisi;
// aksi halde rozet yok (taze + dogrulanmis).
function occSourceBadge(source, lastUpdated) {
  if (source === "liste") {
    return ` <span title="Koltuk haritasi cekilemedi, doluluk bos-koltuk listesinden tahmin edildi" style="opacity:.55;font-size:.82em;cursor:help;">~tahmini</span>`;
  }
  const mins = occMinutesAgo(lastUpdated);
  if (source === "gercek" && mins != null && mins > 90) {
    return ` <span title="Bu deger yaklaşık ${mins} dk önce güncellendi, bayat olabilir" style="opacity:.55;font-size:.82em;cursor:help;">🕓</span>`;
  }
  return "";
}

function occRowHtml(r) {
  const sold = (r.total_seats || 0) - (r.available_seats || 0);
  const p = r.occupancy_percent || 0;
  return `<tr>
    <td>${occEsc((r.origin || "").toUpperCase())} - ${occEsc((r.destination || "").toUpperCase())}</td>
    <td>${occToDot(r.journey_date)}</td>
    <td>${occEsc(r.departure_time || "")}</td>
    <td>${occEsc(r.operator || "")}${r.plate ? `<div style="font-size:0.72rem;opacity:0.65;">Plaka: ${occEsc(r.plate)}</div>` : ""}</td>
    <td><b style="color:${occColor(p)}">%${p}</b>${occSourceBadge(r.source, r.last_updated)}</td>
    <td>${sold} / ${r.total_seats || 0}</td>
    <td>${r.available_seats || 0}</td>
  </tr>`;
}

function occLoadMoreRowHtml(total, shown) {
  const remaining = total - shown;
  if (remaining <= 0) return "";
  const next = Math.min(ST_PAGE, remaining);
  return `<tr id="occLoadMoreRow"><td colspan="7" style="text-align:center;padding:0.9rem;">
    <button id="occLoadMore" class="btn btn-primary" type="button">+${next} daha göster <span style="opacity:.75">(${remaining} kaldı)</span></button>
  </td></tr>`;
}

function occBindLoadMore(rows) {
  const more = document.getElementById("occLoadMore");
  if (!more) return;
  more.addEventListener("click", () => {
    const body = document.getElementById("occTableBody");
    if (!body) return;
    document.getElementById("occLoadMoreRow")?.remove();
    const start = occupancyState.shown || 0;
    const end = Math.min(start + ST_PAGE, rows.length);
    body.insertAdjacentHTML("beforeend", rows.slice(start, end).map(occRowHtml).join(""));
    occupancyState.shown = end;
    body.insertAdjacentHTML("beforeend", occLoadMoreRowHtml(rows.length, end));
    occBindLoadMore(rows);
  });
}

function exportOccupancyCsv() {
  const rows = occupancyState.rows;
  if (!rows.length) { alert("Aktarılacak veri yok."); return; }
  const headers = ["Kalkis", "Varis", "Sefer Tarihi", "Saat", "Firma", "Plaka", "Doluluk %", "Dolu", "Toplam", "Bos"];
  const csvRows = rows.map((r) => {
    const sold = (r.total_seats || 0) - (r.available_seats || 0);
    return [r.origin || "", r.destination || "", occToDot(r.journey_date), r.departure_time || "", r.operator || "", r.plate || "", r.occupancy_percent || 0, sold, r.total_seats || 0, r.available_seats || 0]
      .map((v) => String(v).replace(/;/g, ",")).join(";");
  });
  const csv = "﻿" + [headers.join(";"), ...csvRows].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `doluluk-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ============================================================
// TALEP RADARI (otonom yogunluk analizi — oBilet Takip'ten bagimsiz)
// Sistem populer rotalari arka planda tarar; yogun/bos gunleri analiz eder.
// ============================================================
const demandState = { wired: false, data: null };

function demColor(p) {
  if (p == null) return "#888";
  if (p >= 80) return "#e74c3c";
  if (p >= 50) return "#e67e22";
  return "#27ae60";
}
function demToDot(iso) {
  const m = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m ? `${m[3]}.${m[2]}.${m[1]}` : (iso || "");
}
function demFmtMeasured(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleString("tr-TR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}
function demBar(pct) {
  const p = Math.max(0, Math.min(100, pct || 0));
  return `<div class="dem-bar"><div class="dem-bar-fill" style="width:${p}%;background:${demColor(pct)}"></div></div>`;
}

function setupDemandPanel() {
  const refreshBtn = document.getElementById("demandRefreshBtn");
  if (!refreshBtn) return;
  if (!demandState.wired) {
    demandState.wired = true;
    refreshBtn.addEventListener("click", () => loadDemand());
    document.getElementById("demandDaysSelect")?.addEventListener("change", () => loadDemand());
    document.getElementById("demandRouteSelect")?.addEventListener("change", () => loadDemand());
    document.getElementById("demandExportBtn")?.addEventListener("click", exportDemandCsv);
    document.getElementById("demandScanBtn")?.addEventListener("click", triggerDemandScan);
    document.getElementById("demandAddBtn")?.addEventListener("click", addDemandRoute);
    // Havuz listesindeki aktif/pasif ve sil butonlari (event delegation — inline onclick YOK, XSS guvenli).
    document.getElementById("demandPoolList")?.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-act]");
      if (!btn) return;
      const id = btn.getAttribute("data-id");
      const act = btn.getAttribute("data-act");
      if (act === "toggle") toggleDemandRoute(id, btn.getAttribute("data-active") !== "1");
      else if (act === "del") deleteDemandRoute(id);
    });
  }
  loadDemandRoutes();
  loadDemand();
  loadDemandStatus();
}

async function loadDemandStatus() {
  const el = document.getElementById("demandStatusInfo");
  if (!el) return;
  try {
    const s = await apiFetch("/api/analysis/status");
    const running = s.workerRunning ? "Tarama çalışıyor" : "Beklemede";
    el.textContent = `${running} · ${s.routeCount} aktif rota · ${s.measurements} ölçüm · son: ${demFmtMeasured(s.lastMeasured)} · ufuk ${s.horizonDays} gün`;
  } catch (e) { el.textContent = ""; }
}

async function loadDemandRoutes() {
  try {
    const data = await apiFetch("/api/analysis/routes");
    const routes = Array.isArray(data?.routes) ? data.routes : [];
    const sel = document.getElementById("demandRouteSelect");
    if (sel) {
      const cur = sel.value;
      const withId = routes.filter((r) => r.route_id);
      sel.innerHTML = `<option value="">Tüm rotalar</option>` +
        withId.map((r) => `<option value="${escapeHtml(r.route_id)}">${escapeHtml(r.origin)} - ${escapeHtml(r.destination)}</option>`).join("");
      if (cur) sel.value = cur;
    }
    const pool = document.getElementById("demandPoolList");
    if (pool) {
      pool.innerHTML = routes.map((r) => {
        const resolved = r.route_id
          ? `<span class="dem-chip dem-chip-ok">${escapeHtml(r.route_id)}</span>`
          : `<span class="dem-chip dem-chip-warn">route_id yok</span>`;
        return `<div class="dem-pool-row ${r.is_active ? "" : "dem-pool-off"}">
          <div class="dem-pool-main">
            <strong>${escapeHtml(r.origin)} - ${escapeHtml(r.destination)}</strong>
            ${resolved}${r.is_seed ? ` <span class="dem-chip">hazır</span>` : ""}
          </div>
          <div class="dem-pool-meta subtle">${r.coverage || 0} sefer ölçüldü · ${demFmtMeasured(r.lastMeasured)}</div>
          <div class="dem-pool-actions">
            <button class="btn btn-small btn-ghost" data-act="toggle" data-id="${r.id}" data-active="${r.is_active ? 1 : 0}">${r.is_active ? "Aktif" : "Pasif"}</button>
            <button class="btn btn-small btn-danger" data-act="del" data-id="${r.id}">Sil</button>
          </div>
        </div>`;
      }).join("") || `<p class="subtle">Rota yok.</p>`;
    }
  } catch (e) { /* sessiz */ }
}

async function addDemandRoute() {
  const oEl = document.getElementById("demandAddOrigin");
  const dEl = document.getElementById("demandAddDest");
  const msg = document.getElementById("demandAddMsg");
  const origin = (oEl?.value || "").trim();
  const destination = (dEl?.value || "").trim();
  if (!origin || !destination) { if (msg) msg.textContent = "Kalkış ve varış girin."; return; }
  try {
    const r = await apiFetch("/api/analysis/routes", { method: "POST", body: JSON.stringify({ origin, destination }) });
    if (msg) msg.textContent = r.resolved ? `Eklendi (route_id: ${r.route_id}).` : "Eklendi, ama şehir tanınmadığı için route_id çözülemedi — tarama bu rotayı atlar.";
    if (oEl) oEl.value = "";
    if (dEl) dEl.value = "";
    loadDemandRoutes();
  } catch (e) { if (msg) msg.textContent = `Hata: ${e.message}`; }
}

async function toggleDemandRoute(id, active) {
  try { await apiFetch(`/api/analysis/routes/${id}`, { method: "PATCH", body: JSON.stringify({ isActive: active }) }); loadDemandRoutes(); }
  catch (e) { /* sessiz */ }
}
async function deleteDemandRoute(id) {
  if (!confirm("Bu rotayı havuzdan silmek istediğinize emin misiniz?")) return;
  try { await apiFetch(`/api/analysis/routes/${id}`, { method: "DELETE" }); loadDemandRoutes(); }
  catch (e) { /* sessiz */ }
}

async function triggerDemandScan() {
  const msg = document.getElementById("demandStatusMsg");
  try {
    const r = await apiFetch("/api/analysis/scan", { method: "POST" });
    if (msg) msg.textContent = r.message || "Tarama başlatıldı.";
    loadDemandStatus();
  } catch (e) { if (msg) msg.textContent = `Hata: ${e.message}`; }
}

async function loadDemand() {
  const statusEl = document.getElementById("demandStatusMsg");
  const days = document.getElementById("demandDaysSelect")?.value || "15";
  const routeRef = document.getElementById("demandRouteSelect")?.value || "";
  if (statusEl) statusEl.textContent = "Analiz yükleniyor...";
  try {
    const params = new URLSearchParams();
    params.set("days", days);
    if (routeRef) params.set("routeRef", routeRef);
    const data = await apiFetch(`/api/analysis/demand?${params.toString()}`);
    demandState.data = data;
    renderDemand(data);
    if (statusEl) {
      statusEl.textContent = data.summary.count
        ? `${data.summary.count} sefer · ${data.summary.routeCount} rota · ortalama doluluk %${data.summary.avgOccupancy} · son ölçüm ${demFmtMeasured(data.summary.lastMeasuredAt)}`
        : "Henüz analiz verisi yok. Sistem popüler rotaları arka planda tarıyor (ilk tur ~6 dk sonra başlar). Hemen başlatmak için 'Şimdi Tara'ya basabilirsin.";
    }
    loadDemandStatus();
  } catch (err) {
    if (statusEl) statusEl.textContent = `Hata: ${err.message}`;
  }
}

function renderDemand(data) {
  const kpi = document.getElementById("demandKpis");
  if (kpi) {
    const s = data.summary || {};
    const card = (label, value, sub, color) =>
      `<article class="dem-kpi"><div class="dem-kpi-label">${escapeHtml(label)}</div>
       <div class="dem-kpi-value"${color ? ` style="color:${color}"` : ""}>${escapeHtml(String(value))}</div>
       ${sub ? `<div class="dem-kpi-sub subtle">${escapeHtml(sub)}</div>` : ""}</article>`;
    kpi.innerHTML =
      card("Ölçülen Sefer", s.count || 0, `${s.routeCount || 0} rota`) +
      card("Ortalama Doluluk", `%${s.avgOccupancy || 0}`, null, demColor(s.avgOccupancy || 0)) +
      card("Yüksek Talep", s.highCount || 0, `≥%${data.high} dolu`, "#e74c3c") +
      card("Boş / Zayıf", s.lowCount || 0, `<%${data.low} dolu`, "#27ae60");
  }
  renderDemandWeekday(data.byWeekday || []);
  renderDemandByDate(data.byDate || []);
  renderDemandRoutes(data.routes || []);
  renderDemandTop("demandTopFull", data.topFull || [], "");
  renderDemandTop("demandTopEmpty", data.topEmpty || [], "");
}

function renderDemandWeekday(byWeekday) {
  const el = document.getElementById("demandWeekday");
  if (!el) return;
  if (!byWeekday.some((w) => w.avg != null)) { el.innerHTML = `<p class="subtle">Gün deseni için yeterli veri yok.</p>`; return; }
  el.innerHTML = byWeekday.map((w) => `<div class="dem-wd">
      <div class="dem-wd-name">${escapeHtml(w.name)}</div>
      ${demBar(w.avg)}
      <div class="dem-wd-val" style="color:${demColor(w.avg)}">${w.avg == null ? "—" : "%" + w.avg}</div>
    </div>`).join("");
}

function renderDemandByDate(byDate) {
  const el = document.getElementById("demandByDate");
  if (!el) return;
  if (!byDate.length) { el.innerHTML = `<p class="subtle">Gelecek gün verisi yok.</p>`; return; }
  el.innerHTML = byDate.map((d) => `<div class="dem-date">
      <div class="dem-date-head"><b>${demToDot(d.date)}</b> <span class="subtle">${escapeHtml(d.weekday)}</span></div>
      ${demBar(d.avg)}
      <div class="dem-date-val" style="color:${demColor(d.avg)}">%${d.avg} · ${d.seferler} sefer</div>
    </div>`).join("");
}

function renderDemandRoutes(routes) {
  const el = document.getElementById("demandRoutes");
  if (!el) return;
  if (!routes.length) { el.innerHTML = `<p class="subtle">Rota verisi yok.</p>`; return; }
  el.innerHTML = routes.map((r) => {
    const b = r.busiestDate, q = r.quietestDate;
    return `<div class="dem-route">
      <div class="dem-route-head">
        <strong>${escapeHtml(r.origin)} - ${escapeHtml(r.destination)}</strong>
        <span class="dem-route-score" style="color:${demColor(r.avgOccupancy)}">%${r.avgOccupancy}</span>
      </div>
      ${demBar(r.avgOccupancy)}
      <div class="dem-route-meta subtle">
        <span>${r.seferCount} sefer</span>
        ${b ? `<span><b>${demToDot(b.date)}</b> ${escapeHtml(b.weekday)} (%${b.avg})</span>` : ""}
        ${q ? `<span><b>${demToDot(q.date)}</b> ${escapeHtml(q.weekday)} (%${q.avg})</span>` : ""}
      </div>
    </div>`;
  }).join("");
}

function renderDemandTop(elId, list, icon) {
  const el = document.getElementById(elId);
  if (!el) return;
  if (!list.length) { el.innerHTML = `<p class="subtle">Veri yok.</p>`; return; }
  el.innerHTML = list.map((s) => `<div class="dem-top-row">
      <div class="dem-top-main">
        <b>${escapeHtml(s.origin)} - ${escapeHtml(s.destination)}</b>
        <span class="subtle">${demToDot(s.journey_date)} ${escapeHtml(s.weekday)} · ${escapeHtml(s.departure_time)} · ${escapeHtml(s.operator)}</span>
      </div>
      <div class="dem-top-val" style="color:${demColor(s.occupancy_percent)}">${icon} %${s.occupancy_percent} <span class="subtle">(${s.sold}/${s.total_seats})</span></div>
    </div>`).join("");
}

function exportDemandCsv() {
  const data = demandState.data;
  const all = new Map();
  for (const s of [...(data?.topFull || []), ...(data?.topEmpty || [])]) {
    all.set(`${s.route_ref}|${s.journey_date}|${s.operator}|${s.departure_time}`, s);
  }
  const list = [...all.values()];
  if (!list.length) { alert("Aktarılacak veri yok."); return; }
  const headers = ["Kalkis", "Varis", "Tarih", "Gun", "Saat", "Firma", "Doluluk %", "Dolu", "Toplam"];
  const csvRows = list.map((s) => [s.origin, s.destination, demToDot(s.journey_date), s.weekday, s.departure_time, s.operator, s.occupancy_percent, s.sold, s.total_seats]
    .map((v) => String(v == null ? "" : v).replace(/;/g, ",")).join(";"));
  const csv = "﻿" + [headers.join(";"), ...csvRows].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `talep-radari-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ============================================================
// SEFER TAKIP — sefer bazli fiyat degisiklik gecmisi
// ============================================================
const seferTakipState = { wired: false };

const IL_LISTESI = ["Adana","Adıyaman","Afyonkarahisar","Ağrı","Aksaray","Amasya","Ankara","Antalya","Ardahan","Artvin","Aydın","Balıkesir","Bartın","Batman","Bayburt","Bilecik","Bingöl","Bitlis","Bolu","Burdur","Bursa","Çanakkale","Çankırı","Çorum","Denizli","Diyarbakır","Düzce","Edirne","Elazığ","Erzincan","Erzurum","Eskişehir","Gaziantep","Giresun","Gümüşhane","Hakkari","Hatay","Iğdır","Isparta","İstanbul","İzmir","Kahramanmaraş","Karabük","Karaman","Kars","Kastamonu","Kayseri","Kilis","Kırıkkale","Kırklareli","Kırşehir","Kocaeli","Konya","Kütahya","Malatya","Manisa","Mardin","Mersin","Muğla","Muş","Nevşehir","Niğde","Ordu","Osmaniye","Rize","Sakarya","Samsun","Siirt","Sinop","Sivas","Şanlıurfa","Şırnak","Tekirdağ","Tokat","Trabzon","Tunceli","Uşak","Van","Yalova","Yozgat","Zonguldak"];

// Bir metin kutusuna 81 il otomatik tamamlama ekler (input'un tam altinda, koyu temali).
function attachCityAutocomplete(input) {
  if (!input || input.dataset.acWired) return;
  input.dataset.acWired = "1";
  const norm = (s) => String(s || "").toLocaleLowerCase("tr-TR");
  const dd = document.createElement("div");
  dd.style.cssText = "position:fixed;z-index:10000;background:#1c2530;border:1px solid rgba(255,255,255,0.15);border-radius:8px;max-height:240px;overflow:auto;display:none;box-shadow:0 8px 24px rgba(0,0,0,0.45);";
  document.body.appendChild(dd);
  let items = [], active = -1;
  const position = () => {
    const r = input.getBoundingClientRect();
    dd.style.left = r.left + "px";
    dd.style.top = (r.bottom + 2) + "px";
    dd.style.minWidth = r.width + "px";
  };
  const hide = () => { dd.style.display = "none"; active = -1; };
  const render = () => {
    const q = norm(input.value);
    items = q ? IL_LISTESI.filter((c) => norm(c).includes(q)) : IL_LISTESI.slice();
    if (!items.length) { hide(); return; }
    dd.innerHTML = items.map((c, i) =>
      `<div data-i="${i}" style="padding:8px 14px;cursor:pointer;color:#e6edf3;${i === active ? "background:rgba(255,255,255,0.09);" : ""}">${c}</div>`
    ).join("");
    position();
    dd.style.display = "block";
    dd.querySelectorAll("[data-i]").forEach((el) => {
      el.addEventListener("mousedown", (e) => {
        e.preventDefault();
        input.value = items[el.dataset.i];
        hide();
        input.dispatchEvent(new Event("change"));
      });
    });
  };
  input.addEventListener("input", render);
  input.addEventListener("focus", render);
  input.addEventListener("blur", () => setTimeout(hide, 150));
  input.addEventListener("keydown", (e) => {
    if (dd.style.display === "none") return;
    if (e.key === "ArrowDown") { active = Math.min(active + 1, items.length - 1); render(); e.preventDefault(); }
    else if (e.key === "ArrowUp") { active = Math.max(active - 1, 0); render(); e.preventDefault(); }
    else if (e.key === "Enter" && active >= 0) { input.value = items[active]; hide(); e.preventDefault(); }
    else if (e.key === "Escape") { hide(); }
  });
  window.addEventListener("scroll", () => { if (dd.style.display !== "none") position(); }, true);
  window.addEventListener("resize", () => { if (dd.style.display !== "none") position(); });
}

function setupSeferTakipPanel() {
  const btn = document.getElementById("stSearchBtn");
  if (!btn) return;
  if (!seferTakipState.wired) {
    seferTakipState.wired = true;
    attachCityAutocomplete(document.getElementById("stOrigin"));
    attachCityAutocomplete(document.getElementById("stDestination"));
    btn.addEventListener("click", () => searchSeferTakip());
    const exportBtn = document.getElementById("stExportBtn");
    if (exportBtn) exportBtn.addEventListener("click", exportSeferTakipExcel);
    // "Rezerve yolcuyu dahil et": SU AN filtrelenen seferlerin GERCEK dolulugunu (koltuk haritasi,
    // baska sehirden binip gecen yolcular dahil) HEMEN cek + panele yaz. Sadece ekrandaki seferleri
    // ceker -> az yuk. Arka plan iscisini beklemeye gerek kalmaz.
    const realOccBtn = document.getElementById("stRealOccBtn");
    if (realOccBtn) realOccBtn.addEventListener("click", async () => {
      const statusEl = document.getElementById("stStatusMsg");
      const date = document.getElementById("stDate")?.value || "";
      const origin = document.getElementById("stOrigin")?.value.trim() || "";
      const destination = document.getElementById("stDestination")?.value.trim() || "";
      const operator = document.getElementById("stOperator")?.value || "";
      const old = realOccBtn.textContent;
      realOccBtn.disabled = true;
      realOccBtn.textContent = "Hesaplanıyor…";
      if (statusEl) statusEl.textContent = "Rezerve yolcular dahil gerçek doluluk çekiliyor (koltuk haritası)… bu birkaç saniye sürebilir.";
      try {
        const r = await apiFetch("/api/obilet/journey-tracking/refresh-occupancy", {
          method: "POST",
          body: JSON.stringify({ date, origin, destination, operator }),
        });
        // Taze degerleri gostermek icin listeyi yeniden yukle (secim/filtre korunur).
        await searchSeferTakip();
        if (statusEl && r && r.message) statusEl.textContent = r.message;
      } catch (err) {
        if (statusEl) statusEl.textContent = `Hata: ${err.message}`;
      } finally {
        realOccBtn.disabled = false;
        realOccBtn.textContent = old;
      }
    });
    const reset = document.getElementById("stResetBtn");
    if (reset) reset.addEventListener("click", () => {
      ["stOrigin", "stDestination"].forEach((id) => { const el = document.getElementById(id); if (el) el.value = ""; });
      const op = document.getElementById("stOperator"); if (op) op.value = "";
      const d = document.getElementById("stDate"); if (d) d.value = "";
      searchSeferTakip();
    });
    ["stOrigin", "stDestination"].forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener("keydown", (e) => { if (e.key === "Enter") searchSeferTakip(); });
      // Kolaylık: doldurulmuş alana tıklayınca metni komple seç — tek tuşla değiştirilebilsin
      // (kullanıcı elle silmek zorunda kalmasın). Boş alana dokunma.
      const selectAll = () => { if (el.value) setTimeout(() => { try { el.select(); } catch (_) {} }, 0); };
      el.addEventListener("focus", selectAll);
      el.addEventListener("click", selectAll);
    });
    // Yön değiştir: Kalkış - Varış yer değiştir ve otomatik tekrar ara.
    const swapBtn = document.getElementById("stSwapBtn");
    if (swapBtn) swapBtn.addEventListener("click", () => {
      const o = document.getElementById("stOrigin");
      const d = document.getElementById("stDestination");
      if (!o || !d) return;
      const tmp = o.value; o.value = d.value; d.value = tmp;
      searchSeferTakip();
    });
    // Admin: satırdaki ile o seferin GERÇEK dolulugunu HEMEN çek (event delegation).
    const stBody = document.getElementById("stTableBody");
    if (stBody) stBody.addEventListener("click", async (e) => {
      const b = e.target.closest(".st-seat-refresh");
      if (!b) return;
      e.preventDefault();
      const { tid, date, time, op } = b.dataset;
      const old = b.textContent;
      b.disabled = true; b.textContent = "";
      try {
        const r = await apiFetch("/api/obilet/seat-refresh", {
          method: "POST",
          body: JSON.stringify({ targetId: Number(tid), date, time, operator: op }),
        });
        const cell = b.closest("td");
        if (r.ok && cell) {
          const pct = r.total ? Math.round((r.sold / r.total) * 100) : 0;
          cell.innerHTML = `<b style="color:${occColor(pct)}">${r.sold}/${r.total}</b> <span style="opacity:.7">(%${pct})</span> <span title="Gerçek koltuk haritasından" style="color:#2ecc71;"></span>`;
        } else {
          b.textContent = ""; b.title = r.message || "Alınamadı";
          setTimeout(() => { b.textContent = old; b.disabled = false; }, 2500);
        }
      } catch (err) {
        b.textContent = ""; b.title = err.message || "Hata";
        setTimeout(() => { b.textContent = old; b.disabled = false; }, 2500);
      }
    });

    // Firma listesini bir kez doldur (bos aramayla).
    searchSeferTakip(true);
  }
}

async function searchSeferTakip(initial = false) {
  const body = document.getElementById("stTableBody");
  const statusEl = document.getElementById("stStatusMsg");
  if (!body) return;
  const date = document.getElementById("stDate")?.value || "";
  const origin = document.getElementById("stOrigin")?.value.trim() || "";
  const destination = document.getElementById("stDestination")?.value.trim() || "";
  const operator = document.getElementById("stOperator")?.value || "";
  if (statusEl) statusEl.textContent = "Aranıyor...";
  try {
    const params = new URLSearchParams();
    if (date) params.set("date", date);
    if (origin) params.set("origin", origin);
    if (destination) params.set("destination", destination);
    if (operator) params.set("operator", operator);
    const data = await apiFetch(`/api/obilet/journey-tracking?${params.toString()}`);
    // Firma dropdown'i doldur (secim korunur).
    const opSel = document.getElementById("stOperator");
    if (opSel && Array.isArray(data.operators)) {
      const cur = opSel.value;
      opSel.innerHTML = `<option value="">Tüm firmalar</option>` +
        data.operators.map((op) => `<option value="${occEsc(op)}">${occEsc(op)}</option>`).join("");
      if (cur && data.operators.includes(cur)) opSel.value = cur;
    }
    seferTakipState.journeys = data.journeys || [];
    renderSeferTakip(seferTakipState.journeys);
    if (statusEl) {
      statusEl.textContent = seferTakipState.journeys.length
        ? `${seferTakipState.journeys.length} sefer bulundu (son 3 günün fiyat değişiklikleri).`
        : "Bu kritere uygun fiyat değişikliği yok. (Not: sadece son 3 gün tutulur.)";
    }
  } catch (err) {
    if (statusEl) statusEl.textContent = `Hata: ${err.message}`;
  }
}

const ST_PAGE = 50; // Kademeli yukleme adim boyu (kasmayi onlemek icin hepsini birden cizmiyoruz)

// Durak adindan " Otogari/Otogan/Terminali" ekini kaldirir: "Adana Otogari" -> "Adana", "Esenler Otogari" -> "Esenler".
function stCleanStop(s) {
  let c = String(s || "").replace(/\s+(otogar[ıi]?|otogan|terminal[ıi]?)$/i, "").trim();
  // İstanbul'un ilçe terminalleri (Esenler, Alibeyköy, Dudullu...) -> "İstanbul" (kullanıcıya şehir adı net gelsin).
  if (/^(esenler|alibeyk[öo]y|dudullu|harem|ata[şs]ehir|bayrampa[şs]a|b[üu]y[üu]k[çc]ekmece|kad[ıi]k[öo]y)$/i.test(c)) return "İstanbul";
  return c;
}
// Güzergah hücresi: ana sefer (keşfedilen gerçek kalkış) öncelikli, yoksa oBilet güzergahı.
function stGuzergahCell(j) {
  if (j.parentOrigin) {
    const from = occEsc(stCleanStop(j.parentOrigin));
    const t = j.parentOriginTime ? ` <span style="opacity:.6;font-size:.85em">${occEsc(j.parentOriginTime)}</span>` : "";
    const dest = occEsc(stCleanStop(j.parentDest) || stCleanStop(j.routeTo) || (j.destination || ""));
    return `<b>${from}</b>${t} <span style="opacity:.55">→</span> <b>${dest}</b>`;
  }
  if (j.routeFrom || j.routeTo) {
    return `<b>${occEsc(stCleanStop(j.routeFrom))}</b> <span style="opacity:.55">→</span> <b>${occEsc(stCleanStop(j.routeTo))}</b>`;
  }
  return `<span style="opacity:.45">-</span>`;
}
// Tek bir sefer satirinin HTML'i.
function stRowHtml(j) {
  // Fiyat geçmişi: eski - ... - güncel, renkli oklarla.
  const seq = (j.prices || []).map((p, i, arr) => {
    let color = "#c9d1d9";
    if (i > 0) color = p < arr[i - 1] ? "#2ecc71" : p > arr[i - 1] ? "#e74c3c" : "#c9d1d9";
    return `<b style="color:${color}">${p}</b>`;
  }).join(" <span style='opacity:.5'>-</span> ");
  const changeBadge = `<span style="background:#31507a;color:#fff;border-radius:10px;padding:1px 8px;font-size:0.8rem;">${j.changeCount}x</span>`;
  // Doluluk: yolcu / toplam koltuk + yuzde renkli.
  let dolCell = "<span style='opacity:.5'>-</span>";
  if (j.totalSeats != null && j.yolcu != null) {
    const pct = j.totalSeats ? Math.round((j.yolcu / j.totalSeats) * 100) : 0;
    dolCell = `<b style="color:${occColor(pct)}">${j.yolcu}/${j.totalSeats}</b> <span style="opacity:.7">(%${pct})</span>${occSourceBadge(j.occSource, j.occLastUpdated)}`;
  }
  // Güzergah: ANA SEFER (feeder keşfiyle bulunan gerçek kalkış) öncelikli — örn. "Şanlıurfa 18:30 → Ankara".
  // Yoksa oBilet'in kendi verdiği güzergaha (stops[0]→son) düş. İkisi de yoksa "-".
  const guzergahCell = stGuzergahCell(j);
  // "Anlik cek" butonu kaldirildi — koltuk/plaka artik dogru geldigi icin gereksiz.
  return `<tr>
    <td>${occToDot(j.journey_date)}</td>
    <td>${occEsc(j.departure_time || "")}</td>
    <td>${occEsc(j.operator || "")}</td>
    <td>
      <b>${occEsc((j.origin || "").toUpperCase())} - ${occEsc((j.destination || "").toUpperCase())}</b>
      ${j.departure_stop ? `<div style="font-size:0.78rem;opacity:0.6;">${occEsc(j.departure_stop)}</div>` : ""}
    </td>
    <td style="text-align:center;">${changeBadge}</td>
    <td>${seq}</td>
    <td><b>${j.currentPrice} TL</b></td>
    <td>${dolCell}</td>
    <td style="font-size:0.82rem;opacity:0.8;">${occEsc(j.lastChangedAt || "")}</td>
    <td>${guzergahCell}</td>
  </tr>`;
}

// "+50 daha göster" satiri (kalan sayisi ile). Kalan yoksa bos.
function stLoadMoreRowHtml(total, shown) {
  const remaining = total - shown;
  if (remaining <= 0) return "";
  const next = Math.min(ST_PAGE, remaining);
  return `<tr id="stLoadMoreRow"><td colspan="10" style="text-align:center;padding:0.9rem;">
    <button id="stLoadMore" class="btn btn-primary" type="button">+${next} daha göster <span style="opacity:.75">(${remaining} kaldı)</span></button>
  </td></tr>`;
}

// Load-more butonunu bagla: mevcut satirlara EKLER (bastan cizmez -> kasmaz).
function stBindLoadMore(journeys) {
  const more = document.getElementById("stLoadMore");
  if (!more) return;
  more.addEventListener("click", () => {
    const body = document.getElementById("stTableBody");
    if (!body) return;
    document.getElementById("stLoadMoreRow")?.remove();
    const start = seferTakipState.shown || 0;
    const end = Math.min(start + ST_PAGE, journeys.length);
    body.insertAdjacentHTML("beforeend", journeys.slice(start, end).map(stRowHtml).join(""));
    seferTakipState.shown = end;
    body.insertAdjacentHTML("beforeend", stLoadMoreRowHtml(journeys.length, end));
    stBindLoadMore(journeys);
  });
}

function renderSeferTakip(journeys) {
  const body = document.getElementById("stTableBody");
  if (!body) return;
  if (!journeys.length) {
    body.innerHTML = `<tr><td colspan="10" style="text-align:center;color:#888;">Kayıt yok</td></tr>`;
    seferTakipState.shown = 0;
    return;
  }
  // Ilk sayfayi ciz + kalan varsa "+50" butonu. Her aramada bastan baslar.
  const end = Math.min(ST_PAGE, journeys.length);
  body.innerHTML = journeys.slice(0, end).map(stRowHtml).join("") + stLoadMoreRowHtml(journeys.length, end);
  seferTakipState.shown = end;
  stBindLoadMore(journeys);
}

function exportSeferTakipExcel() {
  const journeys = seferTakipState.journeys || [];
  if (!journeys.length) { alert("Aktarılacak veri yok. Önce Ara'ya basın."); return; }
  const XLSX = window.XLSX;
  if (!XLSX) { alert("Excel kütüphanesi yüklenemedi."); return; }
  const aoa = [[
    "Sefer Tarihi", "Saat", "Firma", "Güzergah", "Kalkış Durağı",
    "Değişiklik", "Fiyat Geçmişi", "Güncel (TL)", "Yolcu", "Koltuk", "Doluluk %", "Son Değişiklik", "Güzergah",
  ]];
  for (const j of journeys) {
    const pct = (j.totalSeats && j.yolcu != null) ? Math.round((j.yolcu / j.totalSeats) * 100) : "";
    aoa.push([
      occToDot(j.journey_date),
      j.departure_time || "",
      j.operator || "",
      `${(j.origin || "").toUpperCase()} - ${(j.destination || "").toUpperCase()}`,
      j.departure_stop || "",
      `${j.changeCount}x`,
      (j.prices || []).join(" - "),
      j.currentPrice != null ? j.currentPrice : "",
      j.yolcu != null ? j.yolcu : "",
      j.totalSeats != null ? j.totalSeats : "",
      pct === "" ? "" : pct / 100,
      j.lastChangedAt || "",
      j.parentOrigin
        ? `${stCleanStop(j.parentOrigin)}${j.parentOriginTime ? " " + j.parentOriginTime : ""} → ${stCleanStop(j.parentDest) || stCleanStop(j.routeTo) || (j.destination || "")}`
        : ((j.routeFrom || j.routeTo) ? `${stCleanStop(j.routeFrom)} → ${stCleanStop(j.routeTo)}` : ""),
    ]);
  }
  const ws = XLSX.utils.aoa_to_sheet(aoa);
  ws["!cols"] = [{ wch: 12 }, { wch: 7 }, { wch: 22 }, { wch: 20 }, { wch: 16 }, { wch: 10 }, { wch: 30 }, { wch: 11 }, { wch: 8 }, { wch: 8 }, { wch: 10 }, { wch: 20 }, { wch: 12 }];
  // Doluluk % kolonunu (K) yuzde formatina cevir.
  const range = XLSX.utils.decode_range(ws["!ref"]);
  for (let r = 1; r <= range.e.r; r++) {
    const cell = ws[XLSX.utils.encode_cell({ r, c: 10 })];
    if (cell && typeof cell.v === "number") cell.z = "0%";
  }
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Sefer Takip");
  XLSX.writeFile(wb, `sefer-takip-${new Date().toISOString().slice(0, 10)}.xlsx`);
}

function openObiletEditModal(t) {
  document.getElementById("obiletEditBackdrop")?.remove();
  const esc = (s) => String(s == null ? "" : s).replace(/"/g, "&quot;");
  const backdrop = document.createElement("div");
  backdrop.id = "obiletEditBackdrop";
  backdrop.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;z-index:9999;padding:1rem;";
  backdrop.innerHTML = `
    <div style="background:#1c2530;border:1px solid rgba(255,255,255,0.12);border-radius:14px;max-width:560px;width:100%;max-height:90vh;overflow:auto;padding:1.4rem;">
      <h4 style="margin:0 0 1rem;">Hat Düzenle — ${esc((t.origin || "").toUpperCase())} - ${esc((t.destination || "").toUpperCase())}</h4>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:0.8rem;">
        <label style="display:flex;flex-direction:column;gap:0.3rem;"><span>Kalkış</span><input id="oeOrigin" type="text" value="${esc(t.origin)}" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;"><span>Varış</span><input id="oeDest" type="text" value="${esc(t.destination)}" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;"><span>Başlangıç Tarihi</span><input id="oeDate" type="date" value="${esc(t.date)}" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;"><span>Bitiş Tarihi</span><input id="oeEndDate" type="date" value="${esc(t.end_date || t.date)}" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;grid-column:1/3;"><span>Firmalar (virgülle ayır)</span><input id="oeOperators" type="text" value="${esc(t.operators)}" placeholder="Enver Geçgel Turizm, Kamil Koç" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;grid-column:1/3;"><span>Kalkış Durağı Filtresi (opsiyonel)</span><input id="oeStop" type="text" value="${esc(t.departure_stop_filter)}" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;grid-column:1/3;"><span>E-posta Bildirimleri (virgülle)</span><input id="oeEmail" type="text" value="${esc(t.email_notifications)}" /></label>
        <label style="display:flex;flex-direction:column;gap:0.3rem;"><span>oBilet Route ID (opsiyonel)</span><input id="oeRouteId" type="text" value="${esc(t.route_id)}" placeholder="595-356" /></label>
        <label style="display:flex;align-items:center;gap:0.5rem;align-self:end;"><input id="oeActive" type="checkbox" ${t.is_active ? "checked" : ""} /> Aktif</label>
      </div>
      <p id="oeMsg" style="min-height:1.2em;margin:0.6rem 0;"></p>
      <div style="display:flex;gap:0.6rem;justify-content:flex-end;">
        <button id="oeCancel" class="btn btn-ghost" type="button">İptal</button>
        <button id="oeSave" class="btn btn-primary" type="button">Kaydet</button>
      </div>
    </div>`;
  document.body.appendChild(backdrop);
  const close = () => backdrop.remove();
  backdrop.addEventListener("click", (e) => { if (e.target === backdrop) close(); });
  document.getElementById("oeCancel").addEventListener("click", close);
  document.getElementById("oeSave").addEventListener("click", async () => {
    const val = (id) => document.getElementById(id).value.trim();
    const body = {
      origin: val("oeOrigin"),
      destination: val("oeDest"),
      date: document.getElementById("oeDate").value,
      endDate: document.getElementById("oeEndDate").value,
      operators: val("oeOperators"),
      departureStopFilter: val("oeStop"),
      emailNotifications: val("oeEmail"),
      routeId: val("oeRouteId"),
      isActive: document.getElementById("oeActive").checked,
    };
    const msg = document.getElementById("oeMsg");
    msg.style.color = "#e0796f";
    msg.textContent = "Kaydediliyor...";
    try {
      await apiFetch(`/api/obilet/targets/${t.id}`, { method: "PATCH", body: JSON.stringify(body) });
      msg.style.color = "#27ae60";
      msg.textContent = "Kaydedildi. Fiyatlar arka planda güncelleniyor...";
      setTimeout(async () => { close(); await renderObiletTargets(); }, 700);
    } catch (err) {
      msg.style.color = "#e0796f";
      msg.textContent = "Hata: " + err.message;
    }
  });
}

async function renderObiletTargets() {
  const listEl = document.getElementById("obiletTargetsList");
  if (!listEl) return;

  listEl.innerHTML = `<div class="obilet-loading">Yükleniyor...</div>`;
  try {
    const result = await apiFetch("/api/obilet/targets");
    obiletState.targets = result.targets || [];
    renderObiletTargetCards(listEl);
  } catch (err) {
    listEl.innerHTML = `<div class="obilet-empty">Takip listesi yüklenemedi: ${err.message}</div>`;
  }
}

function renderObiletTargetCards(listEl) {
  if (!obiletState.targets.length) {
    listEl.innerHTML = `
      <div class="obilet-empty">
        <span></span>
        <p>Henüz takip edilen hat yok. Aşağıdaki formu kullanarak yeni bir hat ekleyin.</p>
      </div>`;
    return;
  }

  const isAdmin = !!(state.currentUser && state.currentUser.isAdmin);
  listEl.innerHTML = obiletState.targets.map(t => {
    const dateFormatted = (t.date || "").split("-").reverse().join(".");
    const endDateFormatted = ((t.end_date || t.date) || "").split("-").reverse().join(".");
    const periodLabel = endDateFormatted && endDateFormatted !== dateFormatted
      ? `${dateFormatted} - ${endDateFormatted}`
      : dateFormatted;
    const departureStopFilter = String(t.departure_stop_filter || "").trim();
    const emails = (t.email_notifications || "").split(",").map(e => e.trim()).filter(Boolean);
    const syncStatus = String(t.last_sync_status || "").trim();
    const syncAt = String(t.last_sync_at || "").trim();
    const statusClass = /hata|bulunamadi|alinmadi/i.test(syncStatus) ? "obilet-passive" : "obilet-active";
    return `
      <div class="obilet-target-card" data-id="${t.id}">
        <div class="obilet-card-header">
          <div class="obilet-card-route">
            <span class="obilet-route-icon"></span>
            <div>
              <strong>${t.origin.toUpperCase()} - ${t.destination.toUpperCase()}</strong>
              <span class="obilet-date-badge">${periodLabel}</span>
            </div>
          </div>
          <div class="obilet-card-actions">
            <button class="btn btn-sm btn-ghost obilet-refresh-btn" data-id="${t.id}" title="Bu hattı hemen güncelle">Güncelle</button>
            ${isAdmin ? `<button class="btn btn-sm btn-primary obilet-priority-btn" data-id="${t.id}" title="Sıra beklemeden hemen tara (admin)">Anlık Tara</button>` : ""}
            ${isAdmin ? `<button class="btn btn-sm btn-ghost obilet-seatprobe-btn" data-id="${t.id}" title="Bir seferin GERÇEK koltuk haritasını çekip liste değeriyle karşılaştır (test)">Koltuk Testi</button>` : ""}
            <button class="btn btn-sm btn-ghost obilet-edit-btn" data-id="${t.id}" title="Düzenle">Düzenle</button>
            <button class="btn btn-sm btn-success obilet-excel-btn" data-id="${t.id}" title="Excel İndir">Excel</button>
            <button class="btn btn-sm btn-ghost obilet-expand-btn" data-id="${t.id}" title="Fiyatları Göster">Fiyatlar</button>
            <button class="btn btn-sm btn-danger obilet-delete-btn" data-id="${t.id}" title="Sil">Sil</button>
          </div>
        </div>
        <div class="obilet-card-meta">
          <span class="obilet-tag">${t.operators}</span>
          ${departureStopFilter ? `<span class="obilet-tag">${departureStopFilter}</span>` : ""}
          <span class="obilet-tag">${emails.length ? emails.join(", ") : "-"}</span>
          ${t.created_by ? `<span class="obilet-tag" title="Hatti ekleyen kullanici">${t.created_by}</span>` : ""}
          <span class="obilet-tag ${t.is_active ? 'obilet-active' : 'obilet-passive'}">
            ${t.is_active ? 'Aktif' : 'Pasif'}
          </span>
          <span class="obilet-tag ${statusClass}">${syncStatus || "Henüz kontrol edilmedi"}</span>
          ${syncAt ? `<span class="obilet-tag">${syncAt}</span>` : ""}
        </div>
        <div class="obilet-prices-area hidden" id="pricesArea-${t.id}">
          <div class="obilet-loading">Fiyatlar yükleniyor...</div>
        </div>
      </div>`;
  }).join("");

  // Event listeners
  listEl.querySelectorAll(".obilet-delete-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      if (!confirm("Bu takip hattını silmek istediğinize emin misiniz?")) return;
      try {
        await apiFetch(`/api/obilet/targets/${id}`, { method: "DELETE" });
        await renderObiletTargets();
      } catch (err) {
        alert("Silme işlemi başarısız: " + err.message);
      }
    });
  });

  listEl.querySelectorAll(".obilet-edit-btn").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const t = (obiletState.targets || []).find(x => String(x.id) === String(btn.dataset.id));
      if (t) openObiletEditModal(t);
    });
  });

  listEl.querySelectorAll(".obilet-priority-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      const oldText = btn.textContent;
      btn.disabled = true;
      btn.textContent = "Başlatılıyor...";
      try {
        const r = await apiFetch(`/api/obilet/targets/${id}/priority-refresh`, { method: "POST" });
        btn.textContent = "Tarıyor...";
        const statusEl = document.getElementById("obiletActionStatus");
        if (statusEl) { statusEl.style.color = "#27ae60"; statusEl.textContent = r.message || "Öncelikli tarama başlatıldı."; }
        // Birkaç saniye sonra listeyi tazele (sonuç düşsün).
        setTimeout(() => renderObiletTargets().catch(() => null), 15000);
        setTimeout(() => renderObiletTargets().catch(() => null), 40000);
      } catch (err) {
        btn.disabled = false;
        btn.textContent = oldText;
        alert("Öncelikli tarama başlatılamadı: " + err.message);
      }
    });
  });

  listEl.querySelectorAll(".obilet-seatprobe-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      const target = (obiletState.targets || []).find(t => String(t.id) === String(id));
      if (!target) return;
      const date = prompt("Hangi tarih? (YYYY-AA-GG)", target.date || "");
      if (!date) return;
      const time = prompt("Hangi sefer saati? (örn 00:30 — boş bırakırsan ilk sefer)", "");
      const operator = prompt("Hangi firma? (örn Enver Geçgel — boş bırakırsan o saatteki ilk firma)", (target.operators || "").split(",")[0].trim());
      const oldText = btn.textContent;
      btn.disabled = true;
      btn.textContent = "Çekiliyor...";
      try {
        const params = new URLSearchParams({ date });
        if (time) params.set("time", time.trim());
        if (operator) params.set("operator", operator.trim());
        const r = await apiFetch(`/api/obilet/targets/${id}/seatmap-probe?${params.toString()}`, { method: "POST" });
        let list = "";
        if (Array.isArray(r.seferler) && r.seferler.length) {
          list = "\n\nBu saatteki firmalar:\n" + r.seferler.map(s => `${s.firma}: ${(s.total!=null&&s.avail!=null)?(s.total-s.avail):"?"}/${s.total} dolu`).join("\n");
        }
        const noteLine = r.note ? `\n\n⚠ ${r.note}` : "";
        alert(`KOLTUK TESTİ SONUCU\n\n${r.ozet}${noteLine}${list}\n\n(Detay: Railway loglarında "[SeatProbe]". Liste ile gerçek harita farklıysa liste güvenilmez demektir.)`);
      } catch (err) {
        alert("Koltuk testi başarısız: " + err.message);
      } finally {
        btn.disabled = false;
        btn.textContent = oldText;
      }
    });
  });

  listEl.querySelectorAll(".obilet-refresh-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      const target = obiletState.targets.find(t => t.id == id);
      if (!target) return;

      const originalText = btn.textContent || "Güncelle";
      try {
        btn.disabled = true;
        btn.textContent = "...";
        const result = await apiFetch(`/api/obilet/targets/${id}/refresh`, { method: "POST" });

        setTimeout(() => {
          btn.textContent = originalText;
          btn.disabled = false;
        }, 2000);

        // Backend mesajı: "İşleminiz sıraya alındı..." gibi açıklayıcı
        alert(result.message || `${target.origin} - ${target.destination} güncelleniyor...`);

        // Periyodik liste yenileme: kuyrukta beklediği için kesin süre yok,
        // 15s sonra bir kere dene, 30s sonra tekrar dene (status güncellenmiş olur).
        setTimeout(() => renderObiletTargets().catch(() => null), 15000);
        setTimeout(() => renderObiletTargets().catch(() => null), 30000);
      } catch (err) {
        // 409 = "zaten kuyrukta" uyarisi (hata degil, bilgilendirme)
        const isQueueWarning = err.payload?.queued === false;
        setTimeout(() => {
          btn.textContent = originalText;
          btn.disabled = false;
        }, 2000);
        if (isQueueWarning) {
          alert(err.message);
        } else {
          alert("Güncelleme hatasi: " + err.message);
        }
      }
    });
  });

  // Excel İndir butonu
  listEl.querySelectorAll(".obilet-excel-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      const target = obiletState.targets.find(t => t.id == id);
      if (!target) return;

      const originalText = btn.textContent;
      try {
        btn.disabled = true;
        btn.textContent = "";
        
        // Fiyat geçmişini al
        const prices = await apiFetch(`/api/obilet/targets/${id}/prices`);
        
        if (!prices || prices.length === 0) {
          alert("Bu hat için henüz fiyat verisi yok.");
          btn.textContent = originalText;
          btn.disabled = false;
          return;
        }
        
        // CSV formatına çevir
        const csvContent = generateCSV(prices, target);
        
        // İndir
        const blob = new Blob(["\uFEFF" + csvContent], { type: "text/csv;charset=utf-8;" });
        const link = document.createElement("a");
        const fileName = `obilet_${target.origin}_${target.destination}_${new Date().toISOString().split('T')[0]}.csv`;
        link.href = URL.createObjectURL(blob);
        link.download = fileName;
        link.click();
        
        btn.textContent = "";
        setTimeout(() => {
          btn.textContent = originalText;
          btn.disabled = false;
        }, 2000);
      } catch (err) {
        btn.textContent = "";
        setTimeout(() => {
          btn.textContent = originalText;
          btn.disabled = false;
        }, 2000);
        alert("Excel indirme hatası: " + err.message);
      }
    });
  });

  listEl.querySelectorAll(".obilet-expand-btn").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      const pricesArea = document.getElementById(`pricesArea-${id}`);
      if (!pricesArea) return;

      if (!pricesArea.classList.contains("hidden")) {
        pricesArea.classList.add("hidden");
        return;
      }

      pricesArea.classList.remove("hidden");
      pricesArea.innerHTML = `<div class="obilet-loading">Fiyatlar yükleniyor...</div>`;

      try {
        const result = await apiFetch(`/api/obilet/prices/${id}`);
        const prices = result.prices || [];
        if (!prices.length) {
          const syncText = result.targetStatus?.text ? ` Son durum: ${result.targetStatus.text}` : "";
          const syncAt = result.targetStatus?.at ? ` (${result.targetStatus.at})` : "";
          pricesArea.innerHTML = `<p class="obilet-empty-sm">Henüz fiyat verisi yok. "Şimdi Güncelle" butonunu kullanın.${syncText}${syncAt}</p>`;
          return;
        }

        pricesArea.innerHTML = `
          <table class="obilet-prices-table">
            <thead><tr><th>Tarih</th><th>Firma</th><th>Kalkış</th><th>Varış</th><th>Saat</th><th>Fiyat</th><th>Son Güncelleme</th></tr></thead>
            <tbody>
              ${prices.map(p => `
                <tr>
                  <td>${(p.journey_date || "").split("-").reverse().join(".") || "-"}</td>
                  <td><strong>${p.operator}</strong></td>
                  <td>${p.departure_stop || "-"}</td>
                  <td>${p.arrival_stop || "-"}</td>
                  <td>${p.departure_time}</td>
                  <td class="obilet-price-cell">₺${p.price.toLocaleString("tr-TR")}</td>
                  <td class="obilet-updated-cell">${p.last_updated}</td>
                </tr>`).join("")}
            </tbody>
          </table>`;
      } catch (err) {
        pricesArea.innerHTML = `<p class="obilet-empty-sm">Fiyatlar yüklenemedi: ${err.message}</p>`;
      }
    });
  });
}

function setupObiletForm() {
  const form = document.getElementById("obiletAddForm");
  if (!form) return;

  // Remove existing listeners by replacing element
  const newForm = form.cloneNode(true);
  form.parentNode.replaceChild(newForm, form);

  const rowsWrap = newForm.querySelector("#obiletRows");
  const addRowBtn = newForm.querySelector("#obiletAddRowBtn");
  const msgEl = document.getElementById("obiletFormMsg");

  const getRows = () => Array.from(newForm.querySelectorAll(".obilet-row"));

  const updateRowTitles = () => {
    const rows = getRows();
    rows.forEach((row, idx) => {
      const title = row.querySelector(".obilet-row-title");
      if (title) {
        title.textContent = `Hat ${idx + 1}`;
      }
      const removeBtn = row.querySelector(".obilet-remove-row");
      if (removeBtn) {
        removeBtn.disabled = rows.length === 1;
      }
    });
  };

  const resetRowValues = (row) => {
    row.querySelectorAll("input").forEach((input) => {
      if (input.type === "hidden") {
        input.value = "";
        return;
      }
      if (input.type === "checkbox") {
        input.checked = false;
        return;
      }
      input.value = "";
    });
    const selectedEl = row.querySelector(".obilet-operator-selected");
    if (selectedEl) {
      selectedEl.textContent = "Firma seciniz...";
      selectedEl.classList.add("is-placeholder");
    }
    row.querySelector(".obilet-operator-picker")?.classList.remove("open");
    const optionsEl = row.querySelector(".obilet-operator-options");
    if (optionsEl) {
      optionsEl.innerHTML = '<div class="obilet-loading">Firmalar yukleniyor...</div>';
    }
  };

  const bindRowDateSync = (row) => {
    const startDateInput = row.querySelector(".obilet-date");
    const endDateInput = row.querySelector(".obilet-end-date");
    if (!startDateInput || !endDateInput) return;
    startDateInput.addEventListener("change", () => {
      if (!endDateInput.value || endDateInput.value < startDateInput.value) {
        endDateInput.value = startDateInput.value;
      }
    });
    if (startDateInput.value && !endDateInput.value) {
      endDateInput.value = startDateInput.value;
    }
  };

  const initRow = (row) => {
    bindRowDateSync(row);
    setupObiletOperatorPicker(row);
    const removeBtn = row.querySelector(".obilet-remove-row");
    if (removeBtn) {
      removeBtn.addEventListener("click", () => {
        row.remove();
        updateRowTitles();
      });
    }
  };

  getRows().forEach((row) => initRow(row));
  updateRowTitles();

  if (addRowBtn && rowsWrap) {
    addRowBtn.addEventListener("click", () => {
      const rows = getRows();
      const template = rows[0];
      if (!template) return;
      const clone = template.cloneNode(true);
      resetRowValues(clone);
      rowsWrap.appendChild(clone);
      initRow(clone);
      updateRowTitles();
    });
  }

  newForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const submitBtn = newForm.querySelector('[type="submit"]');

    const rows = getRows();
    if (!rows.length) {
      msgEl.style.color = "#d64545";
      msgEl.textContent = "En az bir hat eklemelisiniz.";
      return;
    }

    const payloads = [];
    for (const [index, row] of rows.entries()) {
      const origin = row.querySelector(".obilet-origin")?.value.trim() || "";
      const destination = row.querySelector(".obilet-destination")?.value.trim() || "";
      const date = row.querySelector(".obilet-date")?.value.trim() || "";
      const rawEndDate = row.querySelector(".obilet-end-date")?.value.trim() || "";
      const endDate = rawEndDate || date;
      const departureStopFilter = row.querySelector(".obilet-departure-filter")?.value.trim() || "";
      const routeId = row.querySelector(".obilet-route-id")?.value.trim() || "";
      const operators = row.querySelector(".obilet-operators-hidden")?.value.trim() || "";
      const emails = row.querySelector(".obilet-emails")?.value.trim() || "";

      if (!origin || !destination || !date || !endDate || !operators) {
        msgEl.style.color = "#d64545";
        msgEl.textContent = `Hat ${index + 1}: Zorunlu alanlari doldurun.`;
        return;
      }
      if (endDate < date) {
        msgEl.style.color = "#d64545";
        msgEl.textContent = `Hat ${index + 1}: Bitis tarihi baslangic tarihinden once olamaz.`;
        return;
      }
      if (routeId && !/^\d+-\d+$/.test(routeId)) {
        msgEl.style.color = "#d64545";
        msgEl.textContent = `Hat ${index + 1}: oBilet Route ID formati gecersiz (orn: 595-356).`;
        return;
      }

      payloads.push({
        origin,
        destination,
        date,
        endDate,
        departureStopFilter,
        routeId,
        operators,
        emailNotifications: emails,
      });
    }

    try {
      submitBtn.disabled = true;
      if (addRowBtn) addRowBtn.disabled = true;
      submitBtn.textContent = "Ekleniyor...";
      msgEl.textContent = "";

      let successCount = 0;
      const errors = [];
      for (const [index, payload] of payloads.entries()) {
        try {
          await apiFetch("/api/obilet/targets", {
            method: "POST",
            body: JSON.stringify(payload),
          });
          successCount += 1;
        } catch (err) {
          errors.push(`Hat ${index + 1}: ${err.message}`);
        }
      }

      if (errors.length) {
        msgEl.style.color = "#d64545";
        msgEl.textContent = `Bazilari eklenemedi (${errors.length}). ${errors[0]}`;
      } else {
        msgEl.style.color = "#1f7a1f";
        msgEl.textContent = `${successCount} hat basariyla eklendi! Fiyatlar arka planda cekiliyor...`;
        const rows = getRows();
        const firstRow = rows[0] || null;
        rows.slice(1).forEach((row) => row.remove());
        if (firstRow) {
          const fresh = firstRow.cloneNode(true);
          resetRowValues(fresh);
          firstRow.replaceWith(fresh);
          initRow(fresh);
        }
        updateRowTitles();
        await renderObiletTargets();
        setTimeout(() => { if (msgEl) msgEl.textContent = ""; }, 5000);
      }
    } catch (err) {
      msgEl.style.color = "#d64545";
      msgEl.textContent = "Hata: " + err.message;
    } finally {
      submitBtn.disabled = false;
      if (addRowBtn) addRowBtn.disabled = false;
      submitBtn.textContent = "Hatlari Ekle";
    }
  });
}


