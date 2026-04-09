const TOKEN_KEY = "bus_auth_token_v2";
const THEME_KEY = "bus_theme_v1";

const MENUS = [
  { key: "dashboard", label: "Genel Panel" },
  { key: "routes", label: "Bakanlik Fiyati" },
  { key: "pricing", label: "Fiyat Yukleme" },
  { key: "reports", label: "Tek Yon Fiyatlar" },
  { key: "reporting", label: "Raporlama" },
  { key: "oneops", label: "OneOps Giris" },
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
  loginMessage: document.getElementById("loginMessage"),
  menuList: document.getElementById("menuList"),
  contentCard: document.querySelector(".content"),
  activeTitle: document.getElementById("activeTitle"),
  logoutBtn: document.getElementById("logoutBtn"),
  currentUserLabel: document.getElementById("currentUserLabel"),
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
  reportingSummary: document.getElementById("reportingSummary"),
  reportingTableBody: document.getElementById("reportingTableBody"),
  controlOpenLoginBtn: document.getElementById("controlOpenLoginBtn"),
  controlOpenOpsBtn: document.getElementById("controlOpenOpsBtn"),
  controlBaseUrlInput: document.getElementById("controlBaseUrlInput"),
  controlLoginUrlInput: document.getElementById("controlLoginUrlInput"),
  controlCookieInput: document.getElementById("controlCookieInput"),
  controlCsrfInput: document.getElementById("controlCsrfInput"),
  controlSessionTestUrlInput: document.getElementById("controlSessionTestUrlInput"),
  controlSaveBtn: document.getElementById("controlSaveBtn"),
  controlTestBtn: document.getElementById("controlTestBtn"),
  controlReloadBtn: document.getElementById("controlReloadBtn"),
  controlStatusMsg: document.getElementById("controlStatusMsg"),
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
};

const state = {
  token: localStorage.getItem(TOKEN_KEY) || "",
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
  theme: localStorage.getItem(THEME_KEY) || "light",
  lastOcrPairs: [],
  lastOcrOrigin: "",
  lastOcrDestinationPool: [],
  lastSuspiciousRows: [],
  panelSwitchSeq: 0,
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
    localStorage.setItem(TOKEN_KEY, state.token);
  } else {
    localStorage.removeItem(TOKEN_KEY);
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
  dom.currentUserLabel.textContent = `Aktif: ${state.currentUser.username}${
    state.currentUser.isAdmin ? " (Admin)" : ""
  }`;

  renderMenu(state.currentUser);
  const first = availableMenus(state.currentUser)[0]?.key || "dashboard";
  activatePanel(first);
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

  const query = normalizeSearchText(state.reportingQuery || "");
  const allRows = Array.isArray(state.reportingRows) ? state.reportingRows : [];
  const rows = query
    ? allRows.filter((row) => normalizeSearchText(`${row.routeLabel} ${row.vehiclePlate || ""}`).includes(query))
    : allRows;

  dom.reportingTableBody.innerHTML = "";
  if (!rows.length) {
    dom.reportingTableBody.innerHTML = '<tr><td colspan="8">Bu tarihte rapor kaydi yok. Gunluk raporu cek butonuna bas.</td></tr>';
    dom.reportingSummary.textContent = `${state.reportingDate || "-"} icin kayit bulunamadi.`;
    return;
  }

  const delayedCount = rows.filter((row) => Number(row.delayMinutes) !== 0).length;
  dom.reportingSummary.textContent = `${state.reportingDate} icin ${rows.length} sefer listelendi. Rotarli sefer: ${delayedCount}.`;

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
      <td>${occupancyText}${hasSeatCount ? ` (Bos: ${Number(row.seatsAvailable)})` : ""}</td>
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
  const current = String(result.currentRow?.vehiclePlate || "").trim();
  const firstProbe = Array.isArray(result.probes) && result.probes.length ? result.probes[0] : null;
  const detail = firstProbe
    ? ` | ilk endpoint: HTTP ${firstProbe.status || "-"}${firstProbe.matchedBy ? `, match=${firstProbe.matchedBy}` : ""}`
    : "";

  if (dom.reportingPlateDebugMsg) {
    if (extracted) {
      dom.reportingPlateDebugMsg.style.color = "#1f7a1f";
      dom.reportingPlateDebugMsg.textContent = `Bulunan plaka: ${extracted} | DB'deki mevcut: ${current || "-"}${detail}`;
    } else {
      dom.reportingPlateDebugMsg.style.color = "#d64545";
      dom.reportingPlateDebugMsg.textContent = `Bu ride_uuid icin plaka bulunamadi.${detail}`;
    }
  }
}

async function refreshReportingData() {
  const date = String(state.reportingDate || todayIsoDate()).trim();
  state.reportingDate = date;

  const result = await apiFetch(`/api/operations-reports?date=${encodeURIComponent(date)}&origin=${encodeURIComponent(state.reportingOrigin)}`);
  state.reportingRows = result.rows || [];
  renderReportingPanel();
}

async function syncReportingData() {
  const date = String(state.reportingDate || todayIsoDate()).trim();
  state.reportingDate = date;

  if (dom.reportingSummary) {
    dom.reportingSummary.textContent = "Gunluk rapor cekiliyor...";
  }

  await apiFetch("/api/operations-reports/sync", {
    method: "POST",
    body: JSON.stringify({ date }),
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
  if (!dom.controlStatusMsg) {
    return;
  }

  try {
    const result = await refreshControlIntegrationData();

    if (dom.controlBaseUrlInput) {
      dom.controlBaseUrlInput.value = result.baseUrl || "https://backend.flixbus.com";
    }
    if (dom.controlLoginUrlInput) {
      dom.controlLoginUrlInput.value = result.loginUrl || "https://app.oneops.flixbus.com/users/login";
    }
    if (dom.controlCookieInput) {
      dom.controlCookieInput.value = result.cookieHeader || "";
    }
    if (dom.controlCsrfInput) {
      dom.controlCsrfInput.value = result.csrfToken || "";
    }
    if (dom.controlSessionTestUrlInput) {
      dom.controlSessionTestUrlInput.value = result.sessionTestUrl || "https://app.oneops.flixbus.com/ops-portal/";
    }

    dom.controlStatusMsg.style.color = "var(--muted)";
    dom.controlStatusMsg.textContent = result.hasCookie
      ? `OneOps oturumu kayitli. Son guncelleme: ${result.updatedAt || "-"}`
      : "OneOps oturumu henuz kaydedilmedi.";
  } catch (error) {
    dom.controlStatusMsg.style.color = "#d64545";
    dom.controlStatusMsg.textContent = error.message || "OneOps bilgileri yuklenemedi.";
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

function startNotificationPolling() {
  stopNotificationPolling();
  state.notifPollTimer = setInterval(() => {
    if (state.currentUser) {
      refreshNotificationsData().catch(() => null);
      refreshPricingUploadsData().catch(() => null);
      refreshPricesData().catch(() => null);
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
  const switchSeq = ++state.panelSwitchSeq;
  if (dom.contentCard) {
    dom.contentCard.classList.add("panel-switching");
  }

  await pause(120);
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
  dom.activeTitle.textContent = current ? current.label : "Panel";

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
    (async () => {
      await refreshReportingData();
      if (!state.reportingRows.length) {
        await syncReportingData();
      }
    })().catch((error) => {
      if (dom.reportingSummary) {
        dom.reportingSummary.textContent = error.message || "Rapor verisi yuklenemedi.";
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
}

async function handleLogin(username, password) {
  try {
    const result = await apiFetch("/api/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });

    setToken(result.token);
    state.currentUser = result.user;
    showMessage("Giris basarili.", false);

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
  } catch (error) {
    showMessage(error.message || "Giris basarisiz.");
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
  } catch {
    setToken("");
    state.currentUser = null;
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

async function renderAdminPermissions() {
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
              <tr><th>Kullanici</th><th>Durum</th><th>Olusturma</th><th>Islem</th></tr>
            </thead>
            <tbody id="userRows"></tbody>
          </table>
        </section>
      </div>
    `;

    const rows = document.getElementById("userRows");

    state.usersCache.forEach((user) => {
      const row = dom.templateRow.content.cloneNode(true);
      row.querySelector(".username").textContent = user.username;
      row.querySelector(".created").textContent = user.createdAt;

      const statusCell = row.querySelector(".status");
      statusCell.innerHTML = user.isActive
        ? '<span class="status-badge status-active">Aktif</span>'
        : '<span class="status-badge status-passive">Pasif</span>';

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
    <div class="inline-form" style="grid-template-columns: 1fr 1fr 1fr auto;">
      <label>
        <span>Kullanici Adi</span>
        <input id="editUsername" type="text" value="${user.username}" />
      </label>
      <label>
        <span>Yeni Sifre (opsiyonel)</span>
        <input id="editPassword" type="password" placeholder="Bos birakirsan degismez" />
      </label>
      <label class="checkbox-row" style="align-self:center;">
        <input id="editActive" type="checkbox" ${user.isActive ? "checked" : ""} /> Hesap aktif
      </label>
      <button class="btn btn-primary" id="saveUserBtn">Kaydet</button>
    </div>
    <p class="field-hint">Sifre alanini bos birakirsan mevcut sifre korunur.</p>
    <div class="actions">
      <button class="btn btn-ghost" id="cancelUserBtn">Kapat</button>
    </div>
  `;

  const area = dom.permissionAdminArea.querySelector(".admin-grid");
  area.prepend(wrap);

  wrap.querySelector("#cancelUserBtn").addEventListener("click", () => wrap.remove());
  wrap.querySelector("#saveUserBtn").addEventListener("click", async () => {
    const username = wrap.querySelector("#editUsername").value.trim();
    const password = wrap.querySelector("#editPassword").value.trim();
    const isActive = wrap.querySelector("#editActive").checked;
    await updateUser(userId, { username, password, isActive });
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

async function renderLoginLogs() {
  if (!state.currentUser?.isAdmin) {
    dom.loginLogsArea.innerHTML = '<p class="subtle">Bu bolum sadece admin tarafindan gorulebilir.</p>';
    return;
  }

  try {
    const result = await apiFetch("/api/admin/logs");
    const logs = result.logs || [];

    if (!logs.length) {
      dom.loginLogsArea.innerHTML = '<p class="subtle">Henuz giris kaydi yok.</p>';
      return;
    }

    const rows = logs
      .map((log) => {
        const ok = log.status === "ok";
        const statusText = ok ? "Basarili" : "Basarisiz";
        const statusClass = ok ? "log-ok" : "log-fail";
        const reason = log.reason ? ` (${log.reason})` : "";
        return `<tr><td>${log.time}</td><td>${log.username}</td><td class="${statusClass}">${statusText}${reason}</td><td>${log.ip || "-"}</td></tr>`;
      })
      .join("");

    dom.loginLogsArea.innerHTML = `
      <table class="data-table">
        <thead><tr><th>Zaman</th><th>Kullanici</th><th>Durum</th><th>IP</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  } catch (error) {
    dom.loginLogsArea.innerHTML = `<p class="subtle">${error.message}</p>`;
  }
}

dom.loginForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const username = dom.loginUsername.value.trim();
  const password = dom.loginPassword.value.trim();
  await handleLogin(username, password);
});

dom.logoutBtn.addEventListener("click", async () => {
  await handleLogout();
});

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
