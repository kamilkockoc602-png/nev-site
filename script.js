const TOKEN_KEY = "bus_auth_token_v2";
const THEME_KEY = "bus_theme_v1";

const MENUS = [
  { key: "dashboard", label: "Genel Panel" },
  { key: "routes", label: "Rota Standart" },
  { key: "pricing", label: "Fiyat Politikasi" },
  { key: "reports", label: "Raporlar" },
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
  activeTitle: document.getElementById("activeTitle"),
  logoutBtn: document.getElementById("logoutBtn"),
  currentUserLabel: document.getElementById("currentUserLabel"),
  permissionAdminArea: document.getElementById("permissionAdminArea"),
  loginLogsArea: document.getElementById("loginLogsArea"),
  templateRow: document.getElementById("userRowTemplate"),
  loadingOverlay: document.getElementById("loadingOverlay"),
  routeTableBody: document.getElementById("routeTableBody"),
  avgFare: document.getElementById("avgFare"),
  totalRoutes: document.getElementById("totalRoutes"),
  updateCount: document.getElementById("updateCount"),
  lastPriceUpdate: document.getElementById("lastPriceUpdate"),
  notifBtn: document.getElementById("notifBtn"),
  notifBadge: document.getElementById("notifBadge"),
  notifPanel: document.getElementById("notifPanel"),
  notifList: document.getElementById("notifList"),
  notifEmpty: document.getElementById("notifEmpty"),
  notifReadAllBtn: document.getElementById("notifReadAllBtn"),
  themeToggleBtn: document.getElementById("themeToggleBtn"),
  themeLabel: document.getElementById("themeLabel"),
  ocrOriginInput: document.getElementById("ocrOriginInput"),
  ocrModeSelect: document.getElementById("ocrModeSelect"),
  ocrImageInput: document.getElementById("ocrImageInput"),
  ocrScanBtn: document.getElementById("ocrScanBtn"),
  ocrCopyBtn: document.getElementById("ocrCopyBtn"),
  ocrStatus: document.getElementById("ocrStatus"),
  ocrOutput: document.getElementById("ocrOutput"),
};

const state = {
  token: localStorage.getItem(TOKEN_KEY) || "",
  currentUser: null,
  usersCache: [],
  prices: [],
  notifications: [],
  notifPollTimer: null,
  theme: localStorage.getItem(THEME_KEY) || "light",
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
  const raw = String(name || "").trim();
  if (!raw) {
    return raw;
  }

  const exact = KNOWN_DESTINATIONS.find((city) => slugTr(city) === slugTr(raw));
  if (exact) {
    return toTurkishTitleCase(exact);
  }

  let best = null;
  let bestDistance = Infinity;
  for (const city of KNOWN_DESTINATIONS) {
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

  // OCR bazen sifir yutar: 16 -> 1600, 90 -> 900
  while (n > 0 && n < 300) {
    n = n * 10;
  }

  if (n < 200 || n > 3500) {
    return "";
  }

  const rounded = Math.round(n / 100) * 100;
  return String(rounded);
}

function findDestinationInLine(line) {
  const lineSlug = slugTr(line);
  if (!lineSlug) {
    return "";
  }

  let best = "";
  let bestLen = 0;

  for (const city of KNOWN_DESTINATIONS) {
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
  return correctDestinationName(normalizeCity(beforeDigit));
}

function findKnownDestinationInLine(line) {
  const lineSlug = slugTr(line);
  if (!lineSlug) {
    return "";
  }

  let best = "";
  let bestLen = 0;

  for (const city of KNOWN_DESTINATIONS) {
    const citySlug = slugTr(city);
    if (citySlug && lineSlug.includes(citySlug) && citySlug.length > bestLen) {
      best = city;
      bestLen = citySlug.length;
    }
  }

  return best ? toTurkishTitleCase(best) : "";
}

function extractPriceCandidates(line) {
  const matches = String(line).match(/\d{2,6}/g) || [];
  const out = [];
  for (const m of matches) {
    const price = normalizeBusinessPrice(normalizePrice(m));
    if (price) {
      out.push(price);
    }
  }
  return out;
}

function parseDestinationsAndPrices(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const pairs = [];

  for (const line of lines) {
    const cleaned = line.replace(/[|]/g, " ").replace(/\s+/g, " ").trim();
    if (!cleaned) {
      continue;
    }

    const destination = findDestinationInLine(cleaned);
    const priceCandidates = extractPriceCandidates(cleaned);
    const price = priceCandidates[0] || "";

    if (!isLikelyRow(destination, price)) {
      continue;
    }

    pairs.push({ destination, price });
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

function parseA4SegmentWords(segmentWords) {
  if (!segmentWords || segmentWords.length < 2) {
    return null;
  }

  const text = segmentWords.map((w) => w.text).join(" ");
  const destination = findKnownDestinationInLine(text);
  if (!destination) {
    return null;
  }

  const priceCandidates = extractPriceCandidates(text);
  const price = priceCandidates[priceCandidates.length - 1] || "";
  if (!isLikelyRow(destination, price)) {
    return null;
  }

  return { destination, price };
}

function parseA4FromOcrData(ocrData) {
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

    const leftPair = parseA4SegmentWords(left);
    const rightPair = parseA4SegmentWords(right);

    if (leftPair) {
      pairs.push(leftPair);
    }
    if (rightPair) {
      pairs.push(rightPair);
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
  const mode = dom.ocrModeSelect?.value || "normal";

  if (!file) {
    setOcrStatus("Lutfen once bir fotograf sec.", true);
    return;
  }

  if (!window.Tesseract) {
    setOcrStatus("OCR kutuphanesi yuklenemedi. Internet baglantisini kontrol et.", true);
    return;
  }

  dom.ocrOutput.value = "";
  setOcrStatus("Tarama basladi...");

  try {
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
        parsed.push(...parseDestinationsAndPrices(block));
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

    let pairs = [];
    if (mode === "a4") {
      const a4Pairs = [
        ...parseA4FromOcrData(pass1.ocrData),
        ...parseA4FromOcrData(pass2.ocrData),
        ...parseA4FromOcrData(pass3.ocrData),
        ...pass1.pairs,
        ...pass2.pairs,
        ...pass3.pairs,
      ];
      pairs = dedupePairs(a4Pairs);
    } else {
      const byKey = new Map();
      [pass1.pairs, pass2.pairs, pass3.pairs].forEach((list) => {
        list.forEach((item) => {
          const key = slugTr(item.destination);
          if (!byKey.has(key)) {
            byKey.set(key, item);
          }
        });
      });
      pairs = Array.from(byKey.values());
    }

    if (!pairs.length) {
      setOcrStatus("Satirlar ayrisamadi. Daha net ve dik cekim bir fotograf deneyin.", true);
      return;
    }

    dom.ocrOutput.value = toTsv(origin, pairs);
    setOcrStatus(`${pairs.length} satir olusturuldu. Excel'e dogrudan yapistirabilirsin.`);
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
    throw new Error(data.message || "Islem basarisiz.");
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
  dom.routeTableBody.innerHTML = "";

  if (!state.prices.length) {
    dom.routeTableBody.innerHTML = '<tr><td colspan="2">Fiyat verisi henuz yok.</td></tr>';
    dom.avgFare.textContent = "0 TL";
    dom.totalRoutes.textContent = "0";
    dom.updateCount.textContent = "0";
    dom.lastPriceUpdate.textContent = "Son fiyat guncellemesi: -";
    return;
  }

  let sum = 0;
  for (const item of state.prices) {
    sum += item.standard;
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${item.route}</td><td>${item.standard} TL</td>`;
    dom.routeTableBody.appendChild(tr);
  }

  const avg = Math.round(sum / state.prices.length);
  dom.avgFare.textContent = `${avg} TL`;
  dom.totalRoutes.textContent = String(state.prices.length);
  dom.lastPriceUpdate.textContent = `Son fiyat guncellemesi: ${state.lastUpdated || "-"}`;
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
    const li = document.createElement("li");
    li.className = "notif-item";
    li.innerHTML = `${item.message}<div class="notif-time">${item.time}</div>`;
    dom.notifList.appendChild(li);
  });
}

async function refreshPricesData() {
  const result = await apiFetch("/api/prices");
  state.prices = result.prices || [];
  state.lastUpdated = result.lastUpdated || "-";
  dom.updateCount.textContent = String(result.updateCount || 0);
  renderPrices();
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

function activatePanel(menuKey) {
  document.querySelectorAll(".panel-block").forEach((el) => {
    el.classList.toggle("hidden", el.dataset.menu !== menuKey);
  });

  const current = MENUS.find((m) => m.key === menuKey);
  dom.activeTitle.textContent = current ? current.label : "Panel";

  if (menuKey === "permissions") {
    renderAdminPermissions();
  }

  if (menuKey === "logs") {
    renderLoginLogs();
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
    await refreshNotificationsData();
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
    await refreshNotificationsData();
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

applyTheme(state.theme);
verifySession();
