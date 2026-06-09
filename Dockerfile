# Otobus Fiyat Paneli — Production Dockerfile
# Node.js + Puppeteer (Chromium icin sistem bagimliliklari) icin modern Debian base.
# Railway'in nixpacks builder'i Puppeteer icin eski/kaldirilmis paketler (libappindicator1,
# gconf-service) kurmaya calistigi icin failliyor — bunun yerine kendi imajimizi kuruyoruz.

FROM node:20-slim

# Puppeteer'in indirdigi Chromium'i calistirmak icin gereken sistem kutuphaneleri.
# Liste Puppeteer'in resmi onerilen listesidir (legacy paketler haric).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    fonts-liberation \
    fonts-noto-color-emoji \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libgcc-s1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    lsb-release \
    wget \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Once package dosyalarini kopyala ki npm install Docker layer cache'inden faydalanabilsin.
COPY package*.json ./

# Production dependency'leri kur. Puppeteer post-install'da Chromium'u kendi indirir.
RUN npm install --omit=dev

# Uygulama kodunu kopyala.
COPY . .

# Railway PORT env'i veriyor; varsayilan 8080.
ENV PORT=8080
EXPOSE 8080

CMD ["node", "server.js"]
