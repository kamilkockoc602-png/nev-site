# Otobus Fiyat Paneli — Production Dockerfile
#
# Onceki yaklasim Puppeteer'in indirdigi Chromium'u kullaniyordu, ama Railway
# konteynerinde Chrome'un crashpad_handler subprocess'i posix_spawn ile fail
# oluyor ("Failed to launch the browser process"). Cozum: Google'in resmi Chrome
# stable paketini sistemden kur, Puppeteer'a executablePath ile yonlendir.

FROM node:20-bookworm-slim

# 1) Chrome resmi reposundan google-chrome-stable kurulumu icin gerekli araclar.
# 2) Chrome'un calismasi icin standart fontlar + emoji desteği.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-noto-cjk \
    && curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Puppeteer kendi Chromium'unu indirmesin (npm install'da skip).
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome-stable

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["node", "server.js"]
