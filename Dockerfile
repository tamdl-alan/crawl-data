# Sử dụng image Node chính thức
FROM node:20-slim

# Cài các dependencies cần thiết
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Cài Google Chrome Stable
RUN wget -q -O google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get update && \
    apt-get install -y ./google-chrome.deb && \
    rm google-chrome.deb

# Tạo thư mục app
WORKDIR /app
COPY . .

# Cài thư viện node
RUN npm install

# Đặt biến môi trường trỏ đến Chrome
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome

# Khởi chạy app
CMD ["node", "index.js"]
