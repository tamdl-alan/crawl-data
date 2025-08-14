# Sử dụng image Node chính thức
FROM node:20-slim

# Cài đặt dependencies cần thiết cho Chrome và Puppeteer
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
    libxss1 \
    libxtst6 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libdrm2 \
    libgbm1 \
    libasound2 \
    libatspi2.0-0 \
    libgtk-3-0 \
    libxshmfence1 \
    procps \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Cài Google Chrome Stable với cấu hình tối ưu
RUN wget -q -O google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get update && \
    apt-get install -y ./google-chrome.deb && \
    rm google-chrome.deb && \
    rm -rf /var/lib/apt/lists/*

# Tạo user không phải root để chạy Chrome an toàn
RUN groupadd -r pptruser && useradd -r -g pptruser -G audio,video pptruser \
    && mkdir -p /home/pptruser/Downloads \
    && chown -R pptruser:pptruser /home/pptruser

# Tạo thư mục app và set permissions
WORKDIR /app
COPY . .

# Cài thư viện node
RUN npm install

# Set ownership cho app directory
RUN chown -R pptruser:pptruser /app

# Đặt biến môi trường trỏ đến Chrome
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV NODE_ENV=production

# Tăng limits cho file descriptors và processes
RUN echo "* soft nofile 65536" >> /etc/security/limits.conf && \
    echo "* hard nofile 65536" >> /etc/security/limits.conf && \
    echo "* soft nproc 32768" >> /etc/security/limits.conf && \
    echo "* hard nproc 32768" >> /etc/security/limits.conf

# Switch to non-root user
USER pptruser

# Khởi chạy app
CMD ["node", "index-2.js"]
