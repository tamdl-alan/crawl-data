FROM ghcr.io/puppeteer/puppeteer:24.10.2

# Tạo thư mục làm việc
WORKDIR /app

# Copy code
COPY . .

# Cài npm package khi vẫn còn quyền root
RUN npm install

# Chạy app với user không phải root nếu có sẵn
# USER pptruser  ← nếu cần bảo mật cao thì bật lại dòng này

EXPOSE 3000

CMD ["npm", "start"]
