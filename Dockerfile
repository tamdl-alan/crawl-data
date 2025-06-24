FROM ghcr.io/puppeteer/puppeteer:24.10.2

# Tạo thư mục làm việc
WORKDIR /app

# Copy code
COPY . .

# Cài dependency nếu có (nếu bạn dùng thêm gói npm khác ngoài puppeteer)
RUN npm install

# Expose cổng
EXPOSE 3000

# Chạy app
CMD ["node", "index.js"]
