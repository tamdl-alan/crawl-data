FROM ghcr.io/puppeteer/puppeteer:latest

# Tạo thư mục làm việc và chuyển quyền cho user `pptruser`
WORKDIR /app
COPY . .

# Chuyển quyền để user `pptruser` có thể ghi
RUN chown -R pptruser:pptruser /app

# Chạy dưới quyền user pptruser (nếu chưa có)
USER pptruser

# Cài đặt dependency
RUN npm install

EXPOSE 3000

CMD ["npm", "start"]
