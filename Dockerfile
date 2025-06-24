FROM ghcr.io/puppeteer/puppeteer:24.10.2

# Tạm thời chuyển thành root để cài package
USER root

# Tạo thư mục làm việc và cấp quyền cho pptruser
WORKDIR /app
COPY . .
RUN chown -R pptruser:pptruser /app

# Cài npm packages khi còn là root
RUN npm install

# Quay lại user mặc định bảo mật
USER pptruser

EXPOSE 3000
CMD ["npm", "start"]
