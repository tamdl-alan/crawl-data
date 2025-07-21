# Crawl Data API

API để crawl dữ liệu từ Snkrdunk và GOAT, sau đó lưu vào Airtable.

## Cài đặt

1. Cài đặt dependencies:
```bash
npm install
```

2. Tạo file `.env` với các biến môi trường sau:

```env
# Server Configuration
PORT=3000
MAIN_URL=http://localhost:3000

# Airtable Configuration
AIRTABLE_API_KEY=your_airtable_api_key_here
AIRTABLE_BASE_ID=your_airtable_base_id_here
DATA_CRAWLED_TABLE=your_crawled_data_table_name_here
DATA_SEARCH_TABLE=your_search_data_table_name_here

# Snkrdunk Credentials
EMAIL_SNKRDUNK=your_snkrdunk_email_here
PASSWORD_SNKRDUNK=your_snkrdunk_password_here

# Puppeteer Configuration
PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome
```

3. Chạy server:
```bash
node index-2.js
```

## API Endpoints

### GET /
- Kiểm tra trạng thái API

### GET /status
- Xem thông tin chi tiết về hệ thống (queue, processing status)

### GET /crawl-all
- Trigger crawl tất cả records từ Airtable
- Trả về response ngay lập tức và chạy crawl ở background

### GET /search
- Crawl dữ liệu cho một sản phẩm cụ thể
- Parameters:
  - `recordId`: ID của record trong Airtable
  - `productId`: ID sản phẩm
  - `snkrdunkApi`: API endpoint của Snkrdunk
  - `productType`: Loại sản phẩm (SHOE/CLOTHES)

## Các cải tiến đã thực hiện

1. **Sửa lỗi URL trong triggerAllSearchesFromAirtable()**:
   - Sử dụng `process.env.MAIN_URL` thay vì hardcode localhost
   - Thêm fallback cho trường hợp không có MAIN_URL

2. **Cải thiện error handling**:
   - Thêm try-catch cho tất cả các operation quan trọng
   - Log chi tiết các lỗi
   - Update status về ERROR khi có lỗi

3. **Cải thiện queue system**:
   - Thêm logging để theo dõi queue
   - Xử lý lỗi tốt hơn trong processQueueToCrawl()
   - Đảm bảo response được gửi đúng cách

4. **Thêm API monitoring**:
   - Endpoint `/status` để kiểm tra trạng thái hệ thống
   - Logging chi tiết cho việc debug

5. **Cải thiện /crawl-all**:
   - Trả về response ngay lập tức
   - Chạy crawl ở background
   - Thêm summary report

## Troubleshooting

### Lỗi thường gặp:

1. **"Failed to update status"**:
   - Kiểm tra AIRTABLE_API_KEY và DATA_SEARCH_TABLE
   - Đảm bảo recordId tồn tại trong Airtable

2. **"Too many pending requests"**:
   - Queue đã đầy (100 requests)
   - Đợi một lúc rồi thử lại

3. **"Invalid Product ID or Product Type"**:
   - Kiểm tra parameters được truyền vào
   - Đảm bảo productId và snkrdunkApi không null

4. **Crawl không hoạt động**:
   - Kiểm tra credentials Snkrdunk
   - Kiểm tra Puppeteer executable path
   - Xem logs để debug chi tiết 