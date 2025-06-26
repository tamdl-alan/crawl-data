// Import
require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const cheerio = require('cheerio');
const axios = require('axios');
const ngrok = require('@ngrok/ngrok');
const Airtable = require('airtable');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cors = require('cors');
const express = require('express');
const cron = require('node-cron');

const app = express();
// app.use(cors());
app.use(cors({
  origin: '*', // hoặc origin cụ thể nếu bạn biết origin của Airtable extension
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());
puppeteer.use(StealthPlugin());
// ========== Config Airable Start ========== //
Airtable.configure({
  apiKey: process.env.AIRTABLE_API_KEY
});
const base = Airtable.base(process.env.AIRTABLE_BASE_ID);
const table = base('Crawling Processes');
// ========== Config Airable End ========== //


// ========== Common Start ========== //
const PORT = process.env.PORT || 3000;

const userAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
const viewPortBrowser = { width: 1920, height: 1200 };
const extraHTTPHeaders = {
  'Accept-Language': 'ja,ja-JP;q=0.9,en;q=0.8'
}
const defaultBrowserArgs = {
  headless: 'new',
  args: [
    "--disable-setuid-sandbox",
      "--no-sandbox",
  ]
}

const STATUS_NEW = 'New';
const STATUS_CRAWLING = 'Crawling';
const STATUS_SUCCESS = 'Success';
const STATUS_ERROR = 'Error';
const PRODUCT_TYPE = {
  SHOE: 'SHOE',
  CLOTHES: 'CLOTHES'
}

const PRODUCT_ID = 'Product ID';
const PRODUCT_NAME = 'Product Name';
const SIZE_GOAT = 'Size Goat';
const PRICE_GOAT = 'Price Goat';
const SIZE_SNKRDUNK = 'Size Snkrdunk';
const PRICE_SNKRDUNK = 'Price Snkrdunk';
const PROFIT_AMOUNT = 'Profit Amount';
const IMAGE = 'Image';
const DATE_CREATED = 'Date Created';
const NOTE = 'Note';
const DATA_SEARCH_TABLE = 'Data Search';

let recordId = '';
// ========== Common End ========== //


// ========== Snkrdunk Start ========== //
const EMAIL_SNKRDUNK = process.env.EMAIL_SNKRDUNK || '';
const PASSWORD_SNKRDUNK = process.env.PASSWORD_SNKRDUNK || '';
const DOMAIN_SNKRDUNK = 'https://snkrdunk.com';
const LOGIN_PAGE_SNKRDUNK = `${DOMAIN_SNKRDUNK}/accounts/login`;
let cookieHeader = '';
let retryCount = 0; // Initialize retry count for login attempts
const RETRY_LIMIT = 3; // Retry limit for login attempts

// ========== Snkrdunk End========== //


// ========== Goal Start ========== //
const goalDomain = 'https://www.goat.com';
const searchUrl = 'https://www.goat.com/search';
let productType = PRODUCT_TYPE.SHOE;
// ========== Goal End ========== //

// ====== Queue quản lý request tuần tự ====== //
const requestQueue = [];
let isProcessingQueue = false;

app.get('/', (req, res) => {
  res.send('🟢 API is running!');
});


app.get('/crawl-all', async (req, res) => {
  // Trigger the cron job to crawl all records
  await triggerAllSearchesFromAirtable();
  res.status(200).send('OK');
});

app.get('/search', async (req, res) => {
    const params = req.query;
    const recordIdInQueue = params.recordId;
    const crawlStatusParam = params.crawlStatus;
    if (crawlStatusParam === STATUS_CRAWLING) {
      return res.status(400).send({ error: '⛔ Request is already in progress' });
    }
  // ✅ Cập nhật trạng thái là "Wait" ngay khi vào hàng đợi
    await updateStatus(recordIdInQueue, STATUS_CRAWLING);
    if (requestQueue.length >= 100) {
    return res.status(429).send({ error: '⛔ Too many pending requests' });
  }

  requestQueue.push({ req, res });
  processQueueToCrawl();
});

async function processQueueToCrawl() {
  if (isProcessingQueue) return;
  isProcessingQueue = true;

  while (requestQueue.length > 0) {
    const { req, res } = requestQueue.shift();

    const params = req.query;
    recordId = params.recordId;
    const productId = params.productId;
    const snkrdunkApi = params.snkrdunkApi?.replace(/^\/+/, '');
    const productType = params.productType || PRODUCT_TYPE.SHOE;
    if (!productId || !snkrdunkApi) {
      return res.status(400).send({ error: '⛔ Invalid Product ID or Product Type' });
    }
    try {

      console.log(`------------Crawling data [${productId}] SNKRDUNK Start: [${new Date()}]------------`);
      const dataSnk = await crawlDataSnkrdunk(snkrdunkApi, productType);
      console.log(`------------Crawling data [${productId}] SNKRDUNK End: [${new Date()}]------------`);

      console.log(`------------Crawling data [${productId}] GOAT Start: [${new Date()}]------------`);
      const dataGoat = await crawlDataGoat(productId);
      console.log(`------------Crawling data [${productId}] GOAT End: [${new Date()}]------------`);

      const mergedArr = mergeData(dataSnk, dataGoat);
      if (!mergedArr.length) {
        console.warn(`⚠️ No data found for Product ID: ${productId}`);
        res.status(200).send({ error: '⛔ No data found for the given Product ID' });  
      } else {
        await deleteRecordByProductId(productId);
        await pushToAirtable(mergedArr);
        res.status(200).send({ message: `✅ Done crawling ${productId}` });
      }
      await updateStatus(recordId, STATUS_SUCCESS);
    } catch (error) {
      await updateStatus(recordId, STATUS_ERROR);
      console.error(`❌ Error crawling ${productId}:`, error.message);
      res.status(500).send({ error: error.message });
      isProcessingQueue = false;
    }
  }
  isProcessingQueue = false;
}

async function deleteRecordByProductId(productId) {
  const existingRecords = await table.select({
    filterByFormula: `{${PRODUCT_ID}} = '${productId}'`,
  }).firstPage();

  const recordIds = existingRecords?.map(record => record.id);
  while (recordIds.length > 0) {
    const chunk = recordIds.splice(0, 10);
    await table.destroy(chunk);
  }
  console.log(`✅ Deleted ${existingRecords.length} records with Product ID: ${productId}`);
}

function mergeData(dataSnk, dataGoal) {
  const priceMap = new Map(dataSnk?.map(p => [String(p[SIZE_SNKRDUNK]), p[PRICE_SNKRDUNK]]));
  const merged = dataGoal.map(item => {
    const sizeStr = item[SIZE_GOAT];
    const priceSnk = priceMap.get(sizeStr);
    const priceGoat = parseInt(item[PRICE_GOAT]);

    return {
      ...item,
      [PRICE_GOAT]: priceGoat,
      [PRICE_SNKRDUNK]: priceSnk ?? 0,
      [SIZE_SNKRDUNK]: sizeStr,
      [PROFIT_AMOUNT]: priceSnk != null ? priceGoat - priceSnk : 0,
      [DATE_CREATED]: new Date(),
      [NOTE]: '',
    };
  });
  return merged;
}

async function snkrdunkLogin() {
  const browser = await puppeteer.launch(defaultBrowserArgs);
  try {
    if (cookieHeader) {
      return
    }
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    await page.goto(LOGIN_PAGE_SNKRDUNK, { waitUntil: 'networkidle2' });
    await page.type('input[name="email"]', EMAIL_SNKRDUNK, { delay: 100 });
    await page.type('input[name="password"]', PASSWORD_SNKRDUNK, { delay: 100 });
    await page.evaluate(() => document.querySelector('form').submit());
    const cookies = await page.cookies();
    cookieHeader = cookies.map(c => `${c.name}=${c.value}`).join('; ');
    retryCount = 0; // Reset retry count on successful login
  } catch (err) {
      console.error('Snkrdunk login failed:', err.message);
      // Retry login if it fails
      cookieHeader = '';
      retryCount++;
      if (retryCount < RETRY_LIMIT) {
        console.log(`Retrying login (${retryCount}/${RETRY_LIMIT})...`);
        await snkrdunkLogin();
      }
      throw err;
  } finally {
      await browser.close();
  }
}

async function crawlDataSnkrdunk(apiUrl, productType) {
  try {
    await snkrdunkLogin();
    const dataRes = await snkrdunkfetchData(apiUrl);
    return getSizeAndPriceSnkrdunk(dataRes?.data, productType);
  } catch (err) {
    console.error('Error during Snkrdunk login:', err.message);
    res.status(500).send({ error: err.message });
    throw err;
  }
}

async function snkrdunkfetchData(api) {
  const apiUrl = `${DOMAIN_SNKRDUNK}/v1/${api}`;
  try {
    const response = await axios.get(apiUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
        'Cookie': cookieHeader,
        'Referer': DOMAIN_SNKRDUNK,
        'Origin': DOMAIN_SNKRDUNK
      }
    });
    if (productType === PRODUCT_TYPE.SHOE) {
      return response?.data || null;
    }
    return response || null;
  } catch (err) {
    console.error('API [' + api + '] call failed:', err.message);
    res.status(500).send({ error: err.message });
    throw err;
  }
}

async function crawlDataGoat(productId) {
  const browser = await puppeteer.launch(defaultBrowserArgs);
  const page = await browser.newPage();
  try {
    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);

    await page.goto(`${searchUrl}?query=${productId}`, { waitUntil: 'networkidle2', timeout: 60000 });

    const content = await page.content();
    const $ = cheerio.load(content);

    let fullLink = '';

    // get first product link
    $('div[data-qa="grid_cell_product"]').each((_i, el) => {
      const aTag = $(el).find('a');
      const link = aTag.attr('href');
      fullLink = goalDomain + link;
      return false;
    });

    const details = await extractDetailsFromProductGoat(fullLink, productId);
    return details;
  } catch (err) {
    console.error(`❌ Error crawling ${url}:`, err.message);
    res.status(500).send({ error: err.message });
    throw err;
  } finally {
    await page.close();
    await browser.close();
  }
}

async function extractDetailsFromProductGoat(url, productId) {
  if (!url) {
    return [];
  }
  let browserChild;
  try {
    browserChild = await puppeteer.launch(defaultBrowserArgs);
    const page = await browserChild.newPage();

    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);

    await page.setCookie(
      { name: 'currency', value: 'JPY', domain: 'www.goat.com', path: '/', secure: true },
      { name: 'country', value: 'JP', domain: 'www.goat.com', path: '/', secure: true },
    );
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 100000 });
    await acceptCookiesIfPresent(page);
    const html = await page.content();
    const $ = cheerio.load(html);

    let imgSrc = '';
    let imgAlt = '';
    // let sku = '';
    const products = [];

    // $('div.window-item').each((i, el) => {
    //   const spans = $(el).find('span');
    //   if (spans.length >= 2 && $(spans[0]).text().trim() === 'SKU') {
    //     sku = $(spans[1]).text().trim();
    //   }
    // });

    await page.waitForSelector('div.swiper-slide-active', { timeout: 60000 });
    $('div.swiper-slide-active').each((i, el) => {
      const img = $(el).find('img');
      if (img && !imgSrc && !imgAlt) {
        imgSrc = img.attr('src');
        imgAlt = img.attr('alt');
      }
    });

    await page.waitForSelector('div.swiper-slide', { timeout: 60000 });
    $('div.swiper-slide').each((_i, el) => {
      const elm = $(el).children()?.text()?.split('¥');
      if (conditionCheckSize(elm, products)) {
        const dataRow = {
                          [PRODUCT_ID]: productId,
                          [PRODUCT_NAME]: imgAlt,
                          [IMAGE]: [{ url: imgSrc }],
                          [SIZE_GOAT]: elm[0]?.toString()?.trim()?.toLowerCase(),
                          [PRICE_GOAT]: parseInt(elm[1].replace(/,/g, ""), 10)
                        };
        products.push(dataRow);
      }
    });

    console.log(`✅ Extracted Goat data!!!`);
    console.table(products, [PRODUCT_NAME, PRODUCT_NAME, SIZE_GOAT, PRICE_GOAT]);
    return products;
  } catch (err) {
    await updateStatus(recordId, STATUS_ERROR);
    console.error(`❌ Error parsing product ${url}:`, err.message);
    res.status(500).send({ error: err.message });
    throw err;
  } finally {
    if (browserChild) {
      await browserChild.close();
    }
  }
}

async function pushToAirtable(records) {
  const chunks = chunkArray(records, 10);
  for (const chunk of chunks) {
    await new Promise((resolve) => {
      table.create(chunk.map(item => ({ fields: item })), function (err, records) {
        if (err) {
          console.error('❌ Airtable error:', err);
          res.status(500).send({ error: err.message });
          resolve();
          return;
        }
        records.forEach(record => {
          console.log('✅ Created record ID:', record.getId());
        });
        resolve();
      });
    });
  }
}

function chunkArray(array, size) {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
}

function convertCmToUs(cm) {
  if (cm < 20 || cm > 32) {
    return null;
  }

  const sizeMap = {
    20: 6,
    21: 6.5,
    22: 7,
    23: 7.5,
    24: 8,
    25: 8.5,
    26: 9,
    27: 9.5,
    28: 10,
    29: 10.5,
    30: 11,
    31: 11.5,
    32: 12,
  };

  return sizeMap[cm] ?? null;
}

async function updateStatus(recordId, newStatus) {
  try {
    await base(DATA_SEARCH_TABLE).update([
      {
        id: recordId,
        fields: {
          Status: newStatus
        }
      }
    ]);

    console.log(`✅ Updated the status of ${recordId} to "${newStatus}".`);
  } catch (err) {
    console.error('❌ Error update status:', err);
    res.status(500).send({ error: err.message });
    throw err;
  }
}

function getSizeAndPriceSnkrdunk(data, productType) {
  if (productType === PRODUCT_TYPE.SHOE) {
    return data?.minPriceOfSizeList?.map(item => {
      const size = convertCmToUs(item.size);
      if (size == null) {
        return null;
      }
      return {
        [SIZE_SNKRDUNK]: size.toString()?.trim().toLowerCase(),
        [PRICE_SNKRDUNK]: item.price
      };
    }).filter(item => item !== null);
  }
  return data?.sizePrices?.map(item => {
    return {
      [SIZE_SNKRDUNK]: item.size.localizedName?.toString()?.trim().toLowerCase(),
      [PRICE_SNKRDUNK]: item.minListingPrice
    };
  });
}

function conditionCheckSize(productElm, products) {
  const sizeItem = productElm && productElm[0]?.trim()
  const nameItem = productElm && productElm[1]?.trim()
  if (sizeItem && nameItem) {
    if (productType === PRODUCT_TYPE.SHOE) {
      if (sizeItem <= 12 && sizeItem >= 6 && !products.some(product => product[SIZE_GOAT] === sizeItem)) {
        return true;
      }
    } else {
      return true;
    }
  }
  return false;
}

app.listen(PORT, async () => {
  try {
    const listener = await ngrok.connect({ 
      addr: PORT, 
      authtoken_from_env: true, 
      domain: process.env.NGROK_STATIC_DOMAIN,
      proto: 'http', // Hoặc 'https' nếu ứng dụng của bạn là HTTPS
      host_header: 'rewrite'
    });
    console.log(`🚀 Listening on port ${PORT} | 🌍 Ngrok tunnel: ${listener.url()}`);
  } catch (err) {
    console.error('❌ Failed to connect ngrok:', err);
    res.status(500).send({ error: err.message });
  }
});

cron.schedule('0 0 * * *', () => {
  console.log('⏰ Running scheduled crawl at 0h');
  triggerAllSearchesFromAirtable();
});

async function triggerAllSearchesFromAirtable() {
  try {
    const records = await base(DATA_SEARCH_TABLE).select().all();
    for (const record of records) {
      const recordId = record.id;
      const productId = record.get(PRODUCT_ID);
      const snkrdunkApi = record.get('Snkrdunk API');
      const productType = record.get('Product Type');

      if (!productId || !snkrdunkApi) {
        console.warn(`⚠️ Bỏ qua record thiếu dữ liệu: ${recordId}`);
        continue;
      }

      const url = `https://platypus-poetic-factually.ngrok-free.app/search?recordId=${encodeURIComponent(recordId)}&productId=${encodeURIComponent(productId)}&snkrdunkApi=${encodeURIComponent(snkrdunkApi)}&productType=${encodeURIComponent(productType)}`;

      try {
        console.log(`📤 Triggering crawl for ${productId}`);
        axios.get(url);
      } catch (err) {
        console.error(`❌ Error calling /search for ${productId}:`, err.message);
        res.status(500).send({ error: err.message });
      }
    }

    console.log(`✅ Đã gọi API cho tất cả record lúc 0h.`);
  } catch (err) {
    console.error('❌ Error fetching records from Airtable:', err.message);
    res.status(500).send({ error: err.message });
  }
}
async function acceptCookiesIfPresent(page) {
  try {
    await page.waitForFunction(() => {
    return [...document.querySelectorAll('button')].some(
      btn => btn.innerText.trim().includes('Accept All Cookies')
    );
  }, { timeout: 3000 });

  // 2. Click nút đó
  await page.evaluate(() => {
    const buttons = [...document.querySelectorAll('button')];
    const acceptBtn = buttons.find(btn => btn.innerText.trim().includes('Accept All Cookies'));
    if (acceptBtn) acceptBtn.click();
  });

  console.log(' Clicked Accept All Cookies button');
  } catch (err) {
    console.log('Not found Accept All Cookies button');
  }
}
