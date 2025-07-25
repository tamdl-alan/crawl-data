// Import
require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const cheerio = require('cheerio');
const axios = require('axios');
const Airtable = require('airtable');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cors = require('cors');
const express = require('express');
const cron = require('node-cron');
const pLimit = require('p-limit').default;

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
const table = base(process.env.DATA_CRAWLED_TABLE);
// ========== Config Airable End ========== //


// ========== Common Start ========== //
const PORT = process.env.PORT || 3000;

const userAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
const viewPortBrowser = { width: 1920, height: 1200 };
const extraHTTPHeaders = {
  'Accept-Language': 'ja,ja-JP;q=0.9,en;q=0.8'
}
const defaultBrowserArgs = {
  headless: 'true',
  executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome',
  args: [
    "--disable-setuid-sandbox",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu,"
  ]
}

const STATUS_CRAWLING = 'Crawling';
const STATUS_SUCCESS = 'Success';
const STATUS_ERROR = 'Error';
const PRODUCT_TYPE = {
  SHOE: 'SHOE',
  CLOTHES: 'CLOTHES'
}
const CONCURRENCY_LIMIT = 2; // Số lượng request đồng thời

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
const sizeAndPriceGoatUrl = 'https://www.goat.com/web-api/v1/product_variants/buy_bar_data?productTemplateId'
let productType = PRODUCT_TYPE.SHOE;
// ========== Goal End ========== //

// ====== Queue quản lý request tuần tự ====== //
const requestQueue = [];
let isProcessingQueue = false;
let lastQueueProcessTime = Date.now();
let currentProcessingRequest = null;

// Queue health check
setInterval(() => {
  const now = Date.now();
  const timeSinceLastProcess = now - lastQueueProcessTime;
  
  // If queue has been processing for more than 20 minutes, reset it
  if (isProcessingQueue && timeSinceLastProcess > 20 * 60 * 1000) {
    console.warn('⚠️ Queue has been processing for too long, resetting...');
    isProcessingQueue = false;
    lastQueueProcessTime = now;
    currentProcessingRequest = null;
  }
  
  // If a single request has been processing for more than 10 minutes, log warning
  if (currentProcessingRequest && (now - currentProcessingRequest.startTime) > 10 * 60 * 1000) {
    console.warn(`⚠️ Request ${currentProcessingRequest.productId} has been processing for ${Math.round((now - currentProcessingRequest.startTime)/1000)}s`);
  }
  
  // Log queue status every 5 minutes
  if (now % (5 * 60 * 1000) < 1000) {
    console.log(`📊 Queue Status: length=${requestQueue.length}, processing=${isProcessingQueue}, timeSinceLastProcess=${Math.round(timeSinceLastProcess/1000)}s`);
  }
}, 60000); // Check every minute

app.get('/', (_req, res) => {
  res.send('🟢 API is running!');
});

app.get('/status', (_req, res) => {
  res.json({
    status: 'running',
    timestamp: new Date().toISOString(),
    queue: {
      length: requestQueue.length,
      isProcessing: isProcessingQueue
    },
    environment: {
      port: PORT,
      concurrencyLimit: CONCURRENCY_LIMIT
    }
  });
});

app.get('/queue-info', (_req, res) => {
  const queueInfo = {
    queueLength: requestQueue.length,
    isProcessing: isProcessingQueue,
    timestamp: new Date().toISOString(),
    memoryUsage: process.memoryUsage(),
    uptime: process.uptime(),
    lastQueueProcessTime: lastQueueProcessTime,
    currentProcessingRequest: currentProcessingRequest
  };
  
  console.log('📊 Queue Info:', queueInfo);
  res.json(queueInfo);
});

app.get('/queue-debug', (_req, res) => {
  const debugInfo = {
    queue: {
      length: requestQueue.length,
      isProcessing: isProcessingQueue,
      lastProcessTime: new Date(lastQueueProcessTime).toISOString(),
      timeSinceLastProcess: Date.now() - lastQueueProcessTime
    },
    currentRequest: currentProcessingRequest ? {
      productId: currentProcessingRequest.productId,
      startTime: new Date(currentProcessingRequest.startTime).toISOString(),
      processingTime: Date.now() - currentProcessingRequest.startTime
    } : null,
    system: {
      memory: process.memoryUsage(),
      uptime: process.uptime(),
      timestamp: new Date().toISOString()
    }
  };
  
  res.json(debugInfo);
});


app.get('/crawl-all', async (_req, res) => {
  try {
    console.log('🚀 Starting crawl-all operation...');
    // Send immediate response to client
    res.status(200).send({ 
      message: '✅ Crawl-all operation started successfully',
      timestamp: new Date().toISOString()
    });
    
    // Trigger the crawl operation asynchronously
    triggerAllSearchesFromAirtable().catch(error => {
      console.error('❌ Error in crawl-all operation:', error.message);
    });
    
  } catch (error) {
    console.error('❌ Error starting crawl-all:', error.message);
    if (!res.headersSent) {
      res.status(500).send({ 
        error: '❌ Failed to start crawl-all operation',
        details: error.message 
      });
    }
  }
});

app.get('/search', async (req, res) => {
    const params = req.query;
    const recordIdInQueue = params.recordId;
    const crawlStatusParam = params.crawlStatus;
    
    // Validate required parameters
    if (!recordIdInQueue) {
      return res.status(400).send({ error: '⛔ Missing recordId parameter' });
    }
    
    if (crawlStatusParam === STATUS_CRAWLING) {
      return res.status(400).send({ error: '⛔ Request is already in progress' });
    }
    
    // Check if queue is getting too full
    if (requestQueue.length >= 50) {
      console.warn(`⚠️ Queue is getting full (${requestQueue.length}/50). Consider throttling.`);
    }
    
    // Update status to crawling
    try {
      await updateStatus(recordIdInQueue, STATUS_CRAWLING);
    } catch (error) {
      console.error(`❌ Failed to update status for ${recordIdInQueue}:`, error.message);
      return res.status(500).send({ error: '⛔ Failed to update status' });
    }
    
    if (requestQueue.length >= 100) {
      // Update status back to original if queue is full
      try {
        await updateStatus(recordIdInQueue, STATUS_ERROR);
      } catch (error) {
        console.error(`❌ Failed to update status for ${recordIdInQueue}:`, error.message);
      }
      return res.status(429).send({ error: '⛔ Too many pending requests' });
    }

    // Add request to queue
    requestQueue.push({ req, res });
    console.log(`📥 Added request to queue. Queue length: ${requestQueue.length}`);
    
    // Process queue
    processQueueToCrawl();
});

async function processQueueToCrawl() {
  if (isProcessingQueue) {
    console.log('⏳ Queue is already being processed, skipping...');
    return;
  }
  
  isProcessingQueue = true;
  lastQueueProcessTime = Date.now();
  console.log(`🔄 Starting queue processing. Queue length: ${requestQueue.length}`);

  let processedCount = 0;
  let successCount = 0;
  let errorCount = 0;

  while (requestQueue.length > 0) {
    const { req, res } = requestQueue.shift();
    processedCount++;
    lastQueueProcessTime = Date.now();
    console.log(`📋 Processing request ${processedCount}. Remaining in queue: ${requestQueue.length}`);

    const params = req.query;
    recordId = params.recordId;
    const productId = params.productId;
    const snkrdunkApi = params.snkrdunkApi?.replace(/^\/+/, '');
    productType = params.productType || PRODUCT_TYPE.SHOE;
    
    // Track current processing request
    currentProcessingRequest = {
      productId,
      startTime: Date.now()
    };
    
    // Validate parameters
    if (!productId || !snkrdunkApi) {
      console.error(`❌ Invalid parameters for record ${recordId}: productId=${productId}, snkrdunkApi=${snkrdunkApi}`);
      try {
        await updateStatus(recordId, STATUS_ERROR);
        if (!res.headersSent) {
          res.status(400).send({ error: '⛔ Invalid Product ID or Product Type' });
        }
      } catch (error) {
        console.error(`❌ Failed to update status for ${recordId}:`, error.message);
        if (!res.headersSent) {
          res.status(500).send({ error: '⛔ Internal server error' });
        }
      }
      errorCount++;
      currentProcessingRequest = null;
      continue;
    }
    
    // Process each request with proper timeout and error handling
    try {
      console.log(`------------Crawling data [${productId}] SNKRDUNK Start: [${new Date()}]------------`);
      const dataSnk = await crawlDataSnkrdunk(snkrdunkApi, productType);
      console.log(`------------Crawling data [${productId}] SNKRDUNK End: [${new Date()}]------------`);

      console.log(`------------Crawling data [${productId}] GOAT Start: [${new Date()}]------------`);
      const dataGoat = await crawlDataGoat(productId, productType);
      console.log(`------------Crawling data [${productId}] GOAT End: [${new Date()}]------------`);

      const mergedArr = mergeData(dataSnk, dataGoat);
      
      if (!mergedArr?.length) {
        console.warn(`⚠️ No data found for Product ID: ${productId}`);
        await updateStatus(recordId, STATUS_ERROR);
        if (!res.headersSent) {
          res.status(200).send({ message: '⛔ No data found for the given Product ID' });  
        }
        errorCount++;
      } else {
        await deleteRecordByProductId(productId);
        await pushToAirtable(mergedArr);
        await updateStatus(recordId, STATUS_SUCCESS);
        if (!res.headersSent) {
          res.status(200).send({ message: `✅ Done crawling ${productId}` });
        }
        successCount++;
      }
      
    } catch (error) {
      console.error(`❌ Error crawling ${productId}:`, error.message);
      errorCount++;
      
      // Always try to update status to ERROR
      try {
        await updateStatus(recordId, STATUS_ERROR);
      } catch (updateError) {
        console.error(`❌ Failed to update status for ${recordId}:`, updateError.message);
      }
      
      // Send error response if not already sent
      if (!res.headersSent) {
        res.status(500).send({ 
          error: `❌ Error crawling ${productId}: ${error.message}` 
        });
      }
    }
    
    // Clear current processing request
    currentProcessingRequest = null;
    
    // Always continue to next request regardless of success/failure
    console.log(`✅ Completed processing ${productId}. Moving to next request...`);
  }
  
  isProcessingQueue = false;
  lastQueueProcessTime = Date.now();
  currentProcessingRequest = null;
  console.log(`✅ Queue processing completed. Processed: ${processedCount}, Success: ${successCount}, Errors: ${errorCount}`);
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
  const merged = dataGoal?.map(item => {
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
  return merged || [];
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
    const snkrMapped = getSizeAndPriceSnkrdunk(dataRes, productType)
    console.log(`✅ Extracted Snkrdunk data!!!`);
    console.table(snkrMapped, [SIZE_SNKRDUNK, PRICE_SNKRDUNK]);
    return snkrMapped || [];
  } catch (err) {
    console.error('Error during Snkrdunk crawl:', err.message);
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
      },
      timeout: 30000 // 30 second timeout
    });
    if (productType === PRODUCT_TYPE.SHOE) {
      return response?.data?.data || [];
    }
    return response?.data || [];
  } catch (err) {
    console.error('API [' + api + '] call failed:', err.message);
    throw err;
  }
}

async function crawlDataGoat(productId, productType) {
  let browser = null;
  let page = null;
  try {
    browser = await puppeteer.launch(defaultBrowserArgs);
    page = await browser.newPage();
    
    // Set page timeout
    page.setDefaultTimeout(60000); // 60 seconds timeout
    
    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);

    await page.goto(`${searchUrl}?query=${productId}`, { waitUntil: 'networkidle2' });

    const content = await page.content();
    const $ = cheerio.load(content);

    let fullLink = '';
    let cellItemId = '';
      // get first product link
      $('div[data-qa="grid_cell_product"]').each((_i, el) => {
        const aTag = $(el).find('a');
        const link = aTag.attr('href');
        if (productType === PRODUCT_TYPE.SHOE || link?.replace(/^\/+/, '') === productId?.replace(/^\/+/, '')) {
          fullLink = goalDomain + link;
          cellItemId = $(el).attr('data-grid-cell-name');
          return false;
        }
      });
    
    // Close the current page and browser before creating a new one for details
    if (page) await page.close();
    if (browser) await browser.close();
    
    const details = await extractDetailsFromProductGoat(fullLink, productId, cellItemId);
    return details;
  } catch (err) {
    console.error(`❌ Error crawling ${productId}:`, err.message);
    throw err;
  } finally {
    try {
      if (page) await page.close();
      if (browser) await browser.close();
    } catch (closeError) {
      console.error('❌ Error closing browser:', closeError.message);
    }
  }
}

async function extractDetailsFromProductGoat(url, productId, cellItemIdParam) {
  if (!url || !cellItemIdParam) {
    return [];
  }
  
  let browserChild = null;
  let page = null;
  
  try {
    browserChild = await puppeteer.launch(defaultBrowserArgs);
    page = await browserChild.newPage();
    
    // Set page timeout
    page.setDefaultTimeout(60000); // 60 seconds timeout
    
    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);

    await page.setCookie(
      { name: 'currency', value: 'JPY', domain: 'www.goat.com', path: '/', secure: true },
      { name: 'country', value: 'JP', domain: 'www.goat.com', path: '/', secure: true },
    );
    
    await page.goto(url, { waitUntil: 'networkidle2' });
    
    const response = await page.evaluate(async (cellItemIdParam, sizeAndPriceGoatUrl) => {
      const res = await fetch(`${sizeAndPriceGoatUrl}=${cellItemIdParam}`, {
        credentials: 'include',
        headers: {
          'Accept-Language':	'en-US,en;q=0.9',
          'Accept': 'application/json',
          'Referer': 'https://www.goat.com',
          'Origin': 'https://www.goat.com',
        }
      });
      return res.json();
    }, cellItemIdParam, sizeAndPriceGoatUrl);
    
    const html = await page.content();
    const $ = cheerio.load(html);

    let imgSrc = '';
    let imgAlt = '';

    await page.waitForSelector('div.swiper-slide-active', { timeout: 60000 });
    $('div.swiper-slide-active').each((i, el) => {
      const img = $(el).find('img');
      if (img && !imgSrc && !imgAlt) {
        imgSrc = img.attr('src');
        imgAlt = img.attr('alt');
      }
    });
    
    const dataFiltered = getSizeAndPriceGoat(response, productType);
    const products = dataFiltered?.map(item => {
      return {
        [PRODUCT_ID]: productId,
        [PRODUCT_NAME]: imgAlt,
        [IMAGE]: [{ url: imgSrc }],
        [SIZE_GOAT]: item[SIZE_GOAT],
        [PRICE_GOAT]: item[PRICE_GOAT]
      }
    });
    
    console.log(`✅ Extracted Goat data!!!`);
    console.table(products, [PRODUCT_ID, PRODUCT_NAME, SIZE_GOAT, PRICE_GOAT]);
    return products;
  } catch (err) {
    console.error(`❌ Error extract product:`, err.message);
    throw err;
  } finally {
    try {
      if (page) await page.close();
      if (browserChild) await browserChild.close();
    } catch (closeError) {
      console.error('❌ Error closing browser child:', closeError.message);
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

function convertSizeClothes(size) {
  if (!size) {
    return null;
  }

  if (size === 'XXL') {
    return '2XL'
  } else if (size === 'XXXL') {
    return '3XL'
  } else if (size === 'XXXXL') {
    return '4XL'
  }
  return size
}

async function updateStatus(recordId, newStatus) {
  try {
    await base(process.env.DATA_SEARCH_TABLE).update([
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
        [SIZE_SNKRDUNK]: size.toString()?.trim(),
        [PRICE_SNKRDUNK]: item.price
      };
    }).filter(item => item);
  }
  return data?.sizePrices?.map(item => {
    return {
      [SIZE_SNKRDUNK]: convertSizeClothes(item.size.localizedName?.toString()?.trim()),
      [PRICE_SNKRDUNK]: item.minListingPrice
    };
  });
}

function getSizeAndPriceGoat(data, productType) {
  const dataMap = data?.map(item => {
    if (item.shoeCondition === "new_no_defects" && item.stockStatus !== "not_in_stock") {
      const sizeGoat = item.sizeOption.presentation?.toString()?.trim();
      return {
        [SIZE_GOAT]: productType === PRODUCT_TYPE.SHOE ? sizeGoat : convertSizeClothes(sizeGoat?.toUpperCase()),
        [PRICE_GOAT]: item?.lowestPriceCents?.amount / 100 // Convert cents to yen
      };
    }
    return null;
  }).filter(item => item !== null);
  if (productType === PRODUCT_TYPE.SHOE) {
    return dataMap?.filter(item => {
      const sizeGoat = Number(item[SIZE_GOAT]);
      const priceGoat = Number(item[PRICE_GOAT]);
      return conditionCheckSize(sizeGoat, priceGoat)
    });
  }
  return dataMap || [];
}

function conditionCheckSize(sizeItem, nameItem) {
  if (sizeItem && nameItem) {
    if (productType === PRODUCT_TYPE.SHOE) {
      if (sizeItem >= 6 && sizeItem <= 12) {
        return true;
      }
    } else {
      return true;
    }
  }
  return false;
}

app.listen(PORT, async () => {
  console.log(`🚀 Listening on port ${PORT} for Sy`);
});

cron.schedule('0 0 * * *', async () => {
  console.log('⏰ Running scheduled crawl at 0h');
  await triggerAllSearchesFromAirtable();
});

async function triggerAllSearchesFromAirtable() {
  try {
    // Debug environment variables
    console.log(`🔧 Environment check:`, {
      MAIN_URL: process.env.MAIN_URL,
      PORT: PORT,
      DATA_SEARCH_TABLE: process.env.DATA_SEARCH_TABLE
    });

    const records = await base(process.env.DATA_SEARCH_TABLE).select().all();
    if (records.length === 0) {
      console.warn('⚠️ No records found in the Airtable table.');
      return;
    }

    console.log(`📋 Found ${records.length} records to process`);

    // Reduce concurrency limit to prevent resource exhaustion
    const adjustedConcurrencyLimit = Math.min(CONCURRENCY_LIMIT, 2);
    const limit = pLimit(adjustedConcurrencyLimit);

    // Process records in smaller batches to prevent overwhelming the system
    const batchSize = 5;
    const batches = [];
    
    for (let i = 0; i < records.length; i += batchSize) {
      batches.push(records.slice(i, i + batchSize));
    }

    console.log(`📦 Processing ${records.length} records in ${batches.length} batches`);

    let totalSuccessCount = 0;
    let totalErrorCount = 0;
    let totalSkippedCount = 0;

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      console.log(`🔄 Processing batch ${batchIndex + 1}/${batches.length} with ${batch.length} records`);

      const tasks = batch.map((record) =>
        limit(async () => {
          const recordId = record.id;
          const productId = record.get(PRODUCT_ID);
          const snkrdunkApi = record.get('Snkrdunk API');
          const productType = record.get('Product Type');
          
          // Debug logging
          console.log(`🔍 Record data:`, {
            recordId,
            productId,
            snkrdunkApi,
            productType
          });

          if (!productId || !snkrdunkApi) {
            console.warn(`⚠️ Bỏ qua record thiếu dữ liệu: ${recordId}`);
            return {
              status: 'skipped',
              productId,
            };
          }
          
          // Validate that values are strings and not empty
          if (typeof productId !== 'string' || productId.trim() === '') {
            console.warn(`⚠️ Invalid productId for record ${recordId}: ${productId}`);
            return {
              status: 'skipped',
              productId,
              reason: 'Invalid productId'
            };
          }
          
          if (typeof snkrdunkApi !== 'string' || snkrdunkApi.trim() === '') {
            console.warn(`⚠️ Invalid snkrdunkApi for record ${recordId}: ${snkrdunkApi}`);
            return {
              status: 'skipped',
              productId,
              reason: 'Invalid snkrdunkApi'
            };
          }

          // Use the actual server URL instead of localhost
          const baseUrl = process.env.MAIN_URL || `http://localhost:${PORT || 3000}`;
          
          // Validate baseUrl
          if (!baseUrl || baseUrl === 'undefined') {
            console.error(`❌ Invalid baseUrl: ${baseUrl}`);
            return {
              status: 'rejected',
              productId,
              reason: 'Invalid baseUrl configuration',
            };
          }
          
          // Ensure baseUrl doesn't end with slash
          const cleanBaseUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
          const url = `${cleanBaseUrl}/search?recordId=${encodeURIComponent(recordId)}&productId=${encodeURIComponent(productId)}&snkrdunkApi=${encodeURIComponent(snkrdunkApi)}&productType=${encodeURIComponent(productType || PRODUCT_TYPE.SHOE)}`;
          
          // Validate URL
          // try {
          //   new URL(url);
          // } catch (urlError) {
          //   console.error(`❌ Invalid URL generated: ${url}`);
          //   return {
          //     status: 'rejected',
          //     productId,
          //     reason: `Invalid URL: ${urlError.message}`,
          //   };
          // }

          console.log(`📤 Triggering crawl for ${productId} (${recordId})`);
          console.log(`🔗 URL: ${url}`);

          try {
            const response = await axios.get(url, { 
              timeout: 900000,
              headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; CrawlBot/1.0)',
                'Accept': 'application/json'
              }
            });
            
            console.log(`✅ Successfully triggered crawl for ${productId}: ${response.status}`);
            return {
              status: 'fulfilled',
              productId,
              response: response.data
            };
          } catch (err) {
            console.error(`❌ Error calling for ${productId}:`, err.message);
            console.error(`❌ Error details:`, {
              code: err.code,
              status: err.response?.status,
              statusText: err.response?.statusText,
              url: url
            });
            // Update status to error if the request fails
            try {
              await updateStatus(recordId, STATUS_ERROR);
            } catch (updateErr) {
              console.error(`❌ Failed to update status for ${recordId}:`, updateErr.message);
            }
            return {
              status: 'rejected',
              productId,
              reason: err.message,
            };
          }
        })
      );

      const results = await Promise.allSettled(tasks);

      let batchSuccessCount = 0;
      let batchErrorCount = 0;
      let batchSkippedCount = 0;

      results.forEach((result) => {
        if (result.status === 'fulfilled') {
          const { status, productId, reason } = result.value;
          if (status === 'rejected') {
            console.error(`❌ Lỗi với sản phẩm ${productId}: ${reason}`);
            batchErrorCount++;
          } else if (status === 'skipped') {
            console.warn(`⚠️ Bỏ qua sản phẩm không đủ dữ liệu: ${productId}`);
            batchSkippedCount++;
          } else {
            console.log(`✅ Đã crawl xong: ${productId}`);
            batchSuccessCount++;
          }
        } else {
          console.error(`❌ Promise thất bại ngoài mong đợi`, result.reason);
          batchErrorCount++;
        }
      });

      totalSuccessCount += batchSuccessCount;
      totalErrorCount += batchErrorCount;
      totalSkippedCount += batchSkippedCount;

      console.log(`📊 Batch ${batchIndex + 1} Summary: ${batchSuccessCount} success, ${batchErrorCount} errors, ${batchSkippedCount} skipped`);

      // Add delay between batches to prevent resource exhaustion
      if (batchIndex < batches.length - 1) {
        console.log(`⏳ Waiting 2 seconds before next batch...`);
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    console.log(`📊 Final Crawl Summary: ${totalSuccessCount} success, ${totalErrorCount} errors, ${totalSkippedCount} skipped`);
    
  } catch (err) {
    console.error('❌ Lỗi khi lấy record từ Airtable:', err.message);
    throw err;
  }
}