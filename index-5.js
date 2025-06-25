// Import
require('dotenv').config();
const puppeteer = require('puppeteer-extra');
const cheerio = require('cheerio');
const axios = require('axios');
const Airtable = require('airtable');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cors = require('cors');
const express = require('express');
const app = express();
app.use(cors());
app.use(express.json());
puppeteer.use(StealthPlugin());

// ========== Config Airable Start ========== //
Airtable.configure({
  apiKey: 'pat1PRTNBV90VSC5V.04105a19c23f69b8fc6f65ba2ee3eab9786ae74878aef5aafe03fd25c8a9b2a8'
});
const base = Airtable.base('appQMNeEtUsgz8lQg');
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
  args: ['--no-sandbox', '--disable-setuid-sandbox']
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
const EMAIL_SNKRDUNK = 'cathoiloi1135@gmail.com';
const PASSWORD_SNKRDUNK = 'Sy123456';
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

app.get('/search', async (req, res) => {
  try {

    // ========== Prepare param ========== //
    const params = req.query;
    recordId = params.recordId;
    const productId = params.productId;
    const snkrdunkApi = params.snkrdunkApi;
    productType = params.productType || PRODUCT_TYPE.SHOE;
    await updateStatus(recordId, STATUS_CRAWLING);

    console.log(`------------Crawling data [${productId}] SNKRDUNK Start: [${new Date()}]------------`);
    const dataSnk = await crawlDataSnkrdunk(snkrdunkApi, productType);
    console.log(`------------Crawling data [${productId}] SNKRDUNK End: [${new Date()}------------`);

    console.log(`------------Crawling data [${productId}] GOAT Start: [${new Date()}------------`);
    const dataGoat = await crawlDataGoat(productId)
    console.log(`------------Crawling data [${productId}] GOAT End: [${new Date()}------------`);

    const mergedArr = mergeData(dataSnk, dataGoat);
    await deleteRecordByProductId(productId);
    await pushToAirtable(mergedArr);
  } catch (error) {
    await updateStatus(recordId, STATUS_ERROR);
    console.error(`❌ Error during crawling process:`, error.message);
    throw error;
  }
  await updateStatus(recordId, STATUS_SUCCESS);
});

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
  console.log(' dataSnk:', dataSnk);
  console.log(' dataGoal:', dataGoal);
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
    await browser.close();
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

    // $('a').each((_i, el) => {
    //   const link = $(el).attr('href');
    //   if (link && link.includes(productId?.toLowerCase())) {
    //     fullLink = goalDomain + link;
    //     return;
    //   }
    // });

    // Lấy item đầu tiên trong danh sách sản phẩm
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
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForSelector('div.swiper-slide-active', { timeout: 60000 });
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
      console.log(' elm:', $(el).children()?.text());
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
      console.log('sizeItem:', sizeItem);
      console.log('nameItem:', nameItem);

      return true;
    }
  }
  return false;
}

app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
