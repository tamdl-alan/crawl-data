// Import
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

// ========== Config Airable Start ========== //
Airtable.configure({
  apiKey: 'pat1PRTNBV90VSC5V.04105a19c23f69b8fc6f65ba2ee3eab9786ae74878aef5aafe03fd25c8a9b2a8'
});
const base = Airtable.base('appQMNeEtUsgz8lQg');
const table = base('Crawling Processes');
// ========== Config Airable End ========== //


// ========== Common Start ========== //
const PORT = 3000;

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
// ========== Common End ========== //


// ========== Snkrdunk Start ========== //
const EMAIL_SNKRDUNK = 'cathoiloi1135@gmail.com';
const PASSWORD_SNKRDUNK = 'Sy123456';
const snkrdunkDomain = 'https://snkrdunk.com';
let cookieHeader = '';
// ========== Snkrdunk End========== //


// ========== Goal Start ========== //
const goalDomain = 'https://www.goat.com';
const searchUrl = 'https://www.goat.com/search';
// ========== Goal End ========== //

app.get('/search', async (req, res) => {
  const params = req.query;
  const recordId = params.recordId;
  const productId = params.productId;
  const snkrdunkApi = params.snkrdunkApi;
  const productType = params.productType || PRODUCT_TYPE.SHOE;
  // await updateStatus(recordId, STATUS_CRAWLING);
  const data = await crawlDataSnkrdunk(snkrdunkApi, productType);
  console.log('data:', data);
  const dataGoal = await crawlData(`${searchUrl}?query=${productId}`, productId)
  console.log('dataGoal:', dataGoal);
  // await updateStatus(recordId, STATUS_SUCCESS);
});

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

async function extractDetailsFromProductGoat(url) {
  if (!url) {
    return [];
  }
  let browserChild;
  try {
    puppeteer.use(StealthPlugin());
    browserChild = await puppeteer.launch(defaultBrowserArgs);
    const page = await browserChild.newPage();

    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);

    await page.setCookie(
      { name: 'currency', value: 'JPY', domain: 'www.goat.com', path: '/', secure: true },
      { name: 'country', value: 'JP', domain: 'www.goat.com', path: '/', secure: true },
    );
    await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
    // await acceptCookiesIfPresent(page);

    const html = await page.content();
    const $ = cheerio.load(html);

    let imgSrc = '';
    let imgAlt = '';
    let sku = '';
    const products = [];

    $('div.window-item').each((i, el) => {
      const spans = $(el).find('span');
      if (spans.length >= 2 && $(spans[0]).text().trim() === 'SKU') {
        sku = $(spans[1]).text().trim();
      }
    });

    $('div.swiper-slide-active').each((i, el) => {
      const img = $(el).find('img');
      if (img && !imgSrc && !imgAlt) {
        imgSrc = img.attr('src');
        imgAlt = img.attr('alt');
      }
    });

    $('div.swiper-slide').each((i, el) => {
      const text = $(el).children()?.text()?.split('¥');
      if (text.length && text[0] && text[1] && text[0] <= 12 && text[0] >= 6 && !products.some(product => product.Size === text[0].trim())) {
        const dataRow = {
                          'Product ID': sku,
                          'Production Name': imgAlt,
                          Image: [{ url: imgSrc }],
                          Size: text[0],
                          Price: '¥' + text[1]
                        };
        products.push(dataRow);
      }
    });
    console.log(`✅ Extracted data!!!`);
    console.table(products, ['Product ID', 'Production Name', 'Size', 'Price']);
    return products;
  } catch (err) {
    await updateStatus(recordId, STATUS_ERROR);
    console.error(`❌ Error parsing product ${url}:`, err.message);
    return [];
  } finally {
    if (browserChild) {
      await browserChild.close();
    }
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
    await updateStatus(recordId, STATUS_ERROR);
    console.log('Not found Accept All Cookies button');
  }
}

async function crawlData(url, productId) {
  puppeteer.use(StealthPlugin());
  const browser = await puppeteer.launch(defaultBrowserArgs);
  const page = await browser.newPage();
  try {
    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

    // await autoScrollUntilNoMoreItems(page);
    const content = await page.content();
    const $ = cheerio.load(content);

    let fullLink = '';

    $('a').each(async (i, el) => {
      const link = $(el).attr('href');
      if (link && link.includes(productId?.toLowerCase())) {
        fullLink = goalDomain + link;
        return;
      }
    });

    // const allDetails = [];
    // let i = 0;
    // for (const link of links.slice(0, 2)) {
    //   console.log(`Started link: -----[${link}]-----[${i + 1}/${links.length}]-----[${new Date().toLocaleString()}]`);
      const details = await extractDetailsFromProductGoat(fullLink);
    //   allDetails.push(...details);
    //   console.log(`End link: -----[${link}]-----[${i + 1}/${links.length}]-----[${new Date().toLocaleString()}]`);
    //   i++;
    // }

    // if (allDetails.length > 0) {
      // await pushToAirtable(details);
    // } else {
    //   console.log('❌ No data to save.');
    // }
    return details;
  } catch (err) {
    await updateStatus(recordId, STATUS_ERROR);
    console.error(`❌ Error crawling ${url}:`, err.message);
  } finally {
    await page.close();
    await browser.close();
  }
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
    await base("Data Search").update([
      {
        id: recordId,
        fields: {
          Status: newStatus
        }
      }
    ]);

    console.log(`✅ Đã cập nhật Status của ${recordId} thành "${newStatus}"`);
  } catch (err) {
    console.error('❌ Lỗi khi cập nhật:', err);
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

async function snkrdunkLogin() {
  const browser = await puppeteer.launch(defaultBrowserArgs);
  try {
    if (cookieHeader) {
      return
    }
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 800 });
    await page.goto('https://snkrdunk.com/accounts/login', { waitUntil: 'networkidle2' });
    await page.type('input[name="email"]', EMAIL_SNKRDUNK, { delay: 50 });
    await page.type('input[name="password"]', PASSWORD_SNKRDUNK, { delay: 50 });
    await page.evaluate(() => document.querySelector('form').submit());
    const cookies = await page.cookies();
    cookieHeader = cookies.map(c => `${c.name}=${c.value}`).join('; ');
    await browser.close();
  } catch (err) {
      console.error('Snkrdunk login failed:', err.message);
      // Retry login if it fails
      cookieHeader = '';
      await snkrdunkLogin();
      throw err;
  } finally {
      await browser.close();
  }
}

async function snkrdunkfetchData(api) {
  const apiUrl = `${snkrdunkDomain}/v1/${api}`;
  console.log('📦 Fetching API data...');
  try {
    const response = await axios.get(apiUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
        'Cookie': cookieHeader,
        'Referer': snkrdunkDomain,
        'Origin': snkrdunkDomain
      }
    });
    return response?.data || null;
  } catch (err) {
    console.error('API [' + api + '] call failed:', err.message);
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
        size: size,
        price: item.price
      };
    }).filter(item => item !== null);
  }
  return data?.sizePrices?.map(item => {
    return {
      size: item.size.localizedName,
      price: item.minListingPrice
    };
  });
}

app.listen(PORT, () => console.log(`Listening on port ${PORT}`));