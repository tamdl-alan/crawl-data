const puppeteer = require('puppeteer-extra');
const cheerio = require('cheerio');
const Airtable = require('airtable');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

Airtable.configure({
  apiKey: 'patMi6wrO3KKyVuNt.e2b35c2fd1af07f909fc57f5f7ae66d7e7c9e3d5d42530773c8f2295ac6f3669'
});
const base = Airtable.base('app2N6Vy8V8Tjyt6f');
const table = base('Test Crawling Processes');
const userAgent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
const viewPortBrowser = { width: 1920, height: 1200 };
const extraHTTPHeaders = {
  'Accept-Language': 'ja,ja-JP;q=0.9,en;q=0.8'
}
const defaultBrowserArgs = {
  headless: 'new',
  args: ['--no-sandbox', '--disable-setuid-sandbox']
}
const domainDefault = 'https://www.goat.com';
const crawlURLs = [
  'https://www.goat.com/collections/just-dropped',
  // 'https://www.goat.com/collections/sneaker-release-roundup',
  // 'https://www.goat.com/collections/summer-sale-new-balance'
];

async function autoScrollUntilNoMoreItems(page, selectorProp = 'sneakers', maxIdleRounds = 5, waitPerScroll = 1500) {
  let previousHeight = await page.evaluate(() => document.body.scrollHeight);
  let idleRounds = 0;
  let previousItemCount = 0;
  const selector = `a[href^="/${selectorProp}"]`;
  while (idleRounds < maxIdleRounds) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    await new Promise(resolve => setTimeout(resolve, waitPerScroll));

    const [currentHeight, currentItemCount] = await page.evaluate((selector) => {
      return [
        document.body.scrollHeight,
        document.querySelectorAll(selector).length
      ];
    }, selector);

    if (currentHeight > previousHeight || currentItemCount > previousItemCount) {
      console.log(`🔄 Scroll: ${selectorProp}=${currentItemCount}`);
      previousHeight = currentHeight;
      previousItemCount = currentItemCount;
      idleRounds = 0;
    } else {
      idleRounds++;
      console.log(`⏸️ No new content. Idle round ${idleRounds}/${maxIdleRounds}`);
    }
  }

  console.log(`✅ Scrolling done. Final item count: ${previousItemCount}`);
}

async function extractDetailsFromProduct(url) {
  let browserChild;
  const list = [];
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

    await acceptCookiesIfPresent(page);

    const html = await page.content();
    const $ = cheerio.load(html);

    let imgSrc = '';
    let imgAlt = '';
    let sku = '';

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
      if (text.length && text[0] && text[1] && text[0] <= 12 && text[0] >= 6) {
        const dataRow = {
                          ID: sku,
                          Name: imgAlt,
                          Image: [{ url: imgSrc }],
                          Size: text[0],
                          Price: '¥' + text[1]
                        };
        list.push(dataRow);
      }
    });
    console.log(`✅ Extracted data!!!`);
    console.table(list, ['ID', 'Name', 'Size', 'Price']);
    return list;
  } catch (err) {
    console.error(`❌ Error parsing product ${url}:`, err.message);
    return [];
  } finally {
    if (browserChild) {
      await browserChild.close();
    }
  }
}

function chunkArray(array, size) {
  const result = [];
  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }
  return result;
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

async function crawlData(url) {
  puppeteer.use(StealthPlugin());
  const browser = await puppeteer.launch(defaultBrowserArgs);
  const page = await browser.newPage();
  try {
    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

    await autoScrollUntilNoMoreItems(page);
    const content = await page.content();
    const $ = cheerio.load(content);

    const links = [];

    $('a').each((i, el) => {
      const link = $(el).attr('href');
      if (link && link.startsWith('/sneakers')) {
        const fullLink = domainDefault + link;
        if (!links.includes(fullLink)) {
          links.push(fullLink);
        }
      }
    });

    const allDetails = [];
    let i = 0;
    for (const link of links.slice(0, 2)) {
      console.log(`Started link: -----[${link}]-----[${i + 1}/${links.length}]-----[${new Date().toLocaleString()}]`);
      const details = await extractDetailsFromProduct(link);
      allDetails.push(...details);
      console.log(`End link: -----[${link}]-----[${i + 1}/${links.length}]-----[${new Date().toLocaleString()}]`);
      i++;
    }

    // if (allDetails.length > 0) {
    //   await pushToAirtable(allDetails);
    // } else {
    //   console.log('❌ No data to save.');
    // }

  } catch (err) {
    console.error(`❌ Error crawling ${url}:`, err.message);
  } finally {
    await page.close();
    await browser.close();
  }
}

(async () => {
  for (let i = 0; i < crawlURLs.length; i++) {
    const url = crawlURLs[i];
    console.log(`(${i + 1}/${crawlURLs.length}) Start crawl: ${url} ${new Date().toLocaleString()}`);
    await crawlData(url);
    console.log(`(${i + 1}/${crawlURLs.length}) End crawl: ${url} ${new Date().toLocaleString()}`);
  }
  console.log('🎉 All done.');
})();
