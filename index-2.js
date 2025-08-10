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
    "--disable-gpu",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-features=TranslateUI",
    "--disable-ipc-flooding-protection",
    "--disable-extensions",
    "--disable-plugins",
    "--disable-images",
    "--disable-javascript",
    "--disable-web-security",
    "--disable-features=VizDisplayCompositor",
    "--memory-pressure-off",
    "--max_old_space_size=2048",
    "--single-process",
    "--no-zygote",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--disable-logging",
    "--disable-dev-shm-usage",
    "--disable-accelerated-2d-canvas",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-networking",
    "--disable-component-extensions-with-background-pages",
    "--disable-background-mode"
  ]
}

const STATUS_CRAWLING = 'Crawling';
const STATUS_SUCCESS = 'Success';
const STATUS_ERROR = 'Error';
const PRODUCT_TYPE = {
  SHOE: 'SHOE',
  CLOTHES: 'CLOTHES'
}
const CONCURRENCY_LIMIT = 1; // Giảm xuống 1 để tránh resource exhaustion

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

// ====== Failed Queue Management ====== //
const failedQueue = [];
let isProcessingFailedQueue = false;
let failedQueueProcessedCount = 0;
let failedQueueTotalCount = 0;

// Track retry attempts for each product
const retryAttempts = new Map();
const MAX_RETRY_ATTEMPTS = 2; // Maximum 2 retry attempts

// Legacy failed records tracking (for backward compatibility)
const failedRecords = new Map();

// ====== Crawl-all management ====== //
let isCrawlAllRunning = false;
let crawlAllStartTime = null;
let crawlAllProcessedCount = 0;
let crawlAllTotalCount = 0;
let lastCrawlAllEndTime = null;
const CRAWL_ALL_COOLDOWN = 5 * 60 * 1000; // 5 minutes cooldown between crawl-all

// Browser instance management - RESOURCE OPTIMIZED
let activeBrowsers = new Set();
let browserLaunchSemaphore = 0;
const MAX_CONCURRENT_BROWSERS = 1; // Keep at 1 to prevent resource exhaustion
const BROWSER_LAUNCH_TIMEOUT = 30000; // 30 seconds

// Browser cleanup function - IMPROVED
async function cleanupBrowser(browser) {
  try {
    if (browser && !browser.process()?.killed) {
      const pages = await browser.pages();
      for (const page of pages) {
        try {
          if (!page.isClosed()) {
            await page.close();
          }
        } catch (pageError) {
          console.warn('⚠️ Warning when closing page:', pageError.message);
        }
      }
      await browser.close();
    }
  } catch (error) {
    console.warn('⚠️ Warning when closing browser:', error.message);
  } finally {
    activeBrowsers.delete(browser);
    browserLaunchSemaphore--;
  }
}

// Safe browser launch function - IMPROVED
async function safeLaunchBrowser() {
  if (browserLaunchSemaphore >= MAX_CONCURRENT_BROWSERS) {
    throw new Error('Too many concurrent browsers');
  }
  
  // Check system resources before launching
  if (!checkSystemResources()) {
    throw new Error('System resources insufficient for browser launch');
  }
  
  // Force cleanup if resources are low
  if (activeBrowsers.size > 0) {
    console.warn(`⚠️ Force cleanup before launching new browser...`);
    const browsersToClose = Array.from(activeBrowsers);
    for (const browser of browsersToClose) {
      await cleanupBrowser(browser);
    }
    await new Promise(resolve => setTimeout(resolve, 3000)); // Wait for cleanup
  }
  
  browserLaunchSemaphore++;
  let browser = null;
  
  try {
    browser = await puppeteer.launch({
      ...defaultBrowserArgs,
      timeout: BROWSER_LAUNCH_TIMEOUT
    });
    activeBrowsers.add(browser);
    return browser;
  } catch (error) {
    browserLaunchSemaphore--;
    throw error;
  }
}

// Safe page close function - IMPROVED
async function safeClosePage(page) {
  try {
    if (page && !page.isClosed()) {
      await page.close();
    }
  } catch (error) {
    console.warn('⚠️ Warning when closing page:', error.message);
  }
}

// Helper function to get timeout based on retry attempt
function getTimeoutForRetry(retryAttempt) {
  const baseTimeout = 90000; // 90 seconds base
  const timeoutIncrease = 30000; // 30 seconds increase per retry
  return baseTimeout + (retryAttempt * timeoutIncrease);
}

// Helper function to check system resources
function checkSystemResources() {
  const memUsage = process.memoryUsage();
  const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
  const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
  const externalMB = Math.round(memUsage.external / 1024 / 1024);
  const rssMB = Math.round(memUsage.rss / 1024 / 1024);
  
  console.log(`📊 Memory usage: Heap=${heapUsedMB}MB/${heapTotalMB}MB, External=${externalMB}MB, RSS=${rssMB}MB`);
  
  // Multiple thresholds for different memory types
  if (heapUsedMB > 400) { // Reduced from 500MB to 400MB
    console.warn(`⚠️ High heap memory usage detected: ${heapUsedMB}MB`);
    return false;
  }
  
  if (externalMB > 200) { // External memory threshold
    console.warn(`⚠️ High external memory usage detected: ${externalMB}MB`);
    return false;
  }
  
  if (rssMB > 800) { // RSS memory threshold
    console.warn(`⚠️ High RSS memory usage detected: ${rssMB}MB`);
    return false;
  }
  
  // Check if we have too many active browsers
  if (activeBrowsers.size >= MAX_CONCURRENT_BROWSERS) {
    console.warn(`⚠️ Too many active browsers: ${activeBrowsers.size}`);
    return false;
  }
  
  // Check if semaphore is stuck
  if (browserLaunchSemaphore > activeBrowsers.size + 1) {
    console.warn(`⚠️ Browser semaphore stuck: ${browserLaunchSemaphore} > ${activeBrowsers.size + 1}`);
    return false;
  }
  
  return true;
}

// Queue health check - ENHANCED with resource monitoring
setInterval(() => {
  const now = Date.now();
  const timeSinceLastProcess = now - lastQueueProcessTime;
  
  // Check memory usage
  const memUsage = process.memoryUsage();
  const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
  const rssMB = Math.round(memUsage.rss / 1024 / 1024);
  
  // Emergency cleanup if memory usage is critical
  if (heapUsedMB > 600 || rssMB > 1000) {
    console.warn(`🚨 CRITICAL: High memory usage detected! Heap: ${heapUsedMB}MB, RSS: ${rssMB}MB`);
    console.warn('🚨 Initiating emergency cleanup...');
    
    // Force cleanup all browsers
    const browsersToClose = Array.from(activeBrowsers);
    browsersToClose.forEach(browser => {
      cleanupBrowser(browser).catch(err => console.error('❌ Error during emergency cleanup:', err.message));
    });
    
    // Force garbage collection
    if (global.gc) {
      global.gc();
      console.log('🧹 Emergency garbage collection');
    }
    
    // Reset semaphore
    browserLaunchSemaphore = 0;
  }
  
  // If queue has been processing for more than 10 minutes, reset it (reduced from 15)
  if (isProcessingQueue && timeSinceLastProcess > 10 * 60 * 1000) {
    console.warn('⚠️ Queue has been processing for too long, resetting...');
    isProcessingQueue = false;
    lastQueueProcessTime = now;
    currentProcessingRequest = null;
  }
  
  // If a single request has been processing for more than 5 minutes, log warning (reduced from 8)
  if (currentProcessingRequest && (now - currentProcessingRequest.startTime) > 5 * 60 * 1000) {
    console.warn(`⚠️ Request ${currentProcessingRequest.productId} has been processing for ${Math.round((now - currentProcessingRequest.startTime)/1000)}s`);
  }
  
  // Log queue status every 5 minutes with memory info
  if (now % (5 * 60 * 1000) < 1000) {
    console.log(`📊 Queue Status: length=${requestQueue.length}, processing=${isProcessingQueue}, timeSinceLastProcess=${Math.round(timeSinceLastProcess/1000)}s, activeBrowsers=${activeBrowsers.size}, browserSemaphore=${browserLaunchSemaphore}, memory=${heapUsedMB}MB/${rssMB}MB`);
  }
}, 60000); // Check every 1 minute (increased frequency for better monitoring)

// Periodic browser cleanup - ENHANCED with memory monitoring
setInterval(async () => {
  // Check memory usage
  const memUsage = process.memoryUsage();
  const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
  const rssMB = Math.round(memUsage.rss / 1024 / 1024);
  
  // Cleanup if memory usage is high or too many browsers
  if (activeBrowsers.size > MAX_CONCURRENT_BROWSERS || heapUsedMB > 350 || rssMB > 700) {
    console.warn(`⚠️ Cleanup triggered: browsers=${activeBrowsers.size}, memory=${heapUsedMB}MB/${rssMB}MB`);
    const browsersToClose = Array.from(activeBrowsers);
    for (const browser of browsersToClose) {
      await cleanupBrowser(browser);
    }
    
    // Force garbage collection after cleanup
    if (global.gc) {
      global.gc();
      console.log('🧹 Periodic garbage collection');
    }
  }
  
  // Force cleanup if semaphore is stuck
  if (browserLaunchSemaphore > activeBrowsers.size + 1) {
    console.warn(`⚠️ Browser semaphore stuck (${browserLaunchSemaphore}), resetting...`);
    browserLaunchSemaphore = activeBrowsers.size;
  }
}, 30000); // Check every 30 seconds (increased frequency for better resource management)

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
    failedQueue: {
      length: failedQueue.length,
      isProcessing: isProcessingFailedQueue,
      processedCount: failedQueueProcessedCount,
      totalCount: failedQueueTotalCount,
      maxRetryAttempts: MAX_RETRY_ATTEMPTS,
      retryAttemptsCount: retryAttempts.size
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
    failedQueueLength: failedQueue.length,
    isProcessingFailedQueue: isProcessingFailedQueue,
    failedQueueProcessedCount: failedQueueProcessedCount,
    failedQueueTotalCount: failedQueueTotalCount,
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
    failedQueue: {
      length: failedQueue.length,
      isProcessing: isProcessingFailedQueue,
      processedCount: failedQueueProcessedCount,
      totalCount: failedQueueTotalCount,
      maxRetryAttempts: MAX_RETRY_ATTEMPTS,
      retryAttemptsCount: retryAttempts.size,
      items: failedQueue.slice(0, 5).map(item => ({
        productId: item.productId,
        error: item.error,
        timestamp: item.timestamp,
        retryAttempt: item.retryAttempt
      }))
    },
    currentRequest: currentProcessingRequest ? {
      productId: currentProcessingRequest.productId,
      startTime: new Date(currentProcessingRequest.startTime).toISOString(),
      processingTime: Date.now() - currentProcessingRequest.startTime
    } : null,
    browsers: {
      activeCount: activeBrowsers.size,
      semaphore: browserLaunchSemaphore,
      maxConcurrent: MAX_CONCURRENT_BROWSERS
    },
    system: {
      memory: process.memoryUsage(),
      uptime: process.uptime(),
      timestamp: new Date().toISOString()
    }
  };
  
  res.json(debugInfo);
});

app.get('/failed-queue', (_req, res) => {
  const failedQueueInfo = {
    length: failedQueue.length,
    isProcessing: isProcessingFailedQueue,
    processedCount: failedQueueProcessedCount,
    totalCount: failedQueueTotalCount,
    maxRetryAttempts: MAX_RETRY_ATTEMPTS,
    retryAttempts: Object.fromEntries(retryAttempts),
    items: failedQueue.map(item => ({
      recordId: item.recordId,
      productId: item.productId,
      snkrdunkApi: item.snkrdunkApi,
      productType: item.productType,
      error: item.error,
      timestamp: item.timestamp,
      retryAttempt: item.retryAttempt
    })),
    timestamp: new Date().toISOString()
  };
  
  res.json(failedQueueInfo);
});

app.post('/clear-failed-queue', (_req, res) => {
  const clearedCount = failedQueue.length;
  const clearedRetryAttempts = retryAttempts.size;
  
  failedQueue.length = 0;
  failedQueueProcessedCount = 0;
  failedQueueTotalCount = 0;
  isProcessingFailedQueue = false;
  retryAttempts.clear();
  
  console.log(`🧹 Cleared ${clearedCount} items from failed queue and ${clearedRetryAttempts} retry attempts`);
  
  res.json({
    message: `✅ Cleared ${clearedCount} items from failed queue and ${clearedRetryAttempts} retry attempts`,
    timestamp: new Date().toISOString()
  });
});

app.post('/process-failed-queue', async (_req, res) => {
  if (isProcessingFailedQueue) {
    return res.status(400).json({
      error: '⛔ Failed queue is already being processed'
    });
  }
  
  if (failedQueue.length === 0) {
    return res.status(400).json({
      error: '⛔ Failed queue is empty'
    });
  }
  
  // Send immediate response
  res.json({
    message: `✅ Started processing failed queue with ${failedQueue.length} items`,
    timestamp: new Date().toISOString()
  });
  
  // Process failed queue asynchronously
  processFailedQueue().catch(error => {
    console.error('❌ Error processing failed queue:', error.message);
  });
});

app.get('/retry-attempts', (_req, res) => {
  const retryInfo = {
    maxRetryAttempts: MAX_RETRY_ATTEMPTS,
    currentRetryAttempts: Object.fromEntries(retryAttempts),
    totalProductsWithRetries: retryAttempts.size,
    timestamp: new Date().toISOString()
  };
  
  res.json(retryInfo);
});

app.post('/emergency-cleanup', async (_req, res) => {
  try {
    console.log('🚨 Emergency cleanup initiated...');
    
    // Force garbage collection
    if (global.gc) {
      global.gc();
      console.log('🧹 Forced garbage collection');
    }
    
    // Clear all queues
    const mainQueueLength = requestQueue.length;
    const failedQueueLength = failedQueue.length;
    requestQueue.length = 0;
    failedQueue.length = 0;
    
    // Reset all flags
    isProcessingQueue = false;
    isProcessingFailedQueue = false;
    currentProcessingRequest = null;
    failedQueueProcessedCount = 0;
    failedQueueTotalCount = 0;
    
    // Clean up all browsers
    const browserCount = activeBrowsers.size;
    const browsersToClose = Array.from(activeBrowsers);
    for (const browser of browsersToClose) {
      await cleanupBrowser(browser);
    }
    
    // Reset semaphore
    browserLaunchSemaphore = 0;
    
    // Clear failed records
    const failedCount = failedRecords.size;
    failedRecords.clear();
    
    console.log('🚨 Emergency cleanup completed');
    
    res.json({
      message: '🚨 Emergency cleanup completed',
      details: {
        clearedMainQueue: mainQueueLength,
        clearedFailedQueue: failedQueueLength,
        clearedRetryAttempts: retryAttemptsCount,
        closedBrowsers: browserCount,
        clearedFailedRecords: failedCount,
        memoryUsage: process.memoryUsage()
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      error: 'Failed to perform emergency cleanup',
      details: error.message
    });
  }
});

app.get('/crawl-all-status', (_req, res) => {
  const now = Date.now();
  const status = {
    isRunning: isCrawlAllRunning,
    startTime: crawlAllStartTime ? new Date(crawlAllStartTime).toISOString() : null,
    processedCount: crawlAllProcessedCount,
    totalCount: crawlAllTotalCount,
    lastEndTime: lastCrawlAllEndTime ? new Date(lastCrawlAllEndTime).toISOString() : null,
    cooldownPeriod: CRAWL_ALL_COOLDOWN,
    timestamp: new Date().toISOString()
  };
  
  if (isCrawlAllRunning) {
    const runningTime = now - crawlAllStartTime;
    status.runningTime = runningTime;
    status.runningMinutes = Math.round(runningTime / 60000);
  }
  
  if (lastCrawlAllEndTime) {
    const timeSinceLastEnd = now - lastCrawlAllEndTime;
    status.timeSinceLastEnd = timeSinceLastEnd;
    status.timeSinceLastEndMinutes = Math.round(timeSinceLastEnd / 60000);
    
    if (timeSinceLastEnd < CRAWL_ALL_COOLDOWN) {
      const remainingCooldown = CRAWL_ALL_COOLDOWN - timeSinceLastEnd;
      status.remainingCooldown = remainingCooldown;
      status.remainingCooldownMinutes = Math.round(remainingCooldown / 60000);
      status.canStart = false;
    } else {
      status.canStart = true;
    }
  } else {
    status.canStart = true;
  }
  
  res.json(status);
});

app.post('/force-restart', async (_req, res) => {
  try {
    console.log('🔄 Force restart initiated...');
    
    // Send immediate response
    res.json({
      message: '🔄 Force restart initiated. Application will restart in 5 seconds.',
      timestamp: new Date().toISOString()
    });
    
    // Perform emergency cleanup first
    console.log('🧹 Performing emergency cleanup before restart...');
    
    // Force garbage collection
    if (global.gc) {
      global.gc();
    }
    
    // Clear all queues
    requestQueue.length = 0;
    failedQueue.length = 0;
    
    // Reset all flags
    isProcessingQueue = false;
    isProcessingFailedQueue = false;
    currentProcessingRequest = null;
    failedQueueProcessedCount = 0;
    failedQueueTotalCount = 0;
    
    // Clean up all browsers
    const browsersToClose = Array.from(activeBrowsers);
    for (const browser of browsersToClose) {
      await cleanupBrowser(browser);
    }
    
    // Reset semaphore
    browserLaunchSemaphore = 0;
    
    // Clear failed records
    failedRecords.clear();
    
    console.log('✅ Emergency cleanup completed. Restarting in 5 seconds...');
    
    // Restart after 5 seconds
    setTimeout(() => {
      console.log('🔄 Restarting application...');
      process.exit(0); // This will trigger a restart if using PM2 or similar
    }, 5000);
    
  } catch (error) {
    console.error('❌ Error during force restart:', error.message);
    // Still try to restart even if cleanup fails
    setTimeout(() => {
      console.log('🔄 Force restarting despite cleanup error...');
      process.exit(0);
    }, 5000);
  }
});

app.get('/cleanup-browsers', async (_req, res) => {
  try {
    const browserCount = activeBrowsers.size;
    const browsersToClose = Array.from(activeBrowsers);
    
    for (const browser of browsersToClose) {
      await cleanupBrowser(browser);
    }
    
    res.json({
      message: `✅ Cleaned up ${browserCount} browsers`,
      remainingBrowsers: activeBrowsers.size,
      semaphore: browserLaunchSemaphore
    });
  } catch (error) {
    res.status(500).json({
      error: 'Failed to cleanup browsers',
      details: error.message
    });
  }
});

app.get('/retry-stats', (_req, res) => {
  const stats = {
    failedRecords: Array.from(failedRecords.entries()).map(([recordId, retryCount]) => ({
      recordId,
      retryCount
    })),
    totalFailedRecords: failedRecords.size,
    maxRetries: MAX_RETRIES,
    timestamp: new Date().toISOString()
  };
  
  res.json(stats);
});

app.post('/reset-failed-records', (_req, res) => {
  const clearedCount = failedRecords.size;
  failedRecords.clear();
  
  res.json({
    message: `✅ Cleared ${clearedCount} failed records`,
    timestamp: new Date().toISOString()
  });
});

app.get('/health-check', (_req, res) => {
  const memUsage = process.memoryUsage();
  const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
  const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
  const externalMB = Math.round(memUsage.external / 1024 / 1024);
  const rssMB = Math.round(memUsage.rss / 1024 / 1024);
  
  const healthInfo = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    system: {
      memory: {
        heapUsed: memUsage.heapUsed,
        heapTotal: memUsage.heapTotal,
        external: memUsage.external,
        rss: memUsage.rss,
        heapUsedMB,
        heapTotalMB,
        externalMB,
        rssMB
      },
      uptime: process.uptime(),
      nodeVersion: process.version
    },
    browsers: {
      activeCount: activeBrowsers.size,
      semaphore: browserLaunchSemaphore,
      maxConcurrent: MAX_CONCURRENT_BROWSERS
    },
    queue: {
      length: requestQueue.length,
      isProcessing: isProcessingQueue,
      timeSinceLastProcess: Date.now() - lastQueueProcessTime
    },
    failedQueue: {
      length: failedQueue.length,
      isProcessing: isProcessingFailedQueue,
      processedCount: failedQueueProcessedCount,
      totalCount: failedQueueTotalCount,
      maxRetryAttempts: MAX_RETRY_ATTEMPTS,
      retryAttemptsCount: retryAttempts.size
    },
    failedRecords: failedRecords.size
  };
  
  // Determine overall health status with enhanced criteria
  let overallStatus = 'healthy';
  let warnings = [];
  
  // Memory warnings
  if (heapUsedMB > 400) {
    warnings.push(`High heap memory: ${heapUsedMB}MB`);
    overallStatus = 'warning';
  }
  if (externalMB > 200) {
    warnings.push(`High external memory: ${externalMB}MB`);
    overallStatus = 'warning';
  }
  if (rssMB > 800) {
    warnings.push(`High RSS memory: ${rssMB}MB`);
    overallStatus = 'warning';
  }
  
  // Browser warnings
  if (activeBrowsers.size > MAX_CONCURRENT_BROWSERS) {
    warnings.push(`Too many browsers: ${activeBrowsers.size}`);
    overallStatus = 'warning';
  }
  if (browserLaunchSemaphore > activeBrowsers.size + 1) {
    warnings.push(`Semaphore stuck: ${browserLaunchSemaphore}`);
    overallStatus = 'warning';
  }
  
  // Queue warnings
  if (requestQueue.length > 20) {
    warnings.push(`Queue too long: ${requestQueue.length}`);
    overallStatus = 'warning';
  }
  if (failedQueue.length > 10) {
    warnings.push(`Failed queue too long: ${failedQueue.length}`);
    overallStatus = 'warning';
  }
  if (isProcessingQueue && (Date.now() - lastQueueProcessTime) > 5 * 60 * 1000) {
    warnings.push(`Queue processing too long: ${Math.round((Date.now() - lastQueueProcessTime)/1000)}s`);
    overallStatus = 'error';
  }
  
  // Critical memory usage
  if (heapUsedMB > 600 || rssMB > 1000) {
    overallStatus = 'critical';
    warnings.push('CRITICAL: Memory usage too high!');
  }
  
  healthInfo.status = overallStatus;
  healthInfo.warnings = warnings;
  
  res.json(healthInfo);
});

app.post('/force-cleanup', async (_req, res) => {
  try {
    console.log('🧹 Force cleanup initiated...');
    
    // Clear main queue
    const queueLength = requestQueue.length;
    requestQueue.length = 0;
    
    // Clear failed queue
    const failedQueueLength = failedQueue.length;
    const retryAttemptsCount = retryAttempts.size;
    failedQueue.length = 0;
    failedQueueProcessedCount = 0;
    failedQueueTotalCount = 0;
    isProcessingFailedQueue = false;
    retryAttempts.clear();
    
    // Reset processing flags
    isProcessingQueue = false;
    currentProcessingRequest = null;
    
    // Clean up all browsers
    const browserCount = activeBrowsers.size;
    const browsersToClose = Array.from(activeBrowsers);
    for (const browser of browsersToClose) {
      await cleanupBrowser(browser);
    }
    
    // Reset semaphore
    browserLaunchSemaphore = 0;
    
    // Clear failed records
    const failedCount = failedRecords.size;
    failedRecords.clear();
    
    res.json({
      message: '✅ Force cleanup completed',
      details: {
        clearedQueue: queueLength,
        clearedFailedQueue: failedQueueLength,
        closedBrowsers: browserCount,
        clearedFailedRecords: failedCount,
        resetSemaphore: true
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      error: 'Failed to perform force cleanup',
      details: error.message
    });
  }
});


app.get('/crawl-all', async (_req, res) => {
  try {
    // Check if crawl-all is already running
    if (isCrawlAllRunning) {
      const runningTime = Date.now() - crawlAllStartTime;
      const runningMinutes = Math.round(runningTime / 60000);
      
      console.log(`⛔ Crawl-all is already running for ${runningMinutes} minutes. Rejected new request.`);
      
      return res.status(409).send({ 
        error: '⛔ Crawl-all operation is already in progress',
        details: {
          runningTime: runningTime,
          runningMinutes: runningMinutes,
          processedCount: crawlAllProcessedCount,
          totalCount: crawlAllTotalCount,
          startTime: new Date(crawlAllStartTime).toISOString()
        }
      });
    }
    
    // Check cooldown period
    if (lastCrawlAllEndTime && (Date.now() - lastCrawlAllEndTime) < CRAWL_ALL_COOLDOWN) {
      const remainingCooldown = Math.round((CRAWL_ALL_COOLDOWN - (Date.now() - lastCrawlAllEndTime)) / 1000);
      const remainingMinutes = Math.round(remainingCooldown / 60);
      
      console.log(`⛔ Crawl-all cooldown active. ${remainingMinutes} minutes remaining. Rejected new request.`);
      
      return res.status(429).send({ 
        error: '⛔ Crawl-all cooldown period active',
        details: {
          remainingCooldown: remainingCooldown,
          remainingMinutes: remainingMinutes,
          lastCrawlAllEndTime: new Date(lastCrawlAllEndTime).toISOString(),
          cooldownPeriod: CRAWL_ALL_COOLDOWN
        }
      });
    }
    
    console.log('🚀 Starting crawl-all operation...');
    
    // Set crawl-all as running
    isCrawlAllRunning = true;
    crawlAllStartTime = Date.now();
    crawlAllProcessedCount = 0;
    crawlAllTotalCount = 0;
    
    // Send immediate response to client
    res.status(200).send({ 
      message: '✅ Crawl-all operation started successfully',
      timestamp: new Date().toISOString()
    });
    
    // Trigger the crawl operation asynchronously
    triggerAllSearchesFromAirtable().catch(error => {
      console.error('❌ Error in crawl-all operation:', error.message);
    }).finally(() => {
      // Reset crawl-all status when operation completes
      isCrawlAllRunning = false;
      crawlAllStartTime = null;
      crawlAllProcessedCount = 0;
      crawlAllTotalCount = 0;
      lastCrawlAllEndTime = Date.now();
      console.log('✅ Crawl-all operation completed and status reset');
      console.log(`⏳ Cooldown period started. Next crawl-all available in ${Math.round(CRAWL_ALL_COOLDOWN / 60000)} minutes`);
    });
    
  } catch (error) {
    console.error('❌ Error starting crawl-all:', error.message);
    
    // Reset status on error
    isCrawlAllRunning = false;
    crawlAllStartTime = null;
    crawlAllProcessedCount = 0;
    crawlAllTotalCount = 0;
    lastCrawlAllEndTime = Date.now();
    
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
    
    // Check browser resources before accepting request
    if (activeBrowsers.size >= MAX_CONCURRENT_BROWSERS) {
      console.warn(`⚠️ Too many active browsers (${activeBrowsers.size}), rejecting request`);
      return res.status(503).send({ error: '⛔ Server temporarily unavailable - too many active browsers' });
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
  let retryCount = 0;

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
    
        // Process each request (no retry logic - failed items go to failed queue)
    try {
              // Always cleanup before starting new crawl to prevent resource exhaustion
        if (activeBrowsers.size > 0) {
          console.warn(`⚠️ Cleaning up browsers before crawl to prevent resource exhaustion...`);
          const browsersToClose = Array.from(activeBrowsers);
          for (const browser of browsersToClose) {
            await cleanupBrowser(browser);
          }
          await new Promise(resolve => setTimeout(resolve, 3000)); // Increased wait time for cleanup
          
          // Force garbage collection after cleanup
          if (global.gc) {
            global.gc();
            console.log('🧹 Forced garbage collection after browser cleanup');
          }
        }
      
      console.log(`------------Crawling data [${productId}] SNKRDUNK Start: [${new Date()}]------------`);
      const dataSnk = await crawlDataSnkrdunk(snkrdunkApi, productType);
      console.log(`------------Crawling data [${productId}] SNKRDUNK End: [${new Date()}]------------`);

      console.log(`------------Crawling data [${productId}] GOAT Start: [${new Date()}]------------`);
      const dataGoat = await crawlDataGoat(productId, productType, 0); // Always use retryAttempt = 0 for main queue
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
      
      // Enhanced browser resource error handling
      if (error.message.includes('Failed to launch') || 
          error.message.includes('Resource temporarily unavailable') ||
          error.message.includes('ECONNRESET') ||
          error.message.includes('ENOTFOUND') ||
          error.message.includes('ETIMEDOUT') ||
          error.message.includes('Navigation timeout')) {
        console.warn(`⚠️ Browser resource/timeout error for ${productId}, cleaning up browsers...`);
        try {
          const browsersToClose = Array.from(activeBrowsers);
          for (const browser of browsersToClose) {
            await cleanupBrowser(browser);
          }
          // Reset semaphore if needed
          if (browserLaunchSemaphore > 0) {
            browserLaunchSemaphore = 0;
          }
          // Reduced wait time for better performance
          await new Promise(resolve => setTimeout(resolve, 3000)); // Reduced from 8000ms to 3000ms
        } catch (cleanupError) {
          console.error('❌ Error during browser cleanup:', cleanupError.message);
        }
      }
      
      // Check if it's a resource error that should be added to failed queue
      const isResourceError = error.message.includes('Failed to launch') || 
                             error.message.includes('Resource temporarily unavailable') ||
                             error.message.includes('pthread_create') ||
                             error.message.includes('dbus/bus.cc');
      
      if (isResourceError) {
        // Check retry attempts for this product
        const currentRetries = retryAttempts.get(productId) || 0;
        
        if (currentRetries < MAX_RETRY_ATTEMPTS) {
          // Add to failed queue for retry later
          const failedItem = {
            recordId,
            productId,
            snkrdunkApi,
            productType,
            error: error.message,
            timestamp: new Date().toISOString(),
            retryAttempt: currentRetries + 1
          };
          
          failedQueue.push(failedItem);
          retryAttempts.set(productId, currentRetries + 1);
          console.log(`📥 Added ${productId} to failed queue (retry ${currentRetries + 1}/${MAX_RETRY_ATTEMPTS}). Failed queue length: ${failedQueue.length}`);
          
          // Update status to ERROR
          try {
            await updateStatus(recordId, STATUS_ERROR);
          } catch (updateError) {
            console.error(`❌ Failed to update status for ${recordId}:`, updateError.message);
          }
          
          // Send error response if not already sent
          if (!res.headersSent) {
            res.status(500).send({ 
              error: `❌ Resource error crawling ${productId}: ${error.message}. Added to failed queue for retry (${currentRetries + 1}/${MAX_RETRY_ATTEMPTS}).` 
            });
          }
          
          errorCount++;
        } else {
          // Max retries reached, mark as permanently failed
          console.log(`❌ Max retries (${MAX_RETRY_ATTEMPTS}) reached for ${productId}, marking as permanently failed`);
          
          // Update status to ERROR
          try {
            await updateStatus(recordId, STATUS_ERROR);
          } catch (updateError) {
            console.error(`❌ Failed to update status for ${recordId}:`, updateError.message);
          }
          
          // Send error response if not already sent
          if (!res.headersSent) {
            res.status(500).send({ 
              error: `❌ Resource error crawling ${productId}: ${error.message}. Max retries (${MAX_RETRY_ATTEMPTS}) reached.` 
            });
          }
          
          errorCount++;
        }
      } else {
        // For non-resource errors, just mark as failed without adding to retry queue
        console.log(`❌ Non-resource error for ${productId}, not adding to failed queue`);
        
        // Update status to ERROR
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
        
        errorCount++;
      }
    }
    
    // Clear current processing request
    currentProcessingRequest = null;
    
          // Increased delay between requests to prevent resource exhaustion
      if (requestQueue.length > 0) {
        await new Promise(resolve => setTimeout(resolve, 3000)); // Increased from 2000ms to 3000ms
      }
    
    // Always continue to next request regardless of success/failure
    console.log(`✅ Completed processing ${productId}. Moving to next request...`);
  }
  
  isProcessingQueue = false;
  lastQueueProcessTime = Date.now();
  currentProcessingRequest = null;
  console.log(`✅ Queue processing completed. Processed: ${processedCount}, Success: ${successCount}, Errors: ${errorCount}`);
  
  // Process failed queue if there are failed items
  if (failedQueue.length > 0) {
    console.log(`🔄 Main queue completed. Starting failed queue processing with ${failedQueue.length} items...`);
    await processFailedQueue();
  }
}

async function processFailedQueue() {
  if (isProcessingFailedQueue) {
    console.log('⏳ Failed queue is already being processed, skipping...');
    return;
  }
  
  isProcessingFailedQueue = true;
  failedQueueTotalCount = failedQueue.length;
  failedQueueProcessedCount = 0;
  
  console.log(`🔄 Starting failed queue processing. Failed queue length: ${failedQueue.length}`);

  let processedCount = 0;
  let successCount = 0;
  let errorCount = 0;

  while (failedQueue.length > 0) {
    const failedItem = failedQueue.shift();
    processedCount++;
    failedQueueProcessedCount++;
    
    console.log(`📋 Processing failed item ${processedCount}. Remaining in failed queue: ${failedQueue.length}`);
    console.log(`🔄 Retrying ${failedItem.productId} (retry ${failedItem.retryAttempt}/${MAX_RETRY_ATTEMPTS}, previous error: ${failedItem.error})`);

    const { recordId, productId, snkrdunkApi, productType, retryAttempt } = failedItem;
    
    // Track current processing request
    currentProcessingRequest = {
      productId,
      startTime: Date.now()
    };
    
    try {
      // Only cleanup if really necessary
      if (activeBrowsers.size > MAX_CONCURRENT_BROWSERS + 1) {
        console.warn(`⚠️ Too many browsers before failed item crawl, cleaning up...`);
        const browsersToClose = Array.from(activeBrowsers).slice(0, activeBrowsers.size - MAX_CONCURRENT_BROWSERS);
        for (const browser of browsersToClose) {
          await cleanupBrowser(browser);
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      console.log(`------------Retrying [${productId}] SNKRDUNK Start: [${new Date()}]------------`);
      const dataSnk = await crawlDataSnkrdunk(snkrdunkApi, productType);
      console.log(`------------Retrying [${productId}] SNKRDUNK End: [${new Date()}]------------`);

      console.log(`------------Retrying [${productId}] GOAT Start: [${new Date()}]------------`);
      const dataGoat = await crawlDataGoat(productId, productType, retryAttempt); // Use actual retry attempt
      console.log(`------------Retrying [${productId}] GOAT End: [${new Date()}]------------`);

      const mergedArr = mergeData(dataSnk, dataGoat);
      
      if (!mergedArr?.length) {
        console.warn(`⚠️ No data found for failed Product ID: ${productId}`);
        await updateStatus(recordId, STATUS_ERROR);
        errorCount++;
      } else {
        await deleteRecordByProductId(productId);
        await pushToAirtable(mergedArr);
        await updateStatus(recordId, STATUS_SUCCESS);
        console.log(`✅ Successfully retried ${productId} (attempt ${retryAttempt})`);
        
        // Clear retry attempts for this product on success
        retryAttempts.delete(productId);
        successCount++;
      }
      
    } catch (error) {
      console.error(`❌ Error retrying ${productId}:`, error.message);
      
      // Enhanced browser resource error handling
      if (error.message.includes('Failed to launch') || 
          error.message.includes('Resource temporarily unavailable') ||
          error.message.includes('ECONNRESET') ||
          error.message.includes('ENOTFOUND') ||
          error.message.includes('ETIMEDOUT') ||
          error.message.includes('Navigation timeout')) {
        console.warn(`⚠️ Browser resource/timeout error for failed ${productId}, cleaning up browsers...`);
        try {
          const browsersToClose = Array.from(activeBrowsers);
          for (const browser of browsersToClose) {
            await cleanupBrowser(browser);
          }
          if (browserLaunchSemaphore > 0) {
            browserLaunchSemaphore = 0;
          }
          await new Promise(resolve => setTimeout(resolve, 3000));
        } catch (cleanupError) {
          console.error('❌ Error during browser cleanup:', cleanupError.message);
        }
      }
      
      // Check if this was the final retry attempt
      const currentRetries = retryAttempts.get(productId) || 0;
      
      if (currentRetries >= MAX_RETRY_ATTEMPTS) {
        // Final failure, clear retry attempts
        retryAttempts.delete(productId);
        console.log(`❌ Final failure for ${productId} after ${MAX_RETRY_ATTEMPTS} retry attempts`);
      } else {
        console.log(`❌ Retry ${retryAttempt} failed for ${productId}, will retry again`);
      }
      
      // Update status to ERROR
      try {
        await updateStatus(recordId, STATUS_ERROR);
      } catch (updateError) {
        console.error(`❌ Failed to update status for ${recordId}:`, updateError.message);
      }
      
      errorCount++;
    }
    
    // Clear current processing request
    currentProcessingRequest = null;
    
    // Add delay between failed items
    if (failedQueue.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 1000)); // 1 second delay between failed items
    }
    
    console.log(`✅ Completed retrying ${productId}. Moving to next failed item...`);
  }
  
  isProcessingFailedQueue = false;
  console.log(`✅ Failed queue processing completed. Processed: ${processedCount}, Success: ${successCount}, Errors: ${errorCount}`);
  console.log(`📊 Failed queue summary: ${successCount} recovered, ${errorCount} final failures`);
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

async function crawlDataGoat(productId, productType, retryAttempt = 0) {
  let browser = null;
  let page = null;
  try {
    browser = await safeLaunchBrowser();
    page = await browser.newPage();
    
    // Dynamic timeout based on retry attempt
    const timeout = getTimeoutForRetry(retryAttempt);
    page.setDefaultTimeout(timeout);
    console.log(`⏰ Set timeout to ${timeout}ms for ${productId} (retry ${retryAttempt})`);
    
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
      if (!fullLink && productType === PRODUCT_TYPE.CLOTHES) {
        fullLink = goalDomain + '/' + productId?.replace(/^\/+/, '');
      }
      console.log('🚀 ~ fullLink:', fullLink);
    
    const details = await extractDetailsFromProductGoat(fullLink, productId, retryAttempt);
    return details;
  } catch (err) {
    console.error(`❌ Error crawling ${productId}:`, err.message);
    // Ensure browser is cleaned up on error
    if (browser) {
      try {
        await cleanupBrowser(browser);
      } catch (cleanupErr) {
        console.error('❌ Error during browser cleanup on error:', cleanupErr.message);
      }
    }
    throw err;
  } finally {
    try {
      if (page) await safeClosePage(page);
      if (browser) await cleanupBrowser(browser);
    } catch (closeError) {
      console.error('❌ Error closing browser:', closeError.message);
    }
  }
}

async function extractDetailsFromProductGoat(url, productId, retryAttempt = 0) {
  if (!url) {
    return [];
  }
  
  let browserChild = null;
  let page = null;
  
  try {
    browserChild = await safeLaunchBrowser();
    page = await browserChild.newPage();

    // Dynamic timeout based on retry attempt
    const timeout = getTimeoutForRetry(retryAttempt);
    page.setDefaultTimeout(timeout);
    console.log(`⏰ Set timeout to ${timeout}ms for extractDetails ${productId} (retry ${retryAttempt})`);
    await page.setViewport(viewPortBrowser);
    await page.setUserAgent(userAgent);
    await page.setExtraHTTPHeaders(extraHTTPHeaders);
    await page.setCookie(
      { name: 'currency', value: 'JPY', domain: 'www.goat.com', path: '/', secure: true },
      { name: 'country', value: 'JP', domain: 'www.goat.com', path: '/', secure: true },
    );
    
    let reqUrl = '';
    let requestTimeout = null;
    
    // Set up request listener with timeout
    const requestPromise = new Promise((resolve) => {
      page.on('request', request => {
        const url = request.url();
        if (url.includes(sizeAndPriceGoatUrl)) {
          reqUrl = url;
          if (requestTimeout) clearTimeout(requestTimeout);
          resolve();
        }
      });
      
      // Reduced timeout for better performance
      requestTimeout = setTimeout(() => {
        resolve();
      }, 20000); // Reduced from 30 seconds to 20 seconds
    });
    
    await page.goto(url, { waitUntil: 'networkidle2' });
    await requestPromise;

    if (!reqUrl) {
      console.error('No request URL found for product:', productId);
      throw new Error('No request URL found');
    }

    const response = await page.evaluate(async (reqUrl) => {
      try {
        const res = await fetch(`${reqUrl}`, {
          credentials: 'include',
          headers: {
            'Accept-Language':	'en-US,en;q=0.9',
            'Accept': 'application/json',
            'Referer': 'https://www.goat.com',
            'Origin': 'https://www.goat.com',
          }
        });
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }
        return res.json();
      } catch (fetchError) {
        console.error('Fetch error in page.evaluate:', fetchError.message);
        throw fetchError;
      }
    }, reqUrl);
    
    const html = await page.content();
    const $ = cheerio.load(html);

    let imgSrc = '';
    let imgAlt = '';

    try {
      await page.waitForSelector('div.swiper-slide-active', { timeout: 45000 }); // Reduced from 60 seconds to 45 seconds
      $('div.swiper-slide-active').each((i, el) => {
        const img = $(el).find('img');
        if (img && !imgSrc && !imgAlt) {
          imgSrc = img.attr('src');
          imgAlt = img.attr('alt');
        }
      });
    } catch (selectorError) {
      console.warn(`⚠️ Could not find image selector for ${productId}:`, selectorError.message);
    }
    
    const dataFiltered = getSizeAndPriceGoat(response, productType);
    const products = dataFiltered?.map(item => {
      return {
        [PRODUCT_ID]: productId,
        [PRODUCT_NAME]: imgAlt || productId,
        [IMAGE]: imgSrc ? [{ url: imgSrc }] : [],
        [SIZE_GOAT]: item[SIZE_GOAT],
        [PRICE_GOAT]: item[PRICE_GOAT]
      }
    });
    
    console.log(`✅ Extracted Goat data!!!`);
    console.table(products, [PRODUCT_ID, PRODUCT_NAME, SIZE_GOAT, PRICE_GOAT]);
    return products;
  } catch (err) {
    console.error(`❌ Error extract product ${productId}:`, err.message);
    // Ensure browser is cleaned up on error
    if (browserChild) {
      try {
        await cleanupBrowser(browserChild);
      } catch (cleanupErr) {
        console.error('❌ Error during browser cleanup on error:', cleanupErr.message);
      }
    }
    throw err;
  } finally {
    try {
      if (page) await safeClosePage(page);
      if (browserChild) await cleanupBrowser(browserChild);
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

// Global error handlers
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  console.error('Stack trace:', error.stack);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('🛑 SIGTERM received, shutting down gracefully...');
  
  // Clean up browsers
  const browsersToClose = Array.from(activeBrowsers);
  for (const browser of browsersToClose) {
    await cleanupBrowser(browser);
  }
  
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('🛑 SIGINT received, shutting down gracefully...');
  
  // Clean up browsers
  const browsersToClose = Array.from(activeBrowsers);
  for (const browser of browsersToClose) {
    await cleanupBrowser(browser);
  }
  
  process.exit(0);
});

app.listen(PORT, async () => {
  console.log(`🚀 Listening on port ${PORT}: ${process.env.DATA_SEARCH_TABLE}`);
});

cron.schedule(process.env.CRON_SCHEDULE || '0 * * * *', async () => {
  console.log('⏰ Running scheduled crawl at' + new Date());
  await triggerAllSearchesFromAirtable();
});

async function triggerAllSearchesFromAirtable() {
  try {
    // PRE-CLEANUP: Force cleanup before starting crawl-all
    console.log('🧹 PRE-CLEANUP: Force cleanup before crawl-all...');
    
    // Force garbage collection
    if (global.gc) {
      global.gc();
      console.log('🧹 Forced garbage collection before crawl-all');
    }
    
    // Clear all queues
    const mainQueueLength = requestQueue.length;
    const failedQueueLength = failedQueue.length;
    requestQueue.length = 0;
    failedQueue.length = 0;
    
    // Reset all flags
    isProcessingQueue = false;
    isProcessingFailedQueue = false;
    currentProcessingRequest = null;
    failedQueueProcessedCount = 0;
    failedQueueTotalCount = 0;
    
    // Clean up all browsers
    const browserCount = activeBrowsers.size;
    if (browserCount > 0) {
      console.warn(`⚠️ Cleaning up ${browserCount} browsers before crawl-all...`);
      const browsersToClose = Array.from(activeBrowsers);
      for (const browser of browsersToClose) {
        await cleanupBrowser(browser);
      }
    }
    
    // Reset semaphore
    browserLaunchSemaphore = 0;
    
    // Clear retry attempts
    const retryAttemptsCount = retryAttempts.size;
    retryAttempts.clear();
    
    console.log(`✅ PRE-CLEANUP completed: cleared ${mainQueueLength} main queue, ${failedQueueLength} failed queue, ${browserCount} browsers, ${retryAttemptsCount} retry attempts`);
    
    // Wait for cleanup to settle
    await new Promise(resolve => setTimeout(resolve, 5000));
    
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

    // Reduced concurrency limit to prevent resource exhaustion
    const adjustedConcurrencyLimit = Math.min(CONCURRENCY_LIMIT, 1); // Reduced to 1
    const limit = pLimit(adjustedConcurrencyLimit);

    // Reduced batch size to prevent resource exhaustion
    const batchSize = 3; // Reduced from 8 to 3
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
          
          console.log(`📤 Triggering crawl for ${productId} (${recordId})`);
          console.log(`🔗 URL: ${url}`);

          // Retry logic for API calls
          let retryAttempt = 0;
          const maxApiRetries = 2;
          
          while (retryAttempt <= maxApiRetries) {
            try {
              if (retryAttempt > 0) {
                console.log(`🔄 API Retry attempt ${retryAttempt}/${maxApiRetries} for ${productId}`);
                // Wait before retry with exponential backoff
                await new Promise(resolve => setTimeout(resolve, 3000 * retryAttempt));
              }
              
              const response = await axios.get(url, { 
                timeout: 900000,
                headers: {
                  'User-Agent': 'Mozilla/5.0 (compatible; CrawlBot/1.0)',
                  'Accept': 'application/json'
                },
                validateStatus: function (status) {
                  return status < 500; // Don't throw for 4xx errors
                }
              });
              
              if (response.status >= 400) {
                console.warn(`⚠️ HTTP ${response.status} for ${productId}: ${response.statusText}`);
                return {
                  status: 'rejected',
                  productId,
                  reason: `HTTP ${response.status}: ${response.statusText}`
                };
              }
              
              console.log(`✅ Successfully triggered crawl for ${productId}: ${response.status}`);
              return {
                status: 'fulfilled',
                productId,
                response: response.data
              };
            } catch (err) {
              console.error(`❌ Error calling for ${productId} (attempt ${retryAttempt + 1}):`, err.message);
              console.error(`❌ Error details:`, {
                code: err.code,
                status: err.response?.status,
                statusText: err.response?.statusText,
                url: url
              });
              
              retryAttempt++;
              
              // If it's a 500 error, it might be due to browser resource issues
              if (err.response?.status === 500) {
                console.warn(`⚠️ 500 error for ${productId}, might be browser resource issue`);
                // Clean up browsers if we have too many
                if (activeBrowsers.size > MAX_CONCURRENT_BROWSERS) {
                  console.warn(`⚠️ Cleaning up browsers due to 500 error...`);
                  const browsersToClose = Array.from(activeBrowsers);
                  for (const browser of browsersToClose) {
                    await cleanupBrowser(browser);
                  }
                }
              }
              
              if (retryAttempt > maxApiRetries) {
                // Max retries reached
                console.error(`❌ Failed to call API for ${productId} after ${maxApiRetries} retries`);
                
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
              } else {
                console.log(`⏳ Will retry API call for ${productId} (${retryAttempt}/${maxApiRetries})`);
              }
            }
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

      // Increased delay between batches to prevent resource exhaustion
      if (batchIndex < batches.length - 1) {
        console.log(`⏳ Waiting 10 seconds before next batch...`);
        await new Promise(resolve => setTimeout(resolve, 10000)); // Increased from 5000ms to 10000ms
        
        // Force cleanup between batches
        if (activeBrowsers.size > 0) {
          console.warn(`⚠️ Cleaning up browsers between batches...`);
          const browsersToClose = Array.from(activeBrowsers);
          for (const browser of browsersToClose) {
            await cleanupBrowser(browser);
          }
          
          // Force garbage collection
          if (global.gc) {
            global.gc();
            console.log('🧹 Forced garbage collection between batches');
          }
        }
      }
    }

    console.log(`📊 Final Crawl Summary: ${totalSuccessCount} success, ${totalErrorCount} errors, ${totalSkippedCount} skipped`);
    
    // POST-CLEANUP: Force cleanup after crawl-all
    console.log('🧹 POST-CLEANUP: Force cleanup after crawl-all...');
    
    // Force garbage collection
    if (global.gc) {
      global.gc();
      console.log('🧹 Forced garbage collection after crawl-all');
    }
    
    // Clean up any remaining browsers
    const remainingBrowsers = activeBrowsers.size;
    if (remainingBrowsers > 0) {
      console.warn(`⚠️ Cleaning up ${remainingBrowsers} remaining browsers after crawl-all...`);
      const browsersToClose = Array.from(activeBrowsers);
      for (const browser of browsersToClose) {
        await cleanupBrowser(browser);
      }
    }
    
    // Reset semaphore
    browserLaunchSemaphore = 0;
    
    console.log(`✅ POST-CLEANUP completed: cleaned ${remainingBrowsers} remaining browsers`);
    
    // Wait for cleanup to settle
    await new Promise(resolve => setTimeout(resolve, 3000));
    
  } catch (err) {
    console.error('❌ Lỗi khi lấy record từ Airtable:', err.message);
    
    // Emergency cleanup on error
    console.log('🚨 Emergency cleanup due to error...');
    try {
      if (global.gc) global.gc();
      const browsersToClose = Array.from(activeBrowsers);
      for (const browser of browsersToClose) {
        await cleanupBrowser(browser);
      }
      browserLaunchSemaphore = 0;
      console.log('✅ Emergency cleanup completed');
    } catch (cleanupErr) {
      console.error('❌ Error during emergency cleanup:', cleanupErr.message);
    }
    
    throw err;
  }
}