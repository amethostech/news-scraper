import axios from 'axios';
import * as cheerio from 'cheerio';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { getRealisticHeaders, randomDelay, detectErrorType, getDelayForError } from '../utils/antiBot.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from '../utils/sitemap.js';
import { fetchRSSFeed } from '../utils/rssParser.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';

/**
 * Wait/delay helper for Puppeteer pages (replaces deprecated waitForTimeout)
 */
function waitForDelay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const SOURCE_NAME = 'EndpointsNews';
const BASE_URL = 'https://endpoints.news';

// Reusable browser instance
let browserInstance = null;

/**
 * Get or create browser instance (reuse for multiple requests)
 */
async function getBrowser() {
    if (browserInstance && browserInstance.isConnected()) {
        return browserInstance;
    }

    try {
        console.log('[BROWSER] Initializing Puppeteer-Extra with Stealth plugin...');
        const startTime = Date.now();

        const { addExtra } = await import('puppeteer-extra');
        const StealthPlugin = (await import('puppeteer-extra-plugin-stealth')).default;
        const puppeteerModule = await import('puppeteer');
        const puppeteer = puppeteerModule.default || puppeteerModule;

        console.log('[BROWSER] Modules imported, setting up stealth plugin...');
        const puppeteerExtra = addExtra(puppeteer);
        puppeteerExtra.use(StealthPlugin());

        console.log('[BROWSER] Launching browser (this may take 30-60 seconds on first run)...');
        browserInstance = await puppeteerExtra.launch({
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-background-networking',
                '--single-process' // Use single process to reduce resource usage
            ],
            timeout: 300000 // 300 seconds (5 minutes) for browser launch
        });

        const launchTime = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`[BROWSER] ✓ Using Puppeteer-Extra with Stealth plugin (launched in ${launchTime}s)`);
        return browserInstance;
    } catch (error) {
        console.log(`[BROWSER] ⚠️  Stealth plugin failed: ${error.message}`);
        console.log('[BROWSER] Falling back to regular Puppeteer...');

        try {
            const puppeteer = await import('puppeteer');
            const puppeteerDefault = puppeteer.default || puppeteer;
            browserInstance = await puppeteerDefault.launch({
                headless: 'new',
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-plugins',
                    '--single-process' // Use single process to reduce resource usage
                ],
                timeout: 300000 // 300 seconds (5 minutes) for browser launch
            });
            console.log('[BROWSER] ✓ Using regular Puppeteer (fallback)');
            return browserInstance;
        } catch (fallbackError) {
            console.error(`[BROWSER] ✗ Both stealth and regular Puppeteer failed: ${fallbackError.message}`);
            throw fallbackError;
        }
    }
}

/**
 * Scrape with Puppeteer as fallback
 */
async function scrapeWithPuppeteer(url) {
    let page = null;
    try {
        const browser = await getBrowser();
        page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

        // Set extra headers
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        });

        // Use domcontentloaded instead of networkidle2 (faster, more reliable)
        // Try multiple strategies with fallback
        try {
            await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: 120000 // 120 seconds for Cloudflare
            });
        } catch (navError) {
            // Fallback: try with load event
            try {
                await page.goto(url, {
                    waitUntil: 'load',
                    timeout: 90000
                });
            } catch (loadError) {
                // Last resort: just wait for commit
                await page.goto(url, {
                    waitUntil: 'commit',
                    timeout: 90000
                });
            }
        }

        // Check for Cloudflare challenge
        try {
            const cloudflareCheck = await page.evaluate(() => {
                return document.body?.textContent?.includes('Checking your browser') ||
                    document.body?.textContent?.includes('Just a moment') ||
                    document.title?.includes('Just a moment');
            });

            if (cloudflareCheck) {
                console.log(`[${SOURCE_NAME}] Cloudflare challenge detected, waiting...`);
                await waitForDelay(15000); // Wait 15 seconds for Cloudflare
            }
        } catch (e) {
            // Ignore check errors
        }

        // Wait for content to load (increased from 8 to 10 seconds)
        await waitForDelay(10000);

        const html = await page.content();
        return html;
    } catch (error) {
        throw new Error(`Puppeteer error: ${error.message}`);
    } finally {
        if (page) {
            try {
                await page.close();
            } catch (e) {
                // Ignore
            }
        }
    }
}

/**
 * Scrape article details from Endpoints News with comprehensive error handling
 */
export async function scrapeArticleDetails(url) {
    if (!url || typeof url !== 'string' || !url.includes('endpoints.news')) {
        throw new Error('Invalid URL');
    }

    let html = null;

    // Try axios first
    try {
        await randomDelay(2000, 4000);

        const response = await axios.get(url, {
            headers: getRealisticHeaders(BASE_URL),
            timeout: 20000, // Increased timeout
            maxRedirects: 5,
            validateStatus: function (status) {
                return status < 500;
            }
        });

        if (response.status === 403 || response.status === 429) {
            throw new Error(`Blocked (HTTP ${response.status})`);
        }

        if (response.status === 404) {
            throw new Error('Article not found (404)');
        }

        if (!response.data || typeof response.data !== 'string') {
            throw new Error('Invalid response data');
        }

        html = response.data;

        if (html.includes('Access Denied') || html.includes('403') || html.includes('blocked') || html.includes('Cloudflare')) {
            throw new Error('Blocked (content check)');
        }
    } catch (error) {
        // Fallback to Puppeteer if blocked or axios fails
        if (error.message.includes('404') || error.message.includes('not found')) {
            throw error; // Don't retry 404s
        }

        console.log(`[PUPPETEER] Using browser automation for ${url.substring(0, 60)}...`);
        try {
            html = await scrapeWithPuppeteer(url);
        } catch (puppeteerError) {
            // Retry once with longer wait
            if (puppeteerError.message.includes('timeout') || puppeteerError.message.includes('Timed out')) {
                console.log(`[RETRY] Retrying ${url.substring(0, 60)}... with longer timeout`);
                try {
                    await sleep(5000); // Wait 5 seconds before retry
                    html = await scrapeWithPuppeteer(url);
                } catch (retryError) {
                    console.error(`Error scraping article ${url} (after retry):`, retryError.message);
                    throw retryError;
                }
            } else {
                console.error(`Error scraping article ${url}:`, puppeteerError.message);
                throw puppeteerError;
            }
        }
    }

    if (!html || typeof html !== 'string' || html.length < 100) {
        throw new Error('Invalid or empty HTML content');
    }

    // Parse HTML with Cheerio
    let $;
    try {
        $ = cheerio.load(html);
    } catch (error) {
        throw new Error(`Failed to parse HTML: ${error.message}`);
    }

    try {

        // Extract title
        let title = '';
        const titleSelectors = [
            'h1.article-title',
            'h1',
            'meta[property="og:title"]',
            '.entry-title',
            '.article-header h1',
            'article h1'
        ];
        for (const selector of titleSelectors) {
            if (selector.startsWith('meta')) {
                const meta = $(selector).attr('content');
                if (meta) {
                    title = meta.trim();
                    break;
                }
            } else {
                const el = $(selector).first();
                if (el.length) {
                    title = el.text().trim();
                    if (title.length > 0) break;
                }
            }
        }

        // Extract author
        let author = '';
        const authorSelectors = [
            '.author-name',
            '.byline-author',
            '[rel="author"]',
            '.article-author',
            'meta[name="author"]',
            'meta[property="article:author"]',
            '.contributor-name',
            '.byline',
            '.author'
        ];

        for (const selector of authorSelectors) {
            if (selector.startsWith('meta')) {
                const metaContent = $(selector).attr('content');
                if (metaContent) {
                    author = metaContent.trim();
                    break;
                }
            } else {
                const authorText = $(selector).first().text().trim();
                if (authorText) {
                    author = authorText.replace(/^by\s+/i, '').trim();
                    break;
                }
            }
        }

        // Extract date
        let articleDate = null;
        const dateSelectors = [
            'time[datetime]',
            '.published-date',
            '.article-date',
            '[itemprop="datePublished"]',
            'meta[property="article:published_time"]',
            'meta[name="date"]',
            '.date-published',
            'time'
        ];

        for (const selector of dateSelectors) {
            if (selector.startsWith('meta')) {
                const metaContent = $(selector).attr('content');
                if (metaContent) {
                    articleDate = new Date(metaContent);
                    if (!isNaN(articleDate.getTime())) break;
                }
            } else if (selector.includes('[datetime]')) {
                const datetime = $(selector).first().attr('datetime');
                if (datetime) {
                    articleDate = new Date(datetime);
                    if (!isNaN(articleDate.getTime())) break;
                }
            } else {
                const dateText = $(selector).first().text().trim();
                if (dateText) {
                    articleDate = new Date(dateText);
                    if (!isNaN(articleDate.getTime())) break;
                }
            }
        }

        // Extract body text
        let bodyText = '';
        const bodySelectors = [
            '.article-body',
            '.article-content',
            '[itemprop="articleBody"]',
            '.content-body',
            'article .body',
            '.story-body',
            'main article',
            '#article-content',
            '.post-content',
            '.article-text',
            'article p'
        ];

        for (const selector of bodySelectors) {
            const content = $(selector);
            if (content.length > 0) {
                content.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share').remove();


                bodyText = content.text()
                    .replace(/\s+/g, ' ')
                    .replace(/\n+/g, '\n')
                    .replace(/Get free access to a limited number of articles, plus choose newsletters to get straight to your inbox\./gi, '')
                    .replace(/Unlock this article by subscribing/gi, '')
                    .trim();

                if (bodyText.length > 100) {
                    break;
                }
            }
        }

        // Fallback: get all paragraph text
        if (!bodyText || bodyText.length < 100) {
            const paragraphs = $('article p, .article p, main p')
                .map((i, el) => $(el).text().trim())
                .get()
                .filter(p => p.length > 40);

            bodyText = paragraphs.join('\n\n').replace(/\s+/g, ' ').trim();
        }

        // Validate extracted data
        if (!bodyText || bodyText.trim().length < 50) {
            throw new Error('Insufficient content extracted (less than 50 characters)');
        }

        return {
            title: title || 'Untitled',
            author: author || 'Unknown',
            date: articleDate || null,
            extract: bodyText.trim(),
            link: url,
        };
    } catch (error) {
        console.error(`Error parsing article ${url}:`, error.message);
        throw error;
    }
}

/**
 * Retry wrapper with exponential backoff
 */
async function retryWithBackoff(fn, maxRetries = 3, baseDelay = 1000) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return await fn();
        } catch (error) {
            if (attempt === maxRetries - 1) {
                throw error;
            }
            const delay = baseDelay * Math.pow(2, attempt);
            console.log(`[${SOURCE_NAME}] Retry ${attempt + 1}/${maxRetries} after ${delay}ms...`);
            await sleep(delay);
        }
    }
}

/**
 * Discover article URLs from sitemap with comprehensive error handling
 */
async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from sitemap...`);

    const sitemapUrl = `${BASE_URL}/sitemap.xml`;
    const allUrls = [];
    let errorCount = 0;
    const maxErrors = 10; // Stop if too many errors

    try {
        // Try to fetch sitemap index first with retry
        let sitemaps = [];
        try {
            sitemaps = await retryWithBackoff(
                () => fetchSitemapIndex(sitemapUrl),
                3,
                2000
            );
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Could not fetch sitemap index:`, error.message);
            // Continue to try direct sitemap
        }

        if (sitemaps.length > 0) {
            // It's a sitemap index - fetch each sitemap
            console.log(`[${SOURCE_NAME}] Found ${sitemaps.length} sitemaps in index`);

            for (let i = 0; i < sitemaps.length; i++) {
                const subSitemapUrl = sitemaps[i];
                if (errorCount >= maxErrors) {
                    console.warn(`[${SOURCE_NAME}] Too many errors (${errorCount}), stopping sitemap discovery`);
                    break;
                }

                try {
                    await sleep(1000);
                    const sitemapData = await retryWithBackoff(
                        () => fetchArticleLinksFromSitemap(subSitemapUrl),
                        2,
                        1000
                    );

                    if (Array.isArray(sitemapData)) {
                        sitemapData.forEach(item => {
                            try {
                                const url = typeof item === 'string' ? item : (item?.url || '');
                                if (url && url.includes('endpoints.news') && isValidUrl(cleanUrl(url))) {
                                    allUrls.push(cleanUrl(url));
                                }
                            } catch (e) {
                                // Skip invalid URLs
                            }
                        });
                    }

                    if ((i + 1) % 10 === 0) {
                        console.log(`[${SOURCE_NAME}] Processed ${i + 1}/${sitemaps.length} sitemaps, found ${allUrls.length} URLs...`);
                    }
                } catch (error) {
                    errorCount++;
                    console.warn(`[${SOURCE_NAME}] Error fetching sitemap ${subSitemapUrl}:`, error.message);
                    continue;
                }
            }
        } else {
            // Direct sitemap with URLs
            try {
                const sitemapData = await retryWithBackoff(
                    () => fetchArticleLinksFromSitemap(sitemapUrl),
                    3,
                    2000
                );

                if (Array.isArray(sitemapData)) {
                    sitemapData.forEach(item => {
                        try {
                            const url = typeof item === 'string' ? item : (item?.url || '');
                            if (url && url.includes('endpoints.news') && isValidUrl(cleanUrl(url))) {
                                allUrls.push(cleanUrl(url));
                            }
                        } catch (e) {
                            // Skip invalid URLs
                        }
                    });
                }
            } catch (error) {
                console.warn(`[${SOURCE_NAME}] Error fetching direct sitemap:`, error.message);
            }
        }

        console.log(`[${SOURCE_NAME}] Found ${allUrls.length} article URLs from sitemap`);
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Error discovering URLs from sitemap:`, error.message);
    }

    return Array.from(new Set(allUrls)); // Deduplicate
}

/**
 * Discover article URLs from RSS feeds
 */
async function discoverUrlsFromRSS() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from RSS feeds...`);

    const rssUrls = [
        'https://endpoints.news/rss',
        'https://endpoints.news/feed',
        'https://endpoints.news/rss.xml',
        'https://endpoints.news/feed/rss',
        'https://endpoints.news/atom.xml'
    ];

    const allUrls = [];

    for (const rssUrl of rssUrls) {
        try {
            await sleep(500);
            const metadata = await fetchRSSFeed(rssUrl);

            if (!Array.isArray(metadata)) {
                continue;
            }

            metadata.forEach(item => {
                try {
                    if (item && item.link && typeof item.link === 'string' && item.link.includes('endpoints.news')) {
                        const clean = cleanUrl(item.link);
                        if (isValidUrl(clean)) {
                            allUrls.push(clean);
                        }
                    }
                } catch (e) {
                    // Skip invalid items
                }
            });

            if (metadata.length > 0) {
                console.log(`[${SOURCE_NAME}] Found ${metadata.length} URLs from ${rssUrl}`);
                // Don't break - try all feeds to get maximum coverage
            }
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error fetching RSS feed ${rssUrl}:`, error.message);
            continue;
        }
    }

    return Array.from(new Set(allUrls)); // Deduplicate
}

/**
 * Discover URLs from homepage
 */
async function discoverUrlsFromHomepage() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from homepage...`);
    const allUrls = new Set();

    try {
        await sleep(2000);
        const response = await axios.get(BASE_URL, {
            headers: getRealisticHeaders(),
            timeout: 15000,
            validateStatus: (status) => status < 500
        });

        if (response.status === 200 && response.data) {
            const $ = cheerio.load(response.data);

            $('a[href]').each((i, el) => {
                try {
                    let href = $(el).attr('href');
                    if (!href || typeof href !== 'string') return;

                    if (href.startsWith('/')) {
                        href = `${BASE_URL}${href}`;
                    }

                    if (href.includes('endpoints.news') &&
                        !href.includes('/page/') &&
                        !href.includes('/category/') &&
                        !href.includes('/tag/') &&
                        !href.includes('/author/') &&
                        !href.includes('/feed') &&
                        !href.includes('/rss') &&
                        (href.match(/\/\d{4}\/\d{2}\/\d{2}\//) || // Date-based URLs
                            (href.includes('/') && href.split('/').length >= 4))) { // Article-like URLs
                        const clean = cleanUrl(href);
                        if (isValidUrl(clean)) {
                            allUrls.add(clean);
                        }
                    }
                } catch (e) {
                    // Skip invalid links
                }
            });
        }
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Error discovering URLs from homepage:`, error.message);
    }

    console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs from homepage`);
    return Array.from(allUrls);
}

/**
 * Discover article URLs from category/archive pages with pagination and error handling
 */
async function discoverUrlsFromCategories() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from category/archive pages...`);

    const allUrls = new Set();
    let consecutiveErrors = 0;
    const maxConsecutiveErrors = 5;

    // Try date-based archives (Endpoints News uses date-based URLs)
    const currentYear = new Date().getFullYear();
    const startYear = 2015; // Endpoints News started around 2015

    console.log(`[${SOURCE_NAME}] Checking date-based archives from ${startYear} to ${currentYear}...`);

    for (let year = startYear; year <= currentYear; year++) {
        if (consecutiveErrors >= maxConsecutiveErrors) {
            console.log(`[${SOURCE_NAME}] Too many consecutive errors, stopping archive discovery`);
            break;
        }

        let yearErrors = 0;
        for (let month = 1; month <= 12; month++) {
            try {
                // Try different archive URL patterns
                const archivePatterns = [
                    `${BASE_URL}/${year}/${String(month).padStart(2, '0')}`,
                    `${BASE_URL}/archive/${year}/${String(month).padStart(2, '0')}`,
                    `${BASE_URL}/${year}/${month}`
                ];

                let foundPattern = false;
                for (const archiveUrl of archivePatterns) {
                    try {
                        await sleep(1000);
                        const response = await axios.get(archiveUrl, {
                            headers: getRealisticHeaders(BASE_URL),
                            timeout: 15000,
                            validateStatus: (status) => status < 500,
                            maxRedirects: 5
                        });

                        if (response.status === 404) continue;

                        if (!response.data || typeof response.data !== 'string') {
                            continue;
                        }

                        const $ = cheerio.load(response.data);
                        let foundUrls = 0;

                        // Extract article links
                        $('a[href]').each((i, el) => {
                            try {
                                let href = $(el).attr('href');
                                if (!href || typeof href !== 'string') return;

                                if (href.startsWith('/')) {
                                    href = `${BASE_URL}${href}`;
                                }

                                if (href.includes('endpoints.news') &&
                                    !href.includes('/page/') &&
                                    !href.includes('/category/') &&
                                    !href.includes('/tag/') &&
                                    !href.includes('/author/') &&
                                    !href.includes('/feed') &&
                                    !href.includes('/rss') &&
                                    (href.match(/\/\d{4}\/\d{2}\/\d{2}\//) || // Date-based URLs
                                        (href.includes('/') && href.split('/').length >= 4))) { // Article-like URLs
                                    const clean = cleanUrl(href);
                                    if (isValidUrl(clean)) {
                                        allUrls.add(clean);
                                        foundUrls++;
                                    }
                                }
                            } catch (e) {
                                // Skip invalid links
                            }
                        });

                        if (foundUrls > 0) {
                            foundPattern = true;
                            consecutiveErrors = 0; // Reset error counter
                        }

                        if (allUrls.size > 0 && allUrls.size % 1000 === 0) {
                            console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs so far...`);
                        }

                        break; // Found working pattern
                    } catch (error) {
                        if (error.response?.status !== 404) {
                            yearErrors++;
                        }
                        continue; // Try next pattern
                    }
                }

                if (!foundPattern) {
                    yearErrors++;
                }
            } catch (error) {
                yearErrors++;
                consecutiveErrors++;
                continue;
            }
        }

        if (yearErrors >= 12) {
            consecutiveErrors++;
        } else {
            consecutiveErrors = 0;
        }

        if (year % 2 === 0) {
            console.log(`[${SOURCE_NAME}] Processed ${year}, found ${allUrls.size} URLs so far...`);
        }
    }

    console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs from category/archive pages`);
    return Array.from(allUrls);
}

/**
 * Historical scraping: Discover and scrape all articles
 */
export async function scrapeHistorical(options = {}) {
    const {
        maxArticles = null,
        testMode = false
    } = options;

    console.log(`\n=== ${SOURCE_NAME} HISTORICAL SCRAPING STARTING ===`);
    if (testMode) {
        console.log(`⚠️  TEST MODE: Only scraping first 10 articles`);
    }
    if (maxArticles) {
        console.log(`⚠️  LIMIT: Maximum ${maxArticles} articles`);
    }
    console.log('');

    // Create rate limiter
    const { limiter } = await createRateLimiter(BASE_URL, {
        delayBetweenRequests: 3000, // 3 seconds
        delayJitter: 1000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 30000
    });

    // Discover URLs from multiple sources with graceful degradation
    console.log(`[${SOURCE_NAME}] Step 1: Discovering article URLs from multiple sources...`);

    let sitemapUrls = [];
    let rssUrls = [];
    let categoryUrls = [];
    let homepageUrls = [];

    // Try each discovery method independently - if one fails, continue with others
    try {
        sitemapUrls = await discoverUrlsFromSitemap();
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Sitemap discovery failed:`, error.message);
        sitemapUrls = [];
    }

    try {
        rssUrls = await discoverUrlsFromRSS();
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] RSS discovery failed:`, error.message);
        rssUrls = [];
    }

    try {
        categoryUrls = await discoverUrlsFromCategories();
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Category discovery failed:`, error.message);
        categoryUrls = [];
    }

    try {
        homepageUrls = await discoverUrlsFromHomepage();
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Homepage discovery failed:`, error.message);
        homepageUrls = [];
    }

    // Combine and deduplicate
    const allUrls = Array.from(new Set([...sitemapUrls, ...rssUrls, ...categoryUrls, ...homepageUrls]));

    if (allUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found from any discovery method. Cannot proceed.`);
        if (browserInstance) {
            try {
                await browserInstance.close();
            } catch (e) {
                // Ignore
            }
            browserInstance = null;
        }
        return { total: 0, saved: 0, failed: 0 };
    }

    console.log(`[${SOURCE_NAME}] Discovery summary: Sitemap=${sitemapUrls.length}, RSS=${rssUrls.length}, Categories=${categoryUrls.length}, Homepage=${homepageUrls.length}, Total=${allUrls.length}`);

    console.log(`[${SOURCE_NAME}] Found ${allUrls.length} total article URLs`);

    // Apply limits
    let urlsToProcess = allUrls;
    if (testMode) {
        urlsToProcess = allUrls.slice(0, 10);
        console.log(`[${SOURCE_NAME}] TEST MODE: Processing only first 10 URLs`);
    } else if (maxArticles) {
        urlsToProcess = allUrls.slice(0, maxArticles);
        console.log(`[${SOURCE_NAME}] LIMIT: Processing first ${maxArticles} URLs`);
    }

    // Check existing articles (from both CSV and MongoDB)
    console.log(`[${SOURCE_NAME}] Step 2: Checking for existing articles...`);
    const csvFilePath = getCSVFilePath(SOURCE_NAME);
    const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
    const existingLinks = await readExistingLinksCombined(csvFilePath);
    const newUrls = urlsToProcess.filter(url => !existingLinks.has(url));

    console.log(`[${SOURCE_NAME}] ${newUrls.length} new articles (${urlsToProcess.length - newUrls.length} already exist)`);

    if (newUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] All articles already scraped.`);
        if (browserInstance) {
            try {
                await browserInstance.close();
            } catch (e) {
                // Ignore
            }
            browserInstance = null;
        }
        return { total: allUrls.length, saved: 0, failed: 0, skipped: urlsToProcess.length };
    }

    // Scrape articles
    console.log(`[${SOURCE_NAME}] Step 3: Scraping ${newUrls.length} articles...`);
    console.log(`[${SOURCE_NAME}] Rate: ~3 seconds per article\n`);

    const articles = [];
    let failed = 0;
    const startTime = Date.now();

    // Handle process interruptions gracefully
    let interrupted = false;
    const cleanup = async () => {
        if (articles.length > 0) {
            try {
                await appendArticlesToCSV(articles, SOURCE_NAME);
                console.log(`[${SOURCE_NAME}] 💾 Saved ${articles.length} articles before exit`);
            } catch (e) {
                console.error(`[${SOURCE_NAME}] Error saving final batch:`, e.message);
            }
        }
        if (browserInstance) {
            try {
                await browserInstance.close();
            } catch (e) {
                // Ignore
            }
            browserInstance = null;
        }
    };

    process.on('SIGINT', async () => {
        console.log(`\n[${SOURCE_NAME}] Interrupted! Saving progress...`);
        interrupted = true;
        await cleanup();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log(`\n[${SOURCE_NAME}] Terminated! Saving progress...`);
        interrupted = true;
        await cleanup();
        process.exit(0);
    });

    for (let i = 0; i < newUrls.length; i++) {
        if (interrupted) break;

        const url = newUrls[i];
        let retries = 0;
        const maxRetries = 2;
        let article = null;

        while (retries <= maxRetries) {
            try {
                await limiter.wait();

                article = await scrapeArticleDetails(url);

                // Validate article data
                if (!article || !article.link) {
                    throw new Error('Invalid article data: missing link');
                }

                if (!article.extract || article.extract.length < 50) {
                    throw new Error('Invalid article data: insufficient content');
                }

                article.source = SOURCE_NAME;
                article.scrapedAt = new Date();
                article.title = article.title || 'Untitled';
                article.author = article.author || 'Unknown';

                articles.push(article);
                break; // Success

            } catch (error) {
                retries++;
                if (retries > maxRetries) {
                    failed++;
                    const errorMsg = error.message || 'Unknown error';
                    const errorType = detectErrorType(error);
                    console.error(`[${i + 1}/${newUrls.length}] ✗ Failed after ${maxRetries + 1} attempts: ${url.substring(0, 60)}... - ${errorMsg} [${errorType}]`);
                    break;
                } else {
                    // Adaptive retry delay based on error type
                    const errorType = detectErrorType(error);
                    const baseDelay = getDelayForError(errorType, 2000);
                    const delay = baseDelay * Math.pow(2, retries - 1);
                    console.log(`[${i + 1}/${newUrls.length}] Retry ${retries}/${maxRetries} for ${url.substring(0, 60)}... (${errorType}, waiting ${delay}ms)`);
                    await sleep(delay);
                }
            }
        }

        if (article) {
            const elapsed = Math.floor((Date.now() - startTime) / 1000);
            const rate = elapsed > 0 ? articles.length / elapsed * 60 : 0;
            const remaining = newUrls.length - (i + 1);
            const eta = rate > 0 ? Math.ceil(remaining / rate) : 0;

            const year = article.date ? article.date.getFullYear() : '?';
            const titlePreview = article.title ? article.title.substring(0, 50) : 'Untitled';
            console.log(`[${i + 1}/${newUrls.length}] ✓ [${year}] ${titlePreview}... (ETA: ${eta}m)`);
        }

        // Save in batches (every 50 articles or every 5 minutes)
        if (articles.length >= 50 || (Date.now() - startTime > 300000 && articles.length > 0)) {
            try {
                await appendArticlesToCSV(articles, SOURCE_NAME);
                console.log(`[${SOURCE_NAME}] 💾 Saved batch of ${articles.length} articles`);
                articles.length = 0;
            } catch (error) {
                console.error(`[${SOURCE_NAME}] Error saving batch:`, error.message);
                // Continue anyway - don't lose progress
            }
        }
    }

    // Save remaining articles
    if (articles.length > 0) {
        try {
            await appendArticlesToCSV(articles, SOURCE_NAME);
            console.log(`[${SOURCE_NAME}] 💾 Saved final batch of ${articles.length} articles`);
        } catch (error) {
            console.error(`[${SOURCE_NAME}] Error saving final batch:`, error.message);
        }
    }

    // Cleanup browser
    if (browserInstance) {
        try {
            await browserInstance.close();
        } catch (e) {
            // Ignore cleanup errors
        }
        browserInstance = null;
    }

    // Remove signal handlers
    process.removeAllListeners('SIGINT');
    process.removeAllListeners('SIGTERM');

    const totalTime = Math.floor((Date.now() - startTime) / 1000 / 60);

    console.log(`\n=== ${SOURCE_NAME} HISTORICAL SCRAPING COMPLETE ===`);
    console.log(`Total URLs discovered: ${allUrls.length}`);
    console.log(`URLs processed: ${urlsToProcess.length}`);
    console.log(`New articles: ${newUrls.length}`);
    console.log(`Saved: ${newUrls.length - failed}`);
    console.log(`Failed: ${failed}`);
    console.log(`Time taken: ${totalTime} minutes`);

    return {
        total: allUrls.length,
        processed: urlsToProcess.length,
        new: newUrls.length,
        saved: newUrls.length - failed,
        failed
    };
}

export async function run(options = {}) {
    console.log(`\n=== ${SOURCE_NAME} SCRAPER ===`);
    console.log(`Source: ${BASE_URL}\n`);

    if (options.historical) {
        return await scrapeHistorical({
            testMode: options.testMode || false,
            maxArticles: options.maxArticles || null
        });
    } else {
        // Default: RSS update
        console.log('\n=== ENDPOINTS NEWS SCRAPER STARTING ===');

        const sourceConfig = {
            sourceName: SOURCE_NAME,
            rssUrls: [
                'https://endpoints.news/rss',
                'https://endpoints.news/feed',
                'https://endpoints.news/rss.xml'
            ],
            maxConcurrent: 2,
            delayBetweenScrapes: 3000
        };

        try {
            const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);

            console.log(`\n=== ENDPOINTS NEWS SCRAPER COMPLETE ===`);
            console.log(`Total articles in feed: ${results.total}`);
            console.log(`New articles found: ${results.new}`);
            console.log(`Articles saved: ${results.saved}`);
            console.log(`Failed: ${results.failed}`);
            console.log(`Skipped (already exist): ${results.skipped}`);

            return {
                saved: results.saved || 0,
                failed: results.failed || 0,
                total: results.total || 0
            };
        } catch (error) {
            console.error('Error in Endpoints News scraper:', error);
            // Ensure browser is closed on error
            if (browserInstance) {
                try {
                    await browserInstance.close();
                } catch (e) {
                    // Ignore
                }
                browserInstance = null;
            }
            return { saved: 0, failed: 0, total: 0 };
        }
    }
}

