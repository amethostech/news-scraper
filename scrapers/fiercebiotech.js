import axios from 'axios';
import * as cheerio from 'cheerio';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { getRealisticHeaders, randomDelay, retryWithBackoff, detectErrorType, getDelayForError } from '../utils/antiBot.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from '../utils/sitemap.js';
import { fetchRSSFeed } from '../utils/rssParser.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';

// Reusable browser instance
let browserInstance = null;

/**
 * Wait/delay helper for Puppeteer pages (replaces deprecated waitForTimeout)
 */
function waitForDelay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get or create browser instance (reuse for multiple requests)
 */
async function getBrowser() {
    if (browserInstance && browserInstance.isConnected()) {
        return browserInstance;
    }
    
    try {
        // Try to use puppeteer-extra with stealth plugin (better bot protection bypass)
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
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
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
        // Fallback to regular Puppeteer if stealth not available
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
 * Scrape article details using Puppeteer (for sites with Cloudflare/bot protection)
 */
async function scrapeWithPuppeteer(url) {
    let page = null;
    try {
        const browser = await getBrowser();
        page = await browser.newPage();
        
        // Set realistic viewport and user agent BEFORE navigation
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        // Set extra headers
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        });
        
        // Navigate to page with better error handling and longer timeouts
        try {
            await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: 120000 // 120 seconds for Cloudflare challenges
            });
        } catch (navError) {
            // If navigation fails, try with load event (less strict)
            try {
                await page.goto(url, {
                    waitUntil: 'load',
                    timeout: 120000
                });
            } catch (loadError) {
                // Last resort: just wait for any content
                await page.goto(url, {
                    waitUntil: 'commit',
                    timeout: 120000
                });
            }
        }
        
        // Check for Cloudflare challenge
        try {
            // Wait for body to be available first
            await page.waitForSelector('body', { timeout: 15000 }).catch(() => {});
            
            const cloudflareCheck = await page.evaluate(() => {
                return document.body?.textContent?.includes('Checking your browser') || 
                       document.body?.textContent?.includes('Just a moment') ||
                       document.title?.includes('Just a moment');
            });
            
            if (cloudflareCheck) {
                console.log(`[${SOURCE_NAME}] Cloudflare challenge detected, waiting...`);
                await waitForDelay(25000); // Wait 25 seconds for Cloudflare
            }
        } catch (e) {
            // Ignore check errors
        }
        
        // Additional wait for content to load
        await waitForDelay(8000);
        
        // Get page content
        const html = await page.content();
        
        // Close page but keep browser open
        await page.close();
        page = null;
        
        return html;
    } catch (error) {
        // Clean up page if it exists
        if (page) {
            try {
                await page.close();
            } catch (e) {
                // Ignore cleanup errors
            }
        }
        console.error(`Puppeteer error for ${url}:`, error.message);
        throw error;
    }
}

/**
 * Close browser (call when done scraping)
 */
export async function closeBrowser() {
    if (browserInstance) {
        try {
            await browserInstance.close();
        } catch (e) {
            // Ignore errors
        }
        browserInstance = null;
    }
}

/**
 * Scrape article details from FierceBiotech with anti-bot protection
 * Falls back to Puppeteer if axios fails
 */
export async function scrapeArticleDetails(url) {
    if (!url || typeof url !== 'string' || !url.includes('fiercebiotech.com')) {
        throw new Error('Invalid URL');
    }
    
    let html = null;
    
    // Try axios first (single attempt, no retries)
    try {
        await randomDelay(2000, 4000);
        
        const response = await axios.get(url, {
            headers: getRealisticHeaders('https://www.fiercebiotech.com/'),
            timeout: 20000, // Increased timeout
            maxRedirects: 5,
            validateStatus: function (status) {
                return status < 500;
            }
        });
        
        // Check HTTP status code
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
        
        // Check for blocking messages in content
        if (html.includes('Access Denied') || html.includes('403') || html.includes('blocked') || html.includes('Cloudflare') || html.includes('challenge')) {
            throw new Error('Blocked (content check)');
        }
    } catch (error) {
        // If axios fails, use Puppeteer with retry logic
        if (error.message.includes('404') || error.message.includes('not found')) {
            throw error; // Don't retry 404s
        }
        
        console.log(`[PUPPETEER] Using browser automation for ${url.substring(0, 60)}...`);
        try {
            html = await scrapeWithPuppeteer(url);
        } catch (puppeteerError) {
            // Retry once if timeout error
            if (puppeteerError.message.includes('timeout') || puppeteerError.message.includes('Timed out')) {
                console.log(`[RETRY] Retrying ${url.substring(0, 60)}... with longer wait`);
                await sleep(10000); // Wait 10 seconds before retry
                html = await scrapeWithPuppeteer(url);
            } else {
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
        
        // Extract author - try multiple selectors (FierceBiotech uses "By Author Name" format)
        let author = '';
        const authorSelectors = [
            '.byline', // FierceBiotech uses .byline
            '.author-name',
            '.byline-author',
            '[rel="author"]',
            '.article-author',
            'meta[name="author"]',
            'meta[property="article:author"]',
            '.contributor-name',
            '.article-byline' // Additional FierceBiotech selector
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
                    // Remove "By " prefix and clean up
                    author = authorText.replace(/^by\s+/i, '').trim();
                    // Sometimes it's "By Author Name Date" - extract just the name
                    author = author.split(/\s+Dec|\s+Jan|\s+Feb|\s+Mar|\s+Apr|\s+May|\s+Jun|\s+Jul|\s+Aug|\s+Sep|\s+Oct|\s+Nov/i)[0].trim();
                    if (author.length > 0) break;
                }
            }
        }
        
        // Extract date - try multiple selectors
        let articleDate = null;
        const dateSelectors = [
            'time[datetime]',
            '.published-date',
            '.article-date',
            '[itemprop="datePublished"]',
            'meta[property="article:published_time"]',
            'meta[name="date"]'
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
        
        // Extract body text - try multiple selectors (FierceBiotech specific)
        let bodyText = '';
        const bodySelectors = [
            'article', // FierceBiotech wraps content in <article>
            '.article-body',
            '.article-content',
            '[itemprop="articleBody"]',
            '.content-body',
            'article .body',
            '.story-body',
            'main article',
            '#article-content',
            '.post-content'
        ];
        
        for (const selector of bodySelectors) {
            const content = $(selector);
            if (content.length > 0) {
                // Remove unwanted elements (FierceBiotech specific)
                content.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share, .byline, header, .article-header, .tags, .categories, .subscribe, .newsletter').remove();
                
                // Get paragraphs within the article
                const paragraphs = content.find('p')
                    .map((i, el) => $(el).text().trim())
                    .get()
                    .filter(p => p.length > 40 && !p.match(/^(Subscribe|Related|Tags|Categories)/i));
                
                bodyText = paragraphs.join('\n\n')
                    .replace(/\s+/g, ' ')
                    .replace(/\n+/g, '\n')
                    .trim();
                
                if (bodyText.length > 100) { 
                    break;
                }
            }
        }
        
        // Fallback: get all paragraph text from article
        if (!bodyText || bodyText.length < 100) {
            const paragraphs = $('article p, .article p, main p')
                .map((i, el) => $(el).text().trim())
                .get()
                .filter(p => {
                    // Filter out navigation, ads, and metadata
                    const pLower = p.toLowerCase();
                    return p.length > 40 && 
                           !pLower.includes('subscribe') &&
                           !pLower.includes('related') &&
                           !pLower.includes('tags') &&
                           !pLower.match(/^(by|dec|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov)/i);
                });

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

const SOURCE_NAME = 'FierceBiotech';
const BASE_URL = 'https://www.fiercebiotech.com';

/**
 * Discover article URLs from sitemap (using Puppeteer due to Cloudflare)
 */
async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from sitemap (using Puppeteer for Cloudflare bypass)...`);
    
    const sitemapUrl = `${BASE_URL}/sitemap.xml`;
    const allUrls = new Set();
    
    try {
        // Use Puppeteer to fetch sitemap (Cloudflare blocks axios)
        const browser = await getBrowser();
        const page = await browser.newPage();
        
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        // Set extra headers before navigation
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        });
        
        // Navigate with fallback strategies
        try {
        try {
            await page.goto(sitemapUrl, {
                waitUntil: 'domcontentloaded',
                timeout: 120000 // 120 seconds for Cloudflare challenges
            });
        } catch (navError) {
            try {
                await page.goto(sitemapUrl, {
                    waitUntil: 'load',
                    timeout: 120000
                });
            } catch (loadError) {
                await page.goto(sitemapUrl, {
                    waitUntil: 'commit',
                    timeout: 120000
                });
            }
        }
        } catch (navError) {
            try {
                await page.goto(sitemapUrl, {
                    waitUntil: 'load',
                    timeout: 90000
                });
            } catch (loadError) {
                await page.goto(sitemapUrl, {
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
                console.log(`[${SOURCE_NAME}] Cloudflare challenge detected in sitemap, waiting...`);
                await waitForDelay(15000); // Wait 15 seconds for Cloudflare
            }
        } catch (e) {
            // Ignore check errors
        }
        
        await waitForDelay(5000); // Additional wait for content
        
        const html = await page.content();
        await page.close();
        
        const $ = cheerio.load(html);
        
        // Check if it's a sitemap index
        const sitemapIndex = $('sitemapindex sitemap loc');
        if (sitemapIndex.length > 0) {
            // It's a sitemap index - fetch each sitemap
            const sitemapUrls = [];
            sitemapIndex.each((i, el) => {
                const loc = $(el).text().trim();
                if (loc && loc.includes('fiercebiotech.com')) {
                    sitemapUrls.push(loc);
                }
            });
            
            console.log(`[${SOURCE_NAME}] Found ${sitemapUrls.length} sitemaps in index`);
            
            for (const subSitemapUrl of sitemapUrls) {
                try {
                    await sleep(2000);
                    const subPage = await browser.newPage();
                    await subPage.setViewport({ width: 1920, height: 1080 });
                    await subPage.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                    
                    await subPage.setExtraHTTPHeaders({
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                    });
                    
                    try {
                    await subPage.goto(subSitemapUrl, {
                        waitUntil: 'domcontentloaded',
                        timeout: 120000
                    });
                    } catch (navError) {
                        try {
                            await subPage.goto(subSitemapUrl, {
                                waitUntil: 'load',
                                timeout: 90000
                            });
                        } catch (loadError) {
                            await subPage.goto(subSitemapUrl, {
                                waitUntil: 'commit',
                                timeout: 90000
                            });
                        }
                    }
                    
                    // Check for Cloudflare
                    try {
                        const cloudflareCheck = await subPage.evaluate(() => {
                            return document.body?.textContent?.includes('Checking your browser') || 
                                   document.body?.textContent?.includes('Just a moment');
                        });
                        if (cloudflareCheck) {
                            await waitForDelay(10000);
                        }
                    } catch (e) {
                        // Ignore
                    }
                    
                    await waitForDelay(3000);
                    
                    const subHtml = await subPage.content();
                    await subPage.close();
                    
                    const $sub = cheerio.load(subHtml);
                    $sub('urlset url loc').each((i, el) => {
                        const url = $sub(el).text().trim();
                        if (url && url.includes('fiercebiotech.com')) {
                            // Accept any article URL (not just date-based pattern)
                            // FierceBiotech URLs can be: /biotech/article-name or /research/article-name
                            if (!url.includes('/page/') && 
                                !url.includes('/category/') &&
                                !url.includes('/tag/') &&
                                !url.includes('/author/') &&
                                !url.includes('/feed') &&
                                !url.includes('/rss') &&
                                (url.match(/\/\d{4}\/\d{2}\//) || // Date-based
                                 url.match(/\/biotech\/|\/research\/|\/manufacturing\/|\/regulatory\/|\/deals\/|\/people\//))) { // Category-based
                                allUrls.add(cleanUrl(url));
                            }
                        }
                    });
                } catch (error) {
                    console.warn(`[${SOURCE_NAME}] Error fetching sitemap ${subSitemapUrl}:`, error.message);
                    continue;
                }
            }
        } else {
            // Direct sitemap with URLs
            $('urlset url loc').each((i, el) => {
                const url = $(el).text().trim();
                if (url && url.includes('fiercebiotech.com')) {
                    // Accept any article URL
                    if (!url.includes('/page/') && 
                        !url.includes('/category/') &&
                        !url.includes('/tag/') &&
                        !url.includes('/author/') &&
                        !url.includes('/feed') &&
                        !url.includes('/rss') &&
                        (url.match(/\/\d{4}\/\d{2}\//) || // Date-based
                         url.match(/\/biotech\/|\/research\/|\/manufacturing\/|\/regulatory\/|\/deals\/|\/people\//))) { // Category-based
                        allUrls.add(cleanUrl(url));
                    }
                }
            });
        }
        
        console.log(`[${SOURCE_NAME}] Found ${allUrls.size} article URLs from sitemap`);
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Error discovering URLs from sitemap:`, error.message);
    }
    
    return Array.from(allUrls);
}

/**
 * Discover article URLs from RSS feed
 */
async function discoverUrlsFromRSS() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from RSS feed...`);
    
    const rssUrl = `${BASE_URL}/rss/xml`;
    const allUrls = [];
    
    try {
        const metadata = await fetchRSSFeed(rssUrl);
        
        if (!Array.isArray(metadata)) {
            console.warn(`[${SOURCE_NAME}] RSS feed returned invalid data format`);
            return allUrls;
        }
        
        metadata.forEach(item => {
            try {
                if (item && item.link && typeof item.link === 'string' && item.link.includes('fiercebiotech.com')) {
                    const clean = cleanUrl(item.link);
                    if (isValidUrl(clean)) {
                        allUrls.push(clean);
                    }
                }
            } catch (e) {
                // Skip invalid items
            }
        });
        
        console.log(`[${SOURCE_NAME}] Found ${allUrls.length} URLs from RSS feed`);
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Error fetching RSS feed:`, error.message);
    }
    
    return allUrls;
}

/**
 * Discover article URLs from category/archive pages with error handling
 */
async function discoverUrlsFromCategories() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from category/archive pages (using Puppeteer for Cloudflare)...`);
    
    const allUrls = new Set();
    let errorCount = 0;
    const maxErrors = 10;
    
    // FierceBiotech categories
    const categories = [
        '/biotech',
        '/research',
        '/manufacturing',
        '/regulatory',
        '/deals',
        '/people'
    ];
    
    let browser = null;
    try {
        browser = await getBrowser();
        
        // Try category pages
        for (let catIdx = 0; catIdx < categories.length; catIdx++) {
            if (errorCount >= maxErrors) {
                console.warn(`[${SOURCE_NAME}] Too many errors (${errorCount}), stopping category discovery`);
                break;
            }
            
            const category = categories[catIdx];
            try {
                const categoryUrl = `${BASE_URL}${category}`;
                console.log(`[${SOURCE_NAME}] Checking category ${catIdx + 1}/${categories.length}: ${categoryUrl}`);
                
                let page = null;
                try {
                    page = await browser.newPage();
                    await page.setViewport({ width: 1920, height: 1080 });
                    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                    
                    await page.goto(categoryUrl, {
                        waitUntil: 'domcontentloaded',
                        timeout: 120000
                    });
                    await waitForDelay(5000);
                    
                    const html = await page.content();
                    await page.close();
                    page = null;
                    
                    const $ = cheerio.load(html);
                    let foundUrls = 0;
                    
                    // Extract article links
                    $('a[href]').each((i, el) => {
                        try {
                            let href = $(el).attr('href');
                            if (!href || typeof href !== 'string') return;
                            
                            if (href.startsWith('/')) {
                                href = `${BASE_URL}${href}`;
                            }
                            
                            if (href.includes('fiercebiotech.com') && 
                                (href.match(/\/\d{4}\/\d{2}\//) || href.match(/\/biotech\/|\/research\/|\/manufacturing\/|\/regulatory\/|\/deals\/|\/people\//)) &&
                                !href.includes('/page/') &&
                                !href.includes('/category/')) {
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
                    
                // Try pagination (increased limit for more articles)
                let consecutiveEmptyPages = 0;
                for (let pageNum = 2; pageNum <= 200; pageNum++) {
                    if (errorCount >= maxErrors) break;
                    
                    try {
                        const pageUrl = `${categoryUrl}?page=${pageNum}`;
                        const subPage = await browser.newPage();
                        await subPage.setViewport({ width: 1920, height: 1080 });
                        await subPage.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                        
                        await subPage.setExtraHTTPHeaders({
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                        });
                        
                        try {
                            await subPage.goto(pageUrl, {
                                waitUntil: 'domcontentloaded',
                                timeout: 120000
                            });
                        } catch (navError) {
                            try {
                                await subPage.goto(pageUrl, {
                                    waitUntil: 'load',
                                    timeout: 120000
                                });
                            } catch (loadError) {
                                await subPage.goto(pageUrl, {
                                    waitUntil: 'commit',
                                    timeout: 120000
                                });
                            }
                        }
                        
                        // Check for Cloudflare
                        try {
                            const cloudflareCheck = await subPage.evaluate(() => {
                                return document.body?.textContent?.includes('Checking your browser') || 
                                       document.body?.textContent?.includes('Just a moment');
                            });
                            if (cloudflareCheck) {
                                await waitForDelay(10000);
                            }
                        } catch (e) {
                            // Ignore
                        }
                        
                        await waitForDelay(3000);
                        
                        const subHtml = await subPage.content();
                        await subPage.close();
                        
                        const $sub = cheerio.load(subHtml);
                        let foundUrlsPage = 0;
                        
                        $sub('a[href]').each((i, el) => {
                            try {
                                let href = $sub(el).attr('href');
                                if (!href || typeof href !== 'string') return;
                                
                                if (href.startsWith('/')) {
                                    href = `${BASE_URL}${href}`;
                                }
                                
                                if (href.includes('fiercebiotech.com') && 
                                    (href.match(/\/\d{4}\/\d{2}\//) || href.match(/\/biotech\/|\/research\/|\/manufacturing\/|\/regulatory\/|\/deals\/|\/people\//)) &&
                                    !href.includes('/page/')) {
                                    const clean = cleanUrl(href);
                                    if (isValidUrl(clean)) {
                                        allUrls.add(clean);
                                        foundUrlsPage++;
                                    }
                                }
                            } catch (e) {
                                // Skip invalid links
                            }
                        });
                        
                        if (foundUrlsPage === 0) {
                            consecutiveEmptyPages++;
                            if (consecutiveEmptyPages >= 3) {
                                // No articles for 3 consecutive pages, likely done
                                break;
                            }
                        } else {
                            consecutiveEmptyPages = 0;
                        }
                        
                        await sleep(2000);
                    } catch (error) {
                        errorCount++;
                        consecutiveEmptyPages++;
                        if (consecutiveEmptyPages >= 3) {
                            break; // Too many errors or empty pages
                        }
                    }
                }
                    
                    console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs from ${category}`);
                } catch (error) {
                    errorCount++;
                    if (page) {
                        try {
                            await page.close();
                        } catch (e) {
                            // Ignore
                        }
                    }
                    console.warn(`[${SOURCE_NAME}] Error checking category ${category}:`, error.message);
                    continue;
                }
            } catch (error) {
                errorCount++;
                console.warn(`[${SOURCE_NAME}] Error processing category ${category}:`, error.message);
                continue;
            }
        }
        
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Error in category discovery:`, error.message);
    }
    
    console.log(`[${SOURCE_NAME}] Found ${allUrls.size} total URLs from category/archive pages`);
    return Array.from(allUrls);
}

/**
 * Discover URLs from homepage
 */
async function discoverUrlsFromHomepage() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from homepage...`);
    const allUrls = new Set();
    
    try {
        const browser = await getBrowser();
        const page = await browser.newPage();
        
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        });
        
        try {
            await page.goto(BASE_URL, {
                waitUntil: 'domcontentloaded',
                timeout: 120000 // 120 seconds for Cloudflare
            });
        } catch (navError) {
            try {
                await page.goto(BASE_URL, {
                    waitUntil: 'load',
                    timeout: 120000
                });
            } catch (loadError) {
                await page.goto(BASE_URL, {
                    waitUntil: 'commit',
                    timeout: 120000
                });
            }
        }
        
        // Check for Cloudflare
        try {
            const cloudflareCheck = await page.evaluate(() => {
                return document.body?.textContent?.includes('Checking your browser') || 
                       document.body?.textContent?.includes('Just a moment');
            });
            if (cloudflareCheck) {
                console.log(`[${SOURCE_NAME}] Cloudflare challenge detected on homepage, waiting...`);
                await waitForDelay(20000); // Wait 20 seconds for Cloudflare
            }
        } catch (e) {
            // Ignore
        }
        
        await waitForDelay(5000);
        
        const html = await page.content();
        await page.close();
        
        const $ = cheerio.load(html);
        
        $('a[href]').each((i, el) => {
            try {
                let href = $(el).attr('href');
                if (!href || typeof href !== 'string') return;
                
                if (href.startsWith('/')) {
                    href = `${BASE_URL}${href}`;
                }
                
                if (href.includes('fiercebiotech.com') && 
                    (href.match(/\/\d{4}\/\d{2}\//) || 
                     href.match(/\/biotech\/|\/research\/|\/manufacturing\/|\/regulatory\/|\/deals\/|\/people\//)) &&
                    !href.includes('/page/') &&
                    !href.includes('/category/')) {
                    const clean = cleanUrl(href);
                    if (isValidUrl(clean)) {
                        allUrls.add(clean);
                    }
                }
            } catch (e) {
                // Skip invalid links
            }
        });
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Error discovering URLs from homepage:`, error.message);
    }
    
    console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs from homepage`);
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
        delayBetweenRequests: 5000, // 5 seconds (Cloudflare-protected)
        delayJitter: 2000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 60000
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
        await closeBrowser();
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
        await closeBrowser();
        return { total: allUrls.length, saved: 0, failed: 0, skipped: urlsToProcess.length };
    }

    // Scrape articles
    console.log(`[${SOURCE_NAME}] Step 3: Scraping ${newUrls.length} articles...`);
    console.log(`[${SOURCE_NAME}] Rate: ~5 seconds per article (Cloudflare-protected)\n`);
    
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
        await closeBrowser();
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
                    const baseDelay = getDelayForError(errorType, 5000);
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
    await closeBrowser();
    
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
        console.log('\n=== FIERCEBIOTECH SCRAPER STARTING ===');
        
        const sourceConfig = {
            sourceName: SOURCE_NAME,
            rssUrl: 'https://www.fiercebiotech.com/rss/xml',
            maxConcurrent: 1, // Process one at a time with Puppeteer (slower but more reliable)
            delayBetweenScrapes: 5000 // 5 second delay between articles
        };

        try {
            const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);
            
            // Close browser when done
            await closeBrowser();
            
            console.log(`\n=== FIERCEBIOTECH SCRAPER COMPLETE ===`);
            console.log(`Total articles in feed: ${results.total}`);
            console.log(`New articles found: ${results.new}`);
            console.log(`Articles saved: ${results.saved}`);
            console.log(`Failed: ${results.failed}`);
            console.log(`Skipped (already exist): ${results.skipped}`);

            return results.saved;
        } catch (error) {
            // Ensure browser is closed on error
            await closeBrowser();
            console.error('Error in FierceBiotech scraper:', error);
            throw error;
        }
    }
}
