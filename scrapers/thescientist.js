/**
 * The Scientist Scraper
 * 
 * Source: https://www.the-scientist.com/
 * 
 * Implementation:
 * - Historical scraping: 2000-2024 (via category pages, homepage)
 * - Weekly updates: Homepage + category pages + full content scraping
 * 
 * Note: No RSS feed or sitemap available. Using category pages for discovery.
 */

import axios from 'axios';
import * as cheerio from 'cheerio';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { getRealisticHeaders, randomDelay, detectErrorType, getDelayForError } from '../utils/antiBot.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';
import { fetchRSSFeed } from '../utils/rssParser.js';

// Puppeteer for infinite scroll with stealth plugin
let browserInstance = null;
async function getBrowser() {
    if (browserInstance && browserInstance.isConnected()) {
        return browserInstance;
    }

    try {
        // Use puppeteer-extra with stealth plugin for better bot detection bypass
        console.log(`[${SOURCE_NAME}] Initializing Puppeteer-Extra with Stealth plugin...`);
        const startTime = Date.now();

        const { addExtra } = await import('puppeteer-extra');
        const StealthPlugin = (await import('puppeteer-extra-plugin-stealth')).default;
        const puppeteerModule = await import('puppeteer');
        const puppeteer = puppeteerModule.default || puppeteerModule;

        console.log(`[${SOURCE_NAME}] Modules imported, setting up stealth plugin...`);
        const puppeteerExtra = addExtra(puppeteer);
        puppeteerExtra.use(StealthPlugin());

        console.log(`[${SOURCE_NAME}] Launching browser (this may take 30-60 seconds on first run)...`);
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
                '--disable-background-networking'
            ],
            timeout: 180000 // 180 seconds for browser launch
        });

        const launchTime = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`[${SOURCE_NAME}] ✓ Using Puppeteer-Extra with Stealth plugin (launched in ${launchTime}s)`);
        return browserInstance;
    } catch (error) {
        // Fallback to regular Puppeteer if stealth not available
        console.log(`[${SOURCE_NAME}] ⚠️  Stealth plugin failed: ${error.message}`);
        console.log(`[${SOURCE_NAME}] Falling back to regular Puppeteer...`);

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
                    '--disable-plugins'
                ],
                timeout: 180000 // 180 seconds for browser launch
            });
            console.log(`[${SOURCE_NAME}] ✓ Using regular Puppeteer (fallback)`);
            return browserInstance;
        } catch (fallbackError) {
            console.error(`[${SOURCE_NAME}] ✗ Both stealth and regular Puppeteer failed: ${fallbackError.message}`);
            throw fallbackError;
        }
    }
}

const SOURCE_NAME = 'TheScientist';
const BASE_URL = 'https://www.the-scientist.com';

/**
 * Extract JSON-LD structured data from page
 */
function extractJSONLD($) {
    const jsonLdScripts = $('script[type="application/ld+json"]');
    for (let i = 0; i < jsonLdScripts.length; i++) {
        try {
            const data = JSON.parse($(jsonLdScripts[i]).html());
            if (data['@type'] === 'Article' || data['@type'] === 'NewsArticle') {
                return data;
            }
        } catch (e) {
            continue;
        }
    }
    return null;
}

/**
 * Scrape article details from URL with enhanced bot detection bypass
 */
export async function scrapeArticleDetails(url) {
    if (!url || typeof url !== 'string' || !url.includes('the-scientist.com')) {
        throw new Error('Invalid URL');
    }

    let html = null;

    // Try axios first with retry logic
    try {
        await randomDelay(2000, 4000);

        const response = await axios.get(url, {
            headers: getRealisticHeaders(BASE_URL),
            timeout: 20000,
            maxRedirects: 5,
            validateStatus: (status) => status < 500
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

        // Check for blocking messages
        if (html.includes('Access Denied') || html.includes('403') || html.includes('blocked') ||
            html.includes('Cloudflare') || html.includes('challenge') || html.includes('Just a moment')) {
            throw new Error('Blocked (content check)');
        }
    } catch (error) {
        // Fallback to Puppeteer if blocked or axios fails
        if (error.message.includes('404') || error.message.includes('not found')) {
            throw error; // Don't retry 404s
        }

        console.log(`[${SOURCE_NAME}] [PUPPETEER] Using browser automation for ${url.substring(0, 60)}...`);
        try {
            const browser = await getBrowser();
            const page = await browser.newPage();

            await page.setViewport({ width: 1920, height: 1080 });
            await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

            await page.setExtraHTTPHeaders({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            });

            // Navigate with fallback strategies
            try {
                await page.goto(url, {
                    waitUntil: 'domcontentloaded',
                    timeout: 120000
                });
            } catch (navError) {
                try {
                    await page.goto(url, {
                        waitUntil: 'load',
                        timeout: 90000
                    });
                } catch (loadError) {
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
                        document.body?.textContent?.includes('Just a moment');
                });

                if (cloudflareCheck) {
                    console.log(`[${SOURCE_NAME}] Cloudflare challenge detected, waiting...`);
                    await page.waitForTimeout(15000);
                }
            } catch (e) {
                // Ignore
            }

            await page.waitForTimeout(5000);
            html = await page.content();
            await page.close();
        } catch (puppeteerError) {
            // Retry once with longer wait
            if (puppeteerError.message.includes('timeout') || puppeteerError.message.includes('Timed out')) {
                console.log(`[${SOURCE_NAME}] [RETRY] Retrying with longer timeout...`);
                await sleep(10000);
                try {
                    const browser = await getBrowser();
                    const page = await browser.newPage();
                    await page.setViewport({ width: 1920, height: 1080 });
                    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });
                    await page.waitForTimeout(10000);
                    html = await page.content();
                    await page.close();
                } catch (retryError) {
                    throw retryError;
                }
            } else {
                throw puppeteerError;
            }
        }
    }

    if (!html || typeof html !== 'string' || html.length < 100) {
        throw new Error('Invalid or empty HTML content');
    }

    try {
        const $ = cheerio.load(html);

        // Check for subscription/paywall
        const paywallIndicators = [
            'subscribe',
            'subscription',
            'premium',
            'sign in to continue',
            'log in to read',
            'unlock this article',
            'become a member',
            'already a subscriber',
            'free articles remaining',
            'you have reached your',
            'limited free articles'
        ];

        const pageText = $('body').text().toLowerCase();
        const hasPaywall = paywallIndicators.some(indicator => pageText.includes(indicator));

        if (hasPaywall) {
            console.log(`[${SOURCE_NAME}] ⚠️  Subscription/paywall detected for ${url}`);
            // Still try to extract metadata, but content will be limited
        }

        // Extract title
        let title = $('h1').first().text().trim();

        // Extract author
        let author = $('meta[name="author"]').attr('content') || 'Unknown';

        // Extract date - try JSON-LD first, then meta tags
        let articleDate = null;
        const jsonLd = extractJSONLD($);
        if (jsonLd && jsonLd.datePublished) {
            const tempDate = new Date(jsonLd.datePublished);
            if (!isNaN(tempDate.getTime())) {
                articleDate = tempDate.toISOString().split('T')[0]; // YYYY-MM-DD
            }
        } else {
            const dateMeta = $('meta[property="article:published_time"]').attr('content');
            if (dateMeta) {
                const tempDate = new Date(dateMeta);
                if (!isNaN(tempDate.getTime())) {
                    articleDate = tempDate.toISOString().split('T')[0]; // YYYY-MM-DD
                }
            }
        }

        // Extract content - use article element
        let bodyText = '';
        const articleElement = $('article').first();
        if (articleElement.length > 0) {
            // Remove unwanted elements
            articleElement.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share, aside, .paywall, .subscription, .premium').remove();
            bodyText = articleElement.text()
                .replace(/\s+/g, ' ')
                .replace(/\n+/g, '\n')
                .trim();
        }

        // Fallback: try JSON-LD articleBody
        if (!bodyText || bodyText.length < 200) {
            if (jsonLd && jsonLd.articleBody) {
                bodyText = jsonLd.articleBody;
            }
        }

        // Fallback: get all paragraphs (excluding paywall messages)
        if (!bodyText || bodyText.length < 200) {
            const paragraphs = $('main p, .content p, article p')
                .map((i, el) => $(el).text().trim())
                .get()
                .filter(p => {
                    // Filter out paywall messages
                    const pLower = p.toLowerCase();
                    return p.length > 40 &&
                        !paywallIndicators.some(indicator => pLower.includes(indicator));
                });

            bodyText = paragraphs.join('\n\n').trim();
        }

        // If paywall detected and content is minimal, try to get excerpt from meta tags
        if (hasPaywall && (!bodyText || bodyText.length < 200)) {
            const excerpt = $('meta[name="description"]').attr('content') ||
                $('meta[property="og:description"]').attr('content') ||
                jsonLd?.description;
            if (excerpt && excerpt.length > 100) {
                bodyText = excerpt;
                console.log(`[${SOURCE_NAME}] Using meta description as content (paywall detected)`);
            }
        }

        // For subscription sites, accept shorter content (at least metadata)
        const minContentLength = hasPaywall ? 50 : 100;

        if (!title) {
            throw new Error('No title found');
        }

        if (!bodyText || bodyText.length < minContentLength) {
            if (hasPaywall) {
                // For paywall articles, save what we have (metadata + excerpt)
                console.log(`[${SOURCE_NAME}] ⚠️  Limited content due to subscription paywall: ${url}`);
                // Use title as fallback if no body text
                bodyText = bodyText || title;
            } else {
                throw new Error('Insufficient content extracted');
            }
        }

        return {
            author: author || 'Unknown',
            date: articleDate || null,
            extract: bodyText.trim(),
            link: url,
        };

    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error scraping article ${url}:`, error.message);
        throw error;
    }
}

/**
 * Discover article URLs from category pages
 */
export async function discoverUrlsFromCategories() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from category pages...`);

    // All categories except education/career
    const categories = [
        // News and general
        '/type-group/news',

        // Science categories (all available)
        '/category/science/biochemistry',
        '/category/science/cancer',
        '/category/science/cell-and-molecular-biology',
        '/category/science/developmental-biology',
        '/category/science/evolutionary-biology',
        '/category/science/genetics',
        '/category/science/genome-editing',
        '/category/science/immunology',
        '/category/science/microbiology',
        '/category/science/neuroscience',
        '/category/science/omics',
        '/category/science/physiology',

        // Health & Medicine (all available)
        '/category/health/cell-and-gene-therapy',
        '/category/health/diagnostics',
        '/category/health/drug-discovery-and-development',
        '/category/health/public-health',

        // Society (excluding careers/education)
        '/category/society/community',
        '/category/society/research-ethics',
        '/category/society/science-communication',

        // Technology (all available)
        '/category/technology/artificial-intelligence',
        '/category/technology/business',
        '/category/technology/laboratory-technology',
        '/category/technology/synthetic-biology'
    ];

    const allUrls = new Set();

    for (const category of categories) {
        try {
            const url = `${BASE_URL}${category}`;
            await sleep(2000); // Delay between category pages

            const response = await axios.get(url, {
                headers: getRealisticHeaders(BASE_URL),
                timeout: 15000
            });

            const $ = cheerio.load(response.data);

            // Extract URLs from JSON-LD structured data (ItemList)
            const jsonLdScripts = $('script[type="application/ld+json"]');
            for (let i = 0; i < jsonLdScripts.length; i++) {
                try {
                    const data = JSON.parse($(jsonLdScripts[i]).html());
                    if (data['@type'] === 'ItemList' && data.itemListElement) {
                        data.itemListElement.forEach(item => {
                            if (item.url && item.url.includes('the-scientist.com')) {
                                const path = new URL(item.url).pathname;
                                if (/\/[^\/]+-\d+$/.test(path)) {
                                    allUrls.add(cleanUrl(item.url));
                                }
                            }
                        });
                    }
                } catch (e) {
                    continue;
                }
            }

            // Extract URLs from __NEXT_DATA__ script tag
            const nextDataScript = $('#__NEXT_DATA__');
            if (nextDataScript.length > 0) {
                try {
                    const nextData = JSON.parse(nextDataScript.html());
                    // Navigate through the nested structure to find article slugs
                    if (nextData.props?.pageProps?.content?.latest?.content) {
                        nextData.props.pageProps.content.latest.content.forEach(item => {
                            if (item.slug && item.id) {
                                const url = `${BASE_URL}/${item.slug}-${item.id}`;
                                allUrls.add(cleanUrl(url));
                            }
                        });
                    }
                    // Also check other content arrays
                    if (nextData.props?.pageProps?.content?.featured?.content) {
                        nextData.props.pageProps.content.featured.content.forEach(item => {
                            if (item.slug && item.id) {
                                const url = `${BASE_URL}/${item.slug}-${item.id}`;
                                allUrls.add(cleanUrl(url));
                            }
                        });
                    }
                } catch (e) {
                    // Continue if parsing fails
                }
            }

            // Fallback: Find article links in HTML (for server-rendered content)
            $('a[href]').each((i, el) => {
                let href = $(el).attr('href');
                if (!href) return;

                if (href.startsWith('/')) {
                    href = `${BASE_URL}${href}`;
                }

                if (!href.includes('the-scientist.com')) return;

                try {
                    const urlObj = new URL(href);
                    const path = urlObj.pathname;

                    if (path.includes('/page/') ||
                        path.includes('/category/') ||
                        path.includes('/type/') ||
                        path.includes('/series/') ||
                        path.includes('/magazine') ||
                        path.includes('/webinar') ||
                        path.includes('/multimedia') ||
                        path === '/') {
                        return;
                    }

                    if (/\/[^\/]+-\d+$/.test(path)) {
                        const clean = cleanUrl(href.split('?')[0].split('#')[0]);
                        if (isValidUrl(clean)) {
                            allUrls.add(clean);
                        }
                    }
                } catch (e) {
                    return;
                }
            });

            console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs so far from ${category}`);
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error fetching category ${category}:`, error.message);
            continue;
        }
    }

    return Array.from(allUrls);
}

/**
 * Discover article URLs from homepage
 */
export async function discoverUrlsFromHomepage() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from homepage...`);

    try {
        const response = await axios.get(BASE_URL, {
            headers: getRealisticHeaders(BASE_URL),
            timeout: 15000
        });

        const $ = cheerio.load(response.data);
        const urls = new Set();

        // Find article links - The Scientist articles have numeric IDs at the end
        $('a[href]').each((i, el) => {
            let href = $(el).attr('href');
            if (!href) return;

            // Handle relative URLs
            if (href.startsWith('/')) {
                href = `${BASE_URL}${href}`;
            }

            // Must be from the-scientist.com domain
            if (!href.includes('the-scientist.com')) return;

            // Extract path for pattern matching
            try {
                const urlObj = new URL(href);
                const path = urlObj.pathname;

                // Skip non-article pages
                if (path.includes('/page/') ||
                    path.includes('/category/') ||
                    path.includes('/type/') ||
                    path.includes('/series/') ||
                    path.includes('/magazine') ||
                    path.includes('/webinar') ||
                    path.includes('/multimedia') ||
                    path === '/' ||
                    path.startsWith('/page/')) {
                    return;
                }

                // Match article pattern: /slug-12345 (ends with dash and numbers)
                if (/\/[^\/]+-\d+$/.test(path)) {
                    const clean = cleanUrl(href.split('?')[0].split('#')[0]);
                    if (isValidUrl(clean)) {
                        urls.add(clean);
                    }
                }
            } catch (e) {
                // Invalid URL, skip
                return;
            }
        });

        return Array.from(urls);
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error fetching homepage:`, error.message);
        return [];
    }
}

/**
 * Scrape a single article
 */
async function scrapeArticle(url) {
    const details = await scrapeArticleDetails(url);
    return details;
}

/**
 * Apply rate limit
 */
let rateLimiter = null;
async function applyRateLimit(source) {
    if (!rateLimiter) {
        const { limiter } = await createRateLimiter(BASE_URL, {
            delayBetweenRequests: 8000, // 8 seconds (conservative, no explicit crawl-delay)
            delayJitter: 2000,
            maxConcurrent: 1
        });
        rateLimiter = limiter;
    }
    await rateLimiter.wait();
}

/**
 * Update from homepage (for weekly updates)
 */
export async function updateFromHomepage() {
    console.log(`\n=== ${SOURCE_NAME} WEEKLY UPDATE STARTING ===\n`);

    // Discover URLs from homepage
    const urls = await discoverUrlsFromHomepage();

    if (urls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found on homepage.`);
        return { total: 0, saved: 0, failed: 0 };
    }

    console.log(`[${SOURCE_NAME}] Found ${urls.length} article URLs on homepage`);

    // Check existing articles
    const csvFilePath = getCSVFilePath(SOURCE_NAME);
    const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
    const existingLinks = await readExistingLinksCombined(csvFilePath);
    const newUrls = urls.filter(url => !existingLinks.has(url));

    console.log(`[${SOURCE_NAME}] ${newUrls.length} new articles (${urls.length - newUrls.length} already exist)`);

    if (newUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No new articles found.`);
        return { total: urls.length, saved: 0, failed: 0, skipped: urls.length };
    }

    // Scrape articles
    const articles = [];
    let failed = 0;

    for (let i = 0; i < newUrls.length; i++) {
        const url = newUrls[i];
        try {
            const article = await scrapeArticle(url);
            article.source = SOURCE_NAME;
            article.scrapedAt = new Date();
            articles.push(article);

            console.log(`[${i + 1}/${newUrls.length}] ✓ ${article.extract.substring(0, 50)}...`);
        } catch (error) {
            failed++;
            console.error(`[${i + 1}/${newUrls.length}] ✗ Failed: ${url.substring(0, 60)}... - ${error.message}`);
        }
    }

    // Save articles
    if (articles.length > 0) {
        await appendArticlesToCSV(articles, SOURCE_NAME);
        console.log(`[${SOURCE_NAME}] 💾 Saved ${articles.length} articles`);
    }

    console.log(`\n=== ${SOURCE_NAME} WEEKLY UPDATE COMPLETE ===`);
    console.log(`Total URLs: ${urls.length}`);
    console.log(`New articles: ${newUrls.length}`);
    console.log(`Saved: ${articles.length}`);
    console.log(`Failed: ${failed}`);

    return {
        total: urls.length,
        new: newUrls.length,
        saved: articles.length,
        failed
    };
}

/**
 * Discover URLs from RSS/Atom feeds
 */
async function discoverUrlsFromRSSFeeds() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from RSS/Atom feeds...`);

    // All RSS feeds matching our categories (excluding careers)
    const rssFeeds = [
        // Latest News
        '/atom/latest',

        // Science feeds
        '/atom/science/biochemistry',
        '/atom/science/cancer',
        '/atom/science/cell-and-molecular-biology',
        '/atom/science/developmental-biology',
        '/atom/science/evolutionary-biology',
        '/atom/science/genetics',
        '/atom/science/genome-editing',
        '/atom/science/immunology',
        '/atom/science/microbiology',
        '/atom/science/neuroscience',
        '/atom/science/omics',
        '/atom/science/physiology',

        // Health feeds
        '/atom/health/cell-and-gene-therapy',
        '/atom/health/diagnostics',
        '/atom/health/drug-discovery-and-development',
        '/atom/health/public-health',

        // Society feeds (excluding careers)
        '/atom/society/community',
        '/atom/society/research-ethics',
        '/atom/society/science-communication',

        // Technology feeds
        '/atom/technology/artificial-intelligence',
        '/atom/technology/business',
        '/atom/technology/laboratory-technology',
        '/atom/technology/synthetic-biology'
    ];

    const allUrls = new Set();

    for (const feedPath of rssFeeds) {
        try {
            const feedUrl = `${BASE_URL}${feedPath}`;
            await sleep(1000); // Small delay between feeds

            const feed = await fetchRSSFeed(feedUrl);

            if (feed && feed.items) {
                let extractedCount = 0;
                feed.items.forEach(item => {
                    if (item.link) {
                        const url = cleanUrl(item.link);
                        if (isValidUrl(url) && url.includes('the-scientist.com')) {
                            // Accept article URLs - check for article-like paths
                            const path = new URL(url).pathname;
                            // Match: /article-name-12345 OR /category/article-name OR /article-name
                            // Exclude: /atom, /rss, /category (list pages), /page/, /tag/
                            if (!path.includes('/atom') &&
                                !path.includes('/rss') &&
                                !path.includes('/page/') &&
                                !path.includes('/tag/') &&
                                path.length > 1 && // Not just "/"
                                !path.match(/^\/category\/?$/) && // Not category list page
                                !path.match(/^\/type-group\/?$/)) { // Not type-group list page
                                allUrls.add(url);
                                extractedCount++;
                            }
                        }
                    }
                });
                console.log(`[${SOURCE_NAME}] RSS ${feedPath}: Found ${feed?.items?.length || 0} items, extracted ${extractedCount} URLs`);
            } else {
                console.log(`[${SOURCE_NAME}] RSS ${feedPath}: Found 0 items`);
            }
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error fetching RSS feed ${feedPath}:`, error.message);
            continue;
        }
    }

    return Array.from(allUrls);
}

/**
 * Discover URLs from sitemap (if available)
 */
async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Checking for sitemap...`);

    const sitemapUrls = [
        `${BASE_URL}/sitemap.xml`,
        `${BASE_URL}/sitemap_index.xml`,
        `${BASE_URL}/sitemaps/sitemap.xml`
    ];

    const allUrls = new Set();

    for (const sitemapUrl of sitemapUrls) {
        try {
            const response = await axios.get(sitemapUrl, {
                headers: getRealisticHeaders(BASE_URL),
                timeout: 15000,
                validateStatus: (status) => status < 500
            });

            if (response.status === 200 && response.data.includes('urlset')) {
                const $ = cheerio.load(response.data);

                // Extract URLs from sitemap
                $('url > loc').each((i, el) => {
                    const url = $(el).text().trim();
                    if (url && url.includes('the-scientist.com')) {
                        try {
                            const urlObj = new URL(url);
                            const path = urlObj.pathname;

                            // Only article URLs
                            if (/\/[^\/]+-\d+$/.test(path)) {
                                allUrls.add(cleanUrl(url));
                            }
                        } catch (e) {
                            // Skip invalid URLs
                        }
                    }
                });

                if (allUrls.size > 0) {
                    console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs from sitemap: ${sitemapUrl}`);
                    break; // Found working sitemap
                }
            }
        } catch (error) {
            continue; // Try next sitemap URL
        }
    }

    return Array.from(allUrls);
}

/**
 * Discover URLs from year-based archive pages
 */
async function discoverUrlsFromArchives() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from year-based archives...`);

    // Try all years from 2000 to current year (no restrictions)
    const currentYear = new Date().getFullYear();
    const years = [];
    for (let year = 2000; year <= currentYear; year++) {
        years.push(year);
    }
    const allUrls = new Set();

    for (const year of years) {
        try {
            // Try different archive URL patterns
            const archivePatterns = [
                `${BASE_URL}/archive/${year}`,
                `${BASE_URL}/${year}`,
                `${BASE_URL}/archive?year=${year}`,
                `${BASE_URL}/articles/${year}`
            ];

            for (const archiveUrl of archivePatterns) {
                try {
                    await sleep(2000);

                    const response = await axios.get(archiveUrl, {
                        headers: getRealisticHeaders(BASE_URL),
                        timeout: 15000,
                        validateStatus: (status) => status < 500
                    });

                    if (response.status === 404) continue;

                    const $ = cheerio.load(response.data);
                    let foundUrls = 0;

                    // Extract URLs from JSON-LD
                    const jsonLdScripts = $('script[type="application/ld+json"]');
                    for (let i = 0; i < jsonLdScripts.length; i++) {
                        try {
                            const data = JSON.parse($(jsonLdScripts[i]).html());
                            if (data['@type'] === 'ItemList' && data.itemListElement) {
                                data.itemListElement.forEach(item => {
                                    if (item.url && item.url.includes('the-scientist.com')) {
                                        const path = new URL(item.url).pathname;
                                        if (/\/[^\/]+-\d+$/.test(path)) {
                                            allUrls.add(cleanUrl(item.url));
                                            foundUrls++;
                                        }
                                    }
                                });
                            }
                        } catch (e) {
                            continue;
                        }
                    }

                    // Extract from __NEXT_DATA__
                    const nextDataScript = $('#__NEXT_DATA__');
                    if (nextDataScript.length > 0) {
                        try {
                            const nextData = JSON.parse(nextDataScript.html());
                            if (nextData.props?.pageProps?.content?.latest?.content) {
                                nextData.props.pageProps.content.latest.content.forEach(item => {
                                    if (item.slug && item.id) {
                                        const url = `${BASE_URL}/${item.slug}-${item.id}`;
                                        allUrls.add(cleanUrl(url));
                                        foundUrls++;
                                    }
                                });
                            }
                        } catch (e) {
                            // Continue
                        }
                    }

                    // Extract from HTML links
                    $('a[href]').each((i, el) => {
                        let href = $(el).attr('href');
                        if (!href) return;

                        if (href.startsWith('/')) {
                            href = `${BASE_URL}${href}`;
                        }

                        if (!href.includes('the-scientist.com')) return;

                        try {
                            const urlObj = new URL(href);
                            const path = urlObj.pathname;

                            if (path.includes('/page/') ||
                                path.includes('/category/') ||
                                path.includes('/type/') ||
                                path.includes('/series/') ||
                                path.includes('/magazine') ||
                                path.includes('/webinar') ||
                                path.includes('/multimedia') ||
                                path === '/') {
                                return;
                            }

                            if (/\/[^\/]+-\d+$/.test(path)) {
                                const clean = cleanUrl(href.split('?')[0].split('#')[0]);
                                if (isValidUrl(clean)) {
                                    allUrls.add(clean);
                                    foundUrls++;
                                }
                            }
                        } catch (e) {
                            return;
                        }
                    });

                    if (foundUrls > 0) {
                        console.log(`[${SOURCE_NAME}] Found ${foundUrls} URLs from ${archiveUrl}`);
                        break; // Found working pattern, move to next year
                    }
                } catch (error) {
                    continue; // Try next pattern
                }
            }

            // Try different archive pagination patterns
            const archivePaginationPatterns = [
                { base: `${BASE_URL}/archive/${year}`, pageFormat: '/page/{page}' },
                { base: `${BASE_URL}/${year}`, pageFormat: '/page/{page}' },
                { base: `${BASE_URL}/articles/${year}`, pageFormat: '/page/{page}' },
                { base: `${BASE_URL}/archive`, pageFormat: `?year=${year}&page={page}` }
            ];

            for (const pattern of archivePaginationPatterns) {
                let foundAny = false;
                for (let page = 1; page <= 50; page++) {
                    try {
                        const pageUrl = pattern.pageFormat.includes('?')
                            ? `${pattern.base}${pattern.pageFormat.replace('{page}', page)}`
                            : `${pattern.base}${pattern.pageFormat.replace('{page}', page)}`;

                        await sleep(2000);

                        const response = await axios.get(pageUrl, {
                            headers: getRealisticHeaders(BASE_URL),
                            timeout: 15000,
                            validateStatus: (status) => status < 500
                        });

                        if (response.status === 404) {
                            if (page === 1) break; // Pattern doesn't exist
                            break; // No more pages
                        }

                        const $ = cheerio.load(response.data);
                        let foundUrls = 0;

                        // Extract from JSON-LD
                        const jsonLdScripts = $('script[type="application/ld+json"]');
                        for (let i = 0; i < jsonLdScripts.length; i++) {
                            try {
                                const data = JSON.parse($(jsonLdScripts[i]).html());
                                if (data['@type'] === 'ItemList' && data.itemListElement) {
                                    data.itemListElement.forEach(item => {
                                        if (item.url && item.url.includes('the-scientist.com')) {
                                            const path = new URL(item.url).pathname;
                                            if (/\/[^\/]+-\d+$/.test(path)) {
                                                allUrls.add(cleanUrl(item.url));
                                                foundUrls++;
                                            }
                                        }
                                    });
                                }
                            } catch (e) {
                                continue;
                            }
                        }

                        // Extract from HTML links
                        $('a[href]').each((i, el) => {
                            let href = $(el).attr('href');
                            if (!href) return;

                            if (href.startsWith('/')) {
                                href = `${BASE_URL}${href}`;
                            }

                            if (!href.includes('the-scientist.com')) return;

                            try {
                                const urlObj = new URL(href);
                                const path = urlObj.pathname;

                                if (/\/[^\/]+-\d+$/.test(path) && !path.includes('/page/')) {
                                    const clean = cleanUrl(href.split('?')[0].split('#')[0]);
                                    if (isValidUrl(clean)) {
                                        allUrls.add(clean);
                                        foundUrls++;
                                    }
                                }
                            } catch (e) {
                                return;
                            }
                        });

                        if (foundUrls > 0) {
                            foundAny = true;
                        } else if (foundUrls === 0 && page > 1) {
                            break; // No more articles on this page
                        }
                    } catch (error) {
                        if (page === 1) break; // Pattern doesn't work
                        break;
                    }
                }
                if (foundAny) break; // Found working pattern, move to next year
            }

            console.log(`[${SOURCE_NAME}] Completed ${year}, total URLs: ${allUrls.size}`);
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error processing year ${year}:`, error.message);
            continue;
        }
    }

    return Array.from(allUrls);
}

/**
 * Discover URLs from a single category using Puppeteer with infinite scroll
 */
async function discoverUrlsFromCategoryWithPuppeteer(category) {
    let page = null;
    try {
        const browser = await getBrowser();
        page = await browser.newPage();

        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

        const url = `${BASE_URL}${category}`;
        console.log(`[${SOURCE_NAME}] Loading ${category} with Puppeteer...`);

        // Set extra headers before navigation
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        });

        // Navigate with multiple fallback strategies
        try {
            await page.goto(url, {
                waitUntil: 'domcontentloaded',
                timeout: 90000 // 90 seconds for Cloudflare challenges
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
                await page.waitForTimeout(15000); // Wait 15 seconds for Cloudflare
            }
        } catch (e) {
            // Ignore check errors
        }

        await page.waitForTimeout(5000); // Wait for initial content

        const allUrls = new Set();
        let previousUrlCount = 0;
        let noNewUrlsCount = 0;
        const maxScrollAttempts = 200; // Maximum scroll attempts
        const scrollDelay = 2000; // 2 seconds between scrolls

        for (let scrollAttempt = 0; scrollAttempt < maxScrollAttempts; scrollAttempt++) {
            // Extract URLs from current page state
            const currentUrls = await page.evaluate(() => {
                const urls = new Set();

                // Extract from __NEXT_DATA__
                const nextDataScript = document.getElementById('__NEXT_DATA__');
                if (nextDataScript) {
                    try {
                        const nextData = JSON.parse(nextDataScript.textContent);
                        if (nextData.props?.pageProps?.content?.latest?.content) {
                            nextData.props.pageProps.content.latest.content.forEach(item => {
                                if (item.slug && item.id) {
                                    urls.add(`/${item.slug}-${item.id}`);
                                }
                            });
                        }
                        if (nextData.props?.pageProps?.content?.featured?.content) {
                            nextData.props.pageProps.content.featured.content.forEach(item => {
                                if (item.slug && item.id) {
                                    urls.add(`/${item.slug}-${item.id}`);
                                }
                            });
                        }
                    } catch (e) {
                        // Continue
                    }
                }

                // Extract from article links
                document.querySelectorAll('a[href]').forEach(link => {
                    const href = link.getAttribute('href');
                    if (href && /\/[^\/]+-\d+$/.test(href)) {
                        urls.add(href);
                    }
                });

                return Array.from(urls);
            });

            // Add to set
            currentUrls.forEach(url => {
                const fullUrl = url.startsWith('http') ? url : `${BASE_URL}${url}`;
                allUrls.add(cleanUrl(fullUrl));
            });

            const currentUrlCount = allUrls.size;

            // Check if we got new URLs
            if (currentUrlCount === previousUrlCount) {
                noNewUrlsCount++;
                if (noNewUrlsCount >= 3) {
                    // No new URLs for 3 scrolls, likely done
                    console.log(`[${SOURCE_NAME}] No new URLs after ${scrollAttempt + 1} scrolls. Stopping.`);
                    break;
                }
            } else {
                noNewUrlsCount = 0;
                if (scrollAttempt % 10 === 0) {
                    console.log(`[${SOURCE_NAME}] ${category}: Scrolled ${scrollAttempt + 1} times, found ${currentUrlCount} URLs...`);
                }
            }

            previousUrlCount = currentUrlCount;

            // Scroll down
            await page.evaluate(() => {
                window.scrollBy(0, window.innerHeight);
            });

            await page.waitForTimeout(scrollDelay);
        }

        await page.close();
        return Array.from(allUrls);

    } catch (error) {
        if (page) {
            try {
                await page.close();
            } catch (e) {
                // Ignore
            }
        }
        throw error;
    }
}

/**
 * Discover URLs from category pages with Puppeteer infinite scroll
 */
async function discoverUrlsFromCategoriesWithPagination() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from category pages with Puppeteer (infinite scroll)...`);

    // All categories except education/career
    const categories = [
        // News and general
        '/type-group/news',

        // Science categories (all available)
        '/category/science/biochemistry',
        '/category/science/cancer',
        '/category/science/cell-and-molecular-biology',
        '/category/science/developmental-biology',
        '/category/science/evolutionary-biology',
        '/category/science/genetics',
        '/category/science/genome-editing',
        '/category/science/immunology',
        '/category/science/microbiology',
        '/category/science/neuroscience',
        '/category/science/omics',
        '/category/science/physiology',

        // Health & Medicine (all available)
        '/category/health/cell-and-gene-therapy',
        '/category/health/diagnostics',
        '/category/health/drug-discovery-and-development',
        '/category/health/public-health',

        // Society (excluding careers/education)
        '/category/society/community',
        '/category/society/research-ethics',
        '/category/society/science-communication',

        // Technology (all available)
        '/category/technology/artificial-intelligence',
        '/category/technology/business',
        '/category/technology/laboratory-technology',
        '/category/technology/synthetic-biology'
    ];

    const allUrls = new Set();

    for (let i = 0; i < categories.length; i++) {
        const category = categories[i];
        try {
            console.log(`[${SOURCE_NAME}] Processing category ${i + 1}/${categories.length}: ${category}`);
            const categoryUrls = await discoverUrlsFromCategoryWithPuppeteer(category);

            categoryUrls.forEach(url => allUrls.add(url));

            console.log(`[${SOURCE_NAME}] ✅ Completed ${category}: Found ${categoryUrls.length} URLs (${allUrls.size} total so far)`);

            // Delay between categories
            if (i < categories.length - 1) {
                await sleep(5000); // 5 second delay between categories
            }
        } catch (error) {
            // Continue to next category even if one fails
            console.warn(`[${SOURCE_NAME}] ⚠️  Error processing category ${category}:`, error.message);
            console.log(`[${SOURCE_NAME}] Continuing to next category...`);
            continue;
        }
    }

    // Close browser when done
    if (browserInstance) {
        try {
            await browserInstance.close();
            browserInstance = null;
        } catch (e) {
            // Ignore
        }
    }

    console.log(`[${SOURCE_NAME}] 🎉 Finished discovering URLs from all ${categories.length} categories!`);

    return Array.from(allUrls);
}

/**
 * Quick date check from URL metadata (if available)
 * This is a faster approach - we'll do full date filtering during scraping
 */
async function quickDateCheck(urls, startYear = 2020, endYear = 2025) {
    // For now, return all URLs - we'll filter by date during actual scraping
    // This avoids double-scraping and is more efficient
    console.log(`[${SOURCE_NAME}] Will filter by date (${startYear}-${endYear}) during scraping to avoid double requests`);
    return urls;
}

/**
 * Historical scraping: Discover and scrape ALL articles (no year restrictions)
 */
export async function scrapeHistorical(options = {}) {
    const {
        maxArticles = null,
        testMode = false
    } = options;

    console.log(`\n=== ${SOURCE_NAME} HISTORICAL SCRAPING STARTING ===`);
    console.log(`Target: ALL articles (no year restrictions)`);
    if (testMode) {
        console.log(`⚠️  TEST MODE: Only scraping first 10 articles`);
    }
    if (maxArticles) {
        console.log(`⚠️  LIMIT: Maximum ${maxArticles} articles`);
    }
    console.log('');

    // Create rate limiter
    const { limiter } = await createRateLimiter(BASE_URL, {
        delayBetweenRequests: 8000, // 8 seconds
        delayJitter: 2000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 60000
    });

    // Discover URLs from multiple sources
    console.log(`[${SOURCE_NAME}] Step 1: Discovering article URLs from multiple sources...`);

    // 1. RSS/Atom feeds (FASTEST - gets recent articles with full metadata)
    console.log(`[${SOURCE_NAME}] 1a. Checking RSS/Atom feeds (FASTEST SOURCE)...`);
    const rssUrls = await discoverUrlsFromRSSFeeds();
    console.log(`[${SOURCE_NAME}] Found ${rssUrls.length} URLs from RSS feeds`);

    // 2. Category pages with Puppeteer infinite scroll (COMPREHENSIVE - gets ALL articles)
    console.log(`[${SOURCE_NAME}] 1b. Checking category pages (with Puppeteer infinite scroll) - COMPREHENSIVE SOURCE...`);
    const categoryUrls = await discoverUrlsFromCategoriesWithPagination();
    console.log(`[${SOURCE_NAME}] Found ${categoryUrls.length} URLs from categories`);

    // 3. Homepage (quick check for latest articles)
    console.log(`[${SOURCE_NAME}] 1c. Checking homepage...`);
    const homepageUrls = await discoverUrlsFromHomepage();
    console.log(`[${SOURCE_NAME}] Found ${homepageUrls.length} URLs from homepage`);

    // 4. Sitemap (if available)
    console.log(`[${SOURCE_NAME}] 1d. Checking sitemap...`);
    const sitemapUrls = await discoverUrlsFromSitemap();
    console.log(`[${SOURCE_NAME}] Found ${sitemapUrls.length} URLs from sitemap`);

    // 5. Year-based archives (SKIP - not finding URLs, too slow)
    const archiveUrls = []; // Skip archive discovery for now

    // Combine and deduplicate (prioritize RSS and category URLs)
    let allUrls = Array.from(new Set([...rssUrls, ...categoryUrls, ...homepageUrls, ...sitemapUrls, ...archiveUrls]));

    if (allUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found. Cannot proceed with historical scraping.`);
        return { total: 0, saved: 0, failed: 0 };
    }

    console.log(`[${SOURCE_NAME}] Found ${allUrls.length} article URLs`);
    console.log(`[${SOURCE_NAME}] No year restrictions - will scrape all articles`);

    // Apply limits
    let urlsToProcess = allUrls;
    if (testMode) {
        urlsToProcess = allUrls.slice(0, 10);
        console.log(`[${SOURCE_NAME}] TEST MODE: Processing only first 10 URLs`);
    } else if (maxArticles) {
        urlsToProcess = allUrls.slice(0, maxArticles);
        console.log(`[${SOURCE_NAME}] LIMIT: Processing first ${maxArticles} URLs`);
    }

    // Check existing articles
    console.log(`[${SOURCE_NAME}] Step 3: Checking for existing articles...`);
    const csvFilePath = getCSVFilePath(SOURCE_NAME);
    const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
    const existingLinks = await readExistingLinksCombined(csvFilePath);
    const newUrls = urlsToProcess.filter(url => !existingLinks.has(url));

    console.log(`[${SOURCE_NAME}] ${newUrls.length} new articles (${urlsToProcess.length - newUrls.length} already exist)`);

    if (newUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] All articles already scraped.`);
        return { total: allUrls.length, saved: 0, failed: 0, skipped: urlsToProcess.length };
    }

    // Scrape articles
    console.log(`[${SOURCE_NAME}] Step 4: Scraping ${newUrls.length} articles...`);
    console.log(`[${SOURCE_NAME}] Rate: ~8 seconds per article`);
    console.log(`[${SOURCE_NAME}] Estimated time: ${Math.ceil(newUrls.length * 8 / 60)} minutes\n`);

    const articles = [];
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < newUrls.length; i++) {
        const url = newUrls[i];
        let retries = 0;
        const maxRetries = 2;
        let article = null;

        while (retries <= maxRetries) {
            try {
                await limiter.wait();

                article = await scrapeArticle(url);

                // Validate article data
                if (!article || !article.link) {
                    throw new Error('Invalid article data: missing link');
                }

                if (!article.extract || article.extract.length < 50) {
                    throw new Error('Invalid article data: insufficient content');
                }

                // No date filtering - include all articles
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
                    const baseDelay = getDelayForError(errorType, 8000);
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

/**
 * Weekly RSS update - fast and efficient
 */
export async function updateFromRSS() {
    console.log(`\n=== ${SOURCE_NAME} RSS UPDATE STARTING ===`);

    try {
        // Use the main RSS feed for latest articles
        const rssUrl = `${BASE_URL}/atom/latest`;
        const { collectAndScrapeRSS } = await import('../utils/simpleRSSCollector.js');

        const sourceConfig = {
            sourceName: SOURCE_NAME,
            rssUrl: rssUrl,
            maxConcurrent: 2,
            delayBetweenScrapes: 2000
        };

        const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);

        console.log(`\n=== ${SOURCE_NAME} RSS UPDATE COMPLETE ===`);
        console.log(`Total articles in feed: ${results.total || 0}`);
        console.log(`New articles: ${results.new || 0}`);
        console.log(`Articles saved: ${results.saved || 0}`);
        console.log(`Failed: ${results.failed || 0}`);
        console.log(`Skipped: ${results.skipped || 0}`);

        return {
            saved: results.saved || 0,
            failed: results.failed || 0,
            total: results.total || 0
        };
    } catch (error) {
        console.error(`[${SOURCE_NAME}] RSS update error: ${error.message}`);
        // Fallback to homepage update if RSS fails
        console.log(`[${SOURCE_NAME}] Falling back to homepage update...`);
        return await updateFromHomepage();
    }
}

/**
 * Main run function
 */
export async function run(options = {}) {
    console.log(`\n=== ${SOURCE_NAME} SCRAPER ===`);
    console.log(`Source: ${BASE_URL}\n`);

    if (options.historical) {
        return await scrapeHistorical({
            testMode: options.testMode || false,
            maxArticles: options.maxArticles || null
        });
    } else {
        // Default: RSS feed update (fast and efficient)
        return await updateFromRSS();
    }
}

