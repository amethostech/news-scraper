import axios from 'axios';
import * as cheerio from 'cheerio';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from '../utils/sitemap.js';
import { fetchRSSFeed } from '../utils/rssParser.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'PRNewswire';
const BASE_URL = 'https://www.prnewswire.com';

/**
 * Scrape article details from PR Newswire Health section with error handling
 */
export async function scrapeArticleDetails(url) {
    if (!url || typeof url !== 'string' || !url.includes('prnewswire.com')) {
        throw new Error('Invalid URL');
    }
    
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
        
        const html = response.data;
        
        if (html.includes('Access Denied') || html.includes('403') || html.includes('blocked')) {
            throw new Error('Blocked (content check)');
        }
        
        if (html.length < 100) {
            throw new Error('Invalid or empty HTML content');
        }
        
        let $;
        try {
            $ = cheerio.load(html);
        } catch (error) {
            throw new Error(`Failed to parse HTML: ${error.message}`);
        }
        
        // Extract title
        let title = '';
        const titleSelectors = [
            'h1.release-headline',
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
        
        // Extract author (PR Newswire typically has company/source instead of author)
        let author = '';
        const authorSelectors = [
            '.release-company',
            '.company-name',
            '.source',
            'meta[name="author"]',
            'meta[property="article:author"]',
            '.byline'
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
            '.release-date',
            '.published-date',
            '.article-date',
            '[itemprop="datePublished"]',
            'meta[property="article:published_time"]',
            'meta[name="date"]',
            '.date-published'
        ];
        
        for (const selector of dateSelectors) {
            if (selector.startsWith('meta')) {
                const metaContent = $(selector).attr('content');
                if (metaContent) {
                    const tempDate = new Date(metaContent);

                    if (!isNaN(tempDate.getTime())) {

                        articleDate = tempDate.toISOString().split(\'T\')[0]; // YYYY-MM-DD

                        break;

                    }
                }
            } else if (selector.includes('[datetime]')) {
                const datetime = $(selector).first().attr('datetime');
                if (datetime) {
                    const tempDate = new Date(datetime);

                    if (!isNaN(tempDate.getTime())) {

                        articleDate = tempDate.toISOString().split(\'T\')[0]; // YYYY-MM-DD

                        break;

                    }
                }
            } else {
                const dateText = $(selector).first().text().trim();
                if (dateText) {
                    const tempDate = new Date(dateText);

                    if (!isNaN(tempDate.getTime())) {

                        articleDate = tempDate.toISOString().split(\'T\')[0]; // YYYY-MM-DD

                        break;

                    }
                }
            }
        }
        
        // Extract body text
        let bodyText = '';
        const bodySelectors = [
            '.release-body',
            '.article-body',
            '.article-content',
            '[itemprop="articleBody"]',
            '.content-body',
            'article .body',
            '.story-body',
            'main article',
            '#article-content',
            '.post-content',
            '.article-text'
        ];
        
        for (const selector of bodySelectors) {
            const content = $(selector);
            if (content.length > 0) {
                content.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share').remove();
                
                bodyText = content.text()
                    .replace(/\s+/g, ' ')
                    .replace(/\n+/g, '\n')
                    .trim();
                
                if (bodyText.length > 100) { 
                    break;
                }
            }
        }
        
        // Fallback: get all paragraph text
        if (!bodyText || bodyText.length < 100) {
            const paragraphs = $('article p, .article p, main p, .release-body p')
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
            author: author || 'PR Newswire',
            date: articleDate || null,
            extract: bodyText.trim(),
            link: url,
        };
    } catch (error) {
        console.error(`Error scraping article ${url}:`, error.message);
        throw error;
    }
}

/**
 * Discover article URLs from sitemap
 */
async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from sitemap...`);
    
    const sitemapUrls = [
        `${BASE_URL}/sitemap.xml`,
        `${BASE_URL}/sitemap_index.xml`
    ];
    
    const allUrls = [];
    
    for (const sitemapUrl of sitemapUrls) {
        try {
            // Try to fetch sitemap index first
            const sitemaps = await fetchSitemapIndex(sitemapUrl);
            
            if (sitemaps.length > 0) {
                // It's a sitemap index - fetch each sitemap
                console.log(`[${SOURCE_NAME}] Found ${sitemaps.length} sitemaps in index`);
                
                for (const subSitemapUrl of sitemaps) {
                    try {
                        await sleep(1000);
                        const sitemapData = await fetchArticleLinksFromSitemap(subSitemapUrl);
                        
                        sitemapData.forEach(item => {
                            const url = typeof item === 'string' ? item : item.url;
                            if (url && url.includes('prnewswire.com')) {
                                // Filter for relevant categories: health, biotech, pharma, medical
                                if (url.match(/\/news-releases\/(health|biotech|pharma|medical|biotechnology|pharmaceutical)/i)) {
                                    allUrls.push(cleanUrl(url));
                                }
                            }
                        });
                    } catch (error) {
                        console.warn(`[${SOURCE_NAME}] Error fetching sitemap ${subSitemapUrl}:`, error.message);
                        continue;
                    }
                }
                
                if (allUrls.length > 0) break; // Found working sitemap
            } else {
                // Direct sitemap with URLs
                const sitemapData = await fetchArticleLinksFromSitemap(sitemapUrl);
                sitemapData.forEach(item => {
                    const url = typeof item === 'string' ? item : item.url;
                    if (url && url.includes('prnewswire.com')) {
                        if (url.match(/\/news-releases\/(health|biotech|pharma|medical|biotechnology|pharmaceutical)/i)) {
                            allUrls.push(cleanUrl(url));
                        }
                    }
                });
                
                if (allUrls.length > 0) break; // Found working sitemap
            }
        } catch (error) {
            continue; // Try next sitemap URL
        }
    }
    
    console.log(`[${SOURCE_NAME}] Found ${allUrls.length} article URLs from sitemap`);
    return allUrls;
}

/**
 * Discover article URLs from expanded RSS feeds (multiple categories)
 */
async function discoverUrlsFromRSS() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from RSS feeds (expanded categories)...`);
    
    // Expanded RSS feeds beyond just health
    const rssUrls = [
        // Health category
        'https://www.prnewswire.com/rss/health-latest-news/',
        'https://www.prnewswire.com/rss/health-news/',
        'https://www.prnewswire.com/rss/health/',
        // Biotech category
        'https://www.prnewswire.com/rss/biotechnology-latest-news/',
        'https://www.prnewswire.com/rss/biotechnology-news/',
        // Pharma category
        'https://www.prnewswire.com/rss/pharmaceutical-latest-news/',
        'https://www.prnewswire.com/rss/pharmaceutical-news/',
        // Medical devices
        'https://www.prnewswire.com/rss/medical-devices-latest-news/',
        'https://www.prnewswire.com/rss/medical-devices-news/'
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
                    if (item && item.link && typeof item.link === 'string' && item.link.includes('prnewswire.com')) {
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
            }
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error fetching RSS feed ${rssUrl}:`, error.message);
            // Continue to next feed
            continue;
        }
    }
    
    return Array.from(new Set(allUrls));
}

/**
 * Discover article URLs from category/archive pages with error handling
 */
async function discoverUrlsFromCategories() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from category/archive pages...`);
    
    const allUrls = new Set();
    let errorCount = 0;
    const maxErrors = 10;
    
    // PR Newswire category pages
    const categories = [
        '/news-releases/health-latest-news/',
        '/news-releases/biotechnology-latest-news/',
        '/news-releases/pharmaceutical-latest-news/',
        '/news-releases/medical-devices-latest-news/',
        '/news-releases/health/',
        '/news-releases/biotechnology/',
        '/news-releases/pharmaceutical/'
    ];
    
    for (let catIdx = 0; catIdx < categories.length; catIdx++) {
        if (errorCount >= maxErrors) {
            console.warn(`[${SOURCE_NAME}] Too many errors (${errorCount}), stopping category discovery`);
            break;
        }
        
        const category = categories[catIdx];
        try {
            const categoryUrl = `${BASE_URL}${category}`;
            console.log(`[${SOURCE_NAME}] Checking category ${catIdx + 1}/${categories.length}: ${categoryUrl}`);
            
            await sleep(2000);
            const response = await axios.get(categoryUrl, {
                headers: getRealisticHeaders(BASE_URL),
                timeout: 15000,
                validateStatus: (status) => status < 500,
                maxRedirects: 5
            });
            
            if (response.status === 404) {
                continue;
            }
            
            if (!response.data || typeof response.data !== 'string') {
                errorCount++;
                continue;
            }
            
            const $ = cheerio.load(response.data);
            let foundUrls = 0;
            
            // Extract article links
            $('a[href*="/news-releases/"]').each((i, el) => {
                try {
                    let href = $(el).attr('href');
                    if (!href || typeof href !== 'string') return;
                    
                    if (href.startsWith('/')) {
                        href = `${BASE_URL}${href}`;
                    }
                    
                    if (href.includes('prnewswire.com') && 
                        href.includes('/news-releases/') &&
                        !href.includes('/list/') &&
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
            
            // Try pagination
            for (let pageNum = 2; pageNum <= 100; pageNum++) {
                if (errorCount >= maxErrors) break;
                
                try {
                    const pageUrl = `${categoryUrl}?page=${pageNum}`;
                    await sleep(1000);
                    
                    const pageResponse = await axios.get(pageUrl, {
                        headers: getRealisticHeaders(BASE_URL),
                        timeout: 15000,
                        validateStatus: (status) => status < 500,
                        maxRedirects: 5
                    });
                    
                    if (pageResponse.status === 404) break;
                    
                    if (!pageResponse.data || typeof pageResponse.data !== 'string') {
                        break;
                    }
                    
                    const $page = cheerio.load(pageResponse.data);
                    let foundUrlsPage = 0;
                    
                    $page('a[href*="/news-releases/"]').each((i, el) => {
                        try {
                            let href = $page(el).attr('href');
                            if (!href || typeof href !== 'string') return;
                            
                            if (href.startsWith('/')) {
                                href = `${BASE_URL}${href}`;
                            }
                            
                            if (href.includes('prnewswire.com') && 
                                href.includes('/news-releases/') &&
                                !href.includes('/list/')) {
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
                    
                    if (foundUrlsPage === 0) break; // No more articles
                    
                } catch (error) {
                    errorCount++;
                    break; // No more pages or error
                }
            }
            
            console.log(`[${SOURCE_NAME}] Found ${allUrls.size} URLs from ${category}`);
        } catch (error) {
            errorCount++;
            console.warn(`[${SOURCE_NAME}] Error checking category ${category}:`, error.message);
            continue;
        }
    }
    
    console.log(`[${SOURCE_NAME}] Found ${allUrls.size} total URLs from category/archive pages`);
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
    
    // Combine and deduplicate
    const allUrls = Array.from(new Set([...sitemapUrls, ...rssUrls, ...categoryUrls]));

    if (allUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found from any discovery method. Cannot proceed.`);
        return { total: 0, saved: 0, failed: 0 };
    }
    
    console.log(`[${SOURCE_NAME}] Discovery summary: Sitemap=${sitemapUrls.length}, RSS=${rssUrls.length}, Categories=${categoryUrls.length}, Total=${allUrls.length}`);

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
                article.author = article.author || 'PR Newswire';
                
                articles.push(article);
                break; // Success
                
            } catch (error) {
                retries++;
                if (retries > maxRetries) {
                    failed++;
                    const errorMsg = error.message || 'Unknown error';
                    console.error(`[${i + 1}/${newUrls.length}] ✗ Failed after ${maxRetries + 1} attempts: ${url.substring(0, 60)}... - ${errorMsg}`);
                    break;
                } else {
                    // Retry with exponential backoff
                    const delay = 3000 * Math.pow(2, retries - 1);
                    console.log(`[${i + 1}/${newUrls.length}] Retry ${retries}/${maxRetries} for ${url.substring(0, 60)}... (waiting ${delay}ms)`);
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
        // Default: RSS update (expanded categories)
        console.log('\n=== PR NEWSWIRE SCRAPER STARTING ===');
        
        const sourceConfig = {
            sourceName: SOURCE_NAME,
            rssUrls: [
                'https://www.prnewswire.com/rss/health-latest-news/',
                'https://www.prnewswire.com/rss/health-news/',
                'https://www.prnewswire.com/rss/health/',
                'https://www.prnewswire.com/rss/biotechnology-latest-news/',
                'https://www.prnewswire.com/rss/pharmaceutical-latest-news/',
                'https://www.prnewswire.com/rss/medical-devices-latest-news/'
            ],
            maxConcurrent: 2,
            delayBetweenScrapes: 5000 // Increased delay for Cloudflare
        };

        try {
            const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);
            
            console.log(`\n=== PR NEWSWIRE SCRAPER COMPLETE ===`);
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
            console.error('Error in PR Newswire scraper:', error);
            return { saved: 0, failed: 0, total: 0 };
        }
    }
}

