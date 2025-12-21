/**
 * The Pharma Letter Scraper
 * 
 * Source: https://www.thepharmaletter.com/
 * 
 * Implementation:
 * - Historical scraping: RSS feeds, sitemaps
 * - Weekly updates: RSS feed + full content scraping
 */

import axios from 'axios';
import * as cheerio from 'cheerio';
import { fetchRSSFeed } from '../utils/rssParser.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from '../utils/sitemap.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl } from '../utils/linkValidator.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';
import { scrapeArticleUrlsWithPuppeteer } from '../utils/puppeteerHelper.js';

const SOURCE_NAME = 'Pharmaletter';
const BASE_URL = 'https://www.thepharmaletter.com';

/**
 * Discover article URLs from sitemap
 */
export async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from sitemap...`);

    try {
        const sitemapUrls = [
            `${BASE_URL}/sitemap.xml`,
            `${BASE_URL}/sitemap_index.xml`
        ];

        let sitemapIndex = null;
        for (const url of sitemapUrls) {
            try {
                const response = await axios.get(url, {
                    headers: getRealisticHeaders(),
                    timeout: 10000
                });
                if (response.data.includes('sitemapindex') || response.data.includes('urlset')) {
                    sitemapIndex = url;
                    break;
                }
            } catch (e) {
                continue;
            }
        }

        if (!sitemapIndex) {
            console.log(`[${SOURCE_NAME}] No sitemap found`);
            return [];
        }

        console.log(`[${SOURCE_NAME}] Found sitemap: ${sitemapIndex}`);

        const sitemaps = await fetchSitemapIndex(sitemapIndex);

        if (sitemaps.length === 0) {
            const articles = await fetchArticleLinksFromSitemap(sitemapIndex);
            return articles
                .filter(a => a.url && a.url.includes('thepharmaletter.com'))
                .map(a => a.url);
        }

        const allArticles = [];
        console.log(`[${SOURCE_NAME}] Processing ${sitemaps.length} sitemaps...`);

        for (let i = 0; i < sitemaps.length; i++) {
            const sitemapUrl = sitemaps[i];
            try {
                const articles = await fetchArticleLinksFromSitemap(sitemapUrl);

                const filtered = articles.filter(a => {
                    if (!a.url || !a.url.includes('thepharmaletter.com')) {
                        return false;
                    }
                    // Filter out non-article pages
                    if (a.url.includes('/news/') || a.url.includes('/article/') || a.url.match(/\/\d{4}\/\d{2}\//)) {
                        return true;
                    }
                    return false;
                });

                allArticles.push(...filtered.map(a => a.url));

                if ((i + 1) % 10 === 0) {
                    console.log(`[${SOURCE_NAME}] Processed ${i + 1}/${sitemaps.length} sitemaps, found ${allArticles.length} articles...`);
                }

                await sleep(1000);
            } catch (error) {
                console.warn(`[${SOURCE_NAME}] Error processing sitemap ${sitemapUrl}: ${error.message}`);
                continue;
            }
        }

        console.log(`[${SOURCE_NAME}] Found ${allArticles.length} article URLs from sitemaps`);
        return allArticles;

    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error discovering URLs from sitemap:`, error.message);
        return [];
    }
}

/**
 * Discover article URLs from RSS feed
 */
export async function discoverUrlsFromRSS() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from RSS feeds...`);

    const rssUrls = [
        `${BASE_URL}/rss`,
        `${BASE_URL}/feed`,
        `${BASE_URL}/rss.xml`
    ];

    const allUrls = new Set();

    for (const rssUrl of rssUrls) {
        try {
            await sleep(1000);
            const metadata = await fetchRSSFeed(rssUrl);

            metadata.forEach(item => {
                if (item.link && item.link.includes('thepharmaletter.com')) {
                    allUrls.add(cleanUrl(item.link));
                }
            });

            if (metadata.length > 0) {
                console.log(`[${SOURCE_NAME}] Found ${metadata.length} articles in RSS: ${rssUrl}`);
                break;
            }
        } catch (error) {
            continue;
        }
    }

    return Array.from(allUrls);
}

/**
 * Discover article URLs by scraping listing pages
 */
export async function discoverUrlsFromPages() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from article listing pages...`);

    const allUrls = new Set();

    // Try different listing page patterns
    const listingPages = [
        `${BASE_URL}/news`,
        `${BASE_URL}/insights`,
        `${BASE_URL}/regulatory`,
        `${BASE_URL}/therapy-areas`,
        `${BASE_URL}/pharmaceutical-news`,
        `${BASE_URL}/biotechnology-news`,
        `${BASE_URL}/news/page/1`,
        `${BASE_URL}/news/page/2`,
        `${BASE_URL}/news/page/3`
    ];

    for (const pageUrl of listingPages) {
        try {
            await sleep(3000);
            const response = await axios.get(pageUrl, {
                headers: getRealisticHeaders(BASE_URL),
                timeout: 20000
            });

            const $ = cheerio.load(response.data);

            // Find article links - more comprehensive selectors
            $('a[href]').each((i, el) => {
                const href = $(el).attr('href');
                if (!href) return;

                let fullUrl = href;
                if (href.startsWith('/')) {
                    fullUrl = `${BASE_URL}${href}`;
                } else if (!href.startsWith('http')) {
                    fullUrl = `${BASE_URL}/${href}`;
                }

                // Match article URLs - more flexible patterns
                if (fullUrl.includes('thepharmaletter.com') &&
                    !fullUrl.includes('/category/') &&
                    !fullUrl.includes('/tag/') &&
                    !fullUrl.includes('/author/') &&
                    !fullUrl.includes('/page/') &&
                    !fullUrl.includes('/search') &&
                    !fullUrl.includes('/login') &&
                    !fullUrl.includes('/subscribe') &&
                    !fullUrl.endsWith('/') &&
                    (fullUrl.match(/\/\d{4}\/\d{2}\//) ||
                        fullUrl.includes('/news/') ||
                        fullUrl.includes('/article/') ||
                        fullUrl.includes('/insights/') ||
                        fullUrl.includes('/regulatory/') ||
                        fullUrl.includes('/therapy-areas/') ||
                        fullUrl.includes('/pharmaceutical-news/') ||
                        fullUrl.includes('/biotechnology-news/') ||
                        (fullUrl.split('/').length >= 4 && fullUrl.match(/\/[a-z-]+\/[a-z-]+\//)))) {
                    allUrls.add(cleanUrl(fullUrl));
                }
            });

            // Also check for article links in headings/titles
            $('h1 a, h2 a, h3 a, .title a, .headline a').each((i, el) => {
                const href = $(el).attr('href');
                if (href) {
                    let fullUrl = href;
                    if (href.startsWith('/')) {
                        fullUrl = `${BASE_URL}${href}`;
                    } else if (!href.startsWith('http')) {
                        fullUrl = `${BASE_URL}/${href}`;
                    }
                    if (fullUrl.includes('thepharmaletter.com') && !fullUrl.includes('/category/')) {
                        allUrls.add(cleanUrl(fullUrl));
                    }
                }
            });

            // Also check for article cards with more selectors
            $('.article, .post, .news-item, .story, article, [class*="article"], [class*="post"], [class*="news"]').each((i, el) => {
                const $el = $(el);
                const link = $el.find('a').first().attr('href');
                if (link) {
                    let fullUrl = link;
                    if (link.startsWith('/')) {
                        fullUrl = `${BASE_URL}${link}`;
                    } else if (!link.startsWith('http')) {
                        fullUrl = `${BASE_URL}/${link}`;
                    }

                    if (fullUrl.includes('thepharmaletter.com') &&
                        !fullUrl.includes('/category/') &&
                        !fullUrl.includes('/tag/') &&
                        !fullUrl.includes('/author/')) {
                        allUrls.add(cleanUrl(fullUrl));
                    }
                }
            });

            if (allUrls.size > 0) {
                console.log(`[${SOURCE_NAME}] Found ${allUrls.size} articles from ${pageUrl}`);
            }
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error scraping listing page ${pageUrl}: ${error.message}`);
            continue;
        }
    }

    return Array.from(allUrls);
}

/**
 * Discover article URLs using Puppeteer (for JavaScript-rendered content)
 */
export async function discoverUrlsWithPuppeteer() {
    console.log(`[${SOURCE_NAME}] Discovering URLs with Puppeteer...`);

    const allUrls = new Set();

    const listingPages = [
        BASE_URL,
        `${BASE_URL}/news`,
        `${BASE_URL}/category/news`
    ];

    const articleSelectors = [
        '.card__title a',
        'a.card__image-link',
        'a[href*="/biotechnology/"]',
        'a[href*="/pharmaceutical/"]'
    ];

    for (const pageUrl of listingPages) {
        try {
            const urls = await scrapeArticleUrlsWithPuppeteer(pageUrl, articleSelectors, {
                waitForSelector: '.card__title, .card__content',
                scrollToBottom: true,
                maxScrolls: 3
            });

            // Filter URLs
            urls.forEach(url => {
                if (url.includes('thepharmaletter.com') &&
                    !url.includes('/category/') &&
                    !url.includes('/tag/') &&
                    !url.includes('/author/') &&
                    !url.includes('/page/') &&
                    !url.endsWith('/')) {
                    allUrls.add(cleanUrl(url));
                }
            });

            console.log(`[${SOURCE_NAME}] Found ${urls.length} URLs from ${pageUrl} (${allUrls.size} total after filtering)`);

            // Don't overwhelm the site
            await sleep(3000);
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Puppeteer error on ${pageUrl}: ${error.message}`);
            continue;
        }
    }

    return Array.from(allUrls);
}

/**
 * Scrape article content from URL using Puppeteer (Pharmaletter uses JS rendering)
 */
export async function scrapeArticle(url) {
    try {
        await randomDelay(2000, 4000);

        console.log(`[${SOURCE_NAME}] Scraping with Puppeteer: ${url.substring(0, 60)}...`);

        // Import Puppeteer helper
        const { scrapeArticleContentWithPuppeteer } = await import('../utils/puppeteerHelper.js');

        // Use Puppeteer to get the rendered content
        const article = await scrapeArticleContentWithPuppeteer(url, [
            'div.row.row--boxed',
            '.article-content',
            'article',
            'main'
        ], {
            titleSelector: 'h1',
            dateSelector: '.stats__value.date, time, .date',
            authorSelector: '.author, .byline'
        });

        // Clean up content
        if (article.content) {
            article.content = article.content
                .replace(/Register for free.*$/gi, '')
                .replace(/\s{3,}/g, ' ')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
        }

        // Validate - be more lenient with content length
        if (!article.title || !article.content || article.content.length < 50) {
            throw new Error(`Insufficient content - Title: ${article.title ? 'Yes' : 'No'}, Content: ${article.content?.length || 0} chars`);
        }

        return {
            author: article.author || 'The Pharma Letter',
            date: article.date,
            extract: article.content,
            link: url
        };

    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error scraping article ${url}:`, error.message);
        throw error;
    }
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

    const { limiter } = await createRateLimiter(BASE_URL, {
        delayBetweenRequests: 5000,
        delayJitter: 2000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 60000
    });

    console.log(`[${SOURCE_NAME}] Step 1: Discovering article URLs from RSS feeds + sitemaps + listing pages...`);
    const sitemapUrls = await discoverUrlsFromSitemap();
    const rssUrls = await discoverUrlsFromRSS();
    let pageUrls = sitemapUrls.length === 0 && rssUrls.length === 0 ? await discoverUrlsFromPages() : [];

    // If still no URLs, try Puppeteer as last resort
    let puppeteerUrls = [];
    if (sitemapUrls.length === 0 && rssUrls.length === 0 && pageUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs from standard methods, trying Puppeteer...`);
        puppeteerUrls = await discoverUrlsWithPuppeteer();
    }

    const allUrls = Array.from(new Set([...sitemapUrls, ...rssUrls, ...pageUrls, ...puppeteerUrls]));

    if (allUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found even with Puppeteer. Cannot proceed.`);
        return { total: 0, saved: 0, failed: 0 };
    }

    console.log(`[${SOURCE_NAME}] Found ${allUrls.length} article URLs`);

    let urlsToProcess = allUrls;
    if (testMode) {
        urlsToProcess = allUrls.slice(0, 10);
    } else if (maxArticles) {
        urlsToProcess = allUrls.slice(0, maxArticles);
    }

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

    console.log(`[${SOURCE_NAME}] Step 3: Scraping ${newUrls.length} articles...`);

    const articles = [];
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < newUrls.length; i++) {
        const url = newUrls[i];

        try {
            await limiter.wait();

            const article = await scrapeArticle(url);
            article.source = SOURCE_NAME;
            article.scrapedAt = new Date();
            articles.push(article);

            const elapsed = Math.floor((Date.now() - startTime) / 1000);
            const rate = (i + 1) / elapsed * 60;
            const remaining = newUrls.length - (i + 1);
            const eta = Math.ceil(remaining / rate);

            console.log(`[${i + 1}/${newUrls.length}] ✓ ${article.extract.substring(0, 50)}... (ETA: ${eta}m)`);

            if (articles.length >= 50) {
                await appendArticlesToCSV(articles, SOURCE_NAME);
                console.log(`[${SOURCE_NAME}] 💾 Saved batch of ${articles.length} articles`);
                articles.length = 0;
            }

        } catch (error) {
            failed++;
            console.error(`[${i + 1}/${newUrls.length}] ✗ Failed: ${url.substring(0, 60)}... - ${error.message}`);
        }
    }

    if (articles.length > 0) {
        await appendArticlesToCSV(articles, SOURCE_NAME);
        console.log(`[${SOURCE_NAME}] 💾 Saved final batch of ${articles.length} articles`);
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
 * Weekly RSS update with full content scraping
 */
export async function updateFromRSS() {
    console.log(`\n=== ${SOURCE_NAME} RSS UPDATE STARTING ===`);

    const rssUrl = `${BASE_URL}/rss`;

    try {
        const metadata = await fetchRSSFeed(rssUrl);

        if (metadata.length === 0) {
            console.log(`[${SOURCE_NAME}] No articles in RSS feed`);
            return { total: 0, new: 0, saved: 0 };
        }

        console.log(`[${SOURCE_NAME}] Found ${metadata.length} articles in RSS feed`);

        const csvFilePath = getCSVFilePath(SOURCE_NAME);
        const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
        const existingLinks = await readExistingLinksCombined(csvFilePath);
        const newArticles = metadata.filter(m => !existingLinks.has(m.link));

        console.log(`[${SOURCE_NAME}] ${newArticles.length} new articles`);

        if (newArticles.length === 0) {
            return { total: metadata.length, new: 0, saved: 0, skipped: metadata.length };
        }

        const { limiter } = await createRateLimiter(BASE_URL, {
            delayBetweenRequests: 5000,
            delayJitter: 2000,
            maxConcurrent: 1
        });

        const articles = [];
        let failed = 0;

        for (const meta of newArticles) {
            try {
                await limiter.wait();

                const article = await scrapeArticle(meta.link);
                article.source = SOURCE_NAME;
                article.date = meta.publishedDate || article.date;
                article.author = meta.author || article.author;
                article.scrapedAt = new Date();

                articles.push(article);
                console.log(`[✓] Scraped: ${meta.title?.substring(0, 60)}...`);

            } catch (error) {
                failed++;
                console.error(`[✗] Failed: ${meta.link} - ${error.message}`);
            }
        }

        if (articles.length > 0) {
            await appendArticlesToCSV(articles, SOURCE_NAME);
            console.log(`[${SOURCE_NAME}] Saved ${articles.length} articles`);
        }

        return {
            total: metadata.length,
            new: newArticles.length,
            saved: articles.length,
            failed
        };

    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error in RSS update:`, error.message);
        throw error;
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
        return await updateFromRSS();
    }
}

