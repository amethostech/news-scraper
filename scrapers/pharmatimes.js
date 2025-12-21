/**
 * PharmaTimes Scraper - IMPROVED VERSION
 * 
 * Source: https://pharmatimes.com/
 * 
 * Improvements:
 * - Correct content selector (.et_pb_post_content)
 * - Better metadata extraction (prioritizes meta tags)
 * - Retry logic with exponential backoff
 * - Enhanced content filtering
 * - Increased timeout for reliability
 */

import axios from 'axios';
import * as cheerio from 'cheerio';
import { fetchRSSFeed } from '../utils/rssParser.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from '../utils/sitemap.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'PharmaTimes';
const BASE_URL = 'https://pharmatimes.com';

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
                .filter(a => a.url && a.url.includes('pharmatimes.com'))
                .map(a => a.url);
        }

        const allArticles = [];
        console.log(`[${SOURCE_NAME}] Processing ${sitemaps.length} sitemaps...`);

        for (let i = 0; i < sitemaps.length; i++) {
            const sitemapUrl = sitemaps[i];
            try {
                const articles = await fetchArticleLinksFromSitemap(sitemapUrl);

                const filtered = articles.filter(a => {
                    if (!a.url || !a.url.includes('pharmatimes.com')) {
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
                if (item.link && item.link.includes('pharmatimes.com')) {
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
 * Scrape article content from URL with retry logic
 */
export async function scrapeArticle(url, retries = 2) {
    let lastError;

    for (let attempt = 0; attempt <= retries; attempt++) {
        try {
            // Exponential backoff for retries
            if (attempt > 0) {
                const backoff = Math.pow(2, attempt) * 2000;
                console.log(`[${SOURCE_NAME}] Retry ${attempt}/${retries} after ${backoff}ms...`);
                await sleep(backoff);
            }

            await randomDelay(3000, 6000);

            const response = await axios.get(url, {
                headers: getRealisticHeaders(BASE_URL),
                timeout: 30000,  // 30 seconds timeout
                maxRedirects: 5,
                validateStatus: (status) => status < 500
            });

            if (response.status === 403 || response.status === 429) {
                throw new Error(`Blocked (HTTP ${response.status})`);
            }

            const $ = cheerio.load(response.data);

            // Extract title - PharmaTimes uses h1.entry-title and meta tags
            let title = '';
            const titleSelectors = [
                'meta[property="og:title"]',  // Primary: Open Graph
                'h1.entry-title',              // PharmaTimes specific
                'h1',                          // Generic
                'title'                        // Fallback
            ];

            for (const selector of titleSelectors) {
                if (selector.startsWith('meta')) {
                    const meta = $(selector).attr('content');
                    if (meta) {
                        title = meta.trim();
                        // Remove site suffix if present
                        title = title.replace(/\s*-\s*PharmaTimes\s*$/i, '');
                        break;
                    }
                } else {
                    const el = $(selector).first();
                    if (el.length) {
                        title = el.text().trim();
                        if (title.length > 0) {
                            title = title.replace(/\s*-\s*PharmaTimes\s*$/i, '');
                            break;
                        }
                    }
                }
            }

            // Extract author - PharmaTimes uses meta tags and span.author
            let author = '';
            const authorSelectors = [
                'meta[name="author"]',       // Primary: Meta tag
                'span.author',               // PharmaTimes specific
                'span.author.vcard',         // WordPress standard
                '.byline',                   // Generic
                '.author-name',              // Generic
                'a[rel="author"]'            // Semantic HTML
            ];

            for (const selector of authorSelectors) {
                if (selector.startsWith('meta')) {
                    const meta = $(selector).attr('content');
                    if (meta) {
                        author = meta.trim();
                        break;
                    }
                } else {
                    const el = $(selector).first();
                    if (el.length) {
                        author = el.text().trim();
                        // Clean up common prefixes
                        author = author.replace(/^(by|written by|posted by|author:)\s+/i, '');
                        if (author.length > 0) break;
                    }
                }
            }

            // Extract date - PharmaTimes uses meta tags
            let date = null;
            const dateSelectors = [
                'meta[property="article:published_time"]',  // Primary: Structured metadata
                'meta[property="article:modified_time"]',   // Fallback: Modified time
                'time[datetime]',                            // Fallback: HTML5 time element
                'span.published',                            // Fallback: PharmaTimes specific
                '.published-date',                           // Fallback: Generic
                '.post-date'                                 // Fallback: Generic
            ];

            for (const selector of dateSelectors) {
                if (selector.startsWith('meta')) {
                    const meta = $(selector).attr('content');
                    if (meta) {
                        date = new Date(meta);
                        if (!isNaN(date.getTime())) break;
                    }
                } else if (selector.includes('[datetime]')) {
                    const datetime = $(selector).first().attr('datetime');
                    if (datetime) {
                        date = new Date(datetime);
                        if (!isNaN(date.getTime())) break;
                    }
                } else {
                    const el = $(selector).first();
                    if (el.length) {
                        const dateText = el.text().trim();
                        if (dateText) {
                            date = new Date(dateText);
                            if (!isNaN(date.getTime())) break;
                        }
                    }
                }
            }

            // Extract content - PharmaTimes uses .et_pb_post_content (Divi theme)
            let content = '';
            const contentSelectors = [
                '.et_pb_post_content',      // Primary: Main article content container
                '.entry-content',            // Fallback: Standard WordPress
                '.et_pb_text_inner',         // Fallback: Divi text modules
                '.article-content',          // Fallback: Generic article content
                'article .content'           // Fallback: Article content
            ];

            for (const selector of contentSelectors) {
                const el = $(selector).first();
                if (el.length) {
                    // Clone to avoid modifying original
                    const $clone = el.clone();

                    // Remove unwanted elements
                    $clone.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share, .author-box, .tags, .meta').remove();

                    // Extract paragraphs
                    const paragraphs = $clone.find('p')
                        .map((i, p) => $(p).text().trim())
                        .get()
                        .filter(p => {
                            // Filter out short paragraphs and common noise
                            if (p.length < 30) return false;
                            if (p.match(/^(tags|related|share|subscribe|download)/i)) return false;
                            return true;
                        });

                    if (paragraphs.length >= 3) {
                        content = paragraphs.join('\n\n').trim();
                        if (content.length > 200) {
                            break;
                        }
                    }
                }
            }

            // Fallback: get all paragraphs from main content area
            if (!content || content.length < 200) {
                const paragraphs = $('article p, .article p, main p, .post p')
                    .map((i, el) => {
                        const text = $(el).text().trim();
                        // Skip if parent is in unwanted sections
                        const parent = $(el).closest('.advertisement, .ad, .sidebar, .footer, .header, .nav, .related, .tags, .meta');
                        if (parent.length > 0) return null;
                        return text;
                    })
                    .get()
                    .filter(p => p && p.length > 40);

                content = paragraphs.join('\n\n').trim();
            }

            // Validate extracted data
            if (!title || title.length < 10) {
                throw new Error('Title too short or missing');
            }

            if (!content || content.length < 100) {
                throw new Error(`Insufficient content extracted (${content.length} chars)`);
            }

            // Success - return the article
            return {
                title: title,
                author: author || 'Unknown',
                date: date,
                extract: content,
                link: url
            };

        } catch (error) {
            lastError = error;

            // Don't retry on validation errors (content issues)
            if (error.message.includes('Title too short') ||
                error.message.includes('Insufficient content')) {
                console.error(`[${SOURCE_NAME}] Validation error for ${url}:`, error.message);
                throw error;
            }

            // Retry on network/timeout errors
            if (attempt < retries) {
                console.warn(`[${SOURCE_NAME}] Attempt ${attempt + 1}/${retries + 1} failed for ${url}: ${error.message}`);
                continue;
            }

            // Last attempt failed
            console.error(`[${SOURCE_NAME}] All attempts failed for ${url}:`, error.message);
            throw error;
        }
    }

    // If we get here, all retries failed
    throw lastError;
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

    console.log(`[${SOURCE_NAME}] Step 1: Discovering article URLs from RSS feeds + sitemaps...`);
    const sitemapUrls = await discoverUrlsFromSitemap();
    const rssUrls = await discoverUrlsFromRSS();

    const allUrls = Array.from(new Set([...sitemapUrls, ...rssUrls]));

    if (allUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found. Cannot proceed.`);
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

            console.log(`[${i + 1}/${newUrls.length}] ✓ ${article.title.substring(0, 60)}... (ETA: ${eta}m)`);

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
                console.log(`[✓] Scraped: ${article.title?.substring(0, 60)}...`);

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
