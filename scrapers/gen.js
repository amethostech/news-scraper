/**
 * GEN (Genetic Engineering & Biotechnology News) Scraper
 * 
 * Source: https://www.genengnews.com/
 * 
 * Implementation:
 * - Historical scraping: 2000-2024 (via sitemaps, archives)
 * - Weekly updates: RSS feed + full content scraping
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

const SOURCE_NAME = 'GEN';
const BASE_URL = 'https://www.genengnews.com';

/**
 * Discover article URLs from sitemaps
 */
export async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from sitemap...`);
    
    try {
        // Try common sitemap locations
        const sitemapUrls = [
            `${BASE_URL}/sitemap.xml`,
            `${BASE_URL}/sitemap_index.xml`,
            `${BASE_URL}/sitemaps/sitemap.xml`
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
            console.log(`[${SOURCE_NAME}] No sitemap found, will try archives`);
            return [];
        }

        console.log(`[${SOURCE_NAME}] Found sitemap: ${sitemapIndex}`);

        // Fetch sitemap index
        const sitemaps = await fetchSitemapIndex(sitemapIndex);
        
        if (sitemaps.length === 0) {
            // Try as direct sitemap
            const articles = await fetchArticleLinksFromSitemap(sitemapIndex);
            return articles.filter(a => {
                // Filter by date (2000+)
                if (a.lastmod) {
                    const year = new Date(a.lastmod).getFullYear();
                    return year >= 2000;
                }
                return true; // Include if no date
            });
        }

        // Fetch all sitemaps and collect URLs
        const allArticles = [];
        console.log(`[${SOURCE_NAME}] Processing ${sitemaps.length} sitemaps...`);
        
        for (let i = 0; i < sitemaps.length; i++) {
            const sitemapUrl = sitemaps[i];
            try {
                const articles = await fetchArticleLinksFromSitemap(sitemapUrl);
                
                // Filter by date (2000+) and filter out non-article URLs
                const filtered = articles.filter(a => {
                    // Skip non-article pages
                    if (!a.url || a.url.includes('/latest-posts/') || a.url.includes('/page/')) {
                        return false;
                    }
                    
                    // Filter by date (2000+)
                    if (a.lastmod) {
                        const year = new Date(a.lastmod).getFullYear();
                        return year >= 2000;
                    }
                    return true; // Include if no date (might be old articles)
                });
                
                allArticles.push(...filtered);
                
                if ((i + 1) % 10 === 0) {
                    console.log(`[${SOURCE_NAME}] Processed ${i + 1}/${sitemaps.length} sitemaps, found ${allArticles.length} articles so far...`);
                }
                
                await sleep(1000); // Small delay between sitemaps
            } catch (error) {
                console.warn(`[${SOURCE_NAME}] Error processing sitemap ${sitemapUrl}: ${error.message}`);
                continue;
            }
        }

        console.log(`[${SOURCE_NAME}] Found ${allArticles.length} article URLs from sitemaps`);
        return allArticles.map(a => a.url);
        
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error discovering URLs from sitemap:`, error.message);
        return [];
    }
}

/**
 * Discover article URLs from archive pages
 */
export async function discoverUrlsFromArchives() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from archives...`);
    
    // TODO: Implement archive navigation
    // This will be added after testing sitemap approach
    
    return [];
}

/**
 * Scrape article content from URL
 */
export async function scrapeArticle(url) {
    try {
        await randomDelay(3000, 6000);
        
        const response = await axios.get(url, {
            headers: getRealisticHeaders(BASE_URL),
            timeout: 15000,
            maxRedirects: 5,
            validateStatus: (status) => status < 500
        });

        if (response.status === 403 || response.status === 429) {
            throw new Error(`Blocked (HTTP ${response.status})`);
        }

        const $ = cheerio.load(response.data);

        // Extract title (GEN-specific)
        let title = '';
        const titleSelectors = [
            'h1.td-post-title',
            'h1',
            'h1.entry-title'
        ];
        for (const selector of titleSelectors) {
            const el = $(selector).first();
            if (el.length) {
                title = el.text().trim();
                if (title.length > 0) break;
            }
        }

        // Extract author (GEN-specific: meta tag is most reliable)
        let author = '';
        const authorSelectors = [
            'meta[name="author"]',
            '.td-post-author-name',
            '.td-author-name a',
            '[rel="author"]'
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
                    author = el.text().trim().replace(/^by\s+/i, '');
                    if (author.length > 0) break;
                }
            }
        }

        // Extract date (GEN-specific: time[datetime] or meta tag)
        let date = null;
        const dateSelectors = [
            'time[datetime]',
            'meta[property="article:published_time"]',
            'meta[property="article:datePublished"]',
            '.td-post-date'
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
                    date = new Date(dateText);
                    if (!isNaN(date.getTime())) break;
                }
            }
        }

        // Extract content (GEN-specific: .td-post-content)
        let content = '';
        const contentSelectors = [
            '.td-post-content',
            '.entry-content',
            'article .td-post-content'
        ];
        
        for (const selector of contentSelectors) {
            const el = $(selector).first();
            if (el.length) {
                // Remove unwanted elements
                el.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share, .author-box, .td-post-sharing').remove();
                
                content = el.text()
                    .replace(/\s+/g, ' ')
                    .replace(/\n+/g, '\n')
                    .trim();
                
                if (content.length > 200) {
                    break;
                }
            }
        }

        // Fallback: get all paragraphs
        if (!content || content.length < 200) {
            const paragraphs = $('article p, .article p, main p')
                .map((i, el) => $(el).text().trim())
                .get()
                .filter(p => p.length > 40);
            
            content = paragraphs.join('\n\n').trim();
        }

        if (!title || !content || content.length < 100) {
            throw new Error('Insufficient content extracted');
        }

        return {
            author: author || 'Unknown',
            date: date,
            extract: content,
            link: url
        };

    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error scraping article ${url}:`, error.message);
        throw error;
    }
}

/**
 * Historical scraping: Discover and scrape all articles (2000-2024)
 */
export async function scrapeHistorical(options = {}) {
    const {
        maxArticles = null, // No limit by default
        testMode = false // If true, only scrape first 10 articles
    } = options;

    console.log(`\n=== ${SOURCE_NAME} HISTORICAL SCRAPING STARTING ===`);
    console.log(`Target: All articles from 2000-2024`);
    if (testMode) {
        console.log(`⚠️  TEST MODE: Only scraping first 10 articles`);
    }
    if (maxArticles) {
        console.log(`⚠️  LIMIT: Maximum ${maxArticles} articles`);
    }
    console.log('');

    // Create rate limiter (10 seconds from robots.txt)
    const { limiter } = await createRateLimiter(BASE_URL, {
        delayBetweenRequests: 10000, // 10 seconds (from robots.txt)
        delayJitter: 2000,           // ±2 seconds
        maxConcurrent: 1,
        batchSize: 50,               // Save every 50 articles
        pauseBetweenBatches: 60000   // 1 minute pause every 50 articles
    });

    // Discover URLs
    console.log(`[${SOURCE_NAME}] Step 1: Discovering article URLs from sitemaps...`);
    const urls = await discoverUrlsFromSitemap();
    
    if (urls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs discovered from sitemaps. Trying archives...`);
        const archiveUrls = await discoverUrlsFromArchives();
        urls.push(...archiveUrls);
    }

    if (urls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found. Cannot proceed with historical scraping.`);
        return { total: 0, saved: 0, failed: 0 };
    }

    console.log(`[${SOURCE_NAME}] Found ${urls.length} article URLs`);

    // Apply limits if specified
    let urlsToProcess = urls;
    if (testMode) {
        urlsToProcess = urls.slice(0, 10);
        console.log(`[${SOURCE_NAME}] TEST MODE: Processing only first 10 URLs`);
    } else if (maxArticles) {
        urlsToProcess = urls.slice(0, maxArticles);
        console.log(`[${SOURCE_NAME}] LIMIT: Processing first ${maxArticles} URLs`);
    }

    // Check existing articles
    console.log(`[${SOURCE_NAME}] Step 2: Checking for existing articles...`);
    const csvFilePath = getCSVFilePath(SOURCE_NAME);
    const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
    const existingLinks = await readExistingLinksCombined(csvFilePath);
    const newUrls = urlsToProcess.filter(url => !existingLinks.has(url));
    
    console.log(`[${SOURCE_NAME}] ${newUrls.length} new articles (${urlsToProcess.length - newUrls.length} already exist)`);

    if (newUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] All articles already scraped.`);
        return { total: urls.length, saved: 0, failed: 0, skipped: urlsToProcess.length };
    }

    // Scrape articles
    console.log(`[${SOURCE_NAME}] Step 3: Scraping ${newUrls.length} articles...`);
    console.log(`[${SOURCE_NAME}] Rate: ~10 seconds per article (respecting robots.txt)`);
    console.log(`[${SOURCE_NAME}] Estimated time: ${Math.ceil(newUrls.length * 10 / 60)} minutes\n`);
    
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
            const rate = (i + 1) / elapsed * 60; // articles per minute
            const remaining = newUrls.length - (i + 1);
            const eta = Math.ceil(remaining / rate);
            
            console.log(`[${i + 1}/${newUrls.length}] ✓ ${article.extract.substring(0, 50)}... (ETA: ${eta}m)`);
            
            // Save in batches of 50
            if (articles.length >= 50) {
                await appendArticlesToCSV(articles, SOURCE_NAME);
                console.log(`[${SOURCE_NAME}] 💾 Saved batch of ${articles.length} articles`);
                articles.length = 0; // Clear array
            }
            
        } catch (error) {
            failed++;
            console.error(`[${i + 1}/${newUrls.length}] ✗ Failed: ${url.substring(0, 60)}... - ${error.message}`);
            
            // If too many failures, pause
            if (failed > 10 && failed / (i + 1) > 0.2) {
                console.warn(`[${SOURCE_NAME}] ⚠️  High failure rate (${failed}/${i + 1}). Pausing for 5 minutes...`);
                await sleep(300000); // 5 minutes
            }
        }
    }

    // Save remaining articles
    if (articles.length > 0) {
        await appendArticlesToCSV(articles, SOURCE_NAME);
        console.log(`[${SOURCE_NAME}] 💾 Saved final batch of ${articles.length} articles`);
    }

    const totalTime = Math.floor((Date.now() - startTime) / 1000 / 60);
    
    console.log(`\n=== ${SOURCE_NAME} HISTORICAL SCRAPING COMPLETE ===`);
    console.log(`Total URLs discovered: ${urls.length}`);
    console.log(`URLs processed: ${urlsToProcess.length}`);
    console.log(`New articles: ${newUrls.length}`);
    console.log(`Saved: ${newUrls.length - failed}`);
    console.log(`Failed: ${failed}`);
    console.log(`Time taken: ${totalTime} minutes`);

    return {
        total: urls.length,
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
    
    // RSS feed URL (verified)
    const rssUrl = `${BASE_URL}/feed/`;
    
    try {
        const metadata = await fetchRSSFeed(rssUrl);
        
        if (metadata.length === 0) {
            console.log(`[${SOURCE_NAME}] No articles in RSS feed`);
            return { total: 0, new: 0, saved: 0 };
        }

        console.log(`[${SOURCE_NAME}] Found ${metadata.length} articles in RSS feed`);

        // Check existing
        const csvFilePath = getCSVFilePath(SOURCE_NAME);
        const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
    const existingLinks = await readExistingLinksCombined(csvFilePath);
        const newArticles = metadata.filter(m => !existingLinks.has(m.link));

        console.log(`[${SOURCE_NAME}] ${newArticles.length} new articles`);

        if (newArticles.length === 0) {
            return { total: metadata.length, new: 0, saved: 0, skipped: metadata.length };
        }

        // Create rate limiter
        const { limiter } = await createRateLimiter(BASE_URL, {
            delayBetweenRequests: 5000,
            delayJitter: 2000,
            maxConcurrent: 1
        });

        // Scrape full content for each new article
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
 * @param {Object} options - Options for scraping
 * @param {boolean} options.historical - If true, run historical scraping instead of RSS update
 * @param {boolean} options.testMode - If true, only scrape 10 articles (for testing)
 * @param {number} options.maxArticles - Maximum number of articles to scrape
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
        // Default: RSS update
        return await updateFromRSS();
    }
}

