/**
 * FDA Scraper
 * 
 * Source: https://www.fda.gov/
 * 
 * Implementation:
 * - Historical scraping: RSS feeds (MedWatch, Press Announcements)
 * - Weekly updates: RSS feed + full content scraping
 */

import axios from 'axios';
import * as cheerio from 'cheerio';
import { fetchRSSFeed } from '../utils/rssParser.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl } from '../utils/linkValidator.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'FDA';
const BASE_URL = 'https://www.fda.gov';

/**
 * Discover article URLs from RSS feeds
 */
export async function discoverUrlsFromRSS() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from RSS feeds...`);

    const rssUrls = [
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml',
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/drugs/rss.xml',
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/biologics/rss.xml',
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/medwatch/rss.xml',
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/recalls/rss.xml'
    ];

    const allUrls = new Set();

    for (const rssUrl of rssUrls) {
        try {
            await sleep(1000);
            const metadata = await fetchRSSFeed(rssUrl);

            metadata.forEach(item => {
                if (item.link && item.link.includes('fda.gov')) {
                    allUrls.add(cleanUrl(item.link));
                }
            });

            if (metadata.length > 0) {
                console.log(`[${SOURCE_NAME}] Found ${metadata.length} articles in RSS: ${rssUrl}`);
            }
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error fetching RSS ${rssUrl}: ${error.message}`);
            continue;
        }
    }

    return Array.from(allUrls);
}

/**
 * Scrape article content from URL
 */
export async function scrapeArticle(url) {
    try {
        // Normalize URL to https (FDA redirects http to https with different page structure)
        url = url.replace(/^http:\/\//i, 'https://');

        await randomDelay(2000, 4000);

        // Use simple headers - FDA's anti-bot detects the "realistic" headers
        const response = await axios.get(url, {
            timeout: 15000,
            maxRedirects: 5,
            validateStatus: (status) => status < 500
        });

        if (response.status === 403 || response.status === 429) {
            throw new Error(`Blocked (HTTP ${response.status})`);
        }

        const $ = cheerio.load(response.data);

        // Extract title
        let title = '';
        const titleSelectors = [
            'h1.content-title',
            'article#main-content h1',
            'h1',
            'meta[property="og:title"]'
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

        // Extract date
        let date = null;
        const dateSelectors = [
            'meta[property="article:published_time"]',
            'time[datetime]',
            '.fda-page-date',
            '.published-date',
            'meta[name="date"]'
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

        // Extract content - FDA specific selectors (updated based on actual page structure)
        let content = '';
        const contentSelectors = [
            'div[role="main"]',
            'article#main-content',
            '.field-body',
            '.article-body',
            'main .content'
        ];

        for (const selector of contentSelectors) {
            const el = $(selector).first();
            if (el.length) {
                // Remove unwanted elements
                el.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share, .breadcrumb, .skip-link, .inset-column').remove();

                const paragraphs = el.find('p')
                    .map((i, p) => $(p).text().trim())
                    .get()
                    .filter(p => p.length > 30);

                if (paragraphs.length > 0) {
                    content = paragraphs.join('\n\n').trim();
                    if (content.length > 200) {
                        break;
                    }
                }
            }
        }


        // Fallback: get all paragraphs from main content area
        if (!content || content.length < 200) {
            const paragraphs = $('main p, .content p, article p')
                .map((i, el) => {
                    const text = $(el).text().trim();
                    if (text.length > 30 && !/^(skip|menu|navigation|share|print)$/i.test(text)) {
                        return text;
                    }
                    return null;
                })
                .get()
                .filter(p => p !== null && p.length > 30);

            if (paragraphs.length > 0) {
                content = paragraphs.join('\n\n').trim();
            }
        }

        // Final cleanup
        if (content) {
            content = content
                .replace(/\s{3,}/g, ' ')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
        }

        if (!title || !content || content.length < 100) {
            throw new Error('Insufficient content extracted');
        }

        return {
            author: 'FDA',
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
        delayBetweenRequests: 3000,
        delayJitter: 1000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 30000
    });

    console.log(`[${SOURCE_NAME}] Step 1: Discovering article URLs from RSS feeds + listing pages...`);
    const rssUrls = await discoverUrlsFromRSS();

    // If no RSS URLs, try scraping listing pages
    let pageUrls = [];
    if (rssUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] Trying to discover URLs from listing pages...`);
        // Try multiple FDA news pages
        const fdaPages = [
            `${BASE_URL}/news-events/press-announcements`,
            `${BASE_URL}/news-events/fda-newsroom`,
            `${BASE_URL}/news-events/speeches-fda-officials`
        ];

        for (const pageUrl of fdaPages) {
            try {
                await sleep(2000);
                const response = await axios.get(pageUrl, {
                    headers: getRealisticHeaders(BASE_URL),
                    timeout: 20000
                });
                const $ = cheerio.load(response.data);

                // Find article links - FDA uses various patterns
                $('a[href*="/news-events/"], a[href*="/press-announcements/"], a[href*="/speeches-fda-officials/"]').each((i, el) => {
                    const href = $(el).attr('href');
                    if (href) {
                        let fullUrl = href;
                        if (href.startsWith('/')) {
                            fullUrl = `${BASE_URL}${href}`;
                        } else if (!href.startsWith('http')) {
                            fullUrl = `${BASE_URL}/${href}`;
                        }

                        if (fullUrl.includes('fda.gov') &&
                            (fullUrl.includes('/press-announcements/') ||
                                fullUrl.includes('/news-events/') ||
                                fullUrl.includes('/speeches-fda-officials/')) &&
                            !fullUrl.includes('/rss') &&
                            !fullUrl.includes('/feed') &&
                            !fullUrl.includes('#') &&
                            fullUrl.split('/').length >= 5) { // Ensure it's an article, not a category
                            pageUrls.push(cleanUrl(fullUrl));
                        }
                    }
                });

                // Also check for article containers
                $('article, .article, .news-item, [class*="article"]').each((i, el) => {
                    const $el = $(el);
                    const link = $el.find('a').first().attr('href');
                    if (link) {
                        let fullUrl = link;
                        if (link.startsWith('/')) {
                            fullUrl = `${BASE_URL}${link}`;
                        } else if (!link.startsWith('http')) {
                            fullUrl = `${BASE_URL}/${link}`;
                        }

                        if (fullUrl.includes('fda.gov') && fullUrl.includes('/news-events/')) {
                            pageUrls.push(cleanUrl(fullUrl));
                        }
                    }
                });
            } catch (error) {
                console.warn(`[${SOURCE_NAME}] Error scraping ${pageUrl}: ${error.message}`);
                continue;
            }
        }

        pageUrls = [...new Set(pageUrls)];
        if (pageUrls.length > 0) {
            console.log(`[${SOURCE_NAME}] Found ${pageUrls.length} articles from listing pages`);
        }
    }

    const allUrls = Array.from(new Set([...rssUrls, ...pageUrls]));

    if (allUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found. Cannot proceed.`);
        return { total: 0, saved: 0, failed: 0 };
    }

    console.log(`[${SOURCE_NAME}] Found ${allUrls.length} article URLs`);

    let urlsToProcess = allUrls;
    if (testMode) {
        urlsToProcess = rssUrls.slice(0, 10);
    } else if (maxArticles) {
        urlsToProcess = rssUrls.slice(0, maxArticles);
    }

    console.log(`[${SOURCE_NAME}] Step 2: Checking for existing articles...`);
    const csvFilePath = getCSVFilePath(SOURCE_NAME);
    const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
    const existingLinks = await readExistingLinksCombined(csvFilePath);
    const newUrls = urlsToProcess.filter(url => !existingLinks.has(url));

    console.log(`[${SOURCE_NAME}] ${newUrls.length} new articles (${urlsToProcess.length - newUrls.length} already exist)`);

    if (newUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] All articles already scraped.`);
        return { total: rssUrls.length, saved: 0, failed: 0, skipped: urlsToProcess.length };
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

    const rssUrls = [
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml',
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/drugs/rss.xml',
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/biologics/rss.xml',
        'https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/medwatch/rss.xml'
    ];

    try {
        const allMetadata = [];

        for (const rssUrl of rssUrls) {
            try {
                const metadata = await fetchRSSFeed(rssUrl);
                allMetadata.push(...metadata);
            } catch (error) {
                console.warn(`[${SOURCE_NAME}] Error fetching RSS ${rssUrl}: ${error.message}`);
            }
        }

        if (allMetadata.length === 0) {
            console.log(`[${SOURCE_NAME}] No articles in RSS feeds`);
            return { total: 0, new: 0, saved: 0 };
        }

        console.log(`[${SOURCE_NAME}] Found ${allMetadata.length} articles in RSS feeds`);

        const csvFilePath = getCSVFilePath(SOURCE_NAME);
        const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
        const existingLinks = await readExistingLinksCombined(csvFilePath);
        const newArticles = allMetadata.filter(m => !existingLinks.has(m.link));

        console.log(`[${SOURCE_NAME}] ${newArticles.length} new articles`);

        if (newArticles.length === 0) {
            return { total: allMetadata.length, new: 0, saved: 0, skipped: allMetadata.length };
        }

        const { limiter } = await createRateLimiter(BASE_URL, {
            delayBetweenRequests: 3000,
            delayJitter: 1000,
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
            total: allMetadata.length,
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

