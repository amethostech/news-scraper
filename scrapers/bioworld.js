import axios from 'axios';
import * as cheerio from 'cheerio';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from '../utils/sitemap.js';
import { fetchRSSFeed } from '../utils/rssParser.js';

const SOURCE_NAME = 'BioWorld';
const BASE_URL = 'https://www.bioworld.com';

/**
 * Scrape article details from BioWorld
 */
export async function scrapeArticleDetails(url) {
    try {
        await randomDelay(2000, 4000);

        const response = await axios.get(url, {
            headers: getRealisticHeaders(BASE_URL),
            timeout: 15000,
            maxRedirects: 5,
            validateStatus: function (status) {
                return status < 500;
            }
        });

        if (response.status === 403 || response.status === 429) {
            throw new Error(`Blocked (HTTP ${response.status})`);
        }

        const html = response.data;

        if (html.includes('Access Denied') || html.includes('403') || html.includes('blocked')) {
            throw new Error('Blocked (content check)');
        }

        const $ = cheerio.load(html);

        // Extract title
        let title = '';
        const titleSelectors = [
            'h1.page-article-teaser__headline',
            '.page-article-teaser__headline',
            'h1.article-title',
            'h1',
            'meta[property="og:title"]',
            '.entry-title',
            '.article-header h1'
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
                        articleDate = tempDate.toISOString().split('T')[0]; // YYYY-MM-DD
                        break;
                    }
                }
            } else if (selector.includes('[datetime]')) {
                const datetime = $(selector).first().attr('datetime');
                if (datetime) {
                    const tempDate = new Date(datetime);
                    if (!isNaN(tempDate.getTime())) {
                        articleDate = tempDate.toISOString().split('T')[0]; // YYYY-MM-DD
                        break;
                    }
                }
            } else {
                const dateText = $(selector).first().text().trim();
                if (dateText) {
                    const tempDate = new Date(dateText);
                    if (!isNaN(tempDate.getTime())) {
                        articleDate = tempDate.toISOString().split('T')[0]; // YYYY-MM-DD
                        break;
                    }
                }
            }
        }

        // Extract body text
        let bodyText = '';
        const bodySelectors = [
            '.page-article-teaser__content',
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
            '.box1.article'
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
            const paragraphs = $('article p, .article p, main p')
                .map((i, el) => $(el).text().trim())
                .get()
                .filter(p => p.length > 40);

            bodyText = paragraphs.join('\n\n').replace(/\s+/g, ' ').trim();
        }

        // BioWorld articles may be behind paywall - accept teaser content if full content not available
        if (!bodyText || bodyText.length < 50) {
            // Try to get teaser text as fallback
            const teaser = $('.page-article-teaser__content, .teaser, .excerpt').first().text().trim();
            if (teaser && teaser.length > 50) {
                bodyText = teaser;
            }
        }

        return {
            title: title || 'Untitled',
            author: author || 'Unknown',
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
 * Discover article URLs from topic pages
 */
export async function discoverUrlsFromTopics() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from topic pages...`);

    const topics = [
        '/topics/84-bioworld',
        '/topics/85-bioworld-medtech',
        '/topics/86-bioworld-asia',
        '/topics/520-bioworld-science'
    ];

    const allUrls = new Set();
    const maxPagesPerTopic = 500; // Increased from 200 to 500 for more comprehensive discovery

    for (const topic of topics) {
        console.log(`[${SOURCE_NAME}] Checking topic: ${topic}`);

        for (let page = 1; page <= maxPagesPerTopic; page++) {
            const url = page === 1
                ? `${BASE_URL}${topic}`
                : `${BASE_URL}${topic}?page=${page}`;

            try {
                await sleep(2000); // Delay between requests

                const response = await axios.get(url, {
                    headers: getRealisticHeaders(BASE_URL),
                    timeout: 15000
                });

                const $ = cheerio.load(response.data);

                // Extract article URLs - try multiple selectors
                const articleLinks = [];

                // Method 1: Direct article links
                $('a[href*="/articles/"]').each((i, el) => {
                    const href = $(el).attr('href');
                    if (href) {
                        // Filter out topic pages and other non-article links
                        if (href.includes('/articles/topic/') || href.includes('/articles/topic,')) {
                            return; // Skip topic links
                        }

                        // Check if it's an actual article (has numeric ID pattern)
                        if (href.match(/\/articles\/\d+-/) || href.match(/\/articles\/\d+\//)) {
                            const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
                            const clean = cleanUrl(fullUrl);
                            if (isValidUrl(clean) && !clean.includes('?v=preview')) {
                                articleLinks.push(clean);
                            }
                        }
                    }
                });

                // Method 2: Check featured stories and article listings
                $('.featured-stories__article-title-link, .portal-section__item-link, a[href*="articles/"][href*="-"]').each((i, el) => {
                    const href = $(el).attr('href');
                    if (href && href.includes('/articles/') && !href.includes('/topic/')) {
                        const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
                        const clean = cleanUrl(fullUrl);
                        if (isValidUrl(clean) && !clean.includes('?v=preview') && clean.match(/\/articles\/\d+-/)) {
                            articleLinks.push(clean);
                        }
                    }
                });

                const urlsBefore = allUrls.size;
                articleLinks.forEach(link => allUrls.add(link));
                const urlsAfter = allUrls.size;
                const newUrlsThisPage = urlsAfter - urlsBefore;

                // Stop if no new URLs found for 3 consecutive pages
                if (newUrlsThisPage === 0 && page > 1) {
                    if (page > 3) {
                        console.log(`[${SOURCE_NAME}] No new URLs found on page ${page}, stopping pagination for ${topic}`);
                        break;
                    }
                }

                if (page % 20 === 0 || newUrlsThisPage > 0) {
                    console.log(`[${SOURCE_NAME}] Page ${page} of ${topic}: Found ${newUrlsThisPage} new URLs (${allUrls.size} total so far)`);
                }

                // If no articles found and we're past page 10, likely no more content
                if (articleLinks.length === 0 && page > 10) {
                    console.log(`[${SOURCE_NAME}] No articles found on page ${page}, stopping pagination for ${topic}`);
                    break;
                }

            } catch (error) {
                if (error.response?.status === 404 && page > 1) {
                    // No more pages
                    break;
                }
                console.warn(`[${SOURCE_NAME}] Error fetching ${url}:`, error.message);
                // Continue to next page
            }
        }

        console.log(`[${SOURCE_NAME}] Found ${allUrls.size} total URLs after processing ${topic}`);
    }

    console.log(`[${SOURCE_NAME}] Total URLs discovered from topics: ${allUrls.size}`);
    return Array.from(allUrls);
}

/**
 * Discover article URLs from sitemap
 */
async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from sitemap...`);

    // BioWorld sitemap redirects to /ext/resources/sitemap.xml
    const sitemapUrls = [
        `${BASE_URL}/sitemap.xml`,
        `${BASE_URL}/ext/resources/sitemap.xml`
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
                            if (url && url.includes('bioworld.com') && url.includes('/articles/')) {
                                allUrls.push(cleanUrl(url));
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
                    if (url && url.includes('bioworld.com') && url.includes('/articles/')) {
                        allUrls.push(cleanUrl(url));
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
        delayBetweenRequests: 3000,
        delayJitter: 1000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 30000
    });

    // Discover URLs from multiple sources
    console.log(`[${SOURCE_NAME}] Step 1: Discovering article URLs from multiple sources...`);

    // 1. Sitemap discovery (most comprehensive)
    const sitemapUrls = await discoverUrlsFromSitemap();

    // 2. Topic pages (enhanced pagination)
    console.log(`[${SOURCE_NAME}] Step 2: Discovering URLs from topic pages...`);
    const topicUrls = await discoverUrlsFromTopics();

    // 3. RSS feed URLs
    console.log(`[${SOURCE_NAME}] Step 3: Getting RSS feed URLs...`);
    let rssUrls = [];
    try {
        const rssMetadata = await fetchRSSFeed('https://www.bioworld.com/rss/articles');
        rssUrls = rssMetadata.map(m => m.link);
        console.log(`[${SOURCE_NAME}] Found ${rssUrls.length} URLs from RSS feed`);
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Could not fetch RSS feed: ${error.message}`);
    }

    // Combine and deduplicate (prioritize sitemap and topic URLs)
    const allUrls = Array.from(new Set([...sitemapUrls, ...topicUrls, ...rssUrls]));

    if (allUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] No URLs found. Cannot proceed with historical scraping.`);
        return { total: 0, saved: 0, failed: 0 };
    }

    console.log(`[${SOURCE_NAME}] Found ${allUrls.length} total article URLs`);

    // Apply limits if specified
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
    console.log(`[${SOURCE_NAME}] Rate: ~3 seconds per article\n`);

    const articles = [];
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < newUrls.length; i++) {
        const url = newUrls[i];

        try {
            await limiter.wait();

            const article = await scrapeArticleDetails(url);
            article.source = SOURCE_NAME;
            article.scrapedAt = new Date();
            articles.push(article);

            const elapsed = Math.floor((Date.now() - startTime) / 1000);
            const rate = (i + 1) / elapsed * 60;
            const remaining = newUrls.length - (i + 1);
            const eta = Math.ceil(remaining / rate);

            console.log(`[${i + 1}/${newUrls.length}] ✓ ${article.title.substring(0, 60)}... (ETA: ${eta}m)`);

            // Save in batches of 50
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

    // Save remaining articles
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
        console.log('\n=== BIOWORLD SCRAPER STARTING ===');

        const sourceConfig = {
            sourceName: SOURCE_NAME,
            rssUrls: [
                'https://www.bioworld.com/rss/articles',
                'https://www.bioworld.com/rss'
            ],
            maxConcurrent: 2,
            delayBetweenScrapes: 3000
        };

        try {
            const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);

            console.log(`\n=== BIOWORLD SCRAPER COMPLETE ===`);
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
            console.error('Error in BioWorld scraper:', error);
            return { saved: 0, failed: 0, total: 0 };
        }
    }
}

