import axios from 'axios';
import * as cheerio from 'cheerio';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from '../utils/sitemap.js';

const SOURCE_NAME = 'EuropeanPharmaceuticalReview';
const BASE_URL = 'https://www.europeanpharmaceuticalreview.com';

/**
 * Scrape article details from European Pharmaceutical Review
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
            const paragraphs = $('article p, .article p, main p')
                .map((i, el) => $(el).text().trim())
                .get()
                .filter(p => p.length > 40);

            bodyText = paragraphs.join('\n\n').replace(/\s+/g, ' ').trim();
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
                    headers: getRealisticHeaders(BASE_URL),
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

        // Try to fetch sitemap index
        const sitemaps = await fetchSitemapIndex(sitemapIndex);
        
        if (sitemaps.length === 0) {
            // Try as direct sitemap
            const articles = await fetchArticleLinksFromSitemap(sitemapIndex);
            return articles.map(a => typeof a === 'string' ? a : a.url).filter(url => 
                url && url.includes('/article/') && isValidUrl(cleanUrl(url))
            );
        }

        // Fetch all sitemaps and collect URLs
        const allArticles = [];
        console.log(`[${SOURCE_NAME}] Processing ${sitemaps.length} sitemaps...`);
        
        for (let i = 0; i < sitemaps.length; i++) {
            const sitemapUrl = sitemaps[i];
            try {
                const articles = await fetchArticleLinksFromSitemap(sitemapUrl);
                
                // Filter article URLs
                const filtered = articles
                    .map(a => typeof a === 'string' ? a : a.url)
                    .filter(url => url && url.includes('/article/') && isValidUrl(cleanUrl(url)));
                
                allArticles.push(...filtered);
                
                if ((i + 1) % 10 === 0) {
                    console.log(`[${SOURCE_NAME}] Processed ${i + 1}/${sitemaps.length} sitemaps, found ${allArticles.length} articles so far...`);
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
 * Discover article URLs from category/listing pages
 */
export async function discoverUrlsFromCategories() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from category pages...`);
    
    const allUrls = new Set();
    
    // Try to discover from article listing pages
    // Common patterns for article listing pages
    const listingPatterns = [
        `${BASE_URL}/articles`,
        `${BASE_URL}/article`,
        `${BASE_URL}/news`,
        `${BASE_URL}/latest`
    ];
    
    for (const listingUrl of listingPatterns) {
        try {
            await sleep(2000);
            
            const response = await axios.get(listingUrl, {
                headers: getRealisticHeaders(BASE_URL),
                timeout: 15000
            });
            
            const $ = cheerio.load(response.data);
            
            // Extract article URLs
            $('a[href*="/article/"]').each((i, el) => {
                const href = $(el).attr('href');
                if (href) {
                    const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
                    const clean = cleanUrl(fullUrl);
                    if (isValidUrl(clean)) {
                        allUrls.add(clean);
                    }
                }
            });
            
            // Check for pagination
            const nextPage = $('a[rel="next"], .pagination .next, a:contains("Next")').first().attr('href');
            if (nextPage) {
                // Found pagination, could implement pagination discovery here
                console.log(`[${SOURCE_NAME}] Found pagination on ${listingUrl}`);
            }
            
            if (allUrls.size > 0) {
                console.log(`[${SOURCE_NAME}] Found listing pattern: ${listingUrl}`);
                break;
            }
            
        } catch (error) {
            // Pattern doesn't exist, try next
            continue;
        }
    }
    
    console.log(`[${SOURCE_NAME}] Total URLs discovered from categories: ${allUrls.size}`);
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
        delayBetweenRequests: 3000,
        delayJitter: 1000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 30000
    });

    // Try sitemap first
    console.log(`[${SOURCE_NAME}] Step 1: Trying sitemap discovery...`);
    let sitemapUrls = await discoverUrlsFromSitemap();
    
    // Try category discovery
    console.log(`[${SOURCE_NAME}] Step 2: Trying category discovery...`);
    const categoryUrls = await discoverUrlsFromCategories();
    
    // Combine and deduplicate
    const allUrls = Array.from(new Set([...sitemapUrls, ...categoryUrls]));
    
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
            if (article && article.extract && article.extract.length > 100) {
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
            } else {
                failed++;
                console.error(`[${i + 1}/${newUrls.length}] ✗ Invalid article data: ${url}`);
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
        // Default: Try RSS feed first, fallback to sitemap/categories
        try {
            const sourceConfig = {
                sourceName: SOURCE_NAME,
                rssUrl: 'https://www.europeanpharmaceuticalreview.com/news/feed/',
                maxConcurrent: 3,
                delayBetweenScrapes: 1000
            };
            
            const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);
            if (results && results.saved > 0) {
                return results;
            }
        } catch (error) {
            console.log(`[${SOURCE_NAME}] RSS feed error: ${error.message}, trying sitemap/category discovery...`);
        }
        
        // Fallback: Try sitemap/categories
        console.log('\n=== EUROPEAN PHARMACEUTICAL REVIEW SCRAPER STARTING ===');
        console.log('⚠️  RSS feed not available, trying sitemap/category discovery...');
        
        // Try sitemap first
        const sitemapUrls = await discoverUrlsFromSitemap();
        const categoryUrls = await discoverUrlsFromCategories();
        const allUrls = Array.from(new Set([...sitemapUrls, ...categoryUrls]));
        
        if (allUrls.length === 0) {
            console.log(`[${SOURCE_NAME}] No URLs found from sitemap or categories`);
            return { saved: 0, failed: 0, total: 0 };
        }
        
        // Scrape discovered URLs
        const csvFilePath = getCSVFilePath(SOURCE_NAME);
        const { readExistingLinksCombined } = await import('../utils/csvWriter.js');
    const existingLinks = await readExistingLinksCombined(csvFilePath);
        const newUrls = allUrls.filter(url => !existingLinks.has(url));
        
        if (newUrls.length === 0) {
            console.log(`[${SOURCE_NAME}] All articles already scraped`);
            return { saved: 0, failed: 0, total: allUrls.length };
        }
        
        console.log(`[${SOURCE_NAME}] Found ${newUrls.length} new articles to scrape`);
        
        const { limiter } = await createRateLimiter(BASE_URL, {
            delayBetweenRequests: 3000,
            delayJitter: 1000,
            maxConcurrent: 1
        });
        
        const articles = [];
        let failed = 0;
        
        for (const url of newUrls) {
            try {
                await limiter.wait();
                const article = await scrapeArticleDetails(url);
                if (article && article.extract && article.extract.length > 100) {
                    article.source = SOURCE_NAME;
                    article.scrapedAt = new Date();
                    articles.push(article);
                } else {
                    failed++;
                }
            } catch (error) {
                failed++;
            }
        }
        
        if (articles.length > 0) {
            await appendArticlesToCSV(articles, SOURCE_NAME);
        }
        
        return {
            saved: articles.length,
            failed,
            total: allUrls.length
        };
    }
}

