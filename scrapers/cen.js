import * as cheerio from 'cheerio';
import axios from 'axios';
import { runSitemapScraper } from '../utils/scraperRunner.js';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { appendArticlesToCSV, getCSVFilePath } from '../utils/csvWriter.js';
import { cleanUrl, isValidUrl } from '../utils/linkValidator.js';
import { createRateLimiter } from '../utils/rateLimiter.js';
import { sleep } from '../utils/common.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';

const SOURCE_NAME = 'CEN';
const BASE_URL = 'https://cen.acs.org';

function isArticleUrl(url) {
    // C&EN article URLs can be:
    // - https://cen.acs.org/articles/XX/iXX/...html (old format)
    // - https://cen.acs.org/content/cen/articles/... (new format)
    // - https://cen.acs.org/articles/... (various formats)
    // - https://cen.acs.org/content/cen/news/... (news articles)
    // Exclude static pages, about pages, sponsored content, and sitemaps
    if (!url || typeof url !== 'string') return false;
    
    const isArticle = /cen\.acs\.org\/.*articles/.test(url) || 
                      /cen\.acs\.org\/content\/cen\/articles/.test(url) ||
                      /cen\.acs\.org\/content\/cen\/news/.test(url);
    
    const isExcluded = url.includes('/static/') || 
                       url.includes('/about/') || 
                       url.includes('/sitemap') ||
                       url.includes('/static.html') ||
                       url.includes('/sponsored-content/');
    
    return isArticle && !isExcluded;
}

/**
 * Discover article URLs from C&EN archive/category pages
 */
export async function discoverUrlsFromArchives() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from archive pages...`);
    
    const allUrls = new Set();
    
    // Try to discover from article listing pages
    // C&EN might have year-based archives or category pages
    const currentYear = new Date().getFullYear();
    const startYear = 2000;
    
    for (let year = currentYear; year >= startYear; year--) {
        // Try different archive URL patterns
        const archivePatterns = [
            `${BASE_URL}/articles/${year}`,
            `${BASE_URL}/content/cen/articles/${year}`,
            `${BASE_URL}/archive/${year}`
        ];
        
        for (const archiveUrl of archivePatterns) {
            try {
                await sleep(2000);
                
                const response = await axios.get(archiveUrl, {
                    headers: getRealisticHeaders(BASE_URL),
                    timeout: 15000
                });
                
                const $ = cheerio.load(response.data);
                
                // Extract article URLs
                $('a[href*="/articles/"], a[href*="/content/cen/articles/"], a[href*="/content/cen/news/"]').each((i, el) => {
                    const href = $(el).attr('href');
                    if (href) {
                        const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href}`;
                        const clean = cleanUrl(fullUrl);
                        if (isValidUrl(clean) && isArticleUrl(clean)) {
                            allUrls.add(clean);
                        }
                    }
                });
                
                // If we found URLs, this pattern works - break and try next year
                if (allUrls.size > 0) {
                    console.log(`[${SOURCE_NAME}] Found archive pattern for ${year}: ${archiveUrl}`);
                    break;
                }
                
            } catch (error) {
                // Pattern doesn't exist, try next
                continue;
            }
        }
        
        if (year % 10 === 0) {
            console.log(`[${SOURCE_NAME}] Processed years ${year}-${currentYear}, found ${allUrls.size} URLs so far...`);
        }
    }
    
    console.log(`[${SOURCE_NAME}] Total URLs discovered from archives: ${allUrls.size}`);
    return Array.from(allUrls);
}

async function scrapeArticleDetails(url) {
    try {
        const { data } = await axios.get(url, {
            headers: { 
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' 
            }
        });
        
        const $ = cheerio.load(data);
        
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
        
        // Extract author - try multiple selectors
        let author = '';
        const authorSelectors = [
            'heading-line-bottom',
           '.article-byline--author',
            '.article-byline a[rel="author"]',
            '.author-name',
            '.article-author',
            '[itemprop="author"]',
            '.byline',
            'meta[name="author"]',
            'meta[property="article:author"]',
            '.contributor-name'
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
        
        // Extract date - try multiple selectors and formats
        let articleDate = '';
        const dateSelectors = [
            'time[datetime]',
            '.date',
            '[itemprop="datePublished"]',
            '.article-date',
            '.publish-date',
            'meta[name="date"]',
            'meta[property="article:published_time"]',
            '.date-published'
        ];
        
        for (const selector of dateSelectors) {

            // Meta tag case
            if (selector.startsWith('meta')) {
                const metaContent = $(selector).attr('content');
                if (metaContent) {
                    articleDate = metaContent.trim();
                    break;
                }
                continue;
            }

            // time[datetime] case
            if (selector.includes('[datetime]')) {
                const el = $(selector).first();
                const datetime = el.attr('datetime');
                if (datetime) {
                    articleDate = datetime.trim();
                    break;
                }
                const fallbackText = el.text().trim();
                if (fallbackText) {
                    articleDate = fallbackText;
                    break;
                }
                continue;
            }

            // Generic element with text date
            const dateText = $(selector).first().text().trim();
            if (dateText) {
                articleDate = dateText;
                break;
            }
        }
        
        // Extract body text - try multiple approaches
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
            '#article-body .section'     
        ];
        
        for (const selector of bodySelectors) {
            const content = $(selector);
            if (content.length > 0) {
                content.find('script, style, nav, footer, .advertisement, .ad, .related-articles').remove();
                
                bodyText = content.text()
                    .replace(/\s+/g, ' ')
                    .replace(/\n+/g, '\n')
                    .trim();
                
                if (bodyText.length > 100) { 
                    break;
                }
            }
        }
        
        // Fallback: if no body found, try getting all paragraph text
        if (!bodyText || bodyText.length < 100) {
            const paragraphs = $('article p, .article p, main p')
                .map((i, el) => $(el).text().trim())
                .get();

            bodyText = paragraphs.join('\n').replace(/\s+/g, ' ').trim();
        }
        
        // Log for debugging
        console.log(`Scraped: ${url}`);
        console.log(`Author: ${author || 'Not found'}`);
        console.log(`Date: ${articleDate || 'Not found'}`);
        console.log(`Content length: ${bodyText.length}`);
        
        return {
            title: title || 'Untitled',
            author: author || 'Unknown',
            date: articleDate || '',
            extract: bodyText.trim(),
            link: url,
        };
        
    } catch (error) {
        console.error(`Error fetching article details for ${url}:`, error.message);
        return null;
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

    // Create rate limiter
    const { limiter } = await createRateLimiter(BASE_URL, {
        delayBetweenRequests: 5000,
        delayJitter: 2000,
        maxConcurrent: 1,
        batchSize: 50,
        pauseBetweenBatches: 60000
    });

    // Try sitemap first
    console.log(`[${SOURCE_NAME}] Step 1: Trying sitemap discovery...`);
    let allUrls = [];
    
    try {
        const articlesSaved = await runSitemapScraper({
            sourceName: SOURCE_NAME,
            sitemapIndexUrl: 'https://cen.acs.org/content/cen/sitemap/sitemap_index.xml',
            sitemapFilter: (loc) => /sitemap_\d+\.xml$/.test(loc),
            linkFilter: (link) => isArticleUrl(link),
            scrapeDetails: scrapeArticleDetails
        });
        
        // If sitemap returned 0, try alternative discovery
        if (articlesSaved === 0) {
            console.log(`[${SOURCE_NAME}] Sitemap returned 0 articles, trying archive discovery...`);
            allUrls = await discoverUrlsFromArchives();
        } else {
            // Sitemap worked, but we need to get the URLs
            // For now, use archive discovery as fallback
            allUrls = await discoverUrlsFromArchives();
        }
    } catch (error) {
        console.warn(`[${SOURCE_NAME}] Sitemap discovery failed: ${error.message}`);
        console.log(`[${SOURCE_NAME}] Trying archive discovery...`);
        allUrls = await discoverUrlsFromArchives();
    }
    
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
    console.log(`[${SOURCE_NAME}] Rate: ~5 seconds per article\n`);
    
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
        // Default: Try RSS feed first, fallback to sitemap
        // Try common RSS feed URLs for C&EN
        const rssUrls = [
            'https://cen.acs.org/rss.xml',
            'https://cen.acs.org/feed/',
            'https://cen.acs.org/rss/',
            'https://cen.acs.org/content/cen/rss.xml'
        ];
        
        let rssWorked = false;
        for (const rssUrl of rssUrls) {
            try {
                const sourceConfig = {
                    sourceName: SOURCE_NAME,
                    rssUrl: rssUrl,
                    maxConcurrent: 3,
                    delayBetweenScrapes: 1000
                };
                
                const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);
                if (results && (results.saved > 0 || results.total > 0)) {
                    rssWorked = true;
                    return results;
                }
            } catch (error) {
                // Try next RSS URL
                console.log(`[${SOURCE_NAME}] RSS feed ${rssUrl} failed: ${error.message}, trying next...`);
                continue;
            }
        }
        
        // If RSS didn't work, fallback to sitemap
        if (!rssWorked) {
            console.log(`[${SOURCE_NAME}] RSS feeds not available, using sitemap...`);
            try {
                const articlesSaved = await runSitemapScraper({
                    sourceName: SOURCE_NAME,
                    sitemapIndexUrl: 'https://cen.acs.org/content/cen/sitemap/sitemap_index.xml',
                    sitemapFilter: (loc) => /sitemap_\d+\.xml$/.test(loc),
                    linkFilter: (link) => isArticleUrl(link),
                    scrapeDetails: scrapeArticleDetails
                });
                
                return {
                    saved: articlesSaved || 0,
                    failed: 0,
                    total: 0
                };
            } catch (error) {
                console.log(`[${SOURCE_NAME}] Sitemap also failed: ${error.message}`);
                return {
                    saved: 0,
                    failed: 0,
                    total: 0
                };
            }
        }
    }
}
