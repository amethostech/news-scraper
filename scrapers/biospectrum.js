/**
 * BioSpectrum Scraper
 * 
 * Source: https://www.biospectrumindia.com/ or https://www.biospectrumasia.com/
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

const SOURCE_NAME = 'Biospectrum';
const BASE_URL = 'https://www.biospectrumindia.com';

/**
 * Discover article URLs from sitemap
 */
export async function discoverUrlsFromSitemap() {
    console.log(`[${SOURCE_NAME}] Discovering URLs from sitemap...`);
    
    try {
        const sitemapUrls = [
            `${BASE_URL}/sitemap.xml`,
            `${BASE_URL}/sitemap_index.xml`,
            'https://www.biospectrumasia.com/sitemap.xml'
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
                .filter(a => a.url && (a.url.includes('biospectrumindia.com') || a.url.includes('biospectrumasia.com')))
                .map(a => a.url);
        }

        const allArticles = [];
        console.log(`[${SOURCE_NAME}] Processing ${sitemaps.length} sitemaps...`);
        
        for (let i = 0; i < sitemaps.length; i++) {
            const sitemapUrl = sitemaps[i];
            try {
                const articles = await fetchArticleLinksFromSitemap(sitemapUrl);
                
                const filtered = articles.filter(a => {
                    if (!a.url || (!a.url.includes('biospectrumindia.com') && !a.url.includes('biospectrumasia.com'))) {
                        return false;
                    }
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
        `${BASE_URL}/rss.xml`,
        'https://www.biospectrumasia.com/rss',
        'https://www.biospectrumasia.com/feed'
    ];
    
    const allUrls = new Set();
    
    for (const rssUrl of rssUrls) {
        try {
            await sleep(1000);
            const metadata = await fetchRSSFeed(rssUrl);
            
            metadata.forEach(item => {
                if (item.link && (item.link.includes('biospectrumindia.com') || item.link.includes('biospectrumasia.com'))) {
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

        // Extract title
        let title = '';
        const titleSelectors = [
            'meta[property="og:title"]',
            'meta[name="title"]',
            'h1.article-title',
            'h1',
            '.entry-title',
            '.post-title'
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
            'meta[name="author"]',
            '.article-author',
            '.byline',
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

        // Extract date
        let date = null;
        const dateSelectors = [
            'meta[property="article:published_time"]',
            'time[datetime]',
            '.article-date',
            '.published-date'
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

        // Extract content - Biospectrum specific
        let content = '';
        const contentSelectors = [
            '.article-content',
            '.entry-content',
            '.article-body',
            'article .content',
            '.story-content',
            '.news-content',
            '#article-content',
            '.post-content'
        ];
        
        for (const selector of contentSelectors) {
            const el = $(selector).first();
            if (el.length) {
                el.find('script, style, nav, footer, .advertisement, .ad, .related-articles, .social-share, .author-box, .tags, .share-buttons').remove();
                
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

        // Fallback - try all paragraphs in main content areas
        if (!content || content.length < 200) {
            // Try main article container
            const mainContent = $('main, .main-content, #main, .content-wrapper, article');
            if (mainContent.length) {
                mainContent.find('script, style, nav, footer, header, .ad, .advertisement, .sidebar').remove();
                const paragraphs = mainContent.find('p')
                    .map((i, el) => $(el).text().trim())
                    .get()
                    .filter(p => p.length > 40 && !/^(share|follow|subscribe|read more|click here)/i.test(p));
                
                if (paragraphs.length > 0) {
                    content = paragraphs.join('\n\n').trim();
                }
            }
        }
        
        // Last resort - get all paragraphs from body
        if (!content || content.length < 200) {
            $('script, style, nav, footer, header, .ad, .advertisement, .sidebar, .menu').remove();
            const paragraphs = $('body p')
                .map((i, el) => {
                    const text = $(el).text().trim();
                    // Filter out navigation, buttons, etc.
                    if (text.length > 40 && 
                        !/^(menu|navigation|share|follow|subscribe|read more|click here|home|about|contact)/i.test(text) &&
                        !$(el).closest('nav, footer, header, .menu, .sidebar').length) {
                        return text;
                    }
                    return null;
                })
                .get()
                .filter(p => p !== null && p.length > 40);
            
            if (paragraphs.length > 0) {
                content = paragraphs.join('\n\n').trim();
            }
        }

        // If no title from selectors, try to get from URL or meta
        if (!title) {
            title = $('title').first().text().trim() || '';
        }
        
        if (!title || !content || content.length < 100) {
            throw new Error(`Insufficient content extracted - Title: ${title ? 'Yes' : 'No'}, Content length: ${content.length}`);
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
    
    // If no URLs from RSS/sitemap, try scraping listing pages
    let pageUrls = [];
    if (sitemapUrls.length === 0 && rssUrls.length === 0) {
        console.log(`[${SOURCE_NAME}] Trying to discover URLs from listing pages...`);
        try {
            const response = await axios.get(`${BASE_URL}/news`, {
                headers: getRealisticHeaders(BASE_URL),
                timeout: 15000
            });
            const $ = cheerio.load(response.data);
            $('a[href*="/news/"], a[href*="/article/"]').each((i, el) => {
                const href = $(el).attr('href');
                if (href) {
                    const fullUrl = href.startsWith('http') ? href : `${BASE_URL}${href.startsWith('/') ? '' : '/'}${href}`;
                    if (fullUrl.includes('biospectrum')) {
                        pageUrls.push(cleanUrl(fullUrl));
                    }
                }
            });
            pageUrls = [...new Set(pageUrls)];
            if (pageUrls.length > 0) {
                console.log(`[${SOURCE_NAME}] Found ${pageUrls.length} articles from listing pages`);
            }
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error scraping listing pages: ${error.message}`);
        }
    }
    
    const allUrls = Array.from(new Set([...sitemapUrls, ...rssUrls, ...pageUrls]));

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

