import axios from 'axios';
import * as cheerio from 'cheerio';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { getRealisticHeaders, randomDelay } from '../utils/antiBot.js';

const SOURCE_NAME = 'FiercePharma';
const BASE_URL = 'https://www.fiercepharma.com';

// Reusable browser instance
let browserInstance = null;

/**
 * Get or create browser instance (reuse for multiple requests)
 */
async function getBrowser() {
    if (browserInstance && browserInstance.isConnected()) {
        return browserInstance;
    }
    
    try {
        const { addExtra } = await import('puppeteer-extra');
        const StealthPlugin = (await import('puppeteer-extra-plugin-stealth')).default;
        const puppeteerModule = await import('puppeteer');
        const puppeteer = puppeteerModule.default || puppeteerModule;
        
        const puppeteerExtra = addExtra(puppeteer);
        puppeteerExtra.use(StealthPlugin());
        
        browserInstance = await puppeteerExtra.launch({
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled'
            ]
        });
        return browserInstance;
    } catch (error) {
        const puppeteer = await import('puppeteer');
        const puppeteerDefault = puppeteer.default || puppeteer;
        browserInstance = await puppeteerDefault.launch({
            headless: 'new',
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        });
        return browserInstance;
    }
}

/**
 * Scrape with Puppeteer as fallback
 */
async function scrapeWithPuppeteer(url) {
    let page = null;
    try {
        const browser = await getBrowser();
        page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 90000 });
        await new Promise(resolve => setTimeout(resolve, 3000)); // Wait for content
        
        const html = await page.content();
        return html;
    } catch (error) {
        throw new Error(`Puppeteer error: ${error.message}`);
    } finally {
        if (page) {
            try {
                await page.close();
            } catch (e) {
                // Ignore
            }
        }
    }
}

/**
 * Scrape article details from FiercePharma with anti-bot protection
 * Falls back to Puppeteer if axios fails
 */
export async function scrapeArticleDetails(url) {
    let html = null;
    
    // Try axios first
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
        
        html = response.data;
        
        if (html.includes('Access Denied') || html.includes('403') || html.includes('blocked') || html.includes('Cloudflare')) {
            throw new Error('Blocked (content check)');
        }
    } catch (error) {
        // Fallback to Puppeteer if blocked
        console.log(`[PUPPETEER] Using browser automation for ${url.substring(0, 60)}...`);
        try {
            html = await scrapeWithPuppeteer(url);
        } catch (puppeteerError) {
            console.error(`Error scraping article ${url}:`, puppeteerError.message);
            throw puppeteerError;
        }
    }
    
    // Parse HTML with Cheerio
    const $ = cheerio.load(html);
    
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
        'meta[name="date"]'
    ];
    
    for (const selector of dateSelectors) {
        if (selector.startsWith('meta')) {
            const metaContent = $(selector).attr('content');
            if (metaContent) {
                articleDate = new Date(metaContent);
                if (!isNaN(articleDate.getTime())) break;
            }
        } else if (selector.includes('[datetime]')) {
            const datetime = $(selector).first().attr('datetime');
            if (datetime) {
                articleDate = new Date(datetime);
                if (!isNaN(articleDate.getTime())) break;
            }
        } else {
            const dateText = $(selector).first().text().trim();
            if (dateText) {
                articleDate = new Date(dateText);
                if (!isNaN(articleDate.getTime())) break;
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
        '.post-content'
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
    
    // Final fallback: use meta description if body is too short
    if (!bodyText || bodyText.length < 50) {
        const metaDesc = $('meta[name="description"]').attr('content') || 
                        $('meta[property="og:description"]').attr('content') ||
                        '';
        if (metaDesc && metaDesc.length > 50) {
            bodyText = metaDesc;
        }
    }
    
    // Ensure we have minimum content
    if (!bodyText || bodyText.length < 50) {
        throw new Error(`Insufficient content extracted (${bodyText?.length || 0} chars)`);
    }
    
    return {
        title: title || 'Untitled',
        author: author || 'Unknown',
        date: articleDate || null,
        extract: bodyText.trim(),
        link: url,
    };
}

export async function run(options = {}) {
    console.log('\n=== FIERCEPHARMA SCRAPER STARTING ===');
    
    const sourceConfig = {
        sourceName: SOURCE_NAME,
        rssUrl: 'https://www.fiercepharma.com/rss/xml',
        maxConcurrent: 1,
        delayBetweenScrapes: 5000
    };

    try {
        const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);
        
        console.log(`\n=== FIERCEPHARMA SCRAPER COMPLETE ===`);
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
        console.error('Error in FiercePharma scraper:', error);
        // Ensure browser is closed on error
        if (browserInstance) {
            try {
                await browserInstance.close();
            } catch (e) {
                // Ignore
            }
            browserInstance = null;
        }
        return { saved: 0, failed: 0, total: 0 };
    }
}

