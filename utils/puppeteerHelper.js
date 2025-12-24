/**
 * Puppeteer utilities for advanced web scraping
 * Handles JavaScript-rendered content and anti-bot measures
 */

import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';

// Use stealth plugin to avoid detection
puppeteer.use(StealthPlugin());

/**
 * Launch a browser instance with stealth settings
 */
export async function launchBrowser(options = {}) {
    const {
        headless = true,
        slowMo = 0
    } = options;

    return await puppeteer.launch({
        headless: headless ? 'new' : false,
        slowMo,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--disable-gpu',
            '--window-size=1920x1080',
            '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
    });
}

/**
 * Scrape article URLs from a listing page using Puppeteer
 */
export async function scrapeArticleUrlsWithPuppeteer(url, selectors, options = {}) {
    const {
        waitForSelector = 'article, .article, .post',
        scrollToBottom = true,
        maxScrolls = 3
    } = options;

    const browser = await launchBrowser();
    const page = await browser.newPage();

    try {
        // Set viewport
        await page.setViewport({ width: 1920, height: 1080 });

        // Navigate to page
        console.log(`[Puppeteer] Navigating to ${url}...`);
        await page.goto(url, {
            waitUntil: 'networkidle2',
            timeout: 30000
        });

        // Wait for content to load
        try {
            await page.waitForSelector(waitForSelector, { timeout: 10000 });
        } catch (e) {
            console.warn(`[Puppeteer] Selector ${waitForSelector} not found, continuing anyway...`);
        }

        // Scroll to load more content if needed
        if (scrollToBottom) {
            for (let i = 0; i < maxScrolls; i++) {
                await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }

        // Extract URLs using provided selectors
        const urls = await page.evaluate((selectorsList) => {
            const foundUrls = new Set();

            selectorsList.forEach(selector => {
                const elements = document.querySelectorAll(selector);
                elements.forEach(el => {
                    const href = el.href || el.getAttribute('href');
                    if (href && href.startsWith('http')) {
                        foundUrls.add(href);
                    }
                });
            });

            return Array.from(foundUrls);
        }, selectors);

        console.log(`[Puppeteer] Found ${urls.length} URLs`);
        return urls;

    } catch (error) {
        console.error(`[Puppeteer] Error scraping ${url}:`, error.message);
        return [];
    } finally {
        await browser.close();
    }
}

/**
 * Scrape article content from a page using Puppeteer
 */
export async function scrapeArticleContentWithPuppeteer(url, contentSelectors, options = {}) {
    const {
        titleSelector = 'h1',
        dateSelector = 'time, .date, .published',
        authorSelector = '.author, .byline, [rel="author"]'
    } = options;

    const browser = await launchBrowser();
    const page = await browser.newPage();

    try {
        await page.setViewport({ width: 1920, height: 1080 });

        console.log(`[Puppeteer] Scraping content from ${url}...`);
        await page.goto(url, {
            waitUntil: 'networkidle0',
            timeout: 45000
        });

        // Wait a bit for JavaScript to render
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Extract content
        const article = await page.evaluate((selectors) => {
            const { titleSel, dateSel, authorSel, contentSels } = selectors;

            // Extract title
            let title = '';
            const titleEl = document.querySelector(titleSel);
            if (titleEl) title = titleEl.textContent.trim();

            // Extract date
            let date = null;
            const dateEl = document.querySelector(dateSel);
            if (dateEl) {
                const datetime = dateEl.getAttribute('datetime') || dateEl.textContent.trim();
                try {
                    // Convert to ISO string to prevent "[object Object]" serialization
                    const dateObj = new Date(datetime);
                    if (!isNaN(dateObj.getTime())) {
                        date = dateObj.toISOString().split('T')[0]; // Format as YYYY-MM-DD
                    } else {
                        date = datetime; // Keep original if parsing fails
                    }
                } catch (e) {
                    date = datetime; // Keep original if error occurs
                }
            }

            // Extract author
            let author = '';
            const authorEl = document.querySelector(authorSel);
            if (authorEl) author = authorEl.textContent.trim().replace(/^by\\s+/i, '');

            // Extract content
            let content = '';
            for (const contentSel of contentSels) {
                const contentEl = document.querySelector(contentSel);
                if (contentEl) {
                    // Remove unwanted elements
                    const unwanted = contentEl.querySelectorAll('script, style, nav, footer, .ad, .advertisement, .social-share, .stats, .tags');
                    unwanted.forEach(el => el.remove());

                    // Try to get paragraphs first
                    const paragraphs = Array.from(contentEl.querySelectorAll('p'))
                        .map(p => p.textContent.trim())
                        .filter(p => p.length > 30);

                    if (paragraphs.length > 0) {
                        content = paragraphs.join('\\n\\n');
                        break;
                    }

                    // If no paragraphs, get all text content
                    const textContent = contentEl.textContent.trim();
                    if (textContent.length > 100) {
                        content = textContent;
                        break;
                    }
                }
            }

            return { title, date, author, content };
        }, {
            titleSel: titleSelector,
            dateSel: dateSelector,
            authorSel: authorSelector,
            contentSels: contentSelectors
        });

        return article;

    } catch (error) {
        console.error(`[Puppeteer] Error scraping content from ${url}:`, error.message);
        throw error;
    } finally {
        await browser.close();
    }
}

/**
 * Simple page scraper - just get the HTML
 */
export async function getPageHtml(url) {
    const browser = await launchBrowser();
    const page = await browser.newPage();

    try {
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
        const html = await page.content();
        return html;
    } finally {
        await browser.close();
    }
}
