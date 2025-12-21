/**
 * Puppeteer-based scraper for sites with advanced bot protection
 * Use this as a fallback when regular axios requests fail
 */

let browser = null;

/**
 * Initialize browser (reuse for multiple requests)
 */
async function initBrowser() {
    if (browser) return browser;
    
    try {
        const puppeteer = await import('puppeteer');
        browser = await puppeteer.launch({
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',
                '--window-size=1920x1080'
            ]
        });
        return browser;
    } catch (error) {
        console.error('Error initializing Puppeteer:', error);
        throw error;
    }
}

/**
 * Scrape using Puppeteer (for sites with Cloudflare/bot protection)
 */
export async function scrapeWithPuppeteer(url, selectors = {}) {
    try {
        const browserInstance = await initBrowser();
        const page = await browserInstance.newPage();
        
        // Set realistic viewport and user agent
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        // Navigate to page
        await page.goto(url, {
            waitUntil: 'networkidle2',
            timeout: 30000
        });
        
        // Wait a bit for any dynamic content
        await page.waitForTimeout(2000);
        
        // Extract content
        const content = await page.evaluate((sel) => {
            const result = {};
            
            // Extract author
            if (sel.author) {
                const authorEl = document.querySelector(sel.author);
                result.author = authorEl ? authorEl.textContent.trim() : '';
            }
            
            // Extract date
            if (sel.date) {
                const dateEl = document.querySelector(sel.date);
                result.date = dateEl ? dateEl.textContent.trim() : '';
            }
            
            // Extract body
            if (sel.body) {
                const bodyEl = document.querySelector(sel.body);
                if (bodyEl) {
                    // Remove unwanted elements
                    bodyEl.querySelectorAll('script, style, nav, footer, .ad, .advertisement').forEach(el => el.remove());
                    result.extract = bodyEl.textContent.trim();
                }
            }
            
            return result;
        }, selectors);
        
        await page.close();
        
        return content;
    } catch (error) {
        console.error(`Puppeteer scraping error for ${url}:`, error.message);
        throw error;
    }
}

/**
 * Close browser (call when done)
 */
export async function closeBrowser() {
    if (browser) {
        await browser.close();
        browser = null;
    }
}




