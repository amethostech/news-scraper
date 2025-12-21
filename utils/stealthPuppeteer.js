/**
 * Enhanced Puppeteer with Stealth Plugin
 * Better at bypassing bot detection than regular Puppeteer
 */

let browserInstance = null;

/**
 * Get or create browser instance with stealth mode
 */
export async function getStealthBrowser() {
    if (browserInstance && browserInstance.isConnected()) {
        return browserInstance;
    }
    
    try {
        const puppeteer = await import('puppeteer-extra');
        const StealthPlugin = (await import('puppeteer-extra-plugin-stealth')).default;
        
        // Use stealth plugin
        puppeteer.default.use(StealthPlugin());
        
        browserInstance = await puppeteer.default.launch({
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process'
            ]
        });
        
        return browserInstance;
    } catch (error) {
        // Fallback to regular Puppeteer if stealth plugin not available
        console.warn('Stealth plugin not available, using regular Puppeteer');
        const puppeteer = await import('puppeteer');
        browserInstance = await puppeteer.default.launch({
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage'
            ]
        });
        return browserInstance;
    }
}

/**
 * Scrape using stealth Puppeteer (better bot protection bypass)
 */
export async function scrapeWithStealthPuppeteer(url) {
    let page = null;
    try {
        const browser = await getStealthBrowser();
        page = await browser.newPage();
        
        // Set realistic viewport and user agent
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        // Set extra headers
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        });
        
        // Navigate with stealth mode
        await page.goto(url, {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });
        
        // Wait for content
        await page.waitForTimeout(3000);
        
        // Get HTML content
        const html = await page.content();
        await page.close();
        
        return html;
    } catch (error) {
        if (page) {
            try {
                await page.close();
            } catch (e) {
                // Ignore cleanup errors
            }
        }
        throw error;
    }
}

/**
 * Close browser
 */
export async function closeStealthBrowser() {
    if (browserInstance) {
        try {
            await browserInstance.close();
        } catch (e) {
            // Ignore errors
        }
        browserInstance = null;
    }
}




