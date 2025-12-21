/**
 * Fetch sitemap using Puppeteer (for sites with bot protection)
 */
let browserInstance = null;

async function getBrowser() {
    if (browserInstance && browserInstance.isConnected()) {
        return browserInstance;
    }
    
    const puppeteer = await import('puppeteer');
    browserInstance = await puppeteer.launch({
        headless: 'new',
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled'
        ]
    });
    
    return browserInstance;
}

export async function fetchSitemapWithPuppeteer(sitemapUrl) {
    try {
        const browser = await getBrowser();
        const page = await browser.newPage();
        
        await page.setViewport({ width: 1920, height: 1080 });
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        await page.goto(sitemapUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 30000
        });
        
        await page.waitForTimeout(2000);
        
        const html = await page.content();
        await page.close();
        
        return html;
    } catch (error) {
        console.error(`Puppeteer sitemap error for ${sitemapUrl}:`, error.message);
        throw error;
    }
}

