/**
 * Rate Limiter Utility
 * Manages delays and rate limiting for web scraping
 * Respects robots.txt and provides per-source configuration
 */

import { sleep } from './common.js';
import axios from 'axios';

/**
 * Rate limiter class
 */
export class RateLimiter {
    constructor(config = {}) {
        this.config = {
            delayBetweenRequests: config.delayBetweenRequests || 5000, // 5 seconds default
            delayJitter: config.delayJitter || 2000, // ±2 seconds default
            maxConcurrent: config.maxConcurrent || 1,
            batchSize: config.batchSize || 100,
            pauseBetweenBatches: config.pauseBetweenBatches || 60000, // 1 minute default
            dailyLimit: config.dailyLimit || null, // No limit by default
            respectRobotsTxt: config.respectRobotsTxt !== false, // Default true
            crawlDelay: config.crawlDelay || null, // From robots.txt
            ...config
        };
        
        this.requestCount = 0;
        this.dailyCount = 0;
        this.lastRequestTime = 0;
        this.lastDailyReset = new Date().toDateString();
    }

    /**
     * Get delay with jitter
     */
    getDelay() {
        const base = this.config.delayBetweenRequests;
        const jitter = this.config.delayJitter;
        const jitterValue = (Math.random() * 2 - 1) * jitter; // -jitter to +jitter
        return Math.max(0, base + jitterValue);
    }

    /**
     * Wait before next request
     */
    async wait() {
        // Reset daily count if new day
        const today = new Date().toDateString();
        if (today !== this.lastDailyReset) {
            this.dailyCount = 0;
            this.lastDailyReset = today;
        }

        // Check daily limit
        if (this.config.dailyLimit && this.dailyCount >= this.config.dailyLimit) {
            const tomorrow = new Date();
            tomorrow.setDate(tomorrow.getDate() + 1);
            tomorrow.setHours(0, 0, 0, 0);
            const waitTime = tomorrow.getTime() - Date.now();
            console.log(`[RATE LIMITER] Daily limit reached. Waiting until tomorrow...`);
            await sleep(waitTime);
            this.dailyCount = 0;
        }

        // Calculate delay
        let delay = this.getDelay();
        
        // Use robots.txt crawl-delay if available and higher
        if (this.config.crawlDelay && this.config.crawlDelay > delay) {
            delay = this.config.crawlDelay;
        }

        // Check if we need to pause between batches
        if (this.config.batchSize && this.requestCount > 0 && this.requestCount % this.config.batchSize === 0) {
            console.log(`[RATE LIMITER] Processed ${this.requestCount} requests. Pausing for ${this.config.pauseBetweenBatches / 1000} seconds...`);
            await sleep(this.config.pauseBetweenBatches);
        }

        // Wait for delay
        await sleep(delay);
        
        this.requestCount++;
        this.dailyCount++;
        this.lastRequestTime = Date.now();
    }

    /**
     * Reset counters
     */
    reset() {
        this.requestCount = 0;
        this.dailyCount = 0;
    }

    /**
     * Get statistics
     */
    getStats() {
        return {
            requestCount: this.requestCount,
            dailyCount: this.dailyCount,
            lastRequestTime: this.lastRequestTime,
            config: this.config
        };
    }
}

/**
 * Fetch and parse robots.txt
 */
export async function fetchRobotsTxt(baseUrl) {
    try {
        const robotsUrl = new URL('/robots.txt', baseUrl).href;
        const response = await axios.get(robotsUrl, {
            timeout: 5000,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        });

        const robotsTxt = response.data;
        const crawlDelay = robotsTxt.match(/Crawl-delay:\s*(\d+)/i);
        const userAgent = robotsTxt.match(/User-agent:\s*(.+)/i);
        
        return {
            crawlDelay: crawlDelay ? parseInt(crawlDelay[1]) * 1000 : null, // Convert to milliseconds
            disallowed: [],
            allowed: [],
            sitemap: robotsTxt.match(/Sitemap:\s*(.+)/gi)?.map(m => m.replace(/Sitemap:\s*/i, '').trim()) || []
        };
    } catch (error) {
        console.warn(`[RATE LIMITER] Could not fetch robots.txt: ${error.message}`);
        return {
            crawlDelay: null,
            disallowed: [],
            allowed: [],
            sitemap: []
        };
    }
}

/**
 * Create rate limiter with robots.txt respect
 */
export async function createRateLimiter(baseUrl, config = {}) {
    let robotsData = null;
    
    if (config.respectRobotsTxt !== false) {
        robotsData = await fetchRobotsTxt(baseUrl);
        
        // Use robots.txt crawl-delay if available
        if (robotsData.crawlDelay && (!config.delayBetweenRequests || robotsData.crawlDelay > config.delayBetweenRequests)) {
            config.delayBetweenRequests = robotsData.crawlDelay;
            console.log(`[RATE LIMITER] Using crawl-delay from robots.txt: ${robotsData.crawlDelay / 1000} seconds`);
        }
        
        config.crawlDelay = robotsData.crawlDelay;
    }

    return {
        limiter: new RateLimiter(config),
        robotsData
    };
}




