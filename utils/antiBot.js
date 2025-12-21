/**
 * Anti-bot protection utilities
 */

// Pool of realistic User-Agent strings (expanded)
const USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
];

/**
 * Get a random User-Agent from the pool
 */
export function getRandomUserAgent() {
    return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

/**
 * Get realistic headers for a request
 */
export function getRealisticHeaders(referer = null) {
    const headers = {
        'User-Agent': getRandomUserAgent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': referer ? 'same-origin' : 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'DNT': '1'
    };

    if (referer) {
        headers['Referer'] = referer;
    }

    return headers;
}

/**
 * Add random delay to mimic human behavior
 */
export function randomDelay(min = 1000, max = 3000) {
    const delay = Math.floor(Math.random() * (max - min + 1)) + min;
    return new Promise(resolve => setTimeout(resolve, delay));
}

/**
 * Retry with exponential backoff
 */
export async function retryWithBackoff(fn, maxRetries = 3, baseDelay = 1000) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return await fn();
        } catch (error) {
            if (attempt === maxRetries - 1) {
                throw error;
            }
            
            // Exponential backoff: 1s, 2s, 4s
            const delay = baseDelay * Math.pow(2, attempt);
            console.log(`Retry attempt ${attempt + 1}/${maxRetries} after ${delay}ms delay...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

/**
 * Simulate mouse movement for Puppeteer (makes browsing more human-like)
 */
export async function simulateMouseMovement(page) {
    try {
        await page.mouse.move(
            Math.random() * 800 + 100,
            Math.random() * 600 + 100,
            { steps: Math.floor(Math.random() * 10) + 5 }
        );
    } catch (e) {
        // Ignore mouse movement errors
    }
}

/**
 * Get enhanced headers with rotation
 */
let headerRotationIndex = 0;
export function getEnhancedHeaders(referer = null) {
    headerRotationIndex = (headerRotationIndex + 1) % USER_AGENTS.length;
    
    const headers = {
        'User-Agent': USER_AGENTS[headerRotationIndex],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': referer ? 'same-origin' : 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'DNT': '1',
        'Pragma': 'no-cache'
    };

    if (referer) {
        headers['Referer'] = referer;
    }

    return headers;
}

/**
 * Detect error type from error message or response
 */
export function detectErrorType(error) {
    const errorMsg = error.message || String(error);
    const status = error.response?.status || error.status;
    
    if (status === 403 || errorMsg.includes('403') || errorMsg.includes('Forbidden')) {
        return 'FORBIDDEN';
    }
    if (status === 429 || errorMsg.includes('429') || errorMsg.includes('rate limit')) {
        return 'RATE_LIMIT';
    }
    if (errorMsg.includes('Cloudflare') || errorMsg.includes('challenge') || errorMsg.includes('Just a moment')) {
        return 'CLOUDFLARE';
    }
    if (errorMsg.includes('timeout') || errorMsg.includes('Timed out')) {
        return 'TIMEOUT';
    }
    if (status === 404 || errorMsg.includes('404') || errorMsg.includes('not found')) {
        return 'NOT_FOUND';
    }
    
    return 'UNKNOWN';
}

/**
 * Get delay based on error type (adaptive rate limiting)
 */
export function getDelayForError(errorType, baseDelay = 3000) {
    switch (errorType) {
        case 'RATE_LIMIT':
            return baseDelay * 5; // 15 seconds for rate limit
        case 'CLOUDFLARE':
            return baseDelay * 3; // 9 seconds for Cloudflare
        case 'FORBIDDEN':
            return baseDelay * 4; // 12 seconds for forbidden
        case 'TIMEOUT':
            return baseDelay * 2; // 6 seconds for timeout
        default:
            return baseDelay;
    }
}




