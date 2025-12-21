import axios from 'axios';

/**
 * Validate that a URL is properly formatted
 */
export function isValidUrl(url) {
    if (!url || typeof url !== 'string') {
        return false;
    }
    
    try {
        const urlObj = new URL(url);
        return urlObj.protocol === 'http:' || urlObj.protocol === 'https:';
    } catch (e) {
        return false;
    }
}

/**
 * Check if a URL is accessible (HEAD request)
 */
export async function isUrlAccessible(url, timeout = 5000) {
    if (!isValidUrl(url)) {
        return false;
    }
    
    try {
        const response = await axios.head(url, {
            timeout,
            maxRedirects: 5,
            validateStatus: (status) => status < 500 // Accept 2xx, 3xx, 4xx as "accessible"
        });
        return true;
    } catch (error) {
        // URL might be accessible but HEAD not allowed, try GET
        try {
            const response = await axios.get(url, {
                timeout,
                maxRedirects: 5,
                validateStatus: (status) => status < 500,
                maxContentLength: 1 // Just check if accessible, don't download
            });
            return true;
        } catch (e) {
            return false;
        }
    }
}

/**
 * Validate and clean a URL
 */
export function cleanUrl(url) {
    if (!url) return '';
    
    // Remove whitespace
    url = url.trim();
    
    // Ensure it's a valid URL
    if (!isValidUrl(url)) {
        // Try to fix common issues
        if (url.startsWith('//')) {
            url = 'https:' + url;
        } else if (!url.startsWith('http://') && !url.startsWith('https://')) {
            url = 'https://' + url;
        }
    }
    
    return url;
}

/**
 * Batch validate URLs (with rate limiting)
 */
export async function validateUrls(urls, options = {}) {
    const {
        checkAccessibility = false,
        maxConcurrent = 3,
        delay = 100
    } = options;
    
    const results = {
        valid: [],
        invalid: [],
        inaccessible: []
    };
    
    for (let i = 0; i < urls.length; i += maxConcurrent) {
        const batch = urls.slice(i, i + maxConcurrent);
        
        await Promise.all(batch.map(async (url) => {
            const cleaned = cleanUrl(url);
            
            if (!isValidUrl(cleaned)) {
                results.invalid.push({ original: url, cleaned });
                return;
            }
            
            if (checkAccessibility) {
                const accessible = await isUrlAccessible(cleaned);
                if (!accessible) {
                    results.inaccessible.push(cleaned);
                    return;
                }
            }
            
            results.valid.push(cleaned);
        }));
        
        if (i + maxConcurrent < urls.length) {
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
    
    return results;
}




