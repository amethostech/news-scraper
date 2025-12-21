import { fetchRSSFeed } from './rssParser.js';
import { readExistingLinks, appendArticlesToCSV, getCSVFilePath } from './csvWriter.js';
import { sleep } from './common.js';

/**
 * Simple RSS collector - fetches RSS, scrapes articles, saves to CSV
 * No scoring, no filtering - just collect all articles
 */
export async function collectAndScrapeRSS(sourceConfig, scrapeDetailsFn) {
    const {
        sourceName,
        rssUrl,
        rssUrls,
        maxConcurrent = 3,
        delayBetweenScrapes = 1000
    } = sourceConfig;

    console.log(`\n[${sourceName}] Starting RSS collection...`);

    // Fetch RSS feed(s) - handle errors gracefully
    let metadataArray = [];
    if (rssUrls && Array.isArray(rssUrls)) {
        // Multiple RSS feeds - try each one, continue on failure
        for (const url of rssUrls) {
            try {
                const feed = await fetchRSSFeed(url);
                if (feed && feed.length > 0) {
                    metadataArray.push(...feed);
                    console.log(`[${sourceName}] Successfully fetched ${feed.length} articles from ${url}`);
                }
            } catch (error) {
                console.warn(`[${sourceName}] Failed to fetch ${url}: ${error.message}`);
                // Continue to next feed
            }
            await sleep(500); // Small delay between feeds
        }
    } else if (rssUrl) {
        // Single RSS feed
        try {
            metadataArray = await fetchRSSFeed(rssUrl);
        } catch (error) {
            console.warn(`[${sourceName}] Failed to fetch RSS feed: ${error.message}`);
            metadataArray = [];
        }
    } else {
        throw new Error('Either rssUrl or rssUrls must be provided');
    }

    if (metadataArray.length === 0) {
        console.log(`[${sourceName}] No articles found in RSS feed(s)`);
        return { total: 0, new: 0, skipped: 0 };
    }

    console.log(`[${sourceName}] Found ${metadataArray.length} articles in RSS feed(s)`);

    // Check for duplicates in CSV
    const csvFilePath = getCSVFilePath(sourceName);
    const existingLinks = readExistingLinks(csvFilePath);
    
    // Filter out existing articles
    const newMetadata = metadataArray.filter(m => !existingLinks.has(m.link));
    const skipped = metadataArray.length - newMetadata.length;

    console.log(`[${sourceName}] ${newMetadata.length} new articles (${skipped} already exist)`);

    if (newMetadata.length === 0) {
        return { total: metadataArray.length, new: 0, skipped };
    }

    // Scrape articles in batches
    console.log(`[${sourceName}] Scraping ${newMetadata.length} new articles...`);
    const scrapedArticles = [];
    let failed = 0;

    for (let i = 0; i < newMetadata.length; i += maxConcurrent) {
        const batch = newMetadata.slice(i, i + maxConcurrent);
        
        await Promise.all(batch.map(async (metadata) => {
            try {
                const articleData = await scrapeDetailsFn(metadata.link);
                
                if (articleData && articleData.extract) {
                    // Use RSS description as fallback if extract is too short
                    if (articleData.extract.length < 100 && metadata.description && metadata.description.length > 50) {
                        articleData.extract = metadata.description;
                    }
                    
                    // Minimum 50 characters (reduced from 100 for better success rate)
                    if (articleData.extract.length >= 50) {
                        articleData.source = sourceName;
                        articleData.title = articleData.title || metadata.title || 'Untitled';
                        articleData.date = metadata.publishedDate || articleData.date;
                        articleData.scrapedAt = new Date();
                        scrapedArticles.push(articleData);
                        console.log(`[✓] Scraped: ${articleData.title.substring(0, 60)}...`);
                    } else {
                        console.log(`[✗] Insufficient content (${articleData.extract.length} chars) for: ${metadata.link.substring(0, 60)}...`);
                        failed++;
                    }
                } else {
                    console.log(`[✗] Invalid article data (no extract) for: ${metadata.link.substring(0, 60)}...`);
                    failed++;
                }
            } catch (error) {
                console.error(`[✗] Error scraping ${metadata.link.substring(0, 60)}...: ${error.message}`);
                failed++;
            }
        }));

        // Delay between batches
        if (i + maxConcurrent < newMetadata.length) {
            await sleep(delayBetweenScrapes);
        }
    }

    // Save to CSV
    if (scrapedArticles.length > 0) {
        await appendArticlesToCSV(scrapedArticles, sourceName);
        console.log(`[${sourceName}] Saved ${scrapedArticles.length} articles to CSV`);
    }

    console.log(`[${sourceName}] Complete: ${scrapedArticles.length} saved, ${failed} failed, ${skipped} skipped`);

    return {
        total: metadataArray.length,
        new: newMetadata.length,
        saved: scrapedArticles.length,
        failed,
        skipped
    };
}

