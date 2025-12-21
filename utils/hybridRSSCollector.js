/**
 * Hybrid RSS + Full Article Scraping Collector
 * 
 * Strategy:
 * 1. Fetch RSS feed (fast, reliable, no bot protection)
 * 2. Extract all RSS metadata
 * 3. Attempt to scrape full article content for enhanced data
 * 4. Combine RSS metadata + scraped content intelligently
 * 5. Validate all links before saving
 * 6. Fallback to RSS-only if scraping fails
 * 
 * Result: Maximum factual information with accessible links
 */

import { fetchRSSFeed } from './rssParser.js';
import { readExistingLinks, appendArticlesToCSV, getCSVFilePath } from './csvWriter.js';
import { sleep } from './common.js';
import { cleanUrl, isValidUrl, isUrlAccessible } from './linkValidator.js';

/**
 * Collect articles using hybrid approach: RSS + Full Article Scraping
 * 
 * @param {Object} sourceConfig - Configuration object
 * @param {string} sourceConfig.sourceName - Name of the source
 * @param {string} sourceConfig.rssUrl - Single RSS feed URL
 * @param {string[]} sourceConfig.rssUrls - Multiple RSS feed URLs
 * @param {Function} sourceConfig.scrapeArticleFn - Function to scrape full article content
 * @param {number} sourceConfig.maxConcurrent - Max concurrent scraping operations
 * @param {number} sourceConfig.delayBetweenScrapes - Delay between scrapes (ms)
 * @param {boolean} sourceConfig.validateLinks - Whether to validate links (default: true)
 * @param {number} sourceConfig.minContentLength - Minimum content length to save (default: 100)
 * 
 * @returns {Promise<Object>} Results object with stats
 */
export async function collectHybridRSS(sourceConfig) {
    const {
        sourceName,
        rssUrl,
        rssUrls,
        scrapeArticleFn = null,
        maxConcurrent = 2,
        delayBetweenScrapes = 3000,
        validateLinks = true,
        minContentLength = 100
    } = sourceConfig;

    console.log(`\n[${sourceName}] Starting HYBRID RSS + Full Article Collection...`);

    // Step 1: Fetch RSS feed(s)
    let metadataArray = [];
    if (rssUrls && Array.isArray(rssUrls)) {
        for (const url of rssUrls) {
            const feed = await fetchRSSFeed(url);
            metadataArray.push(...feed);
            await sleep(500);
        }
    } else if (rssUrl) {
        metadataArray = await fetchRSSFeed(rssUrl);
    } else {
        throw new Error('Either rssUrl or rssUrls must be provided');
    }

    if (metadataArray.length === 0) {
        console.log(`[${sourceName}] No articles found in RSS feed(s)`);
        return { total: 0, new: 0, skipped: 0, scraped: 0, rssOnly: 0 };
    }

    console.log(`[${sourceName}] Found ${metadataArray.length} articles in RSS feed(s)`);

    // Step 2: Check for duplicates
    const csvFilePath = getCSVFilePath(sourceName);
    const existingLinks = readExistingLinks(csvFilePath);
    
    const newMetadata = metadataArray.filter(m => !existingLinks.has(m.link));
    const skipped = metadataArray.length - newMetadata.length;

    console.log(`[${sourceName}] ${newMetadata.length} new articles (${skipped} already exist)`);

    if (newMetadata.length === 0) {
        return { 
            total: metadataArray.length, 
            new: 0, 
            skipped,
            scraped: 0,
            rssOnly: 0
        };
    }

    // Step 3: Process articles (validate links + scrape if function provided)
    const articles = [];
    let scrapedCount = 0;
    let rssOnlyCount = 0;
    let failedCount = 0;

    console.log(`[${sourceName}] Processing ${newMetadata.length} articles...`);

    for (let i = 0; i < newMetadata.length; i += maxConcurrent) {
        const batch = newMetadata.slice(i, i + maxConcurrent);
        
        await Promise.all(batch.map(async (metadata) => {
            try {
                // Clean and validate link
                const cleanedLink = cleanUrl(metadata.link);
                
                if (!isValidUrl(cleanedLink)) {
                    console.warn(`[WARNING] Invalid link skipped: ${metadata.link}`);
                    failedCount++;
                    return;
                }

                // Validate link accessibility (optional but recommended)
                if (validateLinks) {
                    try {
                        const isAccessible = await isUrlAccessible(cleanedLink, 5000);
                        if (!isAccessible) {
                            console.warn(`[WARNING] Link not accessible: ${cleanedLink}`);
                            // Still save with RSS data, but mark as potentially inaccessible
                        }
                    } catch (err) {
                        console.warn(`[WARNING] Link validation failed: ${cleanedLink} - ${err.message}`);
                        // Continue anyway - might be temporary issue
                    }
                }

                // Build base article from RSS metadata
                let article = {
                    author: metadata.author || 'Unknown',
                    date: metadata.publishedDate || null,
                    link: cleanedLink,
                    source: sourceName,
                    scrapedAt: new Date()
                };

                // Build extract field with RSS data
                let extractParts = [];
                
                // Title section
                if (metadata.title) {
                    extractParts.push(`TITLE: ${metadata.title}`);
                }
                
                // Author and date
                if (metadata.author && metadata.author !== 'Unknown') {
                    extractParts.push(`AUTHOR: ${metadata.author}`);
                }
                if (metadata.publishedDate) {
                    extractParts.push(`PUBLISHED: ${metadata.publishedDate.toISOString()}`);
                }
                
                // Categories
                if (metadata.categories && metadata.categories.length > 0) {
                    extractParts.push(`CATEGORIES: ${metadata.categories.join(', ')}`);
                }
                
                extractParts.push(''); // Empty line
                extractParts.push('--- RSS SUMMARY ---');
                
                // RSS description/content
                if (metadata.description) {
                    extractParts.push(metadata.description);
                } else if (metadata.title) {
                    extractParts.push(metadata.title);
                }

                // Step 4: Attempt to scrape full article content
                let fullContent = null;
                let scrapedAuthor = null;
                let scrapedDate = null;
                
                if (scrapeArticleFn) {
                    try {
                        await sleep(delayBetweenScrapes / maxConcurrent); // Stagger requests
                        
                        const scrapedData = await scrapeArticleFn(cleanedLink);
                        
                        if (scrapedData && scrapedData.extract && scrapedData.extract.length > minContentLength) {
                            fullContent = scrapedData.extract;
                            scrapedAuthor = scrapedData.author;
                            scrapedDate = scrapedData.date;
                            scrapedCount++;
                            
                            console.log(`[✓] Scraped full content: ${metadata.title?.substring(0, 50)}...`);
                        } else {
                            console.log(`[⚠] Scraped content too short, using RSS only: ${metadata.title?.substring(0, 50)}...`);
                            rssOnlyCount++;
                        }
                    } catch (error) {
                        console.log(`[⚠] Scraping failed, using RSS only: ${metadata.title?.substring(0, 50)}... - ${error.message}`);
                        rssOnlyCount++;
                        // Continue with RSS-only data
                    }
                } else {
                    rssOnlyCount++;
                }

                // Combine RSS + Scraped content
                if (fullContent) {
                    extractParts.push('');
                    extractParts.push('--- FULL ARTICLE CONTENT ---');
                    extractParts.push(fullContent);
                    
                    // Update author/date if scraped version is better
                    if (scrapedAuthor && scrapedAuthor !== 'Unknown') {
                        article.author = scrapedAuthor;
                    }
                    if (scrapedDate) {
                        article.date = scrapedDate;
                    }
                } else {
                    extractParts.push('');
                    extractParts.push('--- FULL ARTICLE CONTENT ---');
                    extractParts.push('Full content not available via scraping. See link for complete article.');
                }

                // Add metadata section
                extractParts.push('');
                extractParts.push('--- METADATA ---');
                if (metadata.guid) {
                    extractParts.push(`GUID: ${metadata.guid}`);
                }
                if (metadata.feedTitle) {
                    extractParts.push(`FEED: ${metadata.feedTitle}`);
                }
                if (metadata.enclosures) {
                    extractParts.push(`ENCLOSURES: ${JSON.stringify(metadata.enclosures)}`);
                }
                if (metadata.mediaContent) {
                    extractParts.push(`MEDIA: ${JSON.stringify(metadata.mediaContent)}`);
                }
                extractParts.push(`LINK: ${cleanedLink}`);
                extractParts.push(`SCRAPED AT: ${new Date().toISOString()}`);

                // Set final extract
                article.extract = extractParts.join('\n').trim();

                // Only save if we have minimum content
                if (article.extract && article.extract.length >= minContentLength) {
                    articles.push(article);
                } else {
                    failedCount++;
                }

            } catch (error) {
                console.error(`[✗] Error processing article ${metadata.link}:`, error.message);
                failedCount++;
            }
        }));

        // Delay between batches
        if (i + maxConcurrent < newMetadata.length) {
            await sleep(delayBetweenScrapes);
        }
    }

    // Step 5: Save to CSV
    if (articles.length > 0) {
        appendArticlesToCSV(articles, sourceName);
        console.log(`[${sourceName}] Saved ${articles.length} articles to CSV`);
    }

    console.log(`[${sourceName}] Complete:`);
    console.log(`  - Total in feed: ${metadataArray.length}`);
    console.log(`  - New articles: ${newMetadata.length}`);
    console.log(`  - Saved: ${articles.length}`);
    console.log(`  - With full content: ${scrapedCount}`);
    console.log(`  - RSS-only: ${rssOnlyCount}`);
    console.log(`  - Failed: ${failedCount}`);
    console.log(`  - Skipped (duplicates): ${skipped}`);

    return {
        total: metadataArray.length,
        new: newMetadata.length,
        saved: articles.length,
        scraped: scrapedCount,
        rssOnly: rssOnlyCount,
        failed: failedCount,
        skipped
    };
}




