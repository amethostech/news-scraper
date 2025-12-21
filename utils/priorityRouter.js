import Article from '../models/Article.js';
import ArticleMetadata from '../models/ArticleMetadata.js';
import { sleep } from './common.js';

/**
 * Routes articles by priority and handles scraping
 * @param {Object} categorized - Categorized articles { high: [], medium: [], low: [] }
 * @param {Function} scrapeDetails - Function to scrape article details
 * @param {Object} options - Options for routing
 * @returns {Promise<Object>} Results { high: { scraped: number }, medium: { queued: number }, low: { ignored: number } }
 */
export async function routeByPriority(categorized, scrapeDetails, options = {}) {
    const {
        scrapeHighPriority = true,
        queueMediumPriority = true,
        ignoreLowPriority = true,
        maxConcurrent = 3,
        delayBetweenScrapes = 1000
    } = options;

    const results = {
        high: { scraped: 0, failed: 0 },
        medium: { queued: 0 },
        low: { ignored: 0 }
    };

    // HIGH PRIORITY → Scrape immediately
    if (scrapeHighPriority && categorized.high.length > 0) {
        console.log(`\n[ROUTING] Processing ${categorized.high.length} high priority articles...`);
        
        // Process in batches with concurrency limit
        for (let i = 0; i < categorized.high.length; i += maxConcurrent) {
            const batch = categorized.high.slice(i, i + maxConcurrent);
            
            await Promise.all(batch.map(async (metadata) => {
                try {
                    // Check if article already exists
                    const existingArticle = await Article.findOne({ link: metadata.link });
                    if (existingArticle) {
                        console.log(`[SKIP] Article already exists: ${metadata.link}`);
                        await ArticleMetadata.updateOne(
                            { link: metadata.link },
                            { status: 'scraped' }
                        );
                        return;
                    }

                    // Scrape article details
                    const articleData = await scrapeDetails(metadata.link);
                    
                    if (articleData && articleData.extract && articleData.extract.length > 100) {
                        articleData.source = metadata.source;
                        articleData.date = metadata.publishedDate || articleData.date;
                        
                        // Save full article
                        await Article.create(articleData);
                        
                        // Update metadata status
                        await ArticleMetadata.updateOne(
                            { link: metadata.link },
                            { 
                                status: 'scraped',
                                scrapedAt: new Date()
                            }
                        );
                        
                        results.high.scraped++;
                        console.log(`[SCRAPED] ${metadata.title.substring(0, 60)}...`);
                    } else {
                        console.log(`[SKIP] Invalid article data for: ${metadata.link}`);
                        results.high.failed++;
                    }
                } catch (error) {
                    console.error(`[ERROR] Failed to scrape ${metadata.link}:`, error.message);
                    results.high.failed++;
                }
            }));

            // Delay between batches
            if (i + maxConcurrent < categorized.high.length) {
                await sleep(delayBetweenScrapes);
            }
        }
    }

    // MEDIUM PRIORITY → Mark as queued (can be processed later)
    if (queueMediumPriority && categorized.medium.length > 0) {
        console.log(`\n[ROUTING] Queueing ${categorized.medium.length} medium priority articles...`);
        
        await ArticleMetadata.updateMany(
            { 
                link: { $in: categorized.medium.map(m => m.link) },
                status: 'pending'
            },
            { status: 'queued' }
        );
        
        results.medium.queued = categorized.medium.length;
    }

    // LOW PRIORITY → Mark as ignored
    if (ignoreLowPriority && categorized.low.length > 0) {
        console.log(`\n[ROUTING] Ignoring ${categorized.low.length} low priority articles...`);
        
        await ArticleMetadata.updateMany(
            { 
                link: { $in: categorized.low.map(m => m.link) },
                status: 'pending'
            },
            { status: 'ignored' }
        );
        
        results.low.ignored = categorized.low.length;
    }

    return results;
}

/**
 * Process queued medium priority articles
 * @param {Function} scrapeDetails - Function to scrape article details
 * @param {Object} options - Options for processing
 * @returns {Promise<Object>} Results
 */
export async function processQueue(scrapeDetails, options = {}) {
    const {
        limit = 50,
        maxConcurrent = 3,
        delayBetweenScrapes = 1000
    } = options;

    console.log(`\n[QUEUE] Processing queued articles (limit: ${limit})...`);

    // Get queued articles, sorted by relevance score (highest first)
    const queuedArticles = await ArticleMetadata.find({
        status: 'queued'
    })
    .sort({ relevanceScore: -1 })
    .limit(limit);

    if (queuedArticles.length === 0) {
        console.log(`[QUEUE] No queued articles to process`);
        return { processed: 0, failed: 0 };
    }

    console.log(`[QUEUE] Found ${queuedArticles.length} queued articles`);

    let processed = 0;
    let failed = 0;

    // Process in batches
    for (let i = 0; i < queuedArticles.length; i += maxConcurrent) {
        const batch = queuedArticles.slice(i, i + maxConcurrent);
        
        await Promise.all(batch.map(async (metadata) => {
            try {
                // Check if article already exists
                const existingArticle = await Article.findOne({ link: metadata.link });
                if (existingArticle) {
                    await ArticleMetadata.updateOne(
                        { _id: metadata._id },
                        { status: 'scraped' }
                    );
                    return;
                }

                // Scrape article details
                const articleData = await scrapeDetails(metadata.link);
                
                if (articleData && articleData.extract && articleData.extract.length > 100) {
                    articleData.source = metadata.source;
                    articleData.date = metadata.publishedDate || articleData.date;
                    
                    await Article.create(articleData);
                    await ArticleMetadata.updateOne(
                        { _id: metadata._id },
                        { 
                            status: 'scraped',
                            scrapedAt: new Date()
                        }
                    );
                    
                    processed++;
                } else {
                    failed++;
                }
            } catch (error) {
                console.error(`[QUEUE ERROR] ${metadata.link}:`, error.message);
                failed++;
            }
        }));

        if (i + maxConcurrent < queuedArticles.length) {
            await sleep(delayBetweenScrapes);
        }
    }

    console.log(`[QUEUE] Processed ${processed} articles, ${failed} failed`);
    return { processed, failed };
}




