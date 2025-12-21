import ArticleMetadata from '../models/ArticleMetadata.js';
import { fetchRSSFeed } from './rssParser.js';
import { batchScoreArticles } from './relevanceScorer.js';
import { sleep } from './common.js';

/**
 * Collects RSS metadata, scores it, and categorizes by priority
 * @param {Object} sourceConfig - Source configuration
 * @returns {Promise<Object>} Categorized articles { high: [], medium: [], low: [] }
 */
export async function collectRSSMetadata(sourceConfig) {
    const {
        sourceName,
        rssUrl,
        rssUrls, // Support multiple RSS feeds
        scoringConfig
    } = sourceConfig;

    console.log(`\n[${sourceName}] Starting RSS collection...`);

    // Fetch RSS feed(s)
    let metadataArray = [];
    if (rssUrls && Array.isArray(rssUrls)) {
        // Multiple RSS feeds
        for (const url of rssUrls) {
            const feed = await fetchRSSFeed(url);
            metadataArray.push(...feed);
            await sleep(500); // Small delay between feeds
        }
    } else if (rssUrl) {
        // Single RSS feed
        metadataArray = await fetchRSSFeed(rssUrl);
    } else {
        throw new Error('Either rssUrl or rssUrls must be provided');
    }

    if (metadataArray.length === 0) {
        console.log(`[${sourceName}] No articles found in RSS feed(s)`);
        return { high: [], medium: [], low: [] };
    }

    console.log(`[${sourceName}] Found ${metadataArray.length} articles in RSS feed(s)`);

    // Check for duplicates in ArticleMetadata collection
    const links = metadataArray.map(m => m.link).filter(Boolean);
    const existingMetadata = await ArticleMetadata.find({ 
        link: { $in: links },
        source: sourceName
    });

    const existingLinks = new Set(existingMetadata.map(m => m.link));
    const newMetadata = metadataArray.filter(m => !existingLinks.has(m.link));

    console.log(`[${sourceName}] ${newMetadata.length} new articles (${metadataArray.length - newMetadata.length} already exist)`);

    if (newMetadata.length === 0) {
        return { high: [], medium: [], low: [] };
    }

    // Score all new articles
    const scoredMetadata = batchScoreArticles(newMetadata, scoringConfig);

    // Add source name to each metadata
    const metadataWithSource = scoredMetadata.map(m => ({
        ...m,
        source: sourceName
    }));

    // Categorize by priority
    const categorized = {
        high: metadataWithSource.filter(m => m.priority === 'high'),
        medium: metadataWithSource.filter(m => m.priority === 'medium'),
        low: metadataWithSource.filter(m => m.priority === 'low')
    };

    console.log(`[${sourceName}] Scoring complete:`);
    console.log(`  - High priority: ${categorized.high.length}`);
    console.log(`  - Medium priority: ${categorized.medium.length}`);
    console.log(`  - Low priority: ${categorized.low.length}`);

    // Store all metadata in database (regardless of priority)
    // This allows us to track what we've seen and potentially re-score later
    try {
        const metadataToSave = metadataWithSource.map(m => ({
            title: m.title,
            description: m.description,
            link: m.link,
            source: m.source,
            publishedDate: m.publishedDate,
            relevanceScore: m.relevanceScore,
            priority: m.priority,
            status: 'pending',
            keywords: m.keywords || [],
            categories: m.categories || []
        }));

        await ArticleMetadata.insertMany(metadataToSave, { ordered: false });
        console.log(`[${sourceName}] Saved ${metadataToSave.length} metadata records`);
    } catch (error) {
        // Handle duplicate key errors gracefully (some might have been inserted)
        if (error.code === 11000) {
            console.log(`[${sourceName}] Some metadata already exists, continuing...`);
        } else {
            console.error(`[${sourceName}] Error saving metadata:`, error.message);
        }
    }

    return categorized;
}




