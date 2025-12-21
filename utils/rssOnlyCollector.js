import { fetchRSSFeed } from './rssParser.js';
import { readExistingLinks, appendArticlesToCSV, getCSVFilePath } from './csvWriter.js';
import { sleep } from './common.js';
import { cleanUrl, isValidUrl } from './linkValidator.js';

/**
 * RSS-Only collector - uses RSS feed data without scraping
 * Fast, reliable, no bot protection issues
 */
export async function collectRSSOnly(sourceConfig) {
    const {
        sourceName,
        rssUrl,
        rssUrls,
        useDescriptionAsExtract = true,
        minDescriptionLength = 100
    } = sourceConfig;

    console.log(`\n[${sourceName}] Starting RSS-only collection...`);

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

    // Convert RSS metadata to article format - use MAXIMUM data
    const articles = newMetadata.map(metadata => {
        // Clean and validate link
        const cleanedLink = cleanUrl(metadata.link);
        
        // Skip if link is invalid
        if (!isValidUrl(cleanedLink)) {
            console.warn(`[WARNING] Invalid link skipped: ${metadata.link}`);
            return null;
        }
        
        // Use full content if available, otherwise use description
        // Priority: content > description > title
        let extract = '';
        
        if (metadata.content && metadata.content.length > 100) {
            // Use full content (strip HTML tags for plain text)
            extract = metadata.content
                .replace(/<[^>]*>/g, ' ')  // Remove HTML tags
                .replace(/\s+/g, ' ')      // Normalize whitespace
                .trim();
        } else if (metadata.description && metadata.description.length > 50) {
            extract = metadata.description;
        } else if (metadata.title) {
            extract = metadata.title;
        }
        
        // If extract is still too short, combine title + description
        if (extract.length < minDescriptionLength && metadata.title) {
            const combined = `${metadata.title}\n\n${metadata.description || ''}`.trim();
            if (combined.length > extract.length) {
                extract = combined;
            }
        }
        
        // If still too short, just use what we have
        if (extract.length < 50) {
            extract = metadata.title || metadata.description || 'No content available';
        }
        
        // Build comprehensive extract with MAXIMUM data
        // Structure: Title, Content, Metadata
        let fullExtract = '';
        
        // Add title if available
        if (metadata.title) {
            fullExtract += `TITLE: ${metadata.title}\n\n`;
        }
        
        // Add main content (full content preferred)
        fullExtract += extract;
        
        // Add metadata section
        const metadataSection = [];
        
        // Add categories if available
        if (metadata.categories && metadata.categories.length > 0) {
            metadataSection.push(`CATEGORIES: ${metadata.categories.join(', ')}`);
        }
        
        // Add GUID if different from link
        if (metadata.guid && metadata.guid !== cleanedLink) {
            metadataSection.push(`GUID: ${metadata.guid}`);
        }
        
        // Add media/enclosures info if available
        if (metadata.thumbnails && metadata.thumbnails.length > 0) {
            metadataSection.push(`THUMBNAILS: ${metadata.thumbnails.join(', ')}`);
        }
        if (metadata.enclosures && metadata.enclosures.length > 0) {
            const encInfo = metadata.enclosures.map(e => `${e.url} (${e.type || 'unknown'})`).join('; ');
            metadataSection.push(`MEDIA: ${encInfo}`);
        }
        if (metadata.mediaContent && metadata.mediaContent.length > 0) {
            const mediaUrls = metadata.mediaContent.map(m => m.url).filter(Boolean).join(', ');
            if (mediaUrls) {
                metadataSection.push(`MEDIA_CONTENT: ${mediaUrls}`);
            }
        }
        
        // Add feed information
        if (metadata.feedTitle) {
            metadataSection.push(`FEED: ${metadata.feedTitle}`);
        }
        
        // Append metadata section
        if (metadataSection.length > 0) {
            fullExtract += `\n\n--- METADATA ---\n${metadataSection.join('\n')}`;
        }

        return {
            author: metadata.author || 'Unknown',
            date: metadata.publishedDate || null,
            extract: fullExtract.trim(),  // Maximum data in extract field
            link: cleanedLink,
            source: sourceName,
            scrapedAt: new Date()
        };
    }).filter(article => article !== null && article.extract && article.extract.length >= 50);

    // Save to CSV
    if (articles.length > 0) {
        appendArticlesToCSV(articles, sourceName);
        console.log(`[${sourceName}] Saved ${articles.length} articles to CSV`);
    }

    console.log(`[${sourceName}] Complete: ${articles.length} saved, ${skipped} skipped`);

    return {
        total: metadataArray.length,
        new: newMetadata.length,
        saved: articles.length,
        failed: 0,
        skipped
    };
}

