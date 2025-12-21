import { fetchRSSFeed } from './rssParser.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from './sitemap.js';
import { readExistingLinks, appendArticlesToCSV, getCSVFilePath } from './csvWriter.js';
import { cleanUrl, isValidUrl } from './linkValidator.js';
import { sleep } from './common.js';

/**
 * Hybrid collector: RSS for recent + Sitemap for historical
 * Gets maximum data from RSS, discovers historical articles from sitemaps
 */
export async function collectHybrid(sourceConfig) {
    const {
        sourceName,
        rssUrl,
        rssUrls,
        sitemapIndexUrl,
        sitemapFilter = (loc) => true,
        linkFilter = (link) => true,
        maxHistoricalLinks = 10000,
        useRSSForRecent = true,
        useSitemapForHistorical = true
    } = sourceConfig;

    console.log(`\n[${sourceName}] Starting HYBRID collection (RSS + Sitemap)...`);
    
    const results = {
        rss: { total: 0, saved: 0 },
        sitemap: { total: 0, saved: 0 },
        total: { saved: 0, skipped: 0 }
    };

    const csvFilePath = getCSVFilePath(sourceName);
    const existingLinks = readExistingLinks(csvFilePath);

    // STEP 1: Collect from RSS feeds (recent articles with full metadata)
    if (useRSSForRecent && (rssUrl || rssUrls)) {
        console.log(`\n[${sourceName}] Step 1: Collecting from RSS feeds...`);
        
        let rssMetadata = [];
        if (rssUrls && Array.isArray(rssUrls)) {
            for (const url of rssUrls) {
                const feed = await fetchRSSFeed(url);
                rssMetadata.push(...feed);
                await sleep(500);
            }
        } else if (rssUrl) {
            rssMetadata = await fetchRSSFeed(rssUrl);
        }

        if (rssMetadata.length > 0) {
            console.log(`[${sourceName}] Found ${rssMetadata.length} articles in RSS feed(s)`);
            
            // Filter new articles
            const newRSS = rssMetadata.filter(m => !existingLinks.has(m.link));
            console.log(`[${sourceName}] ${newRSS.length} new RSS articles`);

            // Convert to article format with maximum data
            const rssArticles = newRSS.map(metadata => {
                const cleanedLink = cleanUrl(metadata.link);
                if (!isValidUrl(cleanedLink)) return null;

                // Build comprehensive extract
                let extract = '';
                if (metadata.title) {
                    extract += `TITLE: ${metadata.title}\n\n`;
                }
                
                // Use full content if available
                if (metadata.content && metadata.content.length > 100) {
                    extract += metadata.content.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
                } else if (metadata.description) {
                    extract += metadata.description;
                }
                
                // Add metadata section
                const metadataSection = [];
                if (metadata.categories && metadata.categories.length > 0) {
                    metadataSection.push(`CATEGORIES: ${metadata.categories.join(', ')}`);
                }
                if (metadata.guid && metadata.guid !== cleanedLink) {
                    metadataSection.push(`GUID: ${metadata.guid}`);
                }
                if (metadata.enclosures && metadata.enclosures.length > 0) {
                    const encInfo = metadata.enclosures.map(e => `${e.url} (${e.type || 'unknown'})`).join('; ');
                    metadataSection.push(`MEDIA: ${encInfo}`);
                }
                if (metadata.thumbnails && metadata.thumbnails.length > 0) {
                    metadataSection.push(`THUMBNAILS: ${metadata.thumbnails.join(', ')}`);
                }
                if (metadata.feedTitle) {
                    metadataSection.push(`FEED: ${metadata.feedTitle}`);
                }
                
                if (metadataSection.length > 0) {
                    extract += `\n\n--- METADATA ---\n${metadataSection.join('\n')}`;
                }

                return {
                    author: metadata.author || 'Unknown',
                    date: metadata.publishedDate || null,
                    extract: extract.trim(),
                    link: cleanedLink,
                    source: sourceName,
                    scrapedAt: new Date()
                };
            }).filter(a => a !== null && a.extract && a.extract.length >= 50);

            if (rssArticles.length > 0) {
                appendArticlesToCSV(rssArticles, sourceName);
                results.rss.saved = rssArticles.length;
                console.log(`[${sourceName}] Saved ${rssArticles.length} RSS articles`);
            }

            results.rss.total = rssMetadata.length;
            
            // Update existing links set
            rssMetadata.forEach(m => existingLinks.add(m.link));
        }
    }

    // STEP 2: Collect historical articles from sitemaps
    if (useSitemapForHistorical && sitemapIndexUrl) {
        console.log(`\n[${sourceName}] Step 2: Collecting historical articles from sitemaps...`);
        
        // Try to fetch sitemap index (will fallback to Puppeteer if blocked)
        let archiveSitemaps = [];
        try {
            archiveSitemaps = await fetchSitemapIndex(sitemapIndexUrl, sitemapFilter);
            console.log(`[${sourceName}] Found ${archiveSitemaps.length} sitemaps`);
        } catch (error) {
            console.warn(`[${sourceName}] Could not fetch sitemap index: ${error.message}`);
            console.log(`[${sourceName}] Skipping historical collection (sitemap blocked)`);
            archiveSitemaps = [];
        }

        const historicalLinks = new Set();
        
        const historicalData = []; // Store URL + metadata
        
        for (const archiveUrl of archiveSitemaps) {
            if (historicalData.length >= maxHistoricalLinks) {
                console.log(`[${sourceName}] Reached limit of ${maxHistoricalLinks} historical links`);
                break;
            }

            await sleep(500);
            const sitemapData = await fetchArticleLinksFromSitemap(archiveUrl);
            
            for (const item of sitemapData) {
                if (historicalData.length >= maxHistoricalLinks) break;
                const link = typeof item === 'string' ? item : item.url;
                if (linkFilter(link) && isValidUrl(cleanUrl(link)) && !existingLinks.has(link)) {
                    historicalData.push(item); // Store full data (URL + metadata)
                    historicalLinks.add(link);
                }
            }

            console.log(`[${sourceName}] Collected ${historicalData.length} historical links so far...`);
        }

        if (historicalData.length > 0) {
            console.log(`[${sourceName}] Found ${historicalData.length} new historical articles`);
            
            // For historical articles, create entries with URL and sitemap metadata
            const historicalArticles = historicalData.map(item => {
                const link = typeof item === 'string' ? item : item.url;
                const cleanedLink = cleanUrl(link);
                
                // Extract basic info from URL
                const urlParts = cleanedLink.split('/');
                const slug = urlParts[urlParts.length - 1] || urlParts[urlParts.length - 2];
                const titleFromUrl = slug
                    .replace(/-/g, ' ')
                    .replace(/\.html?$/g, '')
                    .split(' ')
                    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
                    .join(' ');

                // Build extract with maximum available data
                let extract = `TITLE: ${titleFromUrl}\n\nHISTORICAL ARTICLE\nLink: ${cleanedLink}\n\n--- METADATA ---\nSOURCE: Sitemap discovery\nTYPE: Historical article`;
                
                // Add sitemap metadata if available
                if (typeof item === 'object' && item !== null) {
                    const metadataSection = [];
                    if (item.lastmod) {
                        metadataSection.push(`LAST_MODIFIED: ${item.lastmod}`);
                    }
                    if (item.priority !== null && item.priority !== undefined) {
                        metadataSection.push(`PRIORITY: ${item.priority}`);
                    }
                    if (item.changefreq) {
                        metadataSection.push(`CHANGE_FREQ: ${item.changefreq}`);
                    }
                    if (metadataSection.length > 0) {
                        extract += `\n${metadataSection.join('\n')}`;
                    }
                }
                
                extract += `\nNOTE: Full content requires page scraping`;

                // Try to parse lastmod as date
                let articleDate = null;
                if (typeof item === 'object' && item.lastmod) {
                    articleDate = new Date(item.lastmod);
                    if (isNaN(articleDate.getTime())) {
                        articleDate = null;
                    }
                }

                return {
                    author: 'Unknown',
                    date: articleDate,
                    extract: extract,
                    link: cleanedLink,
                    source: sourceName,
                    scrapedAt: new Date()
                };
            });

            if (historicalArticles.length > 0) {
                appendArticlesToCSV(historicalArticles, sourceName);
                results.sitemap.saved = historicalArticles.length;
                console.log(`[${sourceName}] Saved ${historicalArticles.length} historical article URLs`);
            }

            results.sitemap.total = historicalLinks.size;
        }
    }

    results.total.saved = results.rss.saved + results.sitemap.saved;
    
    console.log(`\n[${sourceName}] HYBRID collection complete:`);
    console.log(`  RSS articles: ${results.rss.saved} saved (${results.rss.total} total)`);
    console.log(`  Historical articles: ${results.sitemap.saved} saved (${results.sitemap.total} total)`);
    console.log(`  Total saved: ${results.total.saved}`);

    return results;
}

