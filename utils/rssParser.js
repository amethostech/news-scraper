import Parser from 'rss-parser';
import axios from 'axios';

const parser = new Parser({
    customFields: {
        item: [
            ['category', 'categories', { keepArray: true }],
            ['pubDate', 'pubDate'],
            ['dc:date', 'dcDate'],
            ['dc:creator', 'dcCreator', { keepArray: true }],
            ['dc:subject', 'dcSubject', { keepArray: true }],
            ['content:encoded', 'contentEncoded'],
            ['enclosure', 'enclosures', { keepArray: true }],
            ['media:content', 'mediaContent', { keepArray: true }],
            ['media:thumbnail', 'mediaThumbnail', { keepArray: true }],
            ['guid', 'guid']
        ]
    }
});

/**
 * Fetches and parses an RSS feed
 * @param {string} rssUrl - URL of the RSS feed
 * @returns {Promise<Array>} Array of article metadata objects
 */
export async function fetchRSSFeed(rssUrl) {
    try {
        console.log(`Fetching RSS feed: ${rssUrl}`);
        
        // Fetch RSS feed
        const response = await axios.get(rssUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            timeout: 10000,
            validateStatus: function (status) {
                return status < 500; // Accept 4xx errors to handle them gracefully
            }
        });

        // Check if response is valid
        if (response.status === 404) {
            console.log(`RSS feed not found (404): ${rssUrl}`);
            return [];
        }

        // Check if response is JSON (some sites return JSON instead of XML)
        let feedData = response.data;
        if (typeof feedData === 'string' && feedData.trim().startsWith('[')) {
            console.log(`RSS feed returned JSON instead of XML: ${rssUrl}`);
            return [];
        }

        // Check if response is HTML (redirect or error page)
        if (typeof feedData === 'string' && feedData.trim().startsWith('<!')) {
            console.log(`RSS feed returned HTML instead of XML: ${rssUrl}`);
            return [];
        }

        // Parse RSS feed (supports RSS 2.0, RSS 1.0, and Atom)
        const feed = await parser.parseString(feedData);
        
        console.log(`Parsed RSS feed: ${feed.title || 'Unknown'}`);
        console.log(`Found ${feed.items?.length || 0} items`);

        // Helper function to extract text from nested objects
        const extractText = (value) => {
            if (!value) return '';
            if (typeof value === 'string') return value;
            if (Array.isArray(value)) {
                // Handle array of objects with _ property
                return value.map(v => {
                    if (typeof v === 'string') return v;
                    if (v && typeof v === 'object' && v._) return v._;
                    if (v && typeof v === 'object' && v.text) return v.text;
                    return String(v);
                }).join(' ').trim();
            }
            if (typeof value === 'object') {
                // Handle object with _ property
                if (value._) return value._;
                if (value.text) return value.text;
                // Handle nested structure like { a: [{ _: "text" }] }
                if (value.a && Array.isArray(value.a)) {
                    return value.a.map(a => a._ || a.text || String(a)).join(' ').trim();
                }
            }
            return String(value);
        };

        // Transform RSS items to metadata format - extract MAXIMUM data
        const metadata = feed.items.map(item => {
            // Parse date - try multiple formats and sources
            let publishedDate = null;
            
            // Try isoDate first (most reliable, ISO 8601 format)
            if (item.isoDate) {
                publishedDate = new Date(item.isoDate);
                if (!isNaN(publishedDate.getTime())) {
                    // Valid date
                } else {
                    publishedDate = null;
                }
            }
            
            // If isoDate not available, try other sources
            if (!publishedDate) {
                const dateSources = [
                    item.pubDate,
                    item.dcDate,
                    item['dc:date']
                ];
                
                for (const dateSource of dateSources) {
                    if (dateSource) {
                        // Extract text if it's nested
                        const dateText = extractText(dateSource);
                        if (dateText) {
                            // Try parsing the date
                            const date = new Date(dateText);
                            if (!isNaN(date.getTime())) {
                                publishedDate = date;
                                break;
                            }
                        }
                    }
                }
            }

            // Extract title - prefer full content over snippet
            const title = extractText(item.title);
            
            // Extract content - prioritize full content, then encoded, then snippet, then description
            const contentSources = [
                item.contentEncoded,      // Full HTML content
                item.content,              // Full content
                item['content:encoded'],   // Alternative full content
                item.contentSnippet,       // Plain text snippet
                item.description           // Fallback description
            ];
            
            let fullContent = '';
            for (const source of contentSources) {
                const extracted = extractText(source);
                if (extracted && extracted.length > fullContent.length) {
                    fullContent = extracted;
                }
            }
            
            // Extract description (short version)
            const description = item.contentSnippet || item.description || '';
            const descriptionText = extractText(description);
            
            // Extract link - try multiple sources
            const link = item.link || (item.guid && typeof item.guid === 'string' ? item.guid : null) || '';
            const linkText = typeof link === 'string' ? link : (link.href || link.url || String(link));
            
            // Extract GUID (unique identifier)
            let guid = null;
            if (item.guid) {
                guid = typeof item.guid === 'string' ? item.guid : (item.guid._ || item.guid.text || String(item.guid));
            }
            
            // Extract author - try multiple sources
            const authorSources = [
                item.creator,
                item.author,
                item.dcCreator,
                item['dc:creator']
            ];
            
            let author = null;
            for (const source of authorSources) {
                const extracted = extractText(source);
                if (extracted && extracted.length > 0) {
                    author = extracted;
                    break;
                }
            }
            
            // Extract categories/tags - combine all sources
            const categorySources = [
                item.categories,
                item.category,
                item.dcSubject,
                item['dc:subject']
            ];
            
            const allCategories = [];
            for (const source of categorySources) {
                if (Array.isArray(source)) {
                    source.forEach(cat => {
                        const extracted = extractText(cat);
                        if (extracted && !allCategories.includes(extracted)) {
                            allCategories.push(extracted);
                        }
                    });
                } else if (source) {
                    const extracted = extractText(source);
                    if (extracted && !allCategories.includes(extracted)) {
                        allCategories.push(extracted);
                    }
                }
            }
            
            // Extract enclosures (media files)
            const enclosures = [];
            if (item.enclosures && Array.isArray(item.enclosures)) {
                item.enclosures.forEach(enc => {
                    if (enc.url) {
                        enclosures.push({
                            url: enc.url,
                            type: enc.type || '',
                            length: enc.length || 0
                        });
                    }
                });
            } else if (item.enclosure) {
                if (item.enclosure.url) {
                    enclosures.push({
                        url: item.enclosure.url,
                        type: item.enclosure.type || '',
                        length: item.enclosure.length || 0
                    });
                }
            }
            
            // Extract media content
            const mediaContent = [];
            if (item.mediaContent && Array.isArray(item.mediaContent)) {
                item.mediaContent.forEach(media => {
                    if (media.url || media.$.url) {
                        mediaContent.push({
                            url: media.url || media.$.url,
                            type: media.type || media.$.type || '',
                            medium: media.medium || media.$.medium || ''
                        });
                    }
                });
            }
            
            // Extract thumbnails
            const thumbnails = [];
            if (item.mediaThumbnail && Array.isArray(item.mediaThumbnail)) {
                item.mediaThumbnail.forEach(thumb => {
                    if (thumb.url || thumb.$.url) {
                        thumbnails.push(thumb.url || thumb.$.url);
                    }
                });
            }

            return {
                title: title,
                description: descriptionText,
                content: fullContent,  // Full content (HTML or plain text)
                link: linkText,
                guid: guid || linkText,  // Use guid if available, fallback to link
                publishedDate: publishedDate,
                categories: allCategories,
                author: author || null,
                enclosures: enclosures,  // Media files (images, videos, etc.)
                mediaContent: mediaContent,  // Media RSS content
                thumbnails: thumbnails,  // Thumbnail images
                // Feed metadata
                feedTitle: feed.title || null,
                feedDescription: feed.description || null,
                feedLink: feed.link || null
            };
        }).filter(item => item.link); // Filter out items without links

        return metadata;
    } catch (error) {
        // Handle specific error types gracefully
        if (error.response) {
            if (error.response.status === 404) {
                console.log(`RSS feed not found (404): ${rssUrl}`);
            } else {
                console.error(`Error fetching RSS feed ${rssUrl}: HTTP ${error.response.status}`);
            }
        } else if (error.message && error.message.includes('Feed not recognized')) {
            // Try to detect if it's an Atom feed or different format
            console.log(`RSS feed format not recognized (might be Atom or different format): ${rssUrl}`);
        } else {
            console.error(`Error fetching/parsing RSS feed ${rssUrl}:`, error.message);
        }
        return [];
    }
}

/**
 * Fetches multiple RSS feeds in parallel
 * @param {Array<string>} rssUrls - Array of RSS feed URLs
 * @returns {Promise<Array>} Array of metadata arrays
 */
export async function fetchMultipleRSSFeeds(rssUrls) {
    const results = await Promise.all(
        rssUrls.map(url => fetchRSSFeed(url))
    );
    return results.flat();
}

