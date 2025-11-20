import Article from '../models/Article.js';
import { sleep } from './common.js';
import { fetchSitemapIndex, fetchArticleLinksFromSitemap } from './sitemap.js';

const MAX_LINKS = 10000;
const DB_WRITE_DELAY_MS = 50; // Tiny gap to prevent overload

export async function runSitemapScraper(config) {
    const {
        sourceName,
        sitemapIndexUrl,
        sitemapFilter = (loc) => true,
        linkFilter = (link) => true,
        scrapeDetails
    } = config;

    console.log(`\nStarting SITEMAP HARVEST for: ${sourceName}`);
    let articlesSaved = 0;
    let uniqueArticleLinks = new Set();

    const archiveSitemaps = await fetchSitemapIndex(sitemapIndexUrl, sitemapFilter);

    console.log(`\nStarting collection from ${archiveSitemaps.length} historical archives...`);
    for (const archiveUrl of archiveSitemaps) {
        if (uniqueArticleLinks.size >= MAX_LINKS) {
            console.log(`\n[LIMIT REACHED] Collected ${uniqueArticleLinks.size} links. Stopping sitemap harvest.`);
            break;
        }

        await sleep(500);
        const links = await fetchArticleLinksFromSitemap(archiveUrl);
        let newLinksFound = 0;

        for (const link of links) {
            if (uniqueArticleLinks.size >= MAX_LINKS) {
                break;
            }
            if (linkFilter(link) && !uniqueArticleLinks.has(link)) {
                uniqueArticleLinks.add(link);
                newLinksFound++;
            }
        }

        console.log(`Collected ${newLinksFound} links from ${archiveUrl.split('/').pop()}. Total links: ${uniqueArticleLinks.size}`);
    }

    if (uniqueArticleLinks.size > 0) {
        console.log(`\n--- STARTING DETAIL SCRAPE for ${uniqueArticleLinks.size} total links ---`);
        let linkArray = Array.from(uniqueArticleLinks);

        for (let i = 0; i < linkArray.length; i++) {
            const link = linkArray[i];

            if (i % 50 === 0) {
                console.log(`Processing article ${i + 1} of ${linkArray.length}...`);
            }

            const existingArticle = await Article.findOne({ link: link });
            if (existingArticle) continue;

            // Tiny delay before processing to prevent DB overload
            await sleep(DB_WRITE_DELAY_MS);

            const articleData = await scrapeDetails(link);

            if (articleData && articleData.extract && articleData.extract.length > 100) {
                articleData.source = sourceName;
                try {
                    await Article.create(articleData);
                    articlesSaved++;
                } catch (dbError) {
                    console.error(`\n[DB ERROR] Failed to save article ${link}. Code: ${dbError.code}. Message: ${dbError.message}\n`);
                }
            }
        }
    }

    console.log(`\n--- SITEMAP HARVEST COMPLETE for ${sourceName} ---`);
    console.log(`Total NEW articles saved to DB: ${articlesSaved}`);
    return articlesSaved;
}
