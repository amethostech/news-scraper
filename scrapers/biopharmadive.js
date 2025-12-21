import axios from 'axios';
import * as cheerio from 'cheerio';
import { runSitemapScraper } from '../utils/scraperRunner.js';
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';
import { sleep } from '../utils/common.js';

async function scrapeArticleDetails(url) {
    try {
        const { data } = await axios.get(url, {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' }
        });
        const $ = cheerio.load(data);

        let author = $('.author').text().trim();
        author = author.replace(/^By\s+/i, '').trim() || 'N/A';

        let dateText = $('.published-info').text().trim();
        const dateMatch = dateText.match(/[A-Z][a-z]{2,}\.\s+\d{1,2},\s+\d{4}/);
        dateText = dateMatch ? dateMatch[0] : new Date().toISOString();

        let articleDate = new Date(dateText);
        if (isNaN(articleDate.getTime())) {
            articleDate = new Date();
            console.warn(`[WARNING] Failed to parse date: "${dateText}". Using current date for article: ${url}`);
        }

        let bodyText = '';
        const articleBodySelectors = [
            'div.article-body-content',
            'div.page-content',
            '.article-body',
            '#article-body',
        ];

        let contentContainer = $();
        for (const selector of articleBodySelectors) {
            contentContainer = $(selector);
            if (contentContainer.length) break;
        }

        contentContainer.find('p').each((i, p) => {
            const paragraphText = $(p).text().trim();
            if (paragraphText.length > 40) {
                bodyText += paragraphText + '\n\n';
            }
        });

        if (!bodyText) {
            bodyText = contentContainer.text().trim();
        }

        return {
            author: author,
            date: articleDate,
            extract: bodyText.trim(),
            link: url,
        };

    } catch (error) {
        console.error(`Error fetching article details for ${url}:`, error.message);
        return null;
    }
}

export async function run(options = {}) {
    const sourceName = 'BioPharma Dive';
    
    if (options.historical) {
        // Historical scraping via sitemap
        return await runSitemapScraper({
            sourceName: sourceName,
            sitemapIndexUrl: 'https://www.biopharmadive.com/sitemap.xml',
            sitemapFilter: (loc) => loc.includes('/news/archive/'),
            linkFilter: (link) => link.includes('/news/'),
            scrapeDetails: scrapeArticleDetails
        });
    } else {
        // Default: RSS feed for weekly updates
        const sourceConfig = {
            sourceName: sourceName,
            rssUrl: 'https://www.biopharmadive.com/feeds/news',
            maxConcurrent: 3,
            delayBetweenScrapes: 1000
        };
        
        const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);
        return results;
    }
}
