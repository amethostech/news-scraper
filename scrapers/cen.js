import * as cheerio from 'cheerio';
import axios from 'axios';
import { runSitemapScraper } from '../utils/scraperRunner.js';

function isArticleUrl(url) {
    return /^https:\/\/cen\.acs\.org\/articles\/\d+\/i\d+\/.+\.html$/.test(url);
}

async function scrapeArticleDetails(url) {
    try {
        const { data } = await axios.get(url, {
            headers: { 
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36' 
            }
        });
        
        const $ = cheerio.load(data);
        
        // Extract author - try multiple selectors
        let author = '';
        const authorSelectors = [
            'heading-line-bottom',
           '.article-byline--author',
            '.article-byline a[rel="author"]',
            '.author-name',
            '.article-author',
            '[itemprop="author"]',
            '.byline',
            'meta[name="author"]',
            'meta[property="article:author"]',
            '.contributor-name'
        ];
        
        for (const selector of authorSelectors) {
            if (selector.startsWith('meta')) {
                const metaContent = $(selector).attr('content');
                if (metaContent) {
                    author = metaContent.trim();
                    break;
                }
            } else {
                const authorText = $(selector).first().text().trim();
                if (authorText) {
                    author = authorText.replace(/^by\s+/i, '').trim();
                    break;
                }
            }
        }
        
        // Extract date - try multiple selectors and formats
        let articleDate = '';
        const dateSelectors = [
            'time[datetime]',
            '.date',
            '[itemprop="datePublished"]',
            '.article-date',
            '.publish-date',
            'meta[name="date"]',
            'meta[property="article:published_time"]',
            '.date-published'
        ];
        
        for (const selector of dateSelectors) {

            // Meta tag case
            if (selector.startsWith('meta')) {
                const metaContent = $(selector).attr('content');
                if (metaContent) {
                    articleDate = metaContent.trim();
                    break;
                }
                continue;
            }

            // time[datetime] case
            if (selector.includes('[datetime]')) {
                const el = $(selector).first();
                const datetime = el.attr('datetime');
                if (datetime) {
                    articleDate = datetime.trim();
                    break;
                }
                const fallbackText = el.text().trim();
                if (fallbackText) {
                    articleDate = fallbackText;
                    break;
                }
                continue;
            }

            // Generic element with text date
            const dateText = $(selector).first().text().trim();
            if (dateText) {
                articleDate = dateText;
                break;
            }
        }
        
        // Extract body text - try multiple approaches
        let bodyText = '';
        const bodySelectors = [
            '.article-body',
            '.article-content',
            '[itemprop="articleBody"]',
            '.content-body',
            'article .body',
            '.story-body',
            'main article',
            '#article-content',
            '#article-body .section'     
        ];
        
        for (const selector of bodySelectors) {
            const content = $(selector);
            if (content.length > 0) {
                content.find('script, style, nav, footer, .advertisement, .ad, .related-articles').remove();
                
                bodyText = content.text()
                    .replace(/\s+/g, ' ')
                    .replace(/\n+/g, '\n')
                    .trim();
                
                if (bodyText.length > 100) { 
                    break;
                }
            }
        }
        
        // Fallback: if no body found, try getting all paragraph text
        if (!bodyText || bodyText.length < 100) {
            const paragraphs = $('article p, .article p, main p')
                .map((i, el) => $(el).text().trim())
                .get();

            bodyText = paragraphs.join('\n').replace(/\s+/g, ' ').trim();
        }
        
        // Log for debugging
        console.log(`Scraped: ${url}`);
        console.log(`Author: ${author || 'Not found'}`);
        console.log(`Date: ${articleDate || 'Not found'}`);
        console.log(`Content length: ${bodyText.length}`);
        
        return {
            author: author || 'Unknown',
            date: articleDate || '',
            extract: bodyText.trim(),
            link: url,
        };
        
    } catch (error) {
        console.error(`Error fetching article details for ${url}:`, error.message);
        return null;
    }
}

export async function run() {
    return await runSitemapScraper({
        sourceName: 'CEN',
        sitemapIndexUrl: 'https://cen.acs.org/content/cen/sitemap/sitemap_index.xml',
        sitemapFilter: (loc) => /sitemap_\d+\.xml$/.test(loc),
        linkFilter: (link) => isArticleUrl(link),
        scrapeDetails: scrapeArticleDetails
    });
}
