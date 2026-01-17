/**
 * GDELT Project Fetcher
 * 
 * Source: https://api.gdeltproject.org
 * Fetches news articles from GDELT DOC 2.0 API (public access)
 */

import axios from 'axios';
import { API_ENDPOINTS, getDefaultDateRange, PHARMA_KEYWORDS } from './config.js';
import { appendArticlesToCSV } from './csvWriter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'GDELT';
const DOC_API = API_ENDPOINTS.GDELT_DOC;

/**
 * Search GDELT DOC 2.0 API
 */
async function searchGDELT(query, options = {}) {
    const {
        mode = 'artlist',
        maxRecords = 250,
        format = 'json',
        startDate,
        endDate
    } = options;

    try {
        // GDELT date format: YYYYMMDDHHMMSS
        const startDT = startDate ? startDate.replace(/-/g, '') + '000000' : '';
        const endDT = endDate ? endDate.replace(/-/g, '') + '235959' : '';

        const params = {
            query: query,
            mode: mode,
            maxrecords: maxRecords,
            format: format,
            sort: 'DateDesc'
        };

        if (startDT) params.startdatetime = startDT;
        if (endDT) params.enddatetime = endDT;

        const response = await axios.get(DOC_API, {
            params,
            timeout: 60000
        });

        return response.data.articles || [];
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Search error:`, error.message);
        throw error;
    }
}

/**
 * Convert GDELT article to standard format
 */
function gdeltToArticle(article) {
    return {
        source: SOURCE_NAME,
        title: article.title || '',
        name: article.title || '',
        date: article.seendate ? new Date(
            article.seendate.substring(0, 4) + '-' +
            article.seendate.substring(4, 6) + '-' +
            article.seendate.substring(6, 8)
        ).toISOString() : new Date().toISOString(),
        author: article.domain || article.sourcecountry || '',
        link: article.url || '',
        extract: article.title || ''
    };
}

/**
 * Fetch pharma/biotech news from GDELT
 */
async function fetchPharmaNews(options = {}) {
    const { maxArticles = 250, testMode = false } = options;

    // Use passed dates or default
    let { startDate, endDate } = options;
    if (!startDate || !endDate) {
        const defaults = getDefaultDateRange();
        startDate = startDate || defaults.startDate;
        endDate = endDate || defaults.endDate;
    }

    console.log(`[${SOURCE_NAME}] Searching for pharma/biotech news...`);
    console.log(`[${SOURCE_NAME}] Date range: ${startDate} to ${endDate}`);

    const articles = [];

    // Search for each keyword
    const keywords = testMode ? PHARMA_KEYWORDS.slice(0, 2) : PHARMA_KEYWORDS;

    for (const keyword of keywords) {
        if (articles.length >= maxArticles) break;

        try {
            await sleep(1000); // Rate limiting

            const results = await searchGDELT(keyword, {
                maxRecords: testMode ? 25 : Math.min(100, maxArticles - articles.length),
                startDate,
                endDate
            });

            console.log(`[${SOURCE_NAME}] Found ${results.length} articles for "${keyword}"`);

            for (const result of results) {
                if (result.url && !articles.find(a => a.link === result.url)) {
                    articles.push(gdeltToArticle(result));
                }
            }
        } catch (error) {
            console.error(`[${SOURCE_NAME}] Error searching "${keyword}":`, error.message);
        }
    }

    return articles;
}

/**
 * Main run function
 */
export async function run(options = {}) {
    const { historical = false, testMode = false, maxArticles = 500, startDate, endDate } = options;

    console.log(`\n=== ${SOURCE_NAME} DATA FETCHER ===`);
    console.log(`Mode: ${historical ? 'Historical' : 'Update'}`);
    if (testMode) console.log('⚠️ TEST MODE: Limited results');

    try {
        const articles = await fetchPharmaNews({
            maxArticles: testMode ? 50 : maxArticles,
            testMode,
            startDate,
            endDate
        });

        if (articles.length > 0) {
            await appendArticlesToCSV(articles, SOURCE_NAME);
            console.log(`[${SOURCE_NAME}] ✓ Saved ${articles.length} articles to CSV`);
        } else {
            console.log(`[${SOURCE_NAME}] No new articles found`);
        }

        return {
            total: articles.length,
            saved: articles.length,
            failed: 0
        };
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Error:`, error.message);
        return {
            total: 0,
            saved: 0,
            failed: 1,
            error: error.message
        };
    }
}

// Run directly if executed as main
if (import.meta.url === `file://${process.argv[1]}`) {
    run({ testMode: true })
        .then(result => {
            console.log('\nResult:', result);
            process.exit(0);
        })
        .catch(error => {
            console.error('Error:', error);
            process.exit(1);
        });
}
