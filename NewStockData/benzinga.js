/**
 * Benzinga News Fetcher
 * 
 * Source: https://api.benzinga.com
 * Fetches financial news articles
 */

import axios from 'axios';
import { API_KEYS, API_ENDPOINTS, getDefaultDateRange, PHARMA_KEYWORDS } from './config.js';
import { appendArticlesToCSV } from './csvWriter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'BENZINGA';
const API_KEY = API_KEYS.BENZINGA;
const BASE_URL = API_ENDPOINTS.BENZINGA;

/**
 * Fetch news from Benzinga API
 */
async function fetchNews(options = {}) {
    const {
        pageSize = 50,
        page = 0,
        startDate,
        endDate,
        tickers
    } = options;

    try {
        const params = {
            token: API_KEY,
            pageSize: pageSize,
            page: page,
            displayOutput: 'full'
        };

        if (startDate) params.dateFrom = startDate;
        if (endDate) params.dateTo = endDate;
        if (tickers) params.tickers = tickers;

        const response = await axios.get(
            `${BASE_URL}/news`,
            {
                params,
                timeout: 30000
            }
        );

        return response.data || [];
    } catch (error) {
        console.error(`[${SOURCE_NAME}] API error:`, error.message);
        throw error;
    }
}

/**
 * Convert Benzinga article to standard format
 */
function benzingaToArticle(article) {
    return {
        source: SOURCE_NAME,
        title: article.title || '',
        name: article.title || '',
        date: article.created || article.updated || new Date().toISOString(),
        author: article.author || 'Benzinga',
        link: article.url || '',
        extract: article.teaser || article.body?.substring(0, 500) || article.title || ''
    };
}

/**
 * Fetch pharma/biotech news
 */
async function fetchPharmaNews(options = {}) {
    const { maxArticles = 100, testMode = false } = options;

    // Use passed dates or default
    let { startDate, endDate } = options;
    if (!startDate || !endDate) {
        const defaults = getDefaultDateRange();
        startDate = startDate || defaults.startDate;
        endDate = endDate || defaults.endDate;
    }

    console.log(`[${SOURCE_NAME}] Fetching pharma/biotech news...`);
    console.log(`[${SOURCE_NAME}] Date range: ${startDate} to ${endDate}`);

    const articles = [];

    // Pharma/biotech tickers to track
    const pharmaTickers = [
        'PFE', 'JNJ', 'MRK', 'ABBV', 'BMY',  // Big pharma
        'AMGN', 'GILD', 'BIIB', 'REGN', 'VRTX', // Biotech
        'LLY', 'AZN', 'NVS', 'GSK', 'SNY' // Global pharma
    ];

    try {
        // Fetch by tickers
        for (const ticker of pharmaTickers) {
            if (articles.length >= maxArticles) break;

            await sleep(1000);

            try {
                const news = await fetchNews({
                    pageSize: testMode ? 5 : 25,
                    startDate,
                    endDate,
                    tickers: ticker
                });

                if (Array.isArray(news)) {
                    console.log(`[${SOURCE_NAME}] Found ${news.length} articles for ${ticker}`);

                    for (const item of news) {
                        if (!articles.find(a => a.link === item.url)) {
                            articles.push(benzingaToArticle(item));
                        }
                    }
                }
            } catch (error) {
                console.warn(`[${SOURCE_NAME}] Error for ${ticker}:`, error.message);
            }

            if (testMode && articles.length >= 20) break;
        }
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Fetch error:`, error.message);
    }

    return articles;
}

/**
 * Main run function
 */
export async function run(options = {}) {
    const { historical = false, testMode = false, maxArticles = 200, startDate, endDate } = options;

    console.log(`\n=== ${SOURCE_NAME} DATA FETCHER ===`);
    console.log(`Mode: ${historical ? 'Historical' : 'Update'}`);
    if (testMode) console.log('⚠️ TEST MODE: Limited results');

    try {
        const articles = await fetchPharmaNews({
            maxArticles: testMode ? 30 : maxArticles,
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
