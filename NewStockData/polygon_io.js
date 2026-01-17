import axios from 'axios';
import { API_KEYS, API_ENDPOINTS, getDefaultDateRange } from './config.js';
import { appendArticlesToCSV } from './csvWriter.js';

const API_KEY = API_KEYS.POLYGON_IO;
const BASE_URL = API_ENDPOINTS.POLYGON_IO;
const SOURCE_NAME = 'POLYGON_IO';

// Target Pharma/Biotech Tickers
const TARGET_TICKERS = [
    'PFE', 'JNJ', 'MRK', 'ABBV', 'BMY',
    'AMGN', 'GILD', 'BIIB', 'REGN', 'VRTX',
    'LLY', 'AZN', 'NVS', 'GSK', 'SNY'
];

/**
 * Get ticker news
 */
async function getTickerNews(ticker, limit = 5, startDate, endDate) {
    try {
        const params = {
            ticker: ticker,
            limit: limit,
            order: 'desc',
            sort: 'published_utc',
            apiKey: API_KEY
        };

        if (startDate) params['published_utc.gte'] = startDate;
        if (endDate) params['published_utc.lte'] = endDate;

        const response = await axios.get(
            `${BASE_URL}/v2/reference/news`,
            {
                params,
                timeout: 10000
            }
        );
        return response.data?.results || [];
    } catch (error) {
        console.error(`[${SOURCE_NAME}] News error for ${ticker}:`, error.message);
        return [];
    }
}

/**
 * Get daily aggregates (Open/Close)
 */
async function getDailyAggregates(ticker, startDate, endDate) {
    // Ensure we have dates
    if (!startDate || !endDate) {
        const defaults = getDefaultDateRange();
        startDate = defaults.startDate;
        endDate = defaults.endDate;
    }

    try {
        const response = await axios.get(
            `${BASE_URL}/v2/aggs/ticker/${ticker}/range/1/day/${startDate}/${endDate}`,
            {
                params: {
                    adjusted: true,
                    sort: 'desc',
                    // limit: 1, // Remove limit to get full range if historical
                    apiKey: API_KEY
                },
                timeout: 10000
            }
        );
        // Return all results if historical, otherwise just latest
        return response.data?.results || [];
    } catch (error) {
        // 403 usually means plan limit or invalid key
        if (error.response?.status === 403) {
            console.error(`[${SOURCE_NAME}] 403 Forbidden for ${ticker} (Check API Key/Plan)`);
        } else {
            console.error(`[${SOURCE_NAME}] Aggs error for ${ticker}:`, error.message);
        }
        return [];
    }
}

/**
 * Main Run Function
 */
export async function run(options = {}) {
    const { testMode = false, startDate, endDate } = options;
    console.log(`\n=== ${SOURCE_NAME} DATA FETCHER (REST API) ===`);
    if (startDate && endDate) {
        console.log(`[${SOURCE_NAME}] Date Range: ${startDate} to ${endDate}`);
    }

    // Limit tickers in test mode to save API calls
    const tickersToProcess = testMode ? TARGET_TICKERS.slice(0, 3) : TARGET_TICKERS;
    const articles = [];

    for (const ticker of tickersToProcess) {
        console.log(`[${SOURCE_NAME}] Fetching data for ${ticker}...`);

        // 1. Get Stock Data (Aggregates) - potentially multiple days
        const aggs = await getDailyAggregates(ticker, startDate, endDate);
        if (aggs && aggs.length > 0) {
            // For historical, we might have many points. 
            // If historical mode, maybe just take the latest or all? 
            // Let's take up to 366 points (full year) if doing batch
            const maxPoints = (startDate && endDate) ? 366 : 1;

            for (const dailyData of aggs.slice(0, maxPoints)) {
                articles.push({
                    source: SOURCE_NAME,
                    title: `${ticker} Daily Market Stats`,
                    name: `${ticker} Stock Data`,
                    date: new Date(dailyData.t).toISOString(),
                    author: 'Polygon.io',
                    link: `https://polygon.io/symbol/${ticker}`,
                    extract: `Open: ${dailyData.o}, Close: ${dailyData.c}, High: ${dailyData.h}, Low: ${dailyData.l}, Vol: ${dailyData.v}`
                });
            }
        }

        // 2. Get News
        const news = await getTickerNews(ticker, testMode ? 2 : 5, startDate, endDate);
        for (const item of news) {
            articles.push({
                source: SOURCE_NAME,
                title: item.title,
                name: item.publisher?.name || 'Unknown',
                date: item.published_utc,
                author: item.author || 'Polygon.io',
                link: item.article_url,
                extract: item.description || item.title
            });
        }

        // Rate limit helper
        await new Promise(r => setTimeout(r, 2000));
    }

    console.log(`[${SOURCE_NAME}] Found ${articles.length} records.`);

    if (articles.length > 0) {
        await appendArticlesToCSV(articles, 'polygon_articles.csv');
        console.log(`[${SOURCE_NAME}] ✓ Saved articles to CSV`);
    }

    return {
        total: articles.length,
        saved: articles.length,
        status: 'success'
    };
}

// Run directly if executed as script
if (import.meta.url === `file://${process.argv[1]}`) {
    run({ testMode: true }).then(() => process.exit(0));
}
