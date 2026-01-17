/**
 * Alpha Vantage Fetcher
 * 
 * Source: https://www.alphavantage.co
 * Fetches stock time series and fundamental data
 */

import axios from 'axios';
import { API_KEYS, API_ENDPOINTS } from './config.js';
import { appendArticlesToCSV } from './csvWriter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'ALPHA_VANTAGE';
const API_KEY = API_KEYS.ALPHA_VANTAGE;
const BASE_URL = API_ENDPOINTS.ALPHA_VANTAGE;

/**
 * Get daily time series
 */
/**
 * Get daily time series
 */
async function getDailyTimeSeries(symbol, outputsize = 'compact') {
    try {
        const response = await axios.get(BASE_URL, {
            params: {
                function: 'TIME_SERIES_DAILY',
                symbol: symbol,
                apikey: API_KEY,
                outputsize: outputsize // compact = 100 days, full = 20+ years
            },
            timeout: 30000 // Increased timeout for full history
        });

        if (response.data['Time Series (Daily)']) {
            return response.data['Time Series (Daily)'];
        } else if (response.data['Note']) {
            console.warn(`[${SOURCE_NAME}] API Limit Reached for ${symbol}: ${response.data['Note']}`);
            return null;
        } else if (response.data['Information']) {
            console.warn(`[${SOURCE_NAME}] API Info for ${symbol}: ${response.data['Information']}`);
            return null;
        } else {
            console.warn(`[${SOURCE_NAME}] Unexpected response for ${symbol}:`, JSON.stringify(response.data).substring(0, 200));
            return null;
        }
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Daily series error for ${symbol}:`, error.message);
        return null;
    }
}

/**
 * Get company overview
 */
async function getCompanyOverview(symbol) {
    try {
        const response = await axios.get(BASE_URL, {
            params: {
                function: 'OVERVIEW',
                symbol: symbol,
                apikey: API_KEY
            },
            timeout: 15000
        });

        return response.data;
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Overview error for ${symbol}:`, error.message);
        return null;
    }
}

/**
 * Get news sentiment
 */
async function getNewsSentiment(tickers, startDate, endDate) {
    try {
        const params = {
            function: 'NEWS_SENTIMENT',
            tickers: tickers,
            apikey: API_KEY,
            limit: 50
        };

        if (startDate) {
            // Format YYYYMMDDTHHMM
            params.time_from = startDate.replace(/-/g, '') + 'T0000';
        }
        if (endDate) {
            params.time_to = endDate.replace(/-/g, '') + 'T2359';
        }

        const response = await axios.get(BASE_URL, {
            params,
            timeout: 30000
        });

        return response.data?.feed || [];
    } catch (error) {
        console.error(`[${SOURCE_NAME}] News sentiment error:`, error.message);
        return [];
    }
}

// ... (newsToArticle remains same) ...

/**
 * Convert stock data to article format
 */
function stockToArticle(symbol, overview, latestData, latestDate) {
    return {
        // ... (remains same) ...
        source: SOURCE_NAME,
        title: `${symbol} - ${overview?.Name || symbol}`,
        name: `${symbol} Stock Data`,
        date: latestDate || new Date().toISOString(),
        author: 'Alpha Vantage',
        link: `https://www.alphavantage.co/query?function=OVERVIEW&symbol=${symbol}`,
        extract: `${symbol} (${overview?.Name || 'N/A'}): ` +
            `Sector: ${overview?.Sector || 'N/A'}, ` +
            `Industry: ${overview?.Industry || 'N/A'}, ` +
            `Close: $${latestData?.['4. close'] || 'N/A'}, ` +
            `Volume: ${latestData?.['5. volume'] || 'N/A'}, ` +
            `Market Cap: $${overview?.MarketCapitalization ? (overview.MarketCapitalization / 1e9).toFixed(2) + 'B' : 'N/A'}, ` +
            `PE Ratio: ${overview?.PERatio || 'N/A'}, ` +
            `Dividend Yield: ${overview?.DividendYield || 'N/A'}`
    };
}

/**
 * Fetch pharma/biotech stock data
 */
async function fetchPharmaData(options = {}) {
    const { maxArticles = 100, testMode = false, startDate, endDate } = options;

    console.log(`[${SOURCE_NAME}] Fetching pharma/biotech data...`);
    if (startDate && endDate) {
        console.log(`[${SOURCE_NAME}] Date range: ${startDate} to ${endDate}`);
    }

    const articles = [];

    // Pharma/biotech tickers
    const pharmaTickers = testMode
        ? ['PFE', 'JNJ']
        : [
            'PFE', 'JNJ', 'MRK', 'ABBV', 'BMY',
            'AMGN', 'GILD', 'BIIB', 'REGN', 'VRTX',
            'LLY', 'AZN', 'NVS'
        ];

    // First, get news sentiment (one API call for multiple tickers)
    console.log(`[${SOURCE_NAME}] Fetching news sentiment...`);
    const tickerString = pharmaTickers.slice(0, 5).join(','); // API limit
    const news = await getNewsSentiment(tickerString, startDate, endDate);

    if (news.length > 0) {
        console.log(`[${SOURCE_NAME}] Found ${news.length} news articles with sentiment`);
        for (const item of news.slice(0, testMode ? 10 : 50)) {
            // Already filtered by API params if supported, but good to be safe
            articles.push(newsToArticle(item));
        }
    }

    await sleep(12000); // Alpha Vantage free tier: 5 calls/min

    // Then get stock data for each ticker
    for (const symbol of pharmaTickers) {
        if (articles.length >= maxArticles) break;

        try {
            await sleep(12000); // Rate limiting (5 calls/min = 12s between calls)

            console.log(`[${SOURCE_NAME}] Fetching data for ${symbol}...`);

            const overview = await getCompanyOverview(symbol);

            await sleep(12000);

            // Fetch daily time series (this returns full/compact dataset, not filtered by date params)
            // We need to outputsize=full if we want historical data beyond last 100 days?
            // For now sticking to compact (last 100 days) to save bandwidth.
            // Determine output size
            // Alpha Vantage Free Tier does NOT support outputsize=full for daily series anymore.
            // We must stick to compact or switch to monthly if we want history (but that changes format).
            // For now, let's revert to compact to avoid API errors, even though it means we won't get 2000s data from AV. 
            // We'll rely on Yahoo Finance for that.
            let outputsize = 'compact';

            // if (startDate) { ... } // optimizing out since it causes errors

            const timeSeries = await getDailyTimeSeries(symbol, outputsize);

            if (timeSeries) {
                const dates = Object.keys(timeSeries).sort().reverse();

                if (startDate && endDate) {
                    // Get all dates in range
                    const rangeDates = dates.filter(d => d >= startDate && d <= endDate);
                    console.log(`[${SOURCE_NAME}] Found ${rangeDates.length} trading days in range`);

                    // To avoid huge CSVs, maybe limit to ~5 key points per month or year?
                    // But user asked for historical data. Let's dump it all?
                    // Actually, fetching 25 years of daily data = ~6000 rows per stock.
                    // That's manageable for CSV.
                    // However, `batch_runner` runs year by year, so ~250 rows per run. That's fine.

                    for (const date of rangeDates) {
                        const dataPoint = timeSeries[date];
                        articles.push(stockToArticle(symbol, overview, dataPoint, date));
                    }
                } else {
                    // Default logic: just get latest
                    const latestDate = dates[0];
                    const latestData = timeSeries[latestDate];
                    articles.push(stockToArticle(symbol, overview, latestData, latestDate));
                }
            }

            if (testMode && articles.length >= 10) break;

        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error for ${symbol}:`, error.message);
        }
    }

    return articles;
}

/**
 * Main run function
 */
export async function run(options = {}) {
    const { historical = false, testMode = false, maxArticles = 100, startDate, endDate } = options;

    console.log(`\n=== ${SOURCE_NAME} DATA FETCHER ===`);
    console.log(`Mode: ${historical ? 'Historical' : 'Update'}`);
    if (testMode) console.log('⚠️ TEST MODE: Limited results');
    console.log('⚠️ Note: Alpha Vantage free tier has 5 calls/min limit');

    try {
        const articles = await fetchPharmaData({
            maxArticles: testMode ? 15 : maxArticles,
            testMode,
            startDate,
            endDate
        });

        if (articles.length > 0) {
            await appendArticlesToCSV(articles, SOURCE_NAME);
            console.log(`[${SOURCE_NAME}] ✓ Saved ${articles.length} records to CSV`);
        } else {
            console.log(`[${SOURCE_NAME}] No new data found`);
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
