/**
 * Yahoo Finance Fetcher (via RapidAPI)
 * 
 * Source: https://rapidapi.com/yahoo-finance
 * Fetches stock quotes, financials, and news
 */

import axios from 'axios';
import { API_KEYS, API_ENDPOINTS, RAPIDAPI_HOST } from './config.js';
import { appendArticlesToCSV } from './csvWriter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'YAHOO_FINANCE';
const API_KEY = API_KEYS.YAHOO_FINANCE;
const BASE_URL = API_ENDPOINTS.YAHOO_FINANCE;

/**
 * Get stock quote (Public API)
 */
async function getQuote(symbol, startDate, endDate) {
    try {
        // Calculate range based on dates if provided
        let range = '1d';
        let period1, period2;

        if (startDate && endDate) {
            range = undefined; // Use period parameters instead
            period1 = Math.floor(new Date(startDate).getTime() / 1000);
            period2 = Math.floor(new Date(endDate).getTime() / 1000);
        }

        const params = {
            interval: '1d'
        };

        if (period1 && period2) {
            params.period1 = period1;
            params.period2 = period2;
        } else {
            params.range = range;
        }

        // Use public query2.finance.yahoo.com
        const response = await axios.get(
            `https://query2.finance.yahoo.com/v8/finance/chart/${symbol}`,
            {
                params,
                headers: {
                    'User-Agent': 'Mozilla/5.0' // Required to avoid 403
                },
                timeout: 10000
            }
        );

        const result = response.data?.chart?.result?.[0];
        if (!result) return null;

        // If historical range, we might get multiple points, but keeping structure simple for now
        // Just return the meta or last point
        const quote = result.meta;
        return {
            symbol: quote.symbol,
            shortName: quote.symbol,
            regularMarketPrice: quote.regularMarketPrice,
            regularMarketChangePercent: 0,
            fiftyTwoWeekHigh: quote.fiftyTwoWeekHigh,
            fiftyTwoWeekLow: quote.fiftyTwoWeekLow,
            marketCap: 0
        };
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Quote error for ${symbol}:`, error.message);
        return null;
    }
}

/**
 * Get stock news (RSS Feed Fallback)
 */
async function getNews(symbol) {
    try {
        // Use Yahoo Finance RSS feed
        const response = await axios.get(
            `https://finance.yahoo.com/rss/headline?s=${symbol}`,
            {
                headers: {
                    'User-Agent': 'Mozilla/5.0'
                },
                timeout: 15000
            }
        );

        // Simple XML parsing (regex) since we can't import xml2js easily
        const xml = response.data;
        const items = [];
        const itemRegex = /<item>([\s\S]*?)<\/item>/g;
        let match;

        while ((match = itemRegex.exec(xml)) !== null) {
            const itemContent = match[1];
            const titleMatch = itemContent.match(/<title>([\s\S]*?)<\/title>/);
            const linkMatch = itemContent.match(/<link>([\s\S]*?)<\/link>/);
            const pubDateMatch = itemContent.match(/<pubDate>([\s\S]*?)<\/pubDate>/);

            if (titleMatch && linkMatch) {
                items.push({
                    title: titleMatch[1].replace(/<!\[CDATA\[(.*?)\]\]>/, '$1'),
                    link: linkMatch[1],
                    pubDate: pubDateMatch ? pubDateMatch[1] : new Date().toISOString(),
                    source: 'Yahoo Finance'
                });
            }
        }

        return items;
    } catch (error) {
        console.error(`[${SOURCE_NAME}] News error for ${symbol}:`, error.message);
        return [];
    }
}

/**
 * Get company profile (Skipping for public API to keep it simple)
 */
async function getProfile(symbol) {
    return null; // Not essential
}

/**
 * Convert Yahoo Finance news to article format
 */
function newsToArticle(newsItem, symbol) {
    return {
        source: SOURCE_NAME,
        title: newsItem.title || '',
        name: newsItem.title || '',
        date: newsItem.pubDate || new Date().toISOString(),
        author: newsItem.source || 'Yahoo Finance',
        link: newsItem.link || '',
        extract: `[${symbol}] ${newsItem.title || ''}. ${newsItem.summary || ''}`
    };
}

/**
 * Convert stock data to article format (for tracking)
 */
function quoteToArticle(quote, symbol) {
    return {
        source: SOURCE_NAME,
        title: `${symbol} Stock Update`,
        name: `${quote.shortName || symbol} Market Data`,
        date: new Date().toISOString(),
        author: 'Yahoo Finance',
        link: `https://finance.yahoo.com/quote/${symbol}`,
        extract: `${symbol} (${quote.shortName || 'N/A'}): ` +
            `Price: $${quote.regularMarketPrice || 'N/A'}, ` +
            `Change: ${quote.regularMarketChangePercent?.toFixed(2) || 'N/A'}%, ` +
            `Market Cap: $${quote.marketCap ? (quote.marketCap / 1e9).toFixed(2) + 'B' : 'N/A'}, ` +
            `52W High: $${quote.fiftyTwoWeekHigh || 'N/A'}, ` +
            `52W Low: $${quote.fiftyTwoWeekLow || 'N/A'}`
    };
}

/**
 * Fetch pharma/biotech stock data
 */
async function fetchPharmaData(options = {}) {
    const { maxArticles = 100, testMode = false, startDate, endDate } = options;

    console.log(`[${SOURCE_NAME}] Fetching pharma/biotech stock data...`);
    if (startDate && endDate) {
        console.log(`[${SOURCE_NAME}] Date range: ${startDate} to ${endDate}`);
    }

    const articles = [];

    // Pharma/biotech tickers
    const pharmaTickers = testMode
        ? ['PFE', 'JNJ', 'MRK']
        : [
            'PFE', 'JNJ', 'MRK', 'ABBV', 'BMY',
            'AMGN', 'GILD', 'BIIB', 'REGN', 'VRTX',
            'LLY', 'AZN', 'NVS', 'GSK', 'SNY'
        ];

    for (const symbol of pharmaTickers) {
        if (articles.length >= maxArticles) break;

        try {
            await sleep(5000); // RapidAPI rate limiting - increased to avoid 429

            // Fetch news for this symbol
            const news = await getNews(symbol);

            if (Array.isArray(news) && news.length > 0) {
                console.log(`[${SOURCE_NAME}] Found ${news.length} news items for ${symbol}`);

                for (const item of news.slice(0, testMode ? 3 : 10)) {
                    // Basic date filtering for RSS items if dates provided
                    if (startDate && endDate) {
                        const itemDate = new Date(item.pubDate);
                        if (itemDate < new Date(startDate) || itemDate > new Date(endDate)) {
                            continue;
                        }
                    }

                    if (!articles.find(a => a.link === item.link)) {
                        articles.push(newsToArticle(item, symbol));
                    }
                }
            }

            await sleep(3000); // Additional delay between calls

            // Also add quote data as a tracking record
            const quote = await getQuote(symbol, startDate, endDate);
            if (quote) {
                articles.push(quoteToArticle(quote, symbol));
            }

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
    const { historical = false, testMode = false, maxArticles = 200, startDate, endDate } = options;

    console.log(`\n=== ${SOURCE_NAME} DATA FETCHER ===`);
    console.log(`Mode: ${historical ? 'Historical' : 'Update'}`);
    if (testMode) console.log('⚠️ TEST MODE: Limited results');

    try {
        const articles = await fetchPharmaData({
            maxArticles: testMode ? 30 : maxArticles,
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
