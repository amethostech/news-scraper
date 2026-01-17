/**
 * SEC EDGAR Fetcher
 * 
 * Source: https://sec-api.io
 * Fetches SEC filings (10-K, 10-Q, 8-K, etc.)
 */

import axios from 'axios';
import { API_KEYS, API_ENDPOINTS, getDefaultDateRange, PHARMA_KEYWORDS } from './config.js';
import { appendArticlesToCSV } from './csvWriter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'SEC_EDGAR';
const API_KEY = API_KEYS.SEC_EDGAR;
const BASE_URL = API_ENDPOINTS.SEC_EDGAR;

/**
 * Search SEC filings
 */
async function searchFilings(query, options = {}) {
    const { from = 0, size = 50 } = options;

    try {
        // SEC-API.io: POST to base URL with query in body
        const response = await axios.post(
            BASE_URL,  // POST to https://api.sec-api.io directly
            {
                query: {
                    query_string: {
                        query: query
                    }
                },
                from: from,
                size: size,
                sort: [{ filedAt: { order: 'desc' } }]
            },
            {
                headers: {
                    'Authorization': API_KEY,
                    'Content-Type': 'application/json'
                },
                timeout: 60000
            }
        );

        return response.data.filings || [];
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Search error:`, error.message);
        throw error;
    }
}

/**
 * Get full text of a filing
 */
async function getFilingFullText(url) {
    try {
        const response = await axios.get(
            `${BASE_URL}/full-text-search`,
            {
                params: { url },
                headers: { 'Authorization': API_KEY },
                timeout: 30000
            }
        );
        return response.data;
    } catch (error) {
        console.error(`[${SOURCE_NAME}] Full text fetch error:`, error.message);
        return null;
    }
}

/**
 * Convert SEC filing to article format
 */
function filingToArticle(filing) {
    return {
        source: SOURCE_NAME,
        title: `${filing.companyName} - ${filing.formType}`,
        name: `${filing.formType}: ${filing.description || filing.companyName}`,
        date: filing.filedAt,
        author: filing.companyName,
        link: filing.linkToFilingDetails || filing.linkToHtml,
        extract: `${filing.formType} filing by ${filing.companyName} (${filing.ticker || 'N/A'}). ` +
            `CIK: ${filing.cik}. Filed on ${filing.filedAt}. ` +
            `${filing.description || ''}`
    };
}

/**
 * Fetch pharmaceutical/biotech SEC filings
 */
async function fetchPharmaFilings(options = {}) {
    const { maxArticles = 100, testMode = false } = options;

    // Use passed dates or default
    let { startDate, endDate } = options;
    if (!startDate || !endDate) {
        const defaults = getDefaultDateRange();
        startDate = startDate || defaults.startDate;
        endDate = endDate || defaults.endDate;
    }

    console.log(`[${SOURCE_NAME}] Searching for pharma/biotech filings...`);
    console.log(`[${SOURCE_NAME}] Date range: ${startDate} to ${endDate}`);

    const articles = [];
    const formTypes = ['10-K', '10-Q', '8-K', 'S-1', '424B'];

    for (const formType of formTypes) {
        if (articles.length >= maxArticles) break;

        // Build query for pharma/biotech companies
        const query = `formType:"${formType}" AND filedAt:[${startDate} TO ${endDate}] AND ` +
            `(companyName:pharmaceutical OR companyName:biotech OR companyName:pharma OR ` +
            `companyName:therapeutics OR companyName:biosciences)`;

        try {
            await sleep(2000); // Rate limiting

            const filings = await searchFilings(query, {
                size: testMode ? 5 : Math.min(50, maxArticles - articles.length)
            });

            console.log(`[${SOURCE_NAME}] Found ${filings.length} ${formType} filings`);

            for (const filing of filings) {
                articles.push(filingToArticle(filing));
            }
        } catch (error) {
            console.error(`[${SOURCE_NAME}] Error fetching ${formType}:`, error.message);
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

    try {
        const articles = await fetchPharmaFilings({
            maxArticles: testMode ? 10 : maxArticles,
            testMode,
            startDate,
            endDate
        });

        if (articles.length > 0) {
            await appendArticlesToCSV(articles, SOURCE_NAME);
            console.log(`[${SOURCE_NAME}] ✓ Saved ${articles.length} filings to CSV`);
        } else {
            console.log(`[${SOURCE_NAME}] No new filings found`);
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
