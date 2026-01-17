/**
 * EMA (European Medicines Agency) Fetcher
 * 
 * Source: https://www.ema.europa.eu
 * Fetches drug/medicine regulatory data via ePI API (public access)
 */

import axios from 'axios';
import { getDefaultDateRange } from './config.js';
import { appendArticlesToCSV } from './csvWriter.js';
import { sleep } from '../utils/common.js';

const SOURCE_NAME = 'EMA';
// EMA ePI API - public access, no key required
const EPI_API_BASE = 'https://pul.ai.ema.europa.eu/ema-epi-hub/Api/v2';

/**
 * Search medicines via ePI API
 */
async function searchMedicines(options = {}) {
    const { pageNumber = 1, pageSize = 50, keyword = '' } = options;

    try {
        const response = await axios.get(
            `${EPI_API_BASE}/ListBySearchParameter`,
            {
                params: {
                    pageNumber,
                    pageSize,
                    productName: keyword || undefined
                },
                headers: {
                    'Accept': 'application/json'
                },
                timeout: 30000
            }
        );

        return response.data?.data || response.data || [];
    } catch (error) {
        console.error(`[${SOURCE_NAME}] ePI API error:`, error.message);
        return [];
    }
}

/**
 * Try alternative: Scrape from EMA website public data
 */
async function fetchPublicMedicinesData() {
    try {
        // EMA provides CSV/JSON downloads
        const response = await axios.get(
            'https://www.ema.europa.eu/en/documents/report/european-public-assessment-reports-download-data_en.zip',
            { timeout: 30000, responseType: 'arraybuffer' }
        );
        // Can't process zip here, try JSON endpoint
        return [];
    } catch (error) {
        return [];
    }
}

/**
 * Convert medicine data to article format
 */
function medicineToArticle(medicine) {
    const name = medicine.productName || medicine.name || medicine.medicineName || 'Unknown';
    const activeSubstance = medicine.activeSubstance || medicine.active_substance || '';
    const authDate = medicine.authDate || medicine.authorizationDate || medicine.marketing_authorisation_date || '';

    return {
        source: SOURCE_NAME,
        title: name,
        name: name,
        date: authDate || new Date().toISOString(),
        author: 'European Medicines Agency',
        link: medicine.url || medicine.epar_url ||
            `https://www.ema.europa.eu/en/medicines/human/EPAR/${name.toLowerCase().replace(/\s+/g, '-')}`,
        extract: `${name}: ` +
            `Active substance: ${activeSubstance || 'N/A'}. ` +
            `Status: ${medicine.status || medicine.authorizationStatus || 'Authorized'}. ` +
            `Therapeutic area: ${medicine.therapeuticArea || medicine.atc_code || 'N/A'}`
    };
}

/**
 * Fetch EMA medicines data
 */
async function fetchEMAData(options = {}) {
    const { maxArticles = 100, testMode = false, startDate, endDate } = options;

    console.log(`[${SOURCE_NAME}] Fetching regulatory data from ePI API...`);
    if (startDate && endDate) {
        console.log(`[${SOURCE_NAME}] Date Range: ${startDate} to ${endDate}`);
    }

    const articles = [];

    // Pharma-related search terms
    const searchTerms = testMode
        ? ['cancer']
        : ['cancer', 'diabetes', 'vaccine', 'antibody', 'enzyme'];

    for (const term of searchTerms) {
        if (articles.length >= maxArticles) break;

        try {
            await sleep(2000);

            console.log(`[${SOURCE_NAME}] Searching for: ${term}`);
            const medicines = await searchMedicines({
                pageSize: testMode ? 10 : 25,
                keyword: term
            });

            if (medicines.length > 0) {
                console.log(`[${SOURCE_NAME}] Found ${medicines.length} medicines for "${term}"`);

                for (const med of medicines) {
                    if (articles.length >= maxArticles) break;

                    const article = medicineToArticle(med);

                    // Client-side date filtering if dates provided
                    if (startDate && endDate) {
                        const articleDate = new Date(article.date);
                        if (articleDate < new Date(startDate) || articleDate > new Date(endDate)) {
                            continue;
                        }
                    }

                    if (!articles.find(a => a.link === article.link)) {
                        articles.push(article);
                    }
                }
            }
        } catch (error) {
            console.warn(`[${SOURCE_NAME}] Error searching "${term}":`, error.message);
        }
    }

    // If no results from ePI, try direct EMA data
    // ... (fallback logic) ...

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
        const articles = await fetchEMAData({
            maxArticles: testMode ? 25 : maxArticles,
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
