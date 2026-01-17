
import axios from 'axios';
import { API_KEYS, API_ENDPOINTS, getDefaultDateRange } from './config.js';

const API_KEY = API_KEYS.SEC_EDGAR;
const BASE_URL = API_ENDPOINTS.SEC_EDGAR;

// Copied from sec_edgar.js
async function searchFilings(query, options = {}) {
    const { from = 0, size = 50 } = options;

    try {
        console.log(`Running query: ${query}`);
        const response = await axios.post(
            BASE_URL,
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
                timeout: 30000
            }
        );

        return response.data.filings || [];
    } catch (error) {
        console.error(`Search error:`, error.message);
        if (error.response) {
            console.error('Response status:', error.response.status);
            console.error('Response data:', error.response.data);
        }
        return [];
    }
}

async function main() {
    const { startDate, endDate } = getDefaultDateRange();
    console.log(`Searching for 'Lilly' filings from ${startDate} to ${endDate}...`);

    // Query for Eli Lilly (Ticker: LLY)
    // Using a broad query to catch Company Name or Ticker
    const query = `filedAt:[${startDate} TO ${endDate}] AND (companyName:"Eli Lilly" OR ticker:"LLY")`;

    const filings = await searchFilings(query, { size: 10 });

    console.log(`\nFound ${filings.length} filings:\n`);

    filings.forEach((f, i) => {
        console.log(`${i + 1}. [${f.filedAt}] ${f.companyName} (${f.ticker}) - ${f.formType}`);
        console.log(`   Link: ${f.linkToFilingDetails}`);
        console.log(`   Description: ${f.description || 'N/A'}\n`);
    });
}

main();
