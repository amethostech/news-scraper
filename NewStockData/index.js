/**
 * NewStockData Module Index
 * 
 * Exports all fetchers for scheduler integration.
 */

import * as sec_edgar from './sec_edgar.js';
import * as gdelt from './gdelt.js';
import * as ema from './ema.js';
import * as benzinga from './benzinga.js';
import * as yahoo_finance from './yahoo_finance.js';
import * as alpha_vantage from './alpha_vantage.js';
import * as polygon_io from './polygon_io.js';

// Export individual modules
export {
    sec_edgar,
    gdelt,
    ema,
    benzinga,
    yahoo_finance,
    alpha_vantage,
    polygon_io
};

// Export as array for easy iteration
export const fetchers = [
    { name: 'SEC EDGAR', module: sec_edgar },
    { name: 'GDELT', module: gdelt },
    { name: 'EMA', module: ema },
    { name: 'Benzinga', module: benzinga },
    { name: 'Yahoo Finance', module: yahoo_finance },
    { name: 'Alpha Vantage', module: alpha_vantage },
    { name: 'Polygon.io', module: polygon_io }
];

/**
 * Run all fetchers
 */
export async function runAll(options = {}) {
    const { testMode = false, sequential = true, startDate, endDate } = options;

    console.log('\n' + '='.repeat(60));
    console.log('    STOCK DATA FETCHERS - ALL SOURCES');
    if (startDate && endDate) {
        console.log(`    Date Range: ${startDate} to ${endDate}`);
    } else {
        console.log('    Date Range: Default (Last 30 Days)');
    }
    console.log('='.repeat(60) + '\n');

    const results = [];

    if (sequential) {
        // Run sequentially (respects rate limits better)
        for (const fetcher of fetchers) {
            console.log(`\n>>> Starting ${fetcher.name}...`);
            try {
                // Pass date options to the fetcher run method
                const result = await fetcher.module.run({ testMode, startDate, endDate });
                results.push({ name: fetcher.name, status: 'success', result });
            } catch (error) {
                console.error(`[${fetcher.name}] Failed:`, error.message);
                results.push({ name: fetcher.name, status: 'error', error: error.message });
            }
        }
    } else {
        // Run in parallel
        const promises = fetchers.map(async (fetcher) => {
            try {
                const result = await fetcher.module.run({ testMode, startDate, endDate });
                return { name: fetcher.name, status: 'success', result };
            } catch (error) {
                return { name: fetcher.name, status: 'error', error: error.message };
            }
        });

        const settled = await Promise.allSettled(promises);
        results.push(...settled.map(s => s.status === 'fulfilled' ? s.value : s.reason));
    }
    // ... (summary logic remains mainly same, just ensuring brackets match) ...
    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('    SUMMARY');
    console.log('='.repeat(60));

    let totalSaved = 0;
    let successCount = 0;

    for (const result of results) {
        const status = result.status === 'success' ? '✓' : '✗';
        const saved = result.result?.saved || 0;
        totalSaved += saved;
        if (result.status === 'success') successCount++;

        console.log(`${status} ${result.name}: ${saved} records`);
    }

    console.log('');
    console.log(`Total records saved: ${totalSaved}`);
    console.log(`Successful fetchers: ${successCount}/${fetchers.length}`);
    console.log('='.repeat(60) + '\n');

    return results;
}

// Run if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    const args = process.argv.slice(2);
    const testMode = args.includes('--test');

    // Parse --startDate=YYYY-MM-DD
    const startDateArg = args.find(a => a.startsWith('--startDate='));
    const startDate = startDateArg ? startDateArg.split('=')[1] : null;

    // Parse --endDate=YYYY-MM-DD
    const endDateArg = args.find(a => a.startsWith('--endDate='));
    const endDate = endDateArg ? endDateArg.split('=')[1] : null;

    runAll({ testMode, startDate, endDate })
        .then(() => process.exit(0))
        .catch(error => {
            console.error('Fatal error:', error);
            process.exit(1);
        });
}
