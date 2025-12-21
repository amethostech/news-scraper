/**
 * Optimized Weekly RSS Feed Scheduler
 * 
 * ONLY includes scrapers that use pure RSS feeds (no sitemaps, no Puppeteer)
 * This significantly reduces scraping time from hours to minutes.
 * 
 * Runs on:
 * - Sunday at 2:00 AM
 * - Wednesday at 2:00 AM
 * - Friday at 2:00 AM
 * 
 * Usage:
 *   node scheduler_rss_only.js                    # Start the scheduler
 *   node scheduler_rss_only.js --test             # Run immediately (for testing)
 */

import cron from 'node-cron';
import * as businessweekly from './scrapers/businessweekly.js';
import * as businesswire from './scrapers/businesswire.js';
import * as endpoints from './scrapers/endpoints.js';
import * as fda from './scrapers/fda.js';
import * as gen from './scrapers/gen.js';
import * as pharmavoice from './scrapers/pharmavoice.js';
import * as pharmatimes from './scrapers/pharmatimes.js';
import * as pharmaphorum from './scrapers/pharmaphorum.js';
import * as prnewswire from './scrapers/prnewswire.js';
import { connectMongoDB } from './utils/mongoWriter.js';

const args = process.argv.slice(2);
const testMode = args.includes('--test');

// RSS-ONLY scrapers (fast, minimal Puppeteer usage)
// These scrapers have RSS support and use axios first (Puppeteer only as fallback)
const scrapers = [
    { name: 'Business Weekly', run: businessweekly.run, type: 'RSS-only' },
    { name: 'Business Wire', run: businesswire.run, type: 'RSS-only' },
    { name: 'EndpointsNews', run: endpoints.run, type: 'RSS + axios (Puppeteer fallback)' },
    { name: 'FDA', run: fda.run, type: 'RSS-only' },
    { name: 'GEN', run: gen.run, type: 'RSS-only' },
    { name: 'PharmaVoice', run: pharmavoice.run, type: 'RSS-only' },
    { name: 'PharmaTimes', run: pharmatimes.run, type: 'RSS-only' },
    { name: 'Pharmaphorum', run: pharmaphorum.run, type: 'RSS-only' },
    { name: 'PR Newswire', run: prnewswire.run, type: 'RSS-only' }
];

// EXCLUDED scrapers (use sitemaps or heavy Puppeteer - too slow for weekly updates):
// - BioPharma Dive (no RSS)
// - BioSpace (no RSS)
// - BioSpectrum (uses sitemap)
// - BioWorld (uses sitemap)
// - C&EN News (no RSS)
// - European Pharmaceutical Review (uses sitemap)
// - FierceBiotech (uses Puppeteer + sitemap)
// - FiercePharma (uses Puppeteer)
// - MDPI (no RSS)
// - Nature Biotechnology (uses sitemap)
// - Pharmaceutical Technology (uses sitemap)
// - PharmaLetter (uses Puppeteer + sitemap)
// - PMLive (uses sitemap)
// - STAT News (uses sitemap)
// - The Scientist (uses Puppeteer)


/**
 * Run all RSS-only scrapers (weekly update)
 */
async function runWeeklyScraping() {
    const startTime = Date.now();
    const timestamp = new Date().toISOString();

    console.log(`
╔══════════════════════════════════════════════════════════════╗
║        OPTIMIZED WEEKLY RSS FEED SCRAPING STARTED            ║
║           ${timestamp}                                        ║
╚══════════════════════════════════════════════════════════════╝

Mode: WEEKLY UPDATE (RSS feeds only - FAST)
Scrapers: ${scrapers.length} (RSS-based sources)
Excluded: 10 slow scrapers (sitemap/heavy Puppeteer)

Expected time: 5-25 minutes (vs 2-4 hours for all scrapers)
`);

    // MongoDB is optional - CSV is primary storage
    await connectMongoDB();

    console.log('Starting RSS-only scrapers in parallel...\n');

    const options = {
        historical: false, // RSS-only mode (weekly update)
        testMode: false,
        maxArticles: null
    };

    // Run all scrapers in parallel
    const results = await Promise.allSettled(
        scrapers.map(async (scraper) => {
            try {
                console.log(`[${scraper.name}] Starting RSS feed collection...`);
                const result = await scraper.run(options);
                return {
                    name: scraper.name,
                    status: 'success',
                    result
                };
            } catch (error) {
                console.error(`[${scraper.name}] Error:`, error.message);
                return {
                    name: scraper.name,
                    status: 'error',
                    error: error.message
                };
            }
        })
    );

    const totalTime = Math.floor((Date.now() - startTime) / 1000 / 60);

    console.log(`
╔══════════════════════════════════════════════════════════════╗
║                    WEEKLY SCRAPING RESULTS                   ║
╚══════════════════════════════════════════════════════════════╝

Execution time: ${totalTime} minutes
Timestamp: ${new Date().toISOString()}

`);

    let totalSaved = 0;
    let totalFailed = 0;
    let successfulScrapers = 0;

    results.forEach((outcome, index) => {
        const scraper = scrapers[index];

        if (outcome.status === 'fulfilled') {
            const data = outcome.value;

            if (data.status === 'success') {
                const result = data.result;
                let saved, failed, total;

                if (typeof result === 'number') {
                    saved = result;
                    failed = 0;
                    total = 0;
                } else if (result && typeof result === 'object') {
                    saved = result.saved ?? 0;
                    failed = result.failed ?? 0;
                    total = result.total ?? result.new ?? result.processed ?? 0;
                } else {
                    saved = 0;
                    failed = 0;
                    total = 0;
                }

                totalSaved += saved;
                totalFailed += failed;
                successfulScrapers++;

                console.log(`✓ ${data.name}:`);
                if (total > 0) console.log(`  - URLs discovered: ${total}`);
                console.log(`  - Articles saved: ${saved}`);
                if (failed > 0) console.log(`  - Failed: ${failed}`);
            } else {
                console.log(`✗ ${data.name}: ERROR - ${data.error}`);
                totalFailed++;
            }
        } else {
            console.log(`✗ ${scraper.name}: ERROR - ${outcome.reason?.message || 'Unknown error'}`);
            totalFailed++;
        }
    });

    console.log(`
╔══════════════════════════════════════════════════════════════╗
║                    SUMMARY                                   ║
╚══════════════════════════════════════════════════════════════╝

Total articles saved: ${totalSaved}
Total failed: ${totalFailed}
Successful scrapers: ${successfulScrapers}/${scrapers.length}
Execution time: ${totalTime} minutes

💡 Note: This scheduler uses only RSS-based scrapers for speed.
   EndpointsNews may occasionally use Puppeteer as fallback if blocked.
   For complete coverage, run full historical scraping separately.
`);

    // Close any open browsers
    try {
        if (endpoints.closeBrowser) await endpoints.closeBrowser();
    } catch (e) {
        // Ignore cleanup errors
    }

    // Clean and lemmatize the merged CSV file
    if (totalSaved > 0) {
        console.log(`
╔══════════════════════════════════════════════════════════════╗
║           CLEANING AND LEMMATIZING ARTICLES                  ║
╚══════════════════════════════════════════════════════════════╝
`);

        try {
            const { exec } = await import('child_process');
            const { promisify } = await import('util');
            const execAsync = promisify(exec);
            const path = await import('path');
            const { fileURLToPath } = await import('url');

            const __filename = fileURLToPath(import.meta.url);
            const __dirname = path.dirname(__filename);
            const cleanScriptPath = path.join(__dirname, 'clean_text.py');

            const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';

            console.log('Running text cleaning and lemmatization...');
            const { stdout, stderr } = await execAsync(`${pythonCmd} "${cleanScriptPath}"`, {
                cwd: __dirname,
                maxBuffer: 10 * 1024 * 1024
            });

            if (stdout) {
                console.log(stdout);
            }
            if (stderr && !stderr.includes('DtypeWarning')) {
                console.warn('Cleaning warnings:', stderr);
            }

            console.log('✓ Text cleaning and lemmatization complete!');
            console.log('  Output saved to: data/merged_articles_cleaned.csv');

            // Run sentiment scoring
            try {
                const sentimentScriptPath = path.join(__dirname, 'add_sentiment.py');
                console.log('\nRunning sentiment scoring (VADER)...');
                const { stdout: sentimentStdout, stderr: sentimentStderr } = await execAsync(
                    `${pythonCmd} "${sentimentScriptPath}"`,
                    {
                        cwd: __dirname,
                        maxBuffer: 10 * 1024 * 1024
                    }
                );
                if (sentimentStdout) {
                    console.log(sentimentStdout);
                }
                if (sentimentStderr && !sentimentStderr.includes('DtypeWarning')) {
                    console.warn('Sentiment warnings:', sentimentStderr);
                }
                console.log('✓ Sentiment scoring complete!');
                console.log('  Output updated: data/merged_articles_cleaned.csv');

                // Run keyword normalization
                try {
                    const normScriptPath = path.join(__dirname, 'normalize_keywords.py');
                    console.log('\nRunning keyword normalization...');
                    const { stdout: normStdout, stderr: normStderr } = await execAsync(
                        `${pythonCmd} "${normScriptPath}"`,
                        {
                            cwd: __dirname,
                            maxBuffer: 10 * 1024 * 1024
                        }
                    );
                    if (normStdout) {
                        console.log(normStdout);
                    }
                    if (normStderr && !normStderr.includes('DtypeWarning')) {
                        console.warn('Normalization warnings:', normStderr);
                    }
                    console.log('✓ Keyword normalization complete!');
                    console.log('  Output updated: data/merged_articles_cleaned.csv');
                } catch (normError) {
                    console.error('✗ Error running normalization script:', normError.message);
                    console.error('  You can manually run: python3 normalize_keywords.py');
                }
            } catch (sentimentError) {
                console.error('✗ Error running sentiment script:', sentimentError.message);
                console.error('  You can manually run: python3 add_sentiment.py');
            }
        } catch (error) {
            console.error('✗ Error running text cleaning script:', error.message);
            console.error('  You can manually run: python3 clean_text.py');
        }
    } else {
        console.log('\nNo new articles saved, skipping text cleaning.');
    }
}

// Cron schedule: Sunday, Wednesday, Friday at 2:00 AM
const schedule = '0 2 * * 0,3,5';

if (testMode) {
    console.log('🧪 TEST MODE: Running immediately (not scheduled)');
    console.log('Schedule would be: Sunday, Wednesday, Friday at 2:00 AM\n');
    runWeeklyScraping()
        .then(() => {
            console.log('\n✓ Test run completed');
            process.exit(0);
        })
        .catch((error) => {
            console.error('\n✗ Test run failed:', error);
            process.exit(1);
        });
} else {
    console.log(`
╔══════════════════════════════════════════════════════════════╗
║      OPTIMIZED WEEKLY RSS FEED SCHEDULER (RSS-ONLY)          ║
╚══════════════════════════════════════════════════════════════╝

Schedule: Sunday, Wednesday, Friday at 2:00 AM
Scrapers: ${scrapers.length} RSS-only sources (FAST)
Mode: RSS feeds only (no sitemaps, no Puppeteer)

Expected time per run: 5-15 minutes

Scheduler is running... Press Ctrl+C to stop.
`);

    // Schedule the job
    cron.schedule(schedule, async () => {
        console.log(`\n⏰ Scheduled run triggered at ${new Date().toISOString()}`);
        await runWeeklyScraping();
    }, {
        scheduled: true,
        timezone: "America/New_York"
    });

    // Keep the process running
    process.on('SIGINT', () => {
        console.log('\n\n⚠️  Scheduler stopped by user');
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        console.log('\n\n⚠️  Scheduler stopped');
        process.exit(0);
    });
}
