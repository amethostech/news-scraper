/**
 * Full Historical & RSS Scheduler
 * 
 * Includes all 24 sources with full historical support.
 * Useful for monthly "Deep" crawls to capture missed data.
 * 
 * Usage:
 *   node scheduler.js --test             # Run once immediately (Full)
 * 
 * For weekly-only RSS updates, use scheduler_rss_only.js.
 */

import cron from 'node-cron';
import * as biopharmadive from './scrapers/biopharmadive.js';
import * as biospace from './scrapers/biospace.js';
import * as bioworld from './scrapers/bioworld.js';
import * as cen from './scrapers/cen.js';
import * as endpoints from './scrapers/endpoints.js';
import * as europeanpharmaceuticalreview from './scrapers/europeanpharmaceuticalreview.js';
import * as fiercebiotech from './scrapers/fiercebiotech.js';
import * as fiercepharma from './scrapers/fiercepharma.js';
import * as gen from './scrapers/gen.js';
import * as mdpi from './scrapers/mdpi.js';
import * as naturebiotech from './scrapers/naturebiotech.js';
import * as biospectrum from './scrapers/biospectrum.js';
import * as pharmaletter from './scrapers/pharmaletter.js';
import * as fda from './scrapers/fda.js';
import * as businesswire from './scrapers/businesswire.js';
import * as businessweekly from './scrapers/businessweekly.js';
import * as pharmavoice from './scrapers/pharmavoice.js';
import * as pharmaceuticaltech from './scrapers/pharmaceuticaltech.js';
import * as pharmatimes from './scrapers/pharmatimes.js';
import * as pharmaphorum from './scrapers/pharmaphorum.js';
import * as pmlive from './scrapers/pmlive.js';
import * as prnewswire from './scrapers/prnewswire.js';
import * as statnews from './scrapers/statnews.js';
import * as thescientist from './scrapers/thescientist.js';
import { connectMongoDB } from './utils/mongoWriter.js';

const args = process.argv.slice(2);
const testMode = args.includes('--test');

// All available scrapers (24 unique sources)
const scrapers = [
    { name: 'BioPharma Dive', run: biopharmadive.run },
    { name: 'BioSpace', run: biospace.run },
    { name: 'BioSpectrum', run: biospectrum.run },
    { name: 'BioWorld', run: bioworld.run },
    { name: 'C&EN News', run: cen.run },
    { name: 'EndpointsNews', run: endpoints.run },
    { name: 'European Pharmaceutical Review', run: europeanpharmaceuticalreview.run },
    { name: 'FierceBiotech', run: fiercebiotech.run },
    { name: 'FiercePharma', run: fiercepharma.run },
    { name: 'GEN', run: gen.run },
    { name: 'MDPI', run: mdpi.run },
    { name: 'Nature Biotechnology', run: naturebiotech.run },
    { name: 'PharmaLetter', run: pharmaletter.run },
    { name: 'FDA', run: fda.run },
    { name: 'Business Wire', run: businesswire.run },
    { name: 'Business Weekly', run: businessweekly.run },
    { name: 'PharmaVoice', run: pharmavoice.run },
    { name: 'Pharmaceutical Technology', run: pharmaceuticaltech.run },
    { name: 'PharmaTimes', run: pharmatimes.run },
    { name: 'Pharmaphorum', run: pharmaphorum.run },
    { name: 'PMLive', run: pmlive.run },
    { name: 'PR Newswire', run: prnewswire.run },
    { name: 'STAT News', run: statnews.run },
    { name: 'The Scientist', run: thescientist.run }
];

/**
 * Run all scrapers in RSS-only mode (weekly update)
 */
async function runWeeklyScraping() {
    const startTime = Date.now();
    const timestamp = new Date().toISOString();

    console.log(`
╔══════════════════════════════════════════════════════════════╗
║           WEEKLY RSS FEED SCRAPING STARTED                   ║
║           ${timestamp}                                        ║
╚══════════════════════════════════════════════════════════════╝

Mode: WEEKLY UPDATE (RSS feeds only)
Scrapers: ${scrapers.length}
`);

    // MongoDB is optional - CSV is primary storage
    // Try to connect silently (no errors if not configured)
    await connectMongoDB();

    console.log('Starting scrapers in parallel...\n');

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
`);

    // Close any open browsers
    try {
        if (fiercebiotech.closeBrowser) await fiercebiotech.closeBrowser();
        if (endpoints.closeBrowser) await endpoints.closeBrowser();
    } catch (e) {
        // Ignore cleanup errors
    }

    // Clean and lemmatize the merged CSV file
    // Close any open browsers
    try {
        if (endpoints.closeBrowser) await endpoints.closeBrowser();
    } catch (e) {
        // Ignore browser close errors
    }

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

            // Try python3 first, fallback to python
            const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';

            console.log('Running text cleaning and lemmatization...');
            const { stdout, stderr } = await execAsync(`${pythonCmd} "${cleanScriptPath}"`, {
                cwd: __dirname,
                maxBuffer: 10 * 1024 * 1024 // 10MB buffer for large outputs
            });

            if (stdout) {
                console.log(stdout);
            }
            if (stderr && !stderr.includes('DtypeWarning')) {
                console.warn('Cleaning warnings:', stderr);
            }

            console.log('✓ Text cleaning and lemmatization complete!');
            console.log('  Output saved to: data/merged_articles_cleaned.csv');

            // Run sentiment scoring on cleaned text (VADER)
            try {
                const sentimentScriptPath = path.join(__dirname, 'add_sentiment.py');
                console.log('\nRunning sentiment scoring (VADER) on cleaned CSV...');
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
                console.log('  Output updated: data/merged_articles_cleaned.csv (sentiment_score added)');
            } catch (sentimentError) {
                console.error('✗ Error running sentiment script:', sentimentError.message);
                console.error('  You can manually run: python3 add_sentiment.py');
            }
        } catch (error) {
            console.error('✗ Error running text cleaning script:', error.message);
            console.error('  The merged_articles.csv was updated, but cleaning failed.');
            console.error('  You can manually run: python3 clean_text.py');
        }
    } else {
        console.log('\nNo new articles saved, skipping text cleaning.');
    }
}

// Cron schedule: Sunday, Wednesday, Friday at 2:00 AM
// Format: minute hour day-of-month month day-of-week
// 0 = Sunday, 3 = Wednesday, 5 = Friday
const schedule = '0 2 * * 0,3,5'; // 2 AM on Sunday (0), Wednesday (3), Friday (5)

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
║           WEEKLY RSS FEED SCHEDULER                          ║
╚══════════════════════════════════════════════════════════════╝

Schedule: Sunday, Wednesday, Friday at 2:00 AM
Scrapers: ${scrapers.length} sources
Mode: RSS feeds only (weekly updates)

Scheduler is running... Press Ctrl+C to stop.

Next scheduled run: ${cron.getTasks().size > 0 ? 'Calculating...' : 'Will be scheduled'}
`);

    // Schedule the job
    cron.schedule(schedule, async () => {
        console.log(`\n⏰ Scheduled run triggered at ${new Date().toISOString()}`);
        await runWeeklyScraping();
    }, {
        scheduled: true,
        timezone: "America/New_York" // Adjust timezone as needed
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

