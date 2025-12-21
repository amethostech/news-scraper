import express from 'express';
import cron from 'node-cron';
import { scrapers } from './scrapers/index.js';
import { sleep } from './utils/common.js';
import { getCSVStats, readArticlesFromCSV, csvFileExists, getCSVFilePath } from './utils/csvWriter.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = 3000;
const SCRAPE_DELAY_MS = 5000;

const app = express();
app.use(express.json());

// Track if scraping is in progress
let isScraping = false;

/**
 * Mapping of scraper modules to their CSV source names
 * This maps the actual source names used when saving to CSV files
 */
const scraperSourceNameMap = new Map();

// Initialize the mapping - we'll populate it by checking each scraper's run function
// or by using a known mapping based on the order in scrapers array
function initializeSourceNameMap() {
    // Source names in the same order as scrapers array in index.js
    const sourceNames = [
        'BioPharmaDive',      // biopharmadive (1)
        'MDPI',               // mdpi (2)
        'BioSpace',           // biospace (3)
        'BioWorld',           // bioworld (4)
        'CEN',                // cen (5)
        'EndpointsNews',      // endpoints (6)
        'EuropeanPharmaceuticalReview', // europeanpharmaceuticalreview (7)
        'FierceBiotech',      // fiercebiotech (8)
        'FiercePharma',       // fiercepharma (9)
        'GEN',                // gen (10)
        'NatureBiotech',      // naturebiotech (11)
        'BioSpectrum',        // biospectrum (12)
        'PharmaLetter',       // pharmaletter (13)
        'PharmaVoice',        // pharmavoice (14)
        'PharmaceuticalTech', // pharmaceuticaltech (15)
        'PharmaTimes',        // pharmatimes (16)
        'Pharmaphorum',       // pharmaphorum (17)
        'PMLive',             // pmlive (18)
        'PRNewswire',         // prnewswire (19)
        'STATNews',           // statnews (20)
        'TheScientist',       // thescientist (21)
        'FDA',                // fda (22)
        'BusinessWire',       // businesswire (23)
        'BusinessWeekly'      // businessweekly (24)
    ];

    scrapers.forEach((scraper, index) => {
        if (index < sourceNames.length) {
            scraperSourceNameMap.set(scraper, sourceNames[index]);
        }
    });
}

/**
 * Get source name from a scraper module
 */
function getSourceName(scraper) {
    // Try to get from map first
    if (scraperSourceNameMap.has(scraper)) {
        return scraperSourceNameMap.get(scraper);
    }

    // Fallback: try to get from scraper properties
    if (scraper.SOURCE_NAME) return scraper.SOURCE_NAME;
    if (scraper.name) return scraper.name;

    return 'unknown';
}

/**
 * Filter scrapers that don't have CSV files yet
 */
function filterScrapersWithoutCSV(scrapers) {
    const scrapersToRun = [];
    const scrapersSkipped = [];

    for (const scraper of scrapers) {
        const sourceName = getSourceName(scraper);
        const hasCSV = csvFileExists(sourceName);

        if (hasCSV) {
            scrapersSkipped.push({ name: sourceName, reason: 'CSV file already exists' });
        } else {
            scrapersToRun.push(scraper);
        }
    }

    return { scrapersToRun, scrapersSkipped };
}

async function runAllScrapers() {
    if (isScraping) {
        console.log('Scraping already in progress, skipping...');
        return { results: [], totalSaved: 0 };
    }

    isScraping = true;

    // Initialize source name mapping
    initializeSourceNameMap();

    console.log('--- CHECKING FOR EXISTING CSV FILES ---');

    // Filter scrapers - only run those without CSV files
    const { scrapersToRun, scrapersSkipped } = filterScrapersWithoutCSV(scrapers);

    if (scrapersSkipped.length > 0) {
        console.log(`\n⏭️  Skipping ${scrapersSkipped.length} scrapers (CSV files already exist):`);
        scrapersSkipped.forEach(s => console.log(`   - ${s.name}`));
    }

    if (scrapersToRun.length === 0) {
        console.log('\n✅ All scrapers already have CSV files. Nothing to scrape.');
        isScraping = false;
        return { results: [], totalSaved: 0, skipped: scrapersSkipped };
    }

    console.log(`\n--- STARTING ${scrapersToRun.length} SCRAPERS IN PARALLEL ---`);
    console.log(`Running scrapers concurrently...\n`);

    const startTime = Date.now();
    let totalSaved = 0;
    const results = [];

    // Run only scrapers without CSV files in parallel using Promise.allSettled
    const scraperPromises = scrapersToRun.map(async (scraper) => {
        const scraperName = getSourceName(scraper);
        try {
            console.log(`[${scraperName}] Starting...`);
            const savedCount = await scraper.run();
            console.log(`[${scraperName}] ✓ Finished, saved ${savedCount || 0} items.`);
            return {
                name: scraperName,
                status: 'success',
                savedCount: savedCount || 0
            };
        } catch (err) {
            console.error(`[${scraperName}] ✗ Error:`, err.message);
            return {
                name: scraperName,
                status: 'error',
                error: err.message
            };
        }
    });

    // Wait for all scrapers to complete (or fail)
    const scraperResults = await Promise.allSettled(scraperPromises);

    // Process results
    scraperResults.forEach((outcome, index) => {
        if (outcome.status === 'fulfilled') {
            const result = outcome.value;
            results.push(result);
            if (result.status === 'success') {
                totalSaved += result.savedCount;
            }
        } else {
            const scraperName = scrapers[index]?.name || scrapers[index]?.SOURCE_NAME || 'unknown';
            console.error(`[${scraperName}] ✗ Promise rejected:`, outcome.reason);
            results.push({
                name: scraperName,
                status: 'error',
                error: outcome.reason?.message || 'Unknown error'
            });
        }
    });

    const elapsedTime = Math.floor((Date.now() - startTime) / 1000);
    const minutes = Math.floor(elapsedTime / 60);
    const seconds = elapsedTime % 60;

    console.log(`\n--- ALL SCRAPERS COMPLETE ---`);
    console.log(`Total Saved: ${totalSaved} articles`);
    console.log(`Execution Time: ${minutes}m ${seconds}s`);
    console.log(`Successful: ${results.filter(r => r.status === 'success').length}/${scrapersToRun.length} scrapers`);
    if (scrapersSkipped.length > 0) {
        console.log(`Skipped: ${scrapersSkipped.length} scrapers (already have CSV files)\n`);
    } else {
        console.log();
    }

    isScraping = false;
    return { results, totalSaved, skipped: scrapersSkipped };
}

// Schedule RSS feed scraping (three times per week: Sunday, Wednesday, Friday at 2 AM)
// Cron format: second minute hour day month dayOfWeek
// 0 = Sunday, 3 = Wednesday, 5 = Friday
cron.schedule('0 0 2 * * 0,3,5', () => {
    console.log('\n=== SCHEDULED RSS FEED SCRAPE TRIGGERED ===');
    console.log(`Time: ${new Date().toISOString()}`);
    runAllScrapers();
}, {
    scheduled: true,
    timezone: "America/New_York"
});

console.log('RSS feed scraper scheduled: Sunday, Wednesday, Friday at 2:00 AM');

app.get('/', (req, res) => {
    res.send(`
        <div style="font-family: Arial, sans-serif; text-align: center; margin-top: 50px;">
            <h1 style="color: #4F46E5;">News Scraper Backend Running on Port ${PORT}</h1>
            <p>Scrapers run automatically 3 times per week (Sunday, Wednesday, Friday at 2 AM).</p>
            <div style="margin-top: 20px;">
                <a href="/scrape-all" style="background-color: #4F46E5; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold; margin: 5px;">
                    Trigger Manual Scrape
                </a>
                <a href="/status" style="background-color: #10B981; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold; margin: 5px;">
                    View Status
                </a>
                <a href="/data" style="background-color: #F59E0B; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold; margin: 5px;">
                    View Data Files
                </a>
            </div>
        </div>
    `);
});

app.get('/scrape-all', async (req, res) => {
    console.log('Manual scrape triggered');
    const { results, totalSaved } = await runAllScrapers();
    res.json({
        message: 'Scraping complete',
        totalSaved,
        results,
        timestamp: new Date().toISOString()
    });
});

app.get('/status', async (req, res) => {
    try {
        const dataDir = path.join(__dirname, 'data');
        const stats = {};

        if (fs.existsSync(dataDir)) {
            const files = fs.readdirSync(dataDir).filter(f => f.endsWith('.csv'));

            for (const file of files) {
                const sourceName = file.replace('_articles.csv', '').replace(/_/g, ' ');
                const fullPath = path.join(dataDir, file);
                const fileStats = fs.statSync(fullPath);

                // Count lines (subtract 1 for header)
                const content = fs.readFileSync(fullPath, 'utf8');
                const lineCount = content.split('\n').filter(l => l.trim()).length - 1;

                stats[sourceName] = {
                    totalArticles: Math.max(0, lineCount),
                    fileSize: fileStats.size,
                    lastModified: fileStats.mtime,
                    filePath: fullPath
                };
            }
        }

        res.json({
            message: 'CSV file statistics',
            sources: stats,
            totalSources: Object.keys(stats).length,
            isScraping: isScraping
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/data', (req, res) => {
    try {
        const dataDir = path.join(__dirname, 'data');

        if (!fs.existsSync(dataDir)) {
            return res.json({ message: 'No data directory found', files: [] });
        }

        const files = fs.readdirSync(dataDir)
            .filter(f => f.endsWith('.csv'))
            .map(file => {
                const fullPath = path.join(dataDir, file);
                const stats = fs.statSync(fullPath);
                return {
                    filename: file,
                    size: stats.size,
                    lastModified: stats.mtime,
                    downloadUrl: `/data/download/${file}`
                };
            });

        res.json({
            message: 'Available data files',
            files,
            dataDirectory: dataDir
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/data/download/:filename', (req, res) => {
    try {
        const filename = req.params.filename;
        const filePath = path.join(__dirname, 'data', filename);

        if (!fs.existsSync(filePath)) {
            return res.status(404).json({ error: 'File not found' });
        }

        res.download(filePath, filename, (err) => {
            if (err) {
                console.error('Error downloading file:', err);
                res.status(500).json({ error: 'Error downloading file' });
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
    console.log('Scrapers will run automatically 3 times per week (Sunday, Wednesday, Friday at 2 AM)');
    console.log('Use /scrape-all endpoint to trigger manual scrape');
});
