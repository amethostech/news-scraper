import express from 'express';
import connectDB from './config/db.js';
import { scrapers } from './scrapers/index.js';
import { sleep } from './utils/common.js';

const PORT = 3000;
const SCRAPE_DELAY_MS = 5000;

const app = express();

// Connect to Database
connectDB();

async function runAllScrapers() {
    console.log('--- STARTING ALL SCRAPERS ---');
    let totalSaved = 0;
    const results = [];

    for (const scraper of scrapers) {
        try {
            const savedCount = await scraper.run();
            console.log(`Scraper ${scraper.name || 'unknown'} finished, saved ${savedCount} items.`);
            results.push({
                savedCount
            });
            totalSaved += savedCount;
        } catch (err) {
            console.error('Error running scraper:', err);
        }
        await sleep(SCRAPE_DELAY_MS);
    }
    console.log(`--- ALL SCRAPERS COMPLETE. Total Saved: ${totalSaved} ---`);
    return { results, totalSaved };
}

app.get('/', (req, res) => {
    res.send(`
        <div style="font-family: Arial, sans-serif; text-align: center; margin-top: 50px;">
            <h1 style="color: #4F46E5;">News Scraper Backend Running on Port ${PORT}</h1>
            <p>Scrapers are running in the background.</p>
            <a href="/scrape-all" style="background-color: #4F46E5; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold;">
                Trigger Manual Scrape
            </a>
        </div>
    `);
});

app.get('/scrape-all', async (req, res) => {
    console.log('Manual scrape triggered');
    const { results, totalSaved } = await runAllScrapers();
    res.json({ message: 'Scraping complete', totalSaved, results });
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
    runAllScrapers();
});