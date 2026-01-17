#!/usr/bin/env node
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Run fetching for a specific year
 */
function runYear(year) {
    return new Promise((resolve, reject) => {
        const startDate = `${year}-01-01`;
        const endDate = `${year}-12-31`;

        console.log(`\n>>> BATCH RUNNER: Processing Year ${year} (${startDate} to ${endDate})`);

        const child = spawn('node', [
            path.join(__dirname, 'index.js'),
            `--startDate=${startDate}`,
            `--endDate=${endDate}`,
            // Pass through other useful flags if needed, e.g. --sequential defaults to true in index.js
        ], {
            stdio: 'inherit' // Pipe output to parent console
        });

        child.on('close', (code) => {
            if (code === 0) {
                console.log(`>>> BATCH RUNNER: Year ${year} completed successfully.`);
                resolve();
            } else {
                console.error(`>>> BATCH RUNNER: Year ${year} failed with code ${code}.`);
                reject(new Error(`Process exited with code ${code}`));
            }
        });
    });
}

/**
 * Sleep helper
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Main batch loop
 */
async function runBatch(startYear, endYear) {
    console.log(`\n==================================================`);
    console.log(`    STOCK DATA BATCH RUNNER`);
    console.log(`    Processing: ${startYear} to ${endYear}`);
    console.log(`==================================================\n`);

    for (let year = startYear; year <= endYear; year++) {
        try {
            await runYear(year);

            // Cooldown between years to be nice to APIs
            if (year < endYear) {
                console.log(`\n... Cooldown: Waiting 30 seconds before next year to reset rate limits ...`);
                await sleep(30000);
            }
        } catch (error) {
            console.error(`Error processing ${year}:`, error.message);
            // Decide whether to continue or stop. Continuing is usually better for batch jobs.
            console.log(`Skipping to next year...`);
        }
    }

    console.log(`\n==================================================`);
    console.log(`    BATCH RUN COMPLETED`);
    console.log(`==================================================\n`);
}

// CLI usage: node batch_runner.js 2000 2025
const args = process.argv.slice(2);
const startYear = parseInt(args[0]) || 2000;
const endYear = parseInt(args[1]) || 2025;

runBatch(startYear, endYear);
