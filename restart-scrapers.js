/**
 * Restart script for EndpointsNews and FierceBiotech scrapers
 * 
 * This script:
 * 1. Stops any running scraper processes
 * 2. Cleans up browser instances
 * 3. Restarts the scrapers
 * 
 * Usage:
 *   node restart-scrapers.js
 */

import { exec } from 'child_process';
import { promisify } from 'util';
import * as fiercebiotech from './scrapers/fiercebiotech.js';
import * as endpoints from './scrapers/endpoints.js';

const execAsync = promisify(exec);

async function stopProcesses() {
    console.log('🛑 Stopping any running scrapers...\n');
    
    try {
        // Close browser instances
        if (fiercebiotech.closeBrowser) {
            await fiercebiotech.closeBrowser().catch(() => {});
        }
        if (endpoints.closeBrowser) {
            await endpoints.closeBrowser().catch(() => {});
        }
        console.log('✅ Closed browser instances');
    } catch (error) {
        // Ignore errors
    }
    
    try {
        // Kill node processes
        await execAsync('pkill -f "run-low-volume-sources" || true');
        await execAsync('pkill -f "node.*scraper" || true');
        console.log('✅ Stopped node processes');
    } catch (error) {
        // Ignore errors
    }
    
    // Wait a moment
    await new Promise(resolve => setTimeout(resolve, 2000));
    console.log('');
}

async function startScrapers() {
    console.log('🚀 Starting scrapers...\n');
    
    // Import and run the main script
    const { spawn } = await import('child_process');
    
    const scraperProcess = spawn('node', ['run-low-volume-sources.js', '--historical'], {
        stdio: 'inherit',
        shell: false
    });
    
    scraperProcess.on('error', (error) => {
        console.error('❌ Error starting scrapers:', error);
        process.exit(1);
    });
    
    scraperProcess.on('exit', (code) => {
        if (code !== 0) {
            console.error(`\n⚠️  Scrapers exited with code ${code}`);
        } else {
            console.log('\n✅ Scrapers completed successfully');
        }
        process.exit(code || 0);
    });
    
    // Handle Ctrl+C
    process.on('SIGINT', () => {
        console.log('\n\n⚠️  Stopping scrapers...');
        scraperProcess.kill('SIGINT');
        process.exit(0);
    });
}

async function main() {
    await stopProcesses();
    await startScrapers();
}

main().catch(error => {
    console.error('❌ Restart failed:', error);
    process.exit(1);
});

