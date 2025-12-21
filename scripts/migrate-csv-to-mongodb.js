/**
 * Migration script to migrate CSV data to MongoDB
 * 
 * Reads all CSV files from data/ directory and imports them into MongoDB
 * Uses upsert to avoid duplicates
 * 
 * Usage: node scripts/migrate-csv-to-mongodb.js
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { connectMongoDB, saveArticles } from '../utils/mongoWriter.js';
import { readArticlesFromCSV, getCSVFilePath } from '../utils/csvWriter.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, '..', 'data');

/**
 * Get all CSV files from data directory
 */
function getCSVFiles() {
    if (!fs.existsSync(DATA_DIR)) {
        return [];
    }
    
    const files = fs.readdirSync(DATA_DIR);
    return files
        .filter(file => file.endsWith('_articles.csv'))
        .map(file => {
            // Extract source name from filename (e.g., "fiercebiotech_articles.csv" -> "fiercebiotech")
            const sourceName = file.replace('_articles.csv', '');
            return {
                filename: file,
                sourceName: sourceName,
                filePath: path.join(DATA_DIR, file)
            };
        });
}

/**
 * Parse date string to Date object
 */
function parseDate(dateStr) {
    if (!dateStr || dateStr.trim() === '') {
        return null;
    }
    
    // Try ISO format first (YYYY-MM-DD)
    const isoMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (isoMatch) {
        const date = new Date(isoMatch[1], parseInt(isoMatch[2]) - 1, isoMatch[3]);
        if (!isNaN(date.getTime())) {
            return date;
        }
    }
    
    // Try other formats
    const date = new Date(dateStr);
    if (!isNaN(date.getTime())) {
        return date;
    }
    
    return null;
}

/**
 * Migrate a single CSV file to MongoDB
 */
async function migrateCSVFile(csvFile) {
    console.log(`\n📄 Processing ${csvFile.filename}...`);
    
    try {
        // Read articles from CSV
        const articles = readArticlesFromCSV(csvFile.sourceName);
        
        if (articles.length === 0) {
            console.log(`   ⚠️  No articles found in ${csvFile.filename}`);
            return { source: csvFile.sourceName, total: 0, saved: 0, failed: 0 };
        }
        
        console.log(`   Found ${articles.length} articles`);
        
        // Transform articles to match Article model
        const transformedArticles = articles.map(article => ({
            source: article.source || csvFile.sourceName,
            title: article.title || article.name || '',
            author: article.author || '',
            date: parseDate(article.date),
            link: article.link || '',
            extract: article.extract || article.summary || '',
            scrapedAt: new Date() // Set current time as scrapedAt
        })).filter(article => article.link); // Filter out articles without links
        
        if (transformedArticles.length === 0) {
            console.log(`   ⚠️  No valid articles (with links) found`);
            return { source: csvFile.sourceName, total: articles.length, saved: 0, failed: 0 };
        }
        
        console.log(`   Migrating ${transformedArticles.length} articles to MongoDB...`);
        
        // Save to MongoDB
        const result = await saveArticles(transformedArticles);
        
        console.log(`   ✓ Saved: ${result.saved}, Failed: ${result.failed}`);
        
        return {
            source: csvFile.sourceName,
            total: articles.length,
            saved: result.saved,
            failed: result.failed
        };
        
    } catch (error) {
        console.error(`   ✗ Error migrating ${csvFile.filename}:`, error.message);
        return {
            source: csvFile.sourceName,
            total: 0,
            saved: 0,
            failed: 0,
            error: error.message
        };
    }
}

/**
 * Main migration function
 */
async function main() {
    console.log(`

    `);
    
    // Connect to MongoDB
    console.log('Connecting to MongoDB...');
    console.log('   (Using MONGO_URI from .env or default: mongodb://localhost:27017/news-scraper)');
    console.log('');
    
    const connected = await connectMongoDB();
    
    if (!connected) {
        console.error('❌ Failed to connect to MongoDB.');
        console.error('');
        console.error('Please check:');
        console.error('   1. MongoDB is running (local) or MongoDB Atlas cluster is active');
        console.error('   2. MONGO_URI is set correctly in .env file');
        console.error('   3. Network access is configured (for MongoDB Atlas)');
        console.error('   4. Username and password are correct in connection string');
        console.error('');
        console.error('For MongoDB Atlas:');
        console.error('   - Get connection string from Atlas dashboard');
        console.error('   - Format: mongodb+srv://username:password@cluster.mongodb.net/news-scraper');
        console.error('   - Add to .env file: MONGO_URI=your_connection_string');
        console.error('');
        process.exit(1);
    }
    
    console.log('✓ Connected to MongoDB\n');
    
    // Get all CSV files
    const csvFiles = getCSVFiles();
    
    if (csvFiles.length === 0) {
        console.log('⚠️  No CSV files found in data/ directory');
        process.exit(0);
    }
    
    console.log(`Found ${csvFiles.length} CSV file(s) to migrate:\n`);
    csvFiles.forEach((file, index) => {
        console.log(`  ${index + 1}. ${file.filename}`);
    });
    
    // Migrate each file
    const results = [];
    for (const csvFile of csvFiles) {
        const result = await migrateCSVFile(csvFile);
        results.push(result);
    }
    
    // Print summary
    console.log(`
    `);
    
    let totalArticles = 0;
    let totalSaved = 0;
    let totalFailed = 0;
    
    results.forEach(result => {
        totalArticles += result.total;
        totalSaved += result.saved;
        totalFailed += result.failed;
        
        const status = result.error ? '✗' : '✓';
        console.log(`${status} ${result.source.padEnd(25)} ${result.total.toString().padStart(6)} articles → ${result.saved.toString().padStart(6)} saved, ${result.failed.toString().padStart(4)} failed`);
    });
    
    console.log(`
───────────────────────────────────────────────────────────────
Total: ${totalArticles} articles processed
      ${totalSaved} saved to MongoDB
      ${totalFailed} failed
───────────────────────────────────────────────────────────────
    `);
    
    console.log('✓ Migration complete!');
    
    process.exit(0);
}

// Run migration
main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});

