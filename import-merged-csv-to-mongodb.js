/**
 * Import merged_articles.csv into MongoDB
 * 
 * This script reads the merged CSV file and imports all articles into MongoDB
 * Format: Amethos Id, Date, Source, News link, Headline, Body/abstract/extract
 */

import mongoose from 'mongoose';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import connectDB from './config/db.js';
import Article from './models/Article.js';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MERGED_CSV_FILE = path.join(__dirname, 'data', 'merged_articles.csv');
const BATCH_SIZE = 1000; // Process in batches to avoid memory issues

/**
 * Parse CSV line handling quoted fields
 */
function parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        
        if (char === '"') {
            if (inQuotes && line[i + 1] === '"') {
                current += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (char === ',' && !inQuotes) {
            result.push(current);
            current = '';
        } else {
            current += char;
        }
    }
    result.push(current);
    return result;
}

/**
 * Format date string to Date object
 */
function parseDate(dateStr) {
    if (!dateStr || !dateStr.trim()) return null;
    
    // Try to parse YYYY-MM-DD format
    if (/^\d{4}-\d{2}-\d{2}/.test(dateStr)) {
        const date = new Date(dateStr.split('T')[0].split(' ')[0]);
        if (!isNaN(date.getTime())) {
            return date;
        }
    }
    
    // Try general date parsing
    const date = new Date(dateStr);
    if (!isNaN(date.getTime())) {
        return date;
    }
    
    return null;
}

/**
 * Import merged CSV to MongoDB
 */
async function importMergedCSV() {
    console.log('📥 Starting MongoDB Import from merged_articles.csv\n');
    console.log('=' .repeat(60));
    
    // Connect to MongoDB
    try {
        console.log('Connecting to MongoDB...');
        await connectDB();
        console.log('✓ MongoDB connected\n');
    } catch (error) {
        console.error('❌ Failed to connect to MongoDB:', error.message);
        console.log('\nPlease check your MONGO_URI in .env file');
        process.exit(1);
    }
    
    // Check if file exists
    if (!fs.existsSync(MERGED_CSV_FILE)) {
        console.error(`❌ File not found: ${MERGED_CSV_FILE}`);
        process.exit(1);
    }
    
    console.log(`Reading ${MERGED_CSV_FILE}...`);
    const content = fs.readFileSync(MERGED_CSV_FILE, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());
    
    if (lines.length <= 1) {
        console.log('❌ No data found in CSV file (only header or empty)');
        process.exit(1);
    }
    
    console.log(`Found ${lines.length - 1} articles (excluding header)\n`);
    
    // Parse CSV
    const articles = [];
    let skipped = 0;
    
    console.log('Parsing CSV file...');
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        try {
            const columns = parseCSVLine(line);
            
            // Format: Amethos Id, Date, Source, News link, Headline, Body/abstract/extract
            // Index:   0           1     2       3          4        5
            if (columns.length >= 6) {
                const link = columns[3]?.trim().replace(/^"/, '').replace(/"$/, '');
                const extract = columns[5]?.trim().replace(/^"/, '').replace(/"$/, '');
                
                if (link && extract && extract.length > 10) {
                    articles.push({
                        link: link,
                        source: columns[2]?.trim().replace(/^"/, '').replace(/"$/, '') || 'Unknown',
                        title: '', // Headline is blank in merged CSV
                        author: '', // Not in merged CSV format
                        extract: extract,
                        date: parseDate(columns[1]?.trim().replace(/^"/, '').replace(/"$/, '')),
                        scrapedAt: new Date()
                    });
                } else {
                    skipped++;
                }
            } else {
                skipped++;
            }
        } catch (error) {
            skipped++;
            if (i % 10000 === 0) {
                console.warn(`  Warning: Error parsing line ${i}: ${error.message}`);
            }
        }
        
        if (i % 10000 === 0) {
            console.log(`  Parsed ${i}/${lines.length - 1} lines...`);
        }
    }
    
    console.log(`\n✓ Parsed ${articles.length} valid articles`);
    if (skipped > 0) {
        console.log(`  Skipped ${skipped} invalid/malformed rows\n`);
    }
    
    // Check existing articles in MongoDB
    console.log('Checking for existing articles in MongoDB...');
    let existingLinks = new Set();
    try {
        const existing = await Article.find({}, { link: 1 }).lean();
        existingLinks = new Set(existing.map(a => a.link).filter(Boolean));
        console.log(`  Found ${existingLinks.size} existing articles in MongoDB\n`);
    } catch (error) {
        console.warn(`  Warning: Could not check existing articles: ${error.message}\n`);
        existingLinks = new Set(); // Use empty set if check fails
    }
    
    // Filter out duplicates
    const newArticles = articles.filter(article => !existingLinks.has(article.link));
    console.log(`Importing ${newArticles.length} new articles (${articles.length - newArticles.length} already exist)\n`);
    
    if (newArticles.length === 0) {
        console.log('✅ All articles already exist in MongoDB. Nothing to import.');
        await mongoose.connection.close();
        process.exit(0);
    }
    
    // Import in batches
    console.log(`Importing in batches of ${BATCH_SIZE}...\n`);
    let imported = 0;
    let failed = 0;
    const startTime = Date.now();
    
    for (let i = 0; i < newArticles.length; i += BATCH_SIZE) {
        const batch = newArticles.slice(i, i + BATCH_SIZE);
        
        try {
            // Use bulkWrite for efficiency
            const operations = batch.map(article => ({
                updateOne: {
                    filter: { link: article.link },
                    update: { $set: article },
                    upsert: true
                }
            }));
            
            await Article.bulkWrite(operations, { ordered: false });
            imported += batch.length;
            
            const progress = ((i + batch.length) / newArticles.length * 100).toFixed(1);
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
            console.log(`  [${progress}%] Imported ${i + batch.length}/${newArticles.length} articles (${elapsed}s elapsed)`);
        } catch (error) {
            failed += batch.length;
            console.error(`  ✗ Error importing batch ${Math.floor(i / BATCH_SIZE) + 1}: ${error.message}`);
        }
    }
    
    const totalTime = ((Date.now() - startTime) / 1000).toFixed(0);
    
    console.log('\n' + '=' .repeat(60));
    console.log('📊 Import Summary:');
    console.log(`  Total articles in CSV: ${articles.length}`);
    console.log(`  Already in MongoDB: ${articles.length - newArticles.length}`);
    console.log(`  New articles imported: ${imported}`);
    console.log(`  Failed: ${failed}`);
    console.log(`  Time taken: ${totalTime} seconds`);
    console.log('=' .repeat(60));
    
    // Verify import
    try {
        const totalInDB = await Article.countDocuments();
        console.log(`\n✅ Total articles in MongoDB: ${totalInDB}`);
    } catch (error) {
        console.warn(`\n⚠️  Could not verify total count: ${error.message}`);
    }
    
    await mongoose.connection.close();
    console.log('\n✅ Import completed successfully!');
}

// Run import
importMergedCSV().catch(error => {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
});
