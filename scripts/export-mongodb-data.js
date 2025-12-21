/**
 * Export MongoDB data to JSON/CSV files
 * 
 * Usage:
 *   node scripts/export-mongodb-data.js                    # Export all to JSON
 *   node scripts/export-mongodb-data.js --format csv       # Export to CSV
 *   node scripts/export-mongodb-data.js --source PRNewswire # Export specific source
 */

import dotenv from 'dotenv';
import mongoose from 'mongoose';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import connectDB from '../config/db.js';
import Article from '../models/Article.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const args = process.argv.slice(2);
const format = args.includes('--format') && args[args.indexOf('--format') + 1] === 'csv' ? 'csv' : 'json';
const sourceFilter = args.includes('--source') ? args[args.indexOf('--source') + 1] : null;

const EXPORT_DIR = path.join(__dirname, '..', 'exports');

// Ensure export directory exists
if (!fs.existsSync(EXPORT_DIR)) {
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
}

/**
 * Convert article to CSV row
 */
function articleToCSVRow(article) {
    const escapeCSV = (field) => {
        if (field === null || field === undefined) return '';
        const str = String(field);
        if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
    };

    const dateStr = article.date ? new Date(article.date).toISOString().split('T')[0] : '';
    
    return [
        escapeCSV(article.source || ''),
        escapeCSV(article.title || ''),
        escapeCSV(dateStr),
        escapeCSV(article.author || ''),
        escapeCSV(article.link || ''),
        escapeCSV(article.extract || '')
    ].join(',');
}

/**
 * Main export function
 */
async function exportData() {
    console.log('Connecting to MongoDB...');
    
    try {
        await connectDB();
        console.log('✓ Connected to MongoDB\n');
    } catch (error) {
        console.error('❌ Failed to connect to MongoDB:', error.message);
        process.exit(1);
    }

    try {
        // Build query
        const query = sourceFilter ? { source: sourceFilter } : {};
        
        console.log('Fetching articles from MongoDB...');
        if (sourceFilter) {
            console.log(`   Filter: source = "${sourceFilter}"`);
        }
        
        const articles = await Article.find(query).lean();
        console.log(`   Found ${articles.length} articles\n`);

        if (articles.length === 0) {
            console.log('No articles found to export.');
            await mongoose.disconnect();
            process.exit(0);
        }

        // Export based on format
        if (format === 'csv') {
            // CSV Export
            const filename = sourceFilter 
                ? `articles_${sourceFilter.toLowerCase()}_${Date.now()}.csv`
                : `articles_all_${Date.now()}.csv`;
            const filepath = path.join(EXPORT_DIR, filename);

            // CSV Header
            const header = 'NewsSite,Name,Date of publishing the Article,Author,Weblink,Summary\n';
            const rows = articles.map(articleToCSVRow).join('\n');
            
            fs.writeFileSync(filepath, header + rows, 'utf8');
            
            console.log(`✓ Exported ${articles.length} articles to CSV`);
            console.log(`  File: ${filepath}`);
            console.log(`  Size: ${(fs.statSync(filepath).size / 1024 / 1024).toFixed(2)} MB`);
        } else {
            // JSON Export
            const filename = sourceFilter 
                ? `articles_${sourceFilter.toLowerCase()}_${Date.now()}.json`
                : `articles_all_${Date.now()}.json`;
            const filepath = path.join(EXPORT_DIR, filename);

            fs.writeFileSync(filepath, JSON.stringify(articles, null, 2), 'utf8');
            
            console.log(`✓ Exported ${articles.length} articles to JSON`);
            console.log(`  File: ${filepath}`);
            console.log(`  Size: ${(fs.statSync(filepath).size / 1024 / 1024).toFixed(2)} MB`);
        }

        // Also create a summary
        const summary = {
            exportedAt: new Date().toISOString(),
            totalArticles: articles.length,
            format: format,
            sourceFilter: sourceFilter || 'all',
            sources: {}
        };

        // Count by source
        articles.forEach(article => {
            const source = article.source || 'Unknown';
            summary.sources[source] = (summary.sources[source] || 0) + 1;
        });

        const summaryFile = path.join(EXPORT_DIR, `export_summary_${Date.now()}.json`);
        fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2), 'utf8');
        
        console.log('\nExport Summary:');
        console.log(JSON.stringify(summary, null, 2));
        console.log(`\n✓ Summary saved to: ${summaryFile}`);

        await mongoose.disconnect();
        console.log('\n✓ Export complete!');
        
    } catch (error) {
        console.error('Error exporting data:', error);
        await mongoose.disconnect();
        process.exit(1);
    }
}

exportData();

