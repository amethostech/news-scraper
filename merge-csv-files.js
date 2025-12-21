/**
 * Merge all CSV files into a single CSV file
 * 
 * Output format:
 * - Amethos Id → left blank
 * - Date → mapped from date
 * - Source → mapped from source
 * - News link → mapped from link
 * - Headline → left blank
 * - Body/abstract/extract → mapped from extract
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, 'data');
const OUTPUT_FILE = path.join(__dirname, 'data', 'merged_articles.csv');

/**
 * Parse CSV line (handles quoted fields with commas)
 */
function parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        
        if (char === '"') {
            if (inQuotes && line[i + 1] === '"') {
                // Escaped quote
                current += '"';
                i++; // Skip next quote
            } else {
                // Toggle quote state
                inQuotes = !inQuotes;
            }
        } else if (char === ',' && !inQuotes) {
            // Field separator
            result.push(current);
            current = '';
        } else {
            current += char;
        }
    }
    
    result.push(current); // Add last field
    return result;
}

/**
 * Escape CSV field
 */
function escapeCSVField(field) {
    if (field === null || field === undefined) {
        return '';
    }
    const str = String(field);
    // If field contains comma, quote, or newline, wrap in quotes and escape quotes
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
}

/**
 * Format date to YYYY-MM-DD
 */
function formatDate(dateValue) {
    if (!dateValue) return '';
    
    // If it's already a string in YYYY-MM-DD format
    if (typeof dateValue === 'string' && /^\d{4}-\d{2}-\d{2}/.test(dateValue)) {
        return dateValue.split('T')[0].split(' ')[0]; // Get just the date part
    }
    
    // Try to parse as Date
    const date = new Date(dateValue);
    if (!isNaN(date.getTime())) {
        return date.toISOString().split('T')[0];
    }
    
    return String(dateValue);
}

/**
 * Parse CSV content handling multi-line fields
 */
function parseCSVContent(content) {
    const rows = [];
    let currentRow = [];
    let currentField = '';
    let inQuotes = false;
    
    for (let i = 0; i < content.length; i++) {
        const char = content[i];
        const nextChar = content[i + 1];
        
        if (char === '"') {
            if (inQuotes && nextChar === '"') {
                // Escaped quote
                currentField += '"';
                i++; // Skip next quote
            } else {
                // Toggle quote state
                inQuotes = !inQuotes;
            }
        } else if (char === ',' && !inQuotes) {
            // Field separator
            currentRow.push(currentField);
            currentField = '';
        } else if ((char === '\n' || char === '\r') && !inQuotes) {
            // Row separator (only if not in quotes)
            if (char === '\n' || (char === '\r' && nextChar !== '\n')) {
                currentRow.push(currentField);
                if (currentRow.length > 0 && currentRow.some(f => f.trim())) {
                    rows.push(currentRow);
                }
                currentRow = [];
                currentField = '';
            }
            // Skip \r before \n
            if (char === '\r' && nextChar === '\n') {
                i++; // Skip the \n
            }
        } else {
            currentField += char;
        }
    }
    
    // Add last field and row
    if (currentField || currentRow.length > 0) {
        currentRow.push(currentField);
        if (currentRow.length > 0 && currentRow.some(f => f.trim())) {
            rows.push(currentRow);
        }
    }
    
    return rows;
}

/**
 * Process CSV file and write to output stream (memory efficient)
 */
function processCSVFile(filePath, outputStream, seenLinks) {
    const content = fs.readFileSync(filePath, 'utf8');
    const rows = parseCSVContent(content);
    
    if (rows.length <= 1) {
        return 0; // Only headers or empty
    }
    
    const header = rows[0].map(h => h.toLowerCase().trim()).join(',');
    const isMongoDBFormat = header.includes('_id') && header.includes('source');
    const isStandardFormat = header.includes('newssite') || header.includes('news site');
    
    let processed = 0;
    
    for (let i = 1; i < rows.length; i++) {
        const columns = rows[i];
        if (columns.length === 0) continue;
        
        let article = null;
        
        if (isMongoDBFormat) {
            // Format: _id,author,date,extract,link,source,scrapedAt,__v
            // Index:  0    1      2    3      4    5      6        7
            if (columns.length >= 6) {
                article = {
                    date: (columns[2] || '').trim(),
                    source: (columns[5] || '').trim(),
                    link: (columns[4] || '').trim(),
                    extract: (columns[3] || '').trim()
                };
            }
        } else if (isStandardFormat) {
            // Format: NewsSite,Name,Date of publishing the Article,Author,Weblink,Summary
            // Index:  0        1    2                           3      4        5
            if (columns.length >= 6) {
                article = {
                    date: (columns[2] || '').trim(),
                    source: (columns[0] || '').trim(),
                    link: (columns[4] || '').trim(),
                    extract: (columns[5] || '').trim()
                };
            }
        } else {
            // Try to detect format by column count and content
            // Assume: source, title, date, author, link, extract
            if (columns.length >= 6) {
                article = {
                    date: (columns[2] || '').trim(),
                    source: (columns[0] || '').trim(),
                    link: (columns[4] || '').trim(),
                    extract: (columns[5] || '').trim()
                };
            }
        }
        
        // Write immediately if unique and has required fields
        if (article && article.link && article.link.length > 0 && !seenLinks.has(article.link)) {
            seenLinks.add(article.link);
            const row = [
                '', // Amethos Id - left blank
                escapeCSVField(formatDate(article.date)),
                escapeCSVField(article.source || ''),
                escapeCSVField(article.link || ''),
                '', // Headline - left blank
                escapeCSVField(article.extract || '')
            ].join(',') + '\n';
            
            outputStream.write(row);
            processed++;
        }
    }
    
    return processed;
}

/**
 * Main function to merge all CSV files (memory efficient streaming)
 */
function mergeAllCSVFiles() {
    console.log('Starting CSV merge process...\n');
    
    // Get all CSV files
    const files = fs.readdirSync(DATA_DIR)
        .filter(file => file.endsWith('.csv') && file !== 'merged_articles.csv')
        .map(file => path.join(DATA_DIR, file));
    
    console.log(`Found ${files.length} CSV files to merge:`);
    files.forEach(file => console.log(`  - ${path.basename(file)}`));
    console.log('');
    
    // Create output file with headers
    const headers = ['Amethos Id', 'Date', 'Source', 'News link', 'Headline', 'Body/abstract/extract'];
    const headerRow = headers.join(',') + '\n';
    
    // Open output stream
    const outputStream = fs.createWriteStream(OUTPUT_FILE, { encoding: 'utf8' });
    outputStream.write(headerRow);
    
    // Track seen links to avoid duplicates
    const seenLinks = new Set();
    let totalProcessed = 0;
    let totalUnique = 0;
    
    // Process each file
    for (const file of files) {
        try {
            console.log(`Processing: ${path.basename(file)}...`);
            const processed = processCSVFile(file, outputStream, seenLinks);
            totalProcessed += processed;
            totalUnique += processed;
            console.log(`  ✓ Processed ${processed} unique articles`);
        } catch (error) {
            console.error(`  ✗ Error processing ${path.basename(file)}: ${error.message}`);
        }
    }
    
    // Close output stream
    outputStream.end();
    
    console.log(`\n✓ Successfully merged ${totalUnique} unique articles into:`);
    console.log(`  ${OUTPUT_FILE}`);
    
    // Wait for file to be written, then get size
    setTimeout(() => {
        if (fs.existsSync(OUTPUT_FILE)) {
            const fileSize = fs.statSync(OUTPUT_FILE).size;
            console.log(`\nFile size: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);
        }
    }, 1000);
}

// Run the merge
try {
    mergeAllCSVFiles();
    console.log('\n✅ Merge completed successfully!');
} catch (error) {
    console.error('\n❌ Error during merge:', error);
    process.exit(1);
}
