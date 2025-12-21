import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, '..', 'data');
const MERGED_CSV_FILE = path.join(DATA_DIR, 'merged_articles.csv');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

/**
 * Escape CSV field (handles commas, quotes, newlines)
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
 * Generate a simple ID from the link (hash or use link itself)
 */
function generateId(link) {
    if (!link) return '';
    // Use a simple hash of the link as ID, or use link itself
    // For simplicity, we'll use a hash of the link
    let hash = 0;
    for (let i = 0; i < link.length; i++) {
        const char = link.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return Math.abs(hash).toString();
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
    if (dateValue instanceof Date) {
        return dateValue.toISOString().split('T')[0];
    }
    
    const date = new Date(dateValue);
    if (!isNaN(date.getTime())) {
        return date.toISOString().split('T')[0];
    }
    
    return String(dateValue);
}

/**
 * Convert article data to CSV row
 * Format: NewsSite, Name, Date of publishing the Article, Author, Weblink, Summary
 */
function articleToCSVRow(article) {
    const dateStr = formatDate(article.date);
    
    return [
        escapeCSVField(article.source || ''),
        escapeCSVField(article.title || article.name || ''),
        escapeCSVField(dateStr),
        escapeCSVField(article.author || ''),
        escapeCSVField(article.link || ''),
        escapeCSVField(article.extract || article.summary || '')
    ].join(',');
}

/**
 * Convert article data to merged CSV row format
 * Format: Amethos Id, Date, Source, News link, Headline, Body/abstract/extract
 */
function articleToMergedCSVRow(article) {
    return [
        '', // Amethos Id - left blank
        escapeCSVField(formatDate(article.date)),
        escapeCSVField(article.source || ''),
        escapeCSVField(article.link || ''),
        '', // Headline - left blank
        escapeCSVField(article.extract || article.summary || '')
    ].join(',');
}

/**
 * Get CSV file path for a source
 */
export function getCSVFilePath(sourceName) {
    const sanitizedSource = sourceName.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
    return path.join(DATA_DIR, `${sanitizedSource}_articles.csv`);
}

/**
 * Check if CSV file exists and has headers
 * Format: NewsSite, Name, Date of publishing the Article, Author, Weblink, Summary
 */
function ensureCSVHeaders(filePath) {
    const headers = ['NewsSite', 'Name', 'Date of publishing the Article', 'Author', 'Weblink', 'Summary'];
    const headerRow = headers.join(',') + '\n';
    
    if (!fs.existsSync(filePath)) {
        // Create new file with headers
        fs.writeFileSync(filePath, headerRow, 'utf8');
    } else {
        // Check if file has headers (read first line)
        const content = fs.readFileSync(filePath, 'utf8');
        if (!content.startsWith('NewsSite,')) {
            // File exists but no headers or old format, prepend headers
            fs.writeFileSync(filePath, headerRow + content, 'utf8');
        }
    }
}

/**
 * Ensure merged CSV file has headers
 * Format: Amethos Id, Date, Source, News link, Headline, Body/abstract/extract
 */
function ensureMergedCSVHeaders() {
    const headers = ['Amethos Id', 'Date', 'Source', 'News link', 'Headline', 'Body/abstract/extract'];
    const headerRow = headers.join(',') + '\n';
    
    if (!fs.existsSync(MERGED_CSV_FILE)) {
        // Create new file with headers
        fs.writeFileSync(MERGED_CSV_FILE, headerRow, 'utf8');
    } else {
        // Check if file has headers (read first line)
        const content = fs.readFileSync(MERGED_CSV_FILE, 'utf8');
        if (!content.startsWith('Amethos Id,')) {
            // File exists but no headers or old format, prepend headers
            fs.writeFileSync(MERGED_CSV_FILE, headerRow + content, 'utf8');
        }
    }
}

// Cache for merged CSV links to avoid reading file multiple times
let mergedCSVLinksCache = null;
let mergedCSVFileSize = 0;

// Write queue for merged CSV to prevent concurrent write conflicts
// This ensures that even when scrapers run in parallel, writes to merged CSV are serialized
// Each write operation chains onto the previous promise, creating a queue
let mergedCSVWriteQueue = Promise.resolve();

/**
 * Read existing links from merged CSV to avoid duplicates
 * Uses caching to avoid reading large files repeatedly
 */
function readMergedCSVLinks() {
    if (!fs.existsSync(MERGED_CSV_FILE)) {
        mergedCSVLinksCache = new Set();
        mergedCSVFileSize = 0;
        return mergedCSVLinksCache;
    }
    
    // Check if file has been modified (simple size check)
    const currentSize = fs.statSync(MERGED_CSV_FILE).size;
    if (mergedCSVLinksCache && currentSize === mergedCSVFileSize) {
        return mergedCSVLinksCache; // Return cached version
    }
    
    // Read file and cache results
    // For very large files, we'll use a streaming approach to read just the link column
    mergedCSVLinksCache = new Set();
    mergedCSVFileSize = currentSize;
    
    try {
        const content = fs.readFileSync(MERGED_CSV_FILE, 'utf8');
        
        // Simple regex to extract links from the CSV (column 4, index 3)
        // Pattern: look for http:// or https:// after the third comma (or after Date,Source,)
        const linkPattern = /(?:^|,)(?:[^,]*?,){3}(https?:\/\/[^,"\n]+)/gm;
        let match;
        while ((match = linkPattern.exec(content)) !== null) {
            const link = match[1].trim().replace(/^"/, '').replace(/"$/, '');
            if (link) {
                mergedCSVLinksCache.add(link);
                // Also add variations (with/without trailing slash)
                mergedCSVLinksCache.add(link.replace(/\/$/, ''));
                mergedCSVLinksCache.add(link + '/');
            }
        }
    } catch (error) {
        console.warn(`[CSV Writer] Error reading merged CSV for duplicate check: ${error.message}`);
        // Return empty set if we can't read the file
    }
    
    return mergedCSVLinksCache;
}

/**
 * Clear merged CSV links cache (call when file is modified externally)
 */
export function clearMergedCSVCache() {
    mergedCSVLinksCache = null;
    mergedCSVFileSize = 0;
}

/**
 * Read existing articles from CSV to check for duplicates
 * Format: NewsSite, Name, Date of publishing the Article, Author, Weblink, Summary
 * Weblink is at index 4
 */
export function readExistingLinks(filePath) {
    if (!fs.existsSync(filePath)) {
        return new Set();
    }
    
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());
    
    if (lines.length <= 1) {
        return new Set(); // Only headers or empty
    }
    
    // Skip header row, extract link (Weblink is at index 4 in new format)
    const links = new Set();
    for (let i = 1; i < lines.length; i++) {
        const columns = parseCSVLine(lines[i]);
        if (columns.length > 4 && columns[4]) {
            links.add(columns[4]); // Weblink is at index 4
        }
    }
    
    return links;
}

/**
 * Read existing links from both CSV and MongoDB
 */
export async function readExistingLinksCombined(filePath) {
    const csvLinks = readExistingLinks(filePath);
    
    // Try to get MongoDB links
    try {
        const { readExistingLinks: readMongoLinks, isMongoDBConnected } = await import('./mongoWriter.js');
        if (isMongoDBConnected()) {
            const mongoLinks = await readMongoLinks();
            // Combine both sets
            return new Set([...csvLinks, ...mongoLinks]);
        }
    } catch (error) {
        // If MongoDB not available, just return CSV links
    }
    
    return csvLinks;
}

/**
 * Simple CSV line parser (handles quoted fields)
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
 * Append article to CSV file
 * Also appends to merged CSV file
 */
export async function appendArticleToCSV(article, sourceName) {
    const filePath = getCSVFilePath(sourceName);
    ensureCSVHeaders(filePath);
    
    const csvRow = articleToCSVRow(article);
    fs.appendFileSync(filePath, csvRow + '\n', 'utf8');
    
    // Also write to merged CSV file (avoiding duplicates)
    // Use write queue to prevent concurrent write conflicts
    mergedCSVWriteQueue = mergedCSVWriteQueue.then(async () => {
        try {
            ensureMergedCSVHeaders();
            const existingLinks = readMergedCSVLinks();
            
            const link = (article.link || '').trim();
            if (link) {
                // Check both with and without trailing slash
                const linkVariations = [
                    link,
                    link.replace(/\/$/, ''),
                    link + '/'
                ];
                const isDuplicate = linkVariations.some(v => existingLinks.has(v));
                
                if (!isDuplicate) {
                    const mergedRow = articleToMergedCSVRow(article);
                    // Write to file (this is now serialized by the queue)
                    fs.appendFileSync(MERGED_CSV_FILE, mergedRow + '\n', 'utf8');
                    
                    // Add all variations to cache
                    linkVariations.forEach(v => existingLinks.add(v));
                    // Invalidate cache since file has changed
                    mergedCSVLinksCache = null;
                    mergedCSVFileSize = fs.statSync(MERGED_CSV_FILE).size;
                }
            }
        } catch (error) {
            // Log but don't fail if merged CSV write fails
            console.warn(`[CSV Writer] Failed to write to merged CSV: ${error.message}`);
        }
    }).catch(error => {
        console.warn(`[CSV Writer] Error in merged CSV write queue: ${error.message}`);
    });
    
    // Wait for this write to complete
    await mergedCSVWriteQueue;
}

/**
 * Append multiple articles to CSV and optionally MongoDB
 * Also appends to merged CSV file in the correct format
 */
export async function appendArticlesToCSV(articles, sourceName) {
    if (articles.length === 0) return;
    
    // Write to individual source CSV file
    const filePath = getCSVFilePath(sourceName);
    ensureCSVHeaders(filePath);
    
    const csvRows = articles.map(article => articleToCSVRow(article));
    fs.appendFileSync(filePath, csvRows.join('\n') + '\n', 'utf8');
    
    // Also write to merged CSV file (avoiding duplicates)
    // Use write queue to prevent concurrent write conflicts
    mergedCSVWriteQueue = mergedCSVWriteQueue.then(async () => {
        try {
            ensureMergedCSVHeaders();
            const existingLinks = readMergedCSVLinks();
            
            const newArticles = articles.filter(article => {
                const link = (article.link || '').trim();
                if (!link) return false;
                // Check both with and without trailing slash
                const linkVariations = [
                    link,
                    link.replace(/\/$/, ''), // without trailing slash
                    link + '/' // with trailing slash
                ];
                return !linkVariations.some(v => existingLinks.has(v));
            });
            
            if (newArticles.length > 0) {
                const mergedRows = newArticles.map(article => {
                    const link = (article.link || '').trim();
                    // Add all variations to cache
                    existingLinks.add(link);
                    existingLinks.add(link.replace(/\/$/, ''));
                    existingLinks.add(link + '/');
                    return articleToMergedCSVRow(article);
                });
                
                // Write to file (this is now serialized by the queue)
                fs.appendFileSync(MERGED_CSV_FILE, mergedRows.join('\n') + '\n', 'utf8');
                
                // Invalidate cache since file has changed
                mergedCSVLinksCache = null;
                mergedCSVFileSize = fs.statSync(MERGED_CSV_FILE).size;
            }
        } catch (error) {
            // Log but don't fail if merged CSV write fails
            console.warn(`[CSV Writer] Failed to write to merged CSV: ${error.message}`);
        }
    }).catch(error => {
        console.warn(`[CSV Writer] Error in merged CSV write queue: ${error.message}`);
    });
    
    // Wait for this write to complete
    await mergedCSVWriteQueue;
    
    // Also write to MongoDB if connected
    try {
        const { saveArticles, isMongoDBConnected } = await import('./mongoWriter.js');
        if (isMongoDBConnected()) {
            const result = await saveArticles(articles);
            if (result.saved > 0) {
                // Optionally log MongoDB saves (commented out to reduce noise)
                // console.log(`[MongoDB] Saved ${result.saved} articles for ${sourceName}`);
            }
        }
    } catch (error) {
        // Silently fail MongoDB writes - CSV is primary
        // console.warn(`MongoDB write failed for ${sourceName}:`, error.message);
    }
}

/**
 * Get all articles from CSV for a source
 * Format: NewsSite, Name, Date of publishing the Article, Author, Weblink, Summary
 */
export function readArticlesFromCSV(sourceName) {
    const filePath = getCSVFilePath(sourceName);
    
    if (!fs.existsSync(filePath)) {
        return [];
    }
    
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());
    
    if (lines.length <= 1) {
        return []; // Only headers or empty
    }
    
    const articles = [];
    for (let i = 1; i < lines.length; i++) {
        const columns = parseCSVLine(lines[i]);
        if (columns.length >= 5) {
            articles.push({
                source: columns[0] || sourceName,
                title: columns[1] || '',
                name: columns[1] || '',
                date: columns[2] || '',
                author: columns[3] || '',
                link: columns[4] || '',
                extract: columns[5] || '',
                summary: columns[5] || ''
            });
        }
    }
    
    return articles;
}

/**
 * Get statistics about CSV file
 */
export function getCSVStats(sourceName) {
    const filePath = getCSVFilePath(sourceName);
    
    if (!fs.existsSync(filePath)) {
        return { totalArticles: 0, filePath };
    }
    
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());
    const totalArticles = Math.max(0, lines.length - 1); // Subtract header
    
    return {
        totalArticles,
        filePath,
        fileSize: fs.statSync(filePath).size
    };
}

/**
 * Check if CSV file exists for a source
 */
export function csvFileExists(sourceName) {
    const filePath = getCSVFilePath(sourceName);
    return fs.existsSync(filePath);
}

