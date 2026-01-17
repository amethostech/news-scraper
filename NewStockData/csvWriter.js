/**
 * CSV Writer for NewStockData fetchers
 * 
 * Saves all output to NewStockData/data/ folder
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, 'data');

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

    if (typeof dateValue === 'string' && /^\d{4}-\d{2}-\d{2}/.test(dateValue)) {
        return dateValue.split('T')[0].split(' ')[0];
    }

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
 * Convert article to CSV row
 * Format: Source, Title, Date, Author, Link, Extract
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
 * Get CSV file path for a source
 */
export function getCSVFilePath(sourceName) {
    const sanitizedSource = sourceName.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
    return path.join(DATA_DIR, `${sanitizedSource}_articles.csv`);
}

/**
 * Ensure CSV file has headers
 */
function ensureCSVHeaders(filePath) {
    const headers = ['Source', 'Title', 'Date', 'Author', 'Link', 'Extract'];
    const headerRow = headers.join(',') + '\n';

    if (!fs.existsSync(filePath)) {
        fs.writeFileSync(filePath, headerRow, 'utf8');
    } else {
        const content = fs.readFileSync(filePath, 'utf8');
        if (!content.startsWith('Source,')) {
            fs.writeFileSync(filePath, headerRow + content, 'utf8');
        }
    }
}

/**
 * Read existing links from CSV
 */
export function readExistingLinks(filePath) {
    if (!fs.existsSync(filePath)) {
        return new Set();
    }

    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());

    if (lines.length <= 1) {
        return new Set();
    }

    const links = new Set();
    for (let i = 1; i < lines.length; i++) {
        // Extract link (column 5, index 4)
        const match = lines[i].match(/(?:^|,)(?:[^,]*,){4}([^,"\n]+|"[^"]*")/);
        if (match) {
            const link = match[1].replace(/^"|"$/g, '');
            links.add(link);
        }
    }

    return links;
}

/**
 * Append articles to CSV file
 */
export async function appendArticlesToCSV(articles, sourceName) {
    if (articles.length === 0) return;

    const filePath = getCSVFilePath(sourceName);
    ensureCSVHeaders(filePath);

    // Filter out duplicates
    const existingLinks = readExistingLinks(filePath);
    const newArticles = articles.filter(a => !existingLinks.has(a.link));

    if (newArticles.length === 0) {
        console.log(`[CSV] No new articles to save for ${sourceName}`);
        return;
    }

    const csvRows = newArticles.map(article => articleToCSVRow(article));
    fs.appendFileSync(filePath, csvRows.join('\n') + '\n', 'utf8');

    console.log(`[CSV] Saved ${newArticles.length} articles to ${path.basename(filePath)}`);
}

/**
 * Get data directory path
 */
export function getDataDir() {
    return DATA_DIR;
}
