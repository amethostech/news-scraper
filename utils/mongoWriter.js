import mongoose from 'mongoose';
import connectDB from '../config/db.js';
import Article from '../models/Article.js';

let isConnected = false;
let connectionAttempted = false;

/**
 * Connect to MongoDB
 * Returns false if MongoDB is not configured or connection fails (no errors thrown)
 */
export async function connectMongoDB() {
    if (isConnected) {
        return true;
    }
    
    if (connectionAttempted && !isConnected) {
        return false; // Already tried and failed
    }
    
    // Check if MongoDB URI is configured
    const MONGO_URI = process.env.MONGO_URI;
    if (!MONGO_URI) {
        // MongoDB not configured - skip silently
        connectionAttempted = true;
        isConnected = false;
        return false;
    }
    
    // Check if it's the default localhost (not configured)
    if (MONGO_URI === 'mongodb://localhost:27017/news-scraper') {
        connectionAttempted = true;
        isConnected = false;
        return false;
    }
    
    try {
        connectionAttempted = true;
        await connectDB();
        isConnected = mongoose.connection.readyState === 1;
        return isConnected;
    } catch (error) {
        // MongoDB connection failed or not configured - continue silently (CSV is primary)
        isConnected = false;
        return false;
    }
}

/**
 * Check if MongoDB is connected
 */
export function isMongoDBConnected() {
    return isConnected && mongoose.connection.readyState === 1;
}

/**
 * Save a single article to MongoDB (upsert by link)
 * Returns false if MongoDB is not available (no errors thrown)
 */
export async function saveArticle(article) {
    if (!isMongoDBConnected()) {
        return false;
    }
    
    try {
        if (!article || !article.link) {
            return false;
        }
        
        // Prepare article data
        const articleData = {
            link: article.link,
            source: article.source || '',
            title: article.title || '',
            author: article.author || '',
            extract: article.extract || article.summary || '',
            date: article.date ? new Date(article.date) : null,
            scrapedAt: article.scrapedAt ? new Date(article.scrapedAt) : new Date()
        };
        
        // Upsert by link (update if exists, insert if not) with timeout
        await Promise.race([
            Article.findOneAndUpdate(
                { link: article.link },
                articleData,
                { upsert: true, new: true }
            ),
            new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
        ]);
        
        return true;
    } catch (error) {
        if (error.code === 11000) {
            // Duplicate key error (shouldn't happen with upsert, but handle it)
            return true; // Consider it successful
        }
        // Silently fail - CSV is primary storage
        return false;
    }
}

/**
 * Save multiple articles to MongoDB (batch upsert)
 * Returns {saved: 0, failed: 0} if MongoDB is not available (no errors thrown)
 */
export async function saveArticles(articles) {
    if (!isMongoDBConnected()) {
        return { saved: 0, failed: 0 };
    }
    
    if (!articles || articles.length === 0) {
        return { saved: 0, failed: 0 };
    }
    
    let saved = 0;
    let failed = 0;
    
    try {
        // Process in batches to avoid overwhelming MongoDB
        const batchSize = 50; // Reduced batch size for better timeout handling
        for (let i = 0; i < articles.length; i += batchSize) {
            const batch = articles.slice(i, i + batchSize);
            
            const operations = batch.map(article => {
                if (!article || !article.link) {
                    failed++;
                    return null;
                }
                
                const articleData = {
                    link: article.link,
                    source: article.source || '',
                    title: article.title || '',
                    author: article.author || '',
                    extract: article.extract || article.summary || '',
                    date: article.date ? new Date(article.date) : null,
                    scrapedAt: article.scrapedAt ? new Date(article.scrapedAt) : new Date()
                };
                
                return {
                    updateOne: {
                        filter: { link: article.link },
                        update: { $set: articleData },
                        upsert: true
                    }
                };
            }).filter(op => op !== null);
            
            if (operations.length > 0) {
                try {
                    // Use timeout to prevent hanging
                    await Promise.race([
                        Article.bulkWrite(operations, { ordered: false }),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 10000))
                    ]);
                    saved += operations.length;
                } catch (error) {
                    // Batch write failed - skip MongoDB for these articles (CSV is primary)
                    // Don't try individual saves to avoid more timeouts
                    failed += operations.length;
                }
            }
        }
        
        return { saved, failed };
    } catch (error) {
        // Silently fail - CSV is primary storage
        return { saved, failed: failed + articles.length - saved };
    }
}

/**
 * Read all existing article links from MongoDB
 * Returns empty Set if MongoDB is not available (no errors thrown)
 */
export async function readExistingLinks() {
    if (!isMongoDBConnected()) {
        return new Set();
    }
    
    try {
        // Use timeout to prevent hanging
        const articles = await Promise.race([
            Article.find({}, { link: 1 }).lean(),
            new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000))
        ]);
        return new Set(articles.map(article => article.link).filter(link => link));
    } catch (error) {
        // Silently return empty set - CSV is primary storage
        return new Set();
    }
}

/**
 * Get count of articles for a source
 */
export async function getArticleCount(sourceName) {
    if (!isMongoDBConnected()) {
        return 0;
    }
    
    try {
        return await Article.countDocuments({ source: sourceName });
    } catch (error) {
        console.warn(`Error getting article count from MongoDB: ${error.message}`);
        return 0;
    }
}

/**
 * Get all existing links from both CSV and MongoDB
 */
export async function readExistingLinksCombined(csvFilePath) {
    const { readExistingLinks: readCSVLinks } = await import('./csvWriter.js');
    
    // Get links from CSV
    const csvLinks = readCSVLinks(csvFilePath);
    
    // Get links from MongoDB
    const mongoLinks = await readExistingLinks();
    
    // Combine both sets
    const allLinks = new Set([...csvLinks, ...mongoLinks]);
    
    return allLinks;
}

