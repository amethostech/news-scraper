const express = require('express');
const mongoose = require('mongoose');
const axios = require('axios');
const cheerio = require('cheerio');
const dotenv = require('dotenv');

dotenv.config();

const PORT = 3000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/news-scraper';
const NEWS_URLS = [
    { name: 'BioPharma Dive', url: 'https://www.biopharmadive.com/' },
    // { name: 'Endpoints News', url: 'https://endpoints.news/' }, // Add other sites here...
    // { name: 'STAT Pharma', url: 'https://www.statnews.com/' },
];

const SCRAPE_DELAY_MS = 3000; 
const ARTICLE_DELAY_MS = 500; 

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


mongoose.connect(MONGO_URI)
    .then(() => console.log('MongoDB connection successful.'))
    .catch(err => console.error('MongoDB connection error:', err));

const articleSchema = new mongoose.Schema({
    author: { type: String, required: false },
    date: { type: Date, required: false },
    extract: { type: String, required: true }, 
    link: { type: String, required: true, unique: true },
    source: { type: String, required: true },
    scrapedAt: { type: Date, default: Date.now }
});

const Article = mongoose.model('Article', articleSchema);


/**
 * @param {string} url The full URL of the article.
 * @returns {object|null} Object containing author, date, and extract, or null on failure.
 */
async function scrapeArticleDetails(url) {
    try {
        await sleep(ARTICLE_DELAY_MS); // Be polite to the server
        const { data } = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });
        const $ = cheerio.load(data);

        
        let author = $('.author').text().trim();
        author = author.replace(/^By\s+/i, '').trim() || 'N/A'; 
        
        let dateText = $('.published-info').text().trim();
        const dateMatch = dateText.match(/[A-Z][a-z]{2,}\.\s+\d{1,2},\s+\d{4}/);
        dateText = dateMatch ? dateMatch[0] : new Date().toISOString(); 
        
        let bodyText = '';
        
        const articleBodySelectors = [
            'div.article-body-content', 
            'div.page-content',         
            '.article-body',            
            '#article-body',            
        ];
        
        let contentContainer = $();
        
        for (const selector of articleBodySelectors) {
            contentContainer = $(selector);
            if (contentContainer.length) break;
        }

        
        contentContainer.find('p').each((i, p) => {
            const paragraphText = $(p).text().trim();
            if (paragraphText.length > 40) { 
                 bodyText += paragraphText + '\n\n';
            }
        });

        if (!bodyText) {
             bodyText = contentContainer.text().trim();
        }

        return {
            author: author,
            date: new Date(dateText),
            extract: bodyText.trim(), 
            link: url,
        };

    } catch (error) {
        console.error(`Error fetching article details for ${url}:`, error.message);
        return null;
    }
}



/**
 * Scrapes the homepage to get a list of article links, then scrapes each link for details.
 * @param {string} url The URL to scrape (homepage).
 * @param {string} sourceName The name of the news source.
 * @returns {number} The number of new articles saved.
 */
async function scrapeSite(url, sourceName) {
    console.log(`\nStarting scrape for: ${sourceName} (${url})`);
    let articlesSaved = 0;
    let articleLinks = [];

    try {
        const { data } = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });
        const $ = cheerio.load(data);

       
        const topStoriesContainer = $('.top-stories'); 
        
        topStoriesContainer.find('a').each((i, element) => {
            const relativeLink = $(element).attr('href');
            if (relativeLink && relativeLink.includes('/news/')) { 
                const link = new URL(relativeLink, url).href;
                if (!articleLinks.includes(link)) {
                     articleLinks.push(link);
                }
            }
        });
        
        $('.item-card').each((i, element) => {
            const linkElement = $(element).find('a.item-card-text').first();
            const relativeLink = linkElement.attr('href');
            if (relativeLink && relativeLink.includes('/news/')) { 
                const link = new URL(relativeLink, url).href;
                if (!articleLinks.includes(link)) { 
                    articleLinks.push(link);
                }
            }
        });

        
        console.log(`Found ${articleLinks.length} article links on the homepage. Starting detail scraping...`);

        for (const link of articleLinks) {
            const existingArticle = await Article.findOne({ link: link });
            if (existingArticle) {
                continue; 
            }

            const articleData = await scrapeArticleDetails(link);

            if (articleData && articleData.extract.length > 100) { 
                 articleData.source = sourceName;
                 try {
                     await Article.create(articleData);
                     articlesSaved++;
                 } catch (dbError) {
                     if (dbError.code !== 11000) {
                          console.error(`Error saving article to DB:`, dbError.message);
                     }
                 }
            } else {
                console.error(`Skipping article due to empty or short extract: ${link}`);
            }
        }
        
        console.log(`Finished processing article links. Saved ${articlesSaved} new articles.`);
        return articlesSaved;

    } catch (error) {
        console.error(`Error scraping ${sourceName}:`, error.message);
        return 0;
    }
}

// --- EXPRESS SERVER ---
const app = express();

app.get('/', (req, res) => {
    res.send(`
        <div style="font-family: Arial, sans-serif; text-align: center; margin-top: 50px;">
            <h1 style="color: #4F46E5;">News Scraper Backend Running on Port ${PORT}</h1>
            <p>Ready to scrape and insert data into MongoDB.</p>
            <p>Access the endpoint below to start the scraping job:</p>
            <a href="/scrape-all" style="background-color: #4F46E5; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold;">
                Start Scraping All Sites
            </a>
            <p style="margin-top: 20px; font-size: 0.9em; color: #6B7280;">
                (Note: Scraping is done sequentially with a ${SCRAPE_DELAY_MS/1000}s delay per site, and a ${ARTICLE_DELAY_MS/1000}s delay between articles.)
            </p>
        </div>
    `);
});

app.get('/scrape-all', async (req, res) => {
    console.log('--- SCRAPING JOB INITIATED ---');
    const results = [];
    let totalSaved = 0;

    for (const site of NEWS_URLS) {
        const savedCount = await scrapeSite(site.url, site.name);
        results.push({
            site: site.name,
            url: site.url,
            savedCount: savedCount
        });
        totalSaved += savedCount;
        await sleep(SCRAPE_DELAY_MS);
    }

    const htmlResponse = `
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Scraping Results</title>
            <style>
                body { font-family: sans-serif; margin: 20px; background-color: #f4f4f9; }
                .container { max-width: 800px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
                h1 { color: #10B981; border-bottom: 2px solid #10B981; padding-bottom: 10px; }
                h2 { color: #374151; margin-top: 20px; }
                ul { list-style: none; padding: 0; }
                li { background: #E5E7EB; margin: 10px 0; padding: 15px; border-radius: 4px; display: flex; justify-content: space-between; align-items: center; }
                .success { color: #10B981; font-weight: bold; }
                .info { color: #6B7280; font-size: 0.9em; }
                .total { background: #10B981; color: white; padding: 15px; text-align: center; margin-top: 30px; border-radius: 6px; font-size: 1.2em; font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>✅ Scraping Job Complete!</h1>
                <div class="total">
                    TOTAL NEW ARTICLES SAVED: ${totalSaved}
                </div>
                <h2>Individual Site Results:</h2>
                <ul>
                    ${results.map(r => `
                        <li>
                            <div>
                                <strong>${r.site}</strong><br>
                                <span class="info">${r.url}</span>
                            </div>
                            <span class="${r.savedCount > 0 ? 'success' : 'info'}">Saved: ${r.savedCount} new articles</span>
                        </li>
                    `).join('')}
                </ul>
                <p>Check your MongoDB database for the complete records!</p>
            </div>
        </body>
        </html>
    `;

    res.send(htmlResponse);
});


app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
    console.log(`Scraping endpoint: http://localhost:${PORT}/scrape-all`);
});