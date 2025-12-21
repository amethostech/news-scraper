import axios from "axios";
import * as cheerio from 'cheerio';
import xml2js from 'xml2js';
import { sleep } from "../utils/common.js";
import Article from "../models/Article.js";
import { collectAndScrapeRSS } from '../utils/simpleRSSCollector.js';

const SITEMAP_URLS = [
    // 'https://www.biospace.com/sitemap-latest.xml',
    'https://www.biospace.com/sitemap-202511.xml',
    // 'https://www.biospace.com/sitemap-202510.xml',
    // 'https://www.biospace.com/sitemap-202509.xml',
    // 'https://www.biospace.com/sitemap-202508.xml',
    // 'https://www.biospace.com/sitemap-202507.xml'
];

const MAX_ARTICLES = 10000;

const MEDICAL_KEYWORDS = [
    'drug', 'fda', 'clinical', 'trial', 'therapy', 'treatment', 'disease', 'cancer',
    'pharma', 'pharmaceutical', 'biotech', 'medicine', 'medical', 'patient',
    'vaccine', 'antibody', 'gene', 'cell', 'protein', 'diagnostic', 'approval',
    'indication', 'oncology', 'immunotherapy', 'rare disease', 'orphan drug',
    'biologics', 'biosimilar', 'nda', 'bla', 'breakthrough', 'fast track',
    'accelerated approval', 'priority review', 'orphan designation',
    'cardiovascular', 'diabetes', 'neurology', 'infectious disease',
    'alzheimer', 'parkinson', 'multiple sclerosis', 'hiv', 'hepatitis',
    'hemophilia', 'crispr', 'car-t', 'mrna', 'coronavirus', 'covid',
    'pandemic', 'epidemic', 'clinical development', 'phase 1', 'phase 2', 'phase 3',
    'pivotal', 'endpoint', 'efficacy', 'safety', 'adverse event',
    'chemotherapy', 'radiation', 'surgery', 'transplant', 'stem cell'
];

function isMedicalContent(url, title, abstract) {
    const searchText = `${url} ${title} ${abstract}`.toLowerCase();
    return MEDICAL_KEYWORDS.some(keyword => searchText.includes(keyword));
}

function isMedicalCategory(url) {
    const medicalCategories = [
        '/fda/',
        '/drug-development/',
        '/clinical-trials/',
        '/regulatory/',
        '/therapeutics/',
        '/oncology/',
        '/rare-disease/',
        '/gene-therapy/',
        '/cell-therapy/',
        '/vaccines/',
        '/antibodies/',
        '/biosimilars/',
        '/medical-devices/',
        '/diagnostics/',
        '/precision-medicine/',
        '/immunology/',
        '/neurology/',
        '/cardiology/',
        '/infectious-disease/'
    ];

    return medicalCategories.some(category => url.includes(category));
}

function shouldExclude(url) {
    const excludeCategories = [
        '/career-advice/',
        '/job-trends/',
        '/hiring-outlook/',
        '/salary/',
        '/compensation/',
        '/layoff-tracker/',
        '/hotbeds/',
        '/venture-capital/',
        '/real-estate/',
        '/conference/',
        '/webinar/',
        '/event/'
    ];

    return excludeCategories.some(category => url.includes(category));
}

function shouldProcessUrl(url) {
    const isArticle = url.includes('/news/') ||
        url.includes('/drug-development/') ||
        url.includes('/policy/') ||
        url.includes('/business/') ||
        url.includes('/fda/') ||
        url.includes('/press-releases/');

    if (!isArticle) return false;

    if (shouldExclude(url)) return false;

    return true;
}

async function parseSitemap(sitemapUrl) {
    try {
        const { data } = await axios.get(sitemapUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            timeout: 15000
        });

        const parser = new xml2js.Parser();
        const result = await parser.parseStringPromise(data);

        const urls = [];

        if (result.urlset && result.urlset.url) {
            for (const urlEntry of result.urlset.url) {
                if (urlEntry.loc && urlEntry.loc[0]) {
                    urls.push(urlEntry.loc[0]);
                }
            }
        }

        return urls;
    } catch (error) {
        console.error(`Error parsing sitemap ${sitemapUrl}:`, error.message);
        return [];
    }
}

async function scrapeArticleDetails(url) {
    try {
        const { data } = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            timeout: 15000
        });

        const $ = cheerio.load(data);

        // Extract author
        let authors = [];

        // Try meta tag
        let authorMeta = $('meta[name="author"]').attr('content');
        if (authorMeta && authorMeta.trim()) {
            authors.push(authorMeta.trim());
        }

        // Try byline or author class
        if (authors.length === 0) {
            $('.author-name, .byline-author, .article-author, [rel="author"], .byline a').each((i, el) => {
                const authorText = $(el).text().trim();
                if (authorText && !authorText.toLowerCase().includes('biospace') && authorText.length < 50) {
                    authors.push(authorText);
                }
            });
        }

        // Try JSON-LD structured data
        if (authors.length === 0) {
            $('script[type="application/ld+json"]').each((i, el) => {
                try {
                    const jsonData = JSON.parse($(el).html());
                    if (jsonData.author) {
                        if (Array.isArray(jsonData.author)) {
                            authors = jsonData.author.map(a => a.name || a).filter(Boolean);
                        } else if (typeof jsonData.author === 'object' && jsonData.author.name) {
                            authors.push(jsonData.author.name);
                        } else if (typeof jsonData.author === 'string') {
                            authors.push(jsonData.author);
                        }
                    }
                } catch (e) {
                    // Ignore JSON parse errors
                }
            });
        }

        // Extract date
        let date = null;

        date = $('meta[property="article:published_time"]').attr('content') ||
            $('meta[name="date"]').attr('content') ||
            $('meta[name="publishdate"]').attr('content') ||
            $('meta[property="og:published_time"]').attr('content');

        if (!date) {
            const timeEl = $('time[datetime]').first();
            if (timeEl.length) {
                date = timeEl.attr('datetime');
            }
        }

        if (!date) {
            const dateText = $('.date, .published-date, .article-date, .dateline, .entry-date').first().text().trim();
            if (dateText) {
                date = dateText;
            }
        }

        if (!date) {
            $('script[type="application/ld+json"]').each((i, el) => {
                try {
                    const jsonData = JSON.parse($(el).html());
                    if (jsonData.datePublished) {
                        date = jsonData.datePublished;
                    }
                } catch (e) {
                    // Ignore
                }
            });
        }

        let title = $('meta[property="og:title"]').attr('content') ||
            $('meta[name="twitter:title"]').attr('content') ||
            $('h1').first().text() ||
            $('title').text();

        let abstract = null;

        abstract = $('meta[name="description"]').attr('content') ||
            $('meta[property="og:description"]').attr('content');

        if (!abstract) {
            abstract = $('.article-summary, .article-excerpt, .entry-summary, .article-intro').first().text().trim();
        }

        if (!abstract) {
            const firstPara = $('article p, .article-content p, .entry-content p, .article-body p').first().text().trim();
            if (firstPara && firstPara.length > 50) {
                abstract = firstPara;
            }
        }

        authors = [...new Set(authors)].filter(a => a && a.trim().length > 0);

        let authorString = authors.length > 0 ? authors.join(', ') : "Unknown";

        if (date) {
            date = date.trim();
            try {
                const dateObj = new Date(date);
                if (!isNaN(dateObj.getTime())) {
                    date = dateObj.toISOString();
                }
            } catch (e) { }
        }

        if (!abstract || abstract.trim().length === 0) {
            const backupPara = $('p').first().text().trim();
            if (backupPara) {
                abstract = backupPara;
            }
        }

        if (!abstract || abstract.trim().length === 0) {
            abstract = title || "No abstract available";
        }


        const isRelevant = isMedicalCategory(url) || isMedicalContent(url, title || '', abstract || '');
        if (!isRelevant) return null;

        return {
            author: authorString,       // String
            date: date || null,
            extract: abstract,          // ALWAYS a string
            link: url,
            source: "BIOSPACE"
        };

    } catch (error) {
        console.error(`Error scraping ${url}:`, error.message);
        return null;
    }
}

// Export scrapeArticleDetails for RSS collector
export { scrapeArticleDetails };

export async function run(options = {}) {
    const sourceName = 'BioSpace';
    
    if (options.historical) {
        // Historical scraping via sitemap
        const allResults = [];
        let totalProcessed = 0;
        let totalFiltered = 0;

        console.log(`\n=== BIOSPACE SCRAPER STARTING ===`);
        console.log(`Sitemaps to process: ${SITEMAP_URLS.length}`);
        console.log(`Maximum articles limit: ${MAX_ARTICLES}`);

    for (const sitemapUrl of SITEMAP_URLS) {
        if (allResults.length >= MAX_ARTICLES) {
            console.log(`\n⚠️  Reached maximum limit of ${MAX_ARTICLES} articles. Stopping scraper.`);
            break;
        }

        console.log(`\nProcessing sitemap: ${sitemapUrl}`);

        try {
            const urls = await parseSitemap(sitemapUrl);
            console.log(`Found ${urls.length} URLs in sitemap`);

            if (urls.length === 0) {
                console.log(`✗ No URLs found in this sitemap`);
                continue;
            }

            // Filter URLs
            const filteredUrls = urls.filter(shouldProcessUrl);
            console.log(`Filtered to ${filteredUrls.length} article URLs`);
            totalFiltered += filteredUrls.length;

            if (filteredUrls.length === 0) {
                console.log(`✗ No relevant article URLs after filtering`);
                continue;
            }

            // Calculate how many URLs we can process
            const remainingSlots = MAX_ARTICLES - allResults.length;
            const urlsToProcess = filteredUrls.slice(0, remainingSlots);

            console.log(`Processing ${urlsToProcess.length} articles...`);

            // Scrape each URL
            let successCount = 0;
            let skipCount = 0;

            for (let i = 0; i < urlsToProcess.length; i++) {
                const url = urlsToProcess[i];

                try {
                    const article = await scrapeArticleDetails(url);

                    if (article) {
                        allResults.push(article);
                        successCount++;

                        // Show progress every 10 articles
                        if ((i + 1) % 10 === 0) {
                            console.log(`  Progress: ${i + 1}/${urlsToProcess.length} (${allResults.length} total)`);
                        }
                    } else {
                        skipCount++;
                    }

                    // Add small delay to avoid rate limiting
                    await sleep(100);

                } catch (error) {
                    console.error(`  Error processing ${url}:`, error.message);
                    skipCount++;
                }

                // Check if we've reached the limit
                if (allResults.length >= MAX_ARTICLES) {
                    console.log(`\n⚠️  Reached maximum limit during processing. Stopping.`);
                    break;
                }
            }

            console.log(`✓ Successfully scraped: ${successCount} articles`);
            console.log(`✓ Skipped (non-medical): ${skipCount} articles`);
            console.log(`✓ Total collected: ${allResults.length}/${MAX_ARTICLES}`);
            totalProcessed += urlsToProcess.length;

            // Delay between sitemaps
            if (allResults.length < MAX_ARTICLES) {
                await sleep(2000);
            }

        } catch (error) {
            console.error(`✗ Error processing sitemap ${sitemapUrl}:`, error.message);
        }
    }

        console.log(`\n=== BIOSPACE SCRAPER COMPLETE ===`);
        console.log(`Total articles collected: ${allResults.length}`);
        console.log(`Total URLs processed: ${totalProcessed}`);
        console.log(`Total URLs filtered: ${totalFiltered}`);
        console.log(`Sitemaps processed: ${SITEMAP_URLS.length}`);



        // MongoDB is optional - CSV is primary storage
        // Only attempt MongoDB save if connected
        try {
            const { isMongoDBConnected } = await import('../utils/mongoWriter.js');
            if (isMongoDBConnected()) {
                console.log(`\n--- DATABASE SYNC STARTING ---`);
                console.log(`Found ${allResults.length} articles to potentially save.`);

                const savePromises = allResults.map(async (article) => {
                    try {
                        // Use timeout to prevent hanging
                        const exists = await Promise.race([
                            Article.findOne({ link: article.link }),
                            new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 3000))
                        ]);
                        if (exists) {
                            return null;
                        }
                        await Promise.race([
                            Article.create(article),
                            new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 3000))
                        ]);
                        return article;
                    } catch (error) {
                        // Silently skip MongoDB errors - CSV is primary
                        return null;
                    }
                });

                const savedArticles = (await Promise.all(savePromises)).filter(Boolean);
                console.log(`--- DATABASE SYNC COMPLETE ---`);
                console.log(`Successfully saved ${savedArticles.length} new articles to MongoDB.`);
            }
        } catch (error) {
            // MongoDB not available or error - continue silently
        }
        
        return allResults;
    } else {
        // Default: RSS feed for weekly updates
        const sourceConfig = {
            sourceName: sourceName,
            rssUrl: 'https://www.biospace.com/rss',
            maxConcurrent: 3,
            delayBetweenScrapes: 1000
        };
        
        const results = await collectAndScrapeRSS(sourceConfig, scrapeArticleDetails);
        return results;
    }
}