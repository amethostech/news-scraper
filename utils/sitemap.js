import axios from 'axios';
import xml2js from 'xml2js';
import zlib from 'zlib';
import { promisify } from 'util';
import { getRealisticHeaders } from './antiBot.js';

const gunzip = promisify(zlib.gunzip);

export async function fetchSitemapIndex(indexUrl, filterFn = null, usePuppeteer = false) {
    console.log(`\nFetching Sitemap Index: ${indexUrl}`);
    try {
        let xmlData;
        const isGzipped = indexUrl.endsWith('.gz');
        
        if (usePuppeteer) {
            // Use Puppeteer if axios fails
            const { fetchSitemapWithPuppeteer } = await import('./sitemapWithPuppeteer.js');
            xmlData = await fetchSitemapWithPuppeteer(indexUrl);
        } else {
            try {
                const response = await axios.get(indexUrl, {
                    headers: getRealisticHeaders(),
                    timeout: 15000,
                    // For gzipped files, we need the raw buffer to decompress
                    responseType: isGzipped ? 'arraybuffer' : 'text'
                });
                
                if (isGzipped) {
                    // Decompress gzipped sitemap
                    const buffer = Buffer.from(response.data);
                    xmlData = (await gunzip(buffer)).toString('utf-8');
                } else {
                    xmlData = response.data;
                }
            } catch (error) {
                // If axios fails with 403, try Puppeteer
                if (error.response && error.response.status === 403) {
                    console.log(`[FALLBACK] Using Puppeteer for sitemap...`);
                    const { fetchSitemapWithPuppeteer } = await import('./sitemapWithPuppeteer.js');
                    xmlData = await fetchSitemapWithPuppeteer(indexUrl);
                } else {
                    throw error;
                }
            }
        }
        
        const result = await xml2js.parseStringPromise(xmlData);

        if (!result.sitemapindex || !result.sitemapindex.sitemap) {
            console.warn('Invalid sitemap index format or no sitemaps found');
            return [];
        }

        const sitemapLocs = result.sitemapindex.sitemap.map(s => s.loc[0]);
        console.log(`Successfully found ${sitemapLocs.length} total sitemaps.`);

        if (filterFn) {
            return sitemapLocs.filter(filterFn);
        }
        return sitemapLocs;
    } catch (error) {
        console.error(`Error fetching/parsing sitemap index:`, error.message);
        return [];
    }
}

export async function fetchArticleLinksFromSitemap(sitemapUrl, usePuppeteer = false) {
    try {
        let xmlData;
        const isGzipped = sitemapUrl.endsWith('.gz');
        
        if (usePuppeteer) {
            const { fetchSitemapWithPuppeteer } = await import('./sitemapWithPuppeteer.js');
            xmlData = await fetchSitemapWithPuppeteer(sitemapUrl);
        } else {
            try {
                const response = await axios.get(sitemapUrl, {
                    headers: getRealisticHeaders(),
                    timeout: 15000,
                    // For gzipped files, we need the raw buffer to decompress
                    responseType: isGzipped ? 'arraybuffer' : 'text'
                });
                
                if (isGzipped) {
                    // Decompress gzipped sitemap
                    const buffer = Buffer.from(response.data);
                    xmlData = (await gunzip(buffer)).toString('utf-8');
                } else {
                    xmlData = response.data;
                }
            } catch (error) {
                // If axios fails with 403, try Puppeteer
                if (error.response && error.response.status === 403) {
                    console.log(`[FALLBACK] Using Puppeteer for sitemap...`);
                    const { fetchSitemapWithPuppeteer } = await import('./sitemapWithPuppeteer.js');
                    xmlData = await fetchSitemapWithPuppeteer(sitemapUrl);
                } else {
                    throw error;
                }
            }
        }
        
        const result = await xml2js.parseStringPromise(xmlData);

        if (!result.urlset || !result.urlset.url) {
            return [];
        }

        // Extract maximum data from sitemap: URL, lastmod, priority, changefreq
        const articleData = result.urlset.url.map(u => {
            const url = u.loc[0];
            const lastmod = u.lastmod ? u.lastmod[0] : null;
            const priority = u.priority ? parseFloat(u.priority[0]) : null;
            const changefreq = u.changefreq ? u.changefreq[0] : null;
            
            return {
                url: url,
                lastmod: lastmod,
                priority: priority,
                changefreq: changefreq
            };
        });
        
        return articleData;
    } catch (error) {
        console.error(`Error fetching/parsing article sitemap ${sitemapUrl}:`, error.message);
        return [];
    }
}
