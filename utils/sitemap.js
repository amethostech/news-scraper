import axios from 'axios';
import xml2js from 'xml2js';

export async function fetchSitemapIndex(indexUrl, filterFn = null) {
    console.log(`\nFetching Sitemap Index: ${indexUrl}`);
    try {
        const response = await axios.get(indexUrl);
        const result = await xml2js.parseStringPromise(response.data);

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

export async function fetchArticleLinksFromSitemap(sitemapUrl) {
    try {
        const response = await axios.get(sitemapUrl);
        const result = await xml2js.parseStringPromise(response.data);

        if (!result.urlset || !result.urlset.url) {
            return [];
        }

        const articleLocs = result.urlset.url.map(u => u.loc[0]);
        return articleLocs;
    } catch (error) {
        console.error(`Error fetching/parsing article sitemap ${sitemapUrl}:`, error.message);
        return [];
    }
}
