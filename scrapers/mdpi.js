import axios from 'axios';
import { sleep } from '../utils/common.js';
import Article from '../models/Article.js';

const JOURNAL_ISSNS = {
    medicines: '2305-6320',
    diagnostics: '2075-4418',
    healthcare: '2227-9032',
    pharmaceutics: '1999-4923',
    biomedicines: '2227-9059',
    cancers: '2072-6694',
    vaccines: '2076-393X',
    pathogens: '2076-0817',
    microorganisms: '2076-2607',
    antibiotics: '2079-6382',
    viruses: '1999-4915',
    genes: '2073-4425',
    life: '2075-1729',
};

const CROSSREF_API = 'https://api.crossref.org/works';
const EMAIL = 'your-email@example.com';
const BATCH_SIZE = 100;

async function fetchArticlesFromCrossref(issn, offset = 0) {
    try {
        const response = await axios.get(CROSSREF_API, {
            params: {
                filter: `issn:${issn}`,
                rows: BATCH_SIZE,
                offset,
                mailto: EMAIL
            },
            timeout: 30000
        });

        return response.data.message;
    } catch (err) {
        console.error(`[ERROR] Crossref request failed: ${err.message}`);
        return null;
    }
}

function convertCrossrefToArticle(item) {
    if (!item) return null;

    const authors = item.author?.map(a =>
        [a.given, a.family].filter(Boolean).join(' ')
    ).join(', ') || 'Unknown';

    let date = new Date();
    if (item.published?.['date-parts']?.[0]) {
        const [year, month = 1, day = 1] = item.published['date-parts'][0];
        date = new Date(year, month - 1, day);
    }

    const abstract = item.abstract?.trim() || "No abstract available";

    const link = item.URL || (item.DOI ? `https://doi.org/${item.DOI}` : null);
    if (!link) return null;

    return {
        author: authors,
        date,
        extract: abstract,
        link,
        source: "MDPI",
    };
}

export async function run() {
    console.log('\n[MDPI Crossref] Auto-Scraper Starting…');

    let savedCount = 0;

    for (const [journal, issn] of Object.entries(JOURNAL_ISSNS)) {
        console.log(`\n[${journal.toUpperCase()}] Starting...`);

        let offset = 0;
        let fetched = 0;

        while (true) {
            const batch = await fetchArticlesFromCrossref(issn, offset);
            if (!batch || !batch.items || batch.items.length === 0) break;

            fetched += batch.items.length;
            console.log(`  Fetched ${fetched}...`);

            for (const item of batch.items) {
                const article = convertCrossrefToArticle(item);
                if (!article) continue;

                // Check DB duplicate
                const exists = await Article.findOne({ link: article.link });
                if (exists) continue;

                // Save like other scrapers (one at a time)
                try {
                    await Article.create(article);
                    savedCount++;
                } catch (err) {
                    console.error(`[DB ERROR] Skipped one article: ${err.message}`);
                }

                await sleep(30); // Prevent DB overload
            }

            if (batch.items.length < BATCH_SIZE) break;

            offset += BATCH_SIZE;
            await sleep(500);
        }

        console.log(`[${journal.toUpperCase()}] Done. Saved so far: ${savedCount}`);
    }

    console.log(`\n[MDPI Crossref] Finished. Total saved: ${savedCount}`);
    return savedCount;
}

export function getFullTextUrl(doi) {
    const id = doi.replace('10.3390/', '');
    return `https://www.mdpi.com/${id}`;
}
