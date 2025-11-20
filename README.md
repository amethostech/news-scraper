# How to Add New Scrapers

The project has been refactored to support multiple scrapers easily.

## Directory Structure

- `scrapers/`: Contains individual scraper modules.
- `utils/`: Shared utilities (sitemap fetching, common functions).
- `models/`: Database models.
- `config/`: Configuration (DB connection).
- `scraper_backend.js`: Main server file.


- `scrapers/`: Contains individual scraper modules.
- `utils/`: Shared utilities (sitemap fetching, common functions).
- `models/`: Database models.
- `config/`: Configuration (DB connection).
- `scraper_backend.js`: Main server file.

## Adding a New Scraper

1.  **Create a new file** in `scrapers/` (e.g., `scraper
## Adding a New Scraper

1.  **Create a new file** in `scrapers/` (e.g., `scrapers/utilitydive.js`).
2.  **Copy the content** from `scrapers/biopharmadive.js` as a template.
3.  **Modify** the `run` function in the new file:
    *   Change `sourceName` to the name of the new site.
    *   Change `sitemapIndexUrl` to the sitemap URL of the new site.
    *   Adjust `sitemapFilter` and `linkFilter` if necessary (e.g., if the URL structure is different).
    *   If the site has different HTML structure for articles, modify `scrapeArticleDetails` function selectors.
4.  **Register the scraper**:
    *   Open `scrapers/index.js`.
    *   Import the new scraper: `import * as utilitydive from './utilitydive.js';`
    *   Add it to the `scrapers` array:
        ```javascript
        export const scrapers = [
            biopharmadive,
            utilitydive,
            // ...
        ];
        ```

## Running

When you start the server with `npm start`, all registered scrapers will run automatically in sequence.
