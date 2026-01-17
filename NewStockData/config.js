/**
 * Stock Data Fetchers Configuration
 * 
 * Centralized API keys and settings for all data sources.
 */

// API Keys
export const API_KEYS = {
    SEC_EDGAR: '1bb985723f77e5cb4047b8cd8bc2a9d46d7a86e18c362af63785dcf47dc3f421',
    BENZINGA: 'bz.JZH6DRXBDZWPVJ4QZ6VRQ63GECADEVVU',
    YAHOO_FINANCE: 'a41a4979a4msh826a148f634efa5p105496jsn6e32a1b7a640',
    ALPHA_VANTAGE: 'A105CQN68K7AB6Y9',
    POLYGON_IO: 'fyOTJ8dHluJ5B35dzylD8PDPhELr24Lw'
};

// API Endpoints
export const API_ENDPOINTS = {
    SEC_EDGAR: 'https://api.sec-api.io',
    GDELT_DOC: 'https://api.gdeltproject.org/api/v2/doc/doc',
    GDELT_GEO: 'https://api.gdeltproject.org/api/v2/geo/geo',
    EMA: 'https://api.ema.europa.eu',
    BENZINGA: 'https://api.benzinga.com/api/v2',
    YAHOO_FINANCE: 'https://yahoo-finance15.p.rapidapi.com/api/v1',
    ALPHA_VANTAGE: 'https://www.alphavantage.co/query',
    POLYGON_IO: 'https://api.polygon.io'
};

// Polygon.io S3 / Flat Files Config
export const POLYGON_S3_CONFIG = {
    endPoint: 'files.massive.com',
    port: 443,
    useSSL: true,
    accessKey: '9b019425-13bd-4a77-899b-a53211c62a87',
    secretKey: 'NS4qCsO_46CwNzVB51hyoJHTdNEshnxy',
    bucket: 'flatfiles'
};

// RapidAPI Host for Yahoo Finance
export const RAPIDAPI_HOST = 'yahoo-finance15.p.rapidapi.com';

// Default search keywords for pharma/biotech
export const PHARMA_KEYWORDS = [
    'pharmaceutical',
    'biotech',
    'drug approval',
    'FDA',
    'clinical trial',
    'pharma',
    'medicine',
    'healthcare',
    'biopharmaceutical'
];

// Rate limiting settings (requests per minute)
export const RATE_LIMITS = {
    SEC_EDGAR: 5,      // Trial tier - limited
    GDELT: 60,         // Public - generous
    EMA: 30,           // Public - moderate
    BENZINGA: 30,      // Depends on plan
    YAHOO_FINANCE: 5,  // RapidAPI - throttled
    ALPHA_VANTAGE: 5,  // Free tier - 5/min
    POLYGON_IO: 5      // Free tier - 5/min
};

// Default date range (last 30 days)
export function getDefaultDateRange() {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 30);

    return {
        startDate: startDate.toISOString().split('T')[0],
        endDate: endDate.toISOString().split('T')[0]
    };
}

// Format date to YYYY-MM-DD
export function formatDate(date) {
    if (date instanceof Date) {
        return date.toISOString().split('T')[0];
    }
    return date;
}
