/**
 * Relevance Scoring Engine
 * Scores articles based on keywords, patterns, and categories
 */

/**
 * Calculate relevance score for an article based on config
 * @param {Object} metadata - Article metadata (title, description, link, categories)
 * @param {Object} config - Scoring configuration
 * @returns {Object} { score: number, matchedKeywords: array, priority: string }
 */
export function calculateRelevanceScore(metadata, config) {
    const { title = '', description = '', link = '', categories = [] } = metadata;
    const searchText = `${title} ${description} ${link} ${categories.join(' ')}`.toLowerCase();
    
    let score = 0;
    const matchedKeywords = [];
    const matchedPatterns = [];

    // 1. Keyword Matching (0-0.6 points)
    if (config.keywords) {
        const keywordScores = calculateKeywordScore(searchText, config.keywords);
        score += keywordScores.score;
        matchedKeywords.push(...keywordScores.matched);
    }

    // 2. URL Pattern Matching (0-0.2 points)
    if (config.urlPatterns) {
        const patternScore = calculatePatternScore(link, config.urlPatterns);
        score += patternScore.score;
        if (patternScore.matched) {
            matchedPatterns.push(...patternScore.matched);
        }
    }

    // 3. Category Matching (0-0.2 points)
    if (config.categories && categories.length > 0) {
        const categoryScore = calculateCategoryScore(categories, config.categories);
        score += categoryScore;
    }

    // Cap score at 1.0
    score = Math.min(score, 1.0);

    // Determine priority
    let priority = 'low';
    if (score >= (config.highThreshold || 0.8)) {
        priority = 'high';
    } else if (score >= (config.mediumThreshold || 0.5)) {
        priority = 'medium';
    }

    return {
        score: Math.round(score * 100) / 100, // Round to 2 decimal places
        priority,
        matchedKeywords,
        matchedPatterns
    };
}

/**
 * Calculate score based on keyword matching
 * @param {string} text - Text to search in
 * @param {Object} keywords - Keyword configuration
 * @returns {Object} { score: number, matched: array }
 */
function calculateKeywordScore(text, keywords) {
    let score = 0;
    const matched = [];

    // High priority keywords
    if (keywords.high) {
        for (const keyword of keywords.high) {
            const regex = new RegExp(`\\b${keyword.toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'gi');
            const matches = text.match(regex);
            if (matches) {
                const weight = keywords.weights?.high || 0.3;
                score += Math.min(matches.length * weight, 0.2); // Cap high keywords at 0.2
                matched.push(keyword);
            }
        }
    }

    // Medium priority keywords
    if (keywords.medium) {
        for (const keyword of keywords.medium) {
            const regex = new RegExp(`\\b${keyword.toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'gi');
            const matches = text.match(regex);
            if (matches) {
                const weight = keywords.weights?.medium || 0.1;
                score += Math.min(matches.length * weight, 0.2); // Cap medium keywords at 0.2
                if (!matched.includes(keyword)) {
                    matched.push(keyword);
                }
            }
        }
    }

    // Low priority keywords (bonus points)
    if (keywords.low) {
        for (const keyword of keywords.low) {
            const regex = new RegExp(`\\b${keyword.toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'gi');
            const matches = text.match(regex);
            if (matches) {
                const weight = keywords.weights?.low || 0.05;
                score += Math.min(matches.length * weight, 0.1); // Cap low keywords at 0.1
            }
        }
    }

    return { score: Math.min(score, 0.6), matched };
}

/**
 * Calculate score based on URL patterns
 * @param {string} url - Article URL
 * @param {Object} patterns - Pattern configuration
 * @returns {Object} { score: number, matched: array }
 */
function calculatePatternScore(url, patterns) {
    let score = 0;
    const matched = [];

    // Include patterns (positive scoring)
    if (patterns.include) {
        for (const pattern of patterns.include) {
            if (url.includes(pattern)) {
                score += 0.1;
                matched.push(pattern);
            }
        }
    }

    // Exclude patterns (negative scoring - reduce score)
    if (patterns.exclude) {
        for (const pattern of patterns.exclude) {
            if (url.includes(pattern)) {
                score -= 0.3; // Heavy penalty for excluded patterns
                matched.push(`exclude:${pattern}`);
            }
        }
    }

    return { score: Math.max(0, Math.min(score, 0.2)), matched };
}

/**
 * Calculate score based on category matching
 * @param {Array} articleCategories - Categories from article
 * @param {Object} configCategories - Category configuration
 * @returns {number} Score (0-0.2)
 */
function calculateCategoryScore(articleCategories, configCategories) {
    if (!configCategories.include || articleCategories.length === 0) {
        return 0;
    }

    const articleCategoriesLower = articleCategories.map(c => c.toLowerCase());
    let matches = 0;

    for (const category of configCategories.include) {
        if (articleCategoriesLower.some(ac => ac.includes(category.toLowerCase()))) {
            matches++;
        }
    }

    // Score based on number of matching categories
    return Math.min(matches * 0.1, 0.2);
}

/**
 * Batch score multiple articles
 * @param {Array} metadataArray - Array of article metadata
 * @param {Object} config - Scoring configuration
 * @returns {Array} Array of scored metadata with priority
 */
export function batchScoreArticles(metadataArray, config) {
    return metadataArray.map(metadata => {
        const scoring = calculateRelevanceScore(metadata, config);
        return {
            ...metadata,
            relevanceScore: scoring.score,
            priority: scoring.priority,
            keywords: scoring.matchedKeywords,
            matchedPatterns: scoring.matchedPatterns
        };
    });
}




