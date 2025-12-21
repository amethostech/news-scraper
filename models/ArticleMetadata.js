import mongoose from 'mongoose';

const articleMetadataSchema = new mongoose.Schema({
    title: { type: String, required: true },
    description: { type: String, required: false },
    link: { type: String, required: true, unique: true },
    source: { type: String, required: true },
    publishedDate: { type: Date, required: false },
    relevanceScore: { type: Number, required: true, min: 0, max: 1 },
    priority: { 
        type: String, 
        required: true, 
        enum: ['high', 'medium', 'low'],
        default: 'low'
    },
    status: { 
        type: String, 
        required: true, 
        enum: ['pending', 'scraped', 'queued', 'ignored'],
        default: 'pending'
    },
    keywords: [{ type: String }],
    categories: [{ type: String }],
    scrapedAt: { type: Date, required: false },
    createdAt: { type: Date, default: Date.now }
});

// Indexes for efficient querying
articleMetadataSchema.index({ link: 1 });
articleMetadataSchema.index({ source: 1, priority: 1 });
articleMetadataSchema.index({ status: 1, priority: 1 });
articleMetadataSchema.index({ relevanceScore: -1 });

const ArticleMetadata = mongoose.model('ArticleMetadata', articleMetadataSchema);

export default ArticleMetadata;




