import mongoose from 'mongoose';

const articleSchema = new mongoose.Schema({
    title: { type: String, required: false },
    author: { type: String, required: false },
    date: { type: Date, required: false },
    extract: { type: String, required: true },
    link: { type: String, required: true, unique: true }, // unique: true creates an index automatically
    source: { type: String, required: true },
    scrapedAt: { type: Date, default: Date.now }
});

// Note: unique: true on link already creates an index, so we don't need to add another one

const Article = mongoose.model('Article', articleSchema);

export default Article;
