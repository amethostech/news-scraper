import mongoose from 'mongoose';

const articleSchema = new mongoose.Schema({
    author: { type: String, required: false },
    date: { type: Date, required: false },
    extract: { type: String, required: true },
    link: { type: String, required: true, unique: true },
    source: { type: String, required: true },
    scrapedAt: { type: Date, default: Date.now }
});

const Article = mongoose.model('Article', articleSchema);

export default Article;
