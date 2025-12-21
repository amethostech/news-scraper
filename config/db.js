import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

// MongoDB URI from environment variable
// Format: mongodb+srv://<username>:<password>@cluster0.z2b6nbh.mongodb.net/<database>?appName=Cluster0
const MONGO_URI = process.env.MONGO_URI;

const connectDB = async () => {
    if (!MONGO_URI) {
        // MongoDB not configured - this is OK, CSV is primary storage
        throw new Error('MongoDB not configured. Set MONGO_URI in .env file');
    }
    
    try {
        // Add database name if not in URI
        let uri = MONGO_URI;
        // Check if database name is missing (URI ends with /? or just ?)
        if (uri.includes('/?') || (uri.endsWith('?') && !uri.match(/\/\w+\?/))) {
            // Add database name before the query string
            uri = uri.replace('/?', '/news-scraper?').replace(/\?appName=/, '/news-scraper?appName=');
        } else if (!uri.match(/\/\w+(\?|$)/)) {
            // No database name and no query string, add it
            uri = uri + '/news-scraper';
        }
        
        await mongoose.connect(uri, {
            serverSelectionTimeoutMS: 10000, // 10 second timeout
            socketTimeoutMS: 45000, // 45 second socket timeout
            connectTimeoutMS: 10000, // 10 second connection timeout
        });
        console.log('✓ MongoDB connected successfully');
    } catch (err) {
        console.error('MongoDB connection error:', err.message);
        if (err.message.includes('authentication')) {
            console.error('   → Check your username and password in the connection string');
        } else if (err.message.includes('timeout')) {
            console.error('   → Check your network connection and MongoDB server status');
        } else if (err.message.includes('ENOTFOUND') || err.message.includes('getaddrinfo')) {
            console.error('   → Check your MongoDB host/URL is correct');
        }
        throw err;
    }
};

export default connectDB;
