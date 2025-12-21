#!/bin/bash
echo "MongoDB Connection Setup"
echo "========================"
echo ""
echo "Choose your MongoDB setup:"
echo "1) MongoDB Atlas (Cloud)"
echo "2) Local MongoDB"
echo "3) Remote MongoDB Server"
echo ""
read -p "Enter choice (1-3): " choice

case $choice in
  1)
    echo ""
    echo "For MongoDB Atlas, you need:"
    echo "  - Connection string from Atlas dashboard"
    echo "  - Format: mongodb+srv://username:password@cluster.mongodb.net/news-scraper"
    echo ""
    read -p "Enter your MongoDB Atlas connection string: " uri
    echo "MONGO_URI=$uri" > .env
    echo ""
    echo "✓ Created .env file with MongoDB Atlas connection"
    ;;
  2)
    echo "MONGO_URI=mongodb://localhost:27017/news-scraper" > .env
    echo ""
    echo "✓ Created .env file for local MongoDB"
    echo "  Make sure MongoDB is running locally"
    ;;
  3)
    echo ""
    echo "Format: mongodb://username:password@host:port/database"
    read -p "Enter your MongoDB connection string: " uri
    echo "MONGO_URI=$uri" > .env
    echo ""
    echo "✓ Created .env file with remote MongoDB connection"
    ;;
  *)
    echo "Invalid choice"
    exit 1
    ;;
esac

echo ""
echo "You can now run: node scripts/migrate-csv-to-mongodb.js"
