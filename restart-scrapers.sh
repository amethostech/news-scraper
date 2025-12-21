#!/bin/bash

# Restart script for EndpointsNews and FierceBiotech scrapers

echo "🛑 Stopping any running scrapers..."

# Kill any running node processes for the scrapers
pkill -f "run-low-volume-sources" 2>/dev/null
pkill -f "node.*scraper" 2>/dev/null

# Wait a moment for processes to stop
sleep 2

# Kill any orphaned Puppeteer/Chrome processes (optional - be careful with this)
# pkill -f "chrome.*puppeteer" 2>/dev/null

echo "✅ Stopped existing processes"
echo ""
echo "🚀 Starting scrapers..."
echo ""

# Start the scrapers
node run-low-volume-sources.js --historical

