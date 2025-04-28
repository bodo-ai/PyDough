#!/bin/bash

echo "MetaViewer - Graph Visualization Tool"
echo "------------------------------------"

# Detect if Python 3 is installed
if command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    echo "Error: Python is not installed. Please install Python to serve the application."
    exit 1
fi

# Check if a port was specified
if [ "$1" ]; then
    PORT="$1"
else
    PORT="8000"
fi

echo "Starting web server on port $PORT..."
echo "You can access the application at: http://localhost:$PORT"
echo "Press Ctrl+C to stop the server."
echo ""

# Change to the public directory and start a simple HTTP server
cd public && $PYTHON_CMD -m http.server $PORT 