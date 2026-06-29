#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
if command -v python3 &>/dev/null; then
    python3 app.py
elif command -v python &>/dev/null; then
    python app.py
else
    echo "Python not found. Please install Python 3.8+ from https://python.org"
    read -r -p "Press Enter to close..."
fi
