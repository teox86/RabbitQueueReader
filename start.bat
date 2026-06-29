@echo off
title RabbitMQ Stream Viewer
echo Starting RabbitMQ Stream Viewer...
python "%~dp0app.py"
if errorlevel 1 (
    echo.
    echo Python not found. Please install Python 3.8+ from https://python.org
    pause
)
