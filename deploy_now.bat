@echo off
color 0A
title NEMESIS AI Deployer
cd /d "%~dp0"
echo Starting Autonomous Deployer...
call venv\Scripts\activate.bat
python scripts\autonomous_deployer.py
echo.
echo ========================================================
echo Deployment Loop Finished. Please copy the text above and
echo paste it to the AI.
echo ========================================================
pause
