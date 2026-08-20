@echo off
REM ====================================================================
REM  Stock Trade Control Center Launcher (Port 6061)
REM ====================================================================
cd /d "%~dp0"
echo Starting Stock Trade Control Center on http://localhost:6061 ...
python Trade_Stock/app_Stock_Trade.py
pause
