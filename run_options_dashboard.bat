@echo off
REM ====================================================================
REM  Options Trade Control Center Launcher (Port 6060)
REM ====================================================================
cd /d "%~dp0"
echo Starting Options Trade Control Center on http://localhost:6060 ...
python Trade_Option/app_option_Trade.py
pause
