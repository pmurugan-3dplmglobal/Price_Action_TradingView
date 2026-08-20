@echo off
REM ====================================================================
REM  Unified Trading Suite Dashboards Launcher
REM ====================================================================
cd /d "%~dp0"
echo Launching Options Dashboard (Port 6060) and Stock Dashboard (Port 6061)...

start "Options Dashboard (6060)" python Trade_Option/app_option_Trade.py
start "Stock Dashboard (6061)" python Trade_Stock/app_Stock_Trade.py

echo.
echo Both dashboards have been launched in background windows:
echo   - Options Control Center: http://localhost:6060
echo   - Stock Control Center:   http://localhost:6061
echo.
pause
