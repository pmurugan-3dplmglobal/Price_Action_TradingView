@echo off
REM ====================================================================
REM  Automated Strategy Daily Exporter Launcher
REM ====================================================================
cd /d "%~dp0"

echo Select Execution Mode:
echo 1. Run Export Now (Auto-detect slot or 10_30_AM)
echo 2. Run Export Daemon (Stays active and runs at 10:30 AM, 1:00 PM, 3:15 PM)
echo.
set /p mode="Enter choice (1 or 2): "

if "%mode%"=="2" (
    python Trade_Option/run_export_scheduler_daemon.py
) else (
    python Trade_Option/automated_strategy_exporter.py --slot=10_30_AM
)
pause
