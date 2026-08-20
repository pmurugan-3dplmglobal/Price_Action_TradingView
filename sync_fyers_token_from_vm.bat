@echo off
REM ====================================================================
REM  Sync Fyers Access Token from Oracle Cloud VM to Local Machine
REM ====================================================================
cd /d "%~dp0"

set "KEY_PATH=G:\Poovendan\AI\Trading\Cloud\Oracle_Cloud\ssh-key-2026-08-05.key"
set "VM_HOST=opc@140.245.197.71"
set "VM_TOKEN_PATH=/home/opc/Price_Action_TradingView/input/fyers_access_token.txt"

echo [1/3] Downloading Fyers Access Token from Oracle Cloud VM...
scp -o StrictHostKeyChecking=no -i "%KEY_PATH%" %VM_HOST%:%VM_TOKEN_PATH% "input\fyers_access_token.txt"

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to download token from VM. Please check your network / SSH key.
    pause
    exit /b %ERRORLEVEL%
)

echo [2/3] Mirroring token to Trade_Option and Trade_Stock input folders...
if not exist "Trade_Option\input" mkdir "Trade_Option\input"
if not exist "Trade_Stock\input" mkdir "Trade_Stock\input"

copy /Y "input\fyers_access_token.txt" "Trade_Option\input\fyers_access_token.txt" >nul
copy /Y "input\fyers_access_token.txt" "Trade_Stock\input\fyers_access_token.txt" >nul

echo [3/3] Verifying Fyers Authentication...
python -c "import sys; sys.path.insert(0, 'common'); from fyers_session import is_fyers_authenticated; auth = is_fyers_authenticated(); print('--> Fyers Authenticated Locally:', auth); sys.exit(0 if auth else 1)"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ====================================================================
    echo  SUCCESS: Fyers token synchronized and verified successfully!
    echo ====================================================================
) else (
    echo.
    echo [WARNING] Token file copied but authentication check did not pass.
)

echo.
pause
