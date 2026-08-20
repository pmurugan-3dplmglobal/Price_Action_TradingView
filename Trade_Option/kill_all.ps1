Write-Host "Killing Prod_code_01/Price_Action_Strategy processes..." -ForegroundColor Yellow
Write-Host ""

$procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'"
$targets = $procs | Where-Object {
    $_.CommandLine -match 'Price_Action_Strategy' -and (
        $_.CommandLine -match 'index_options_trade_engine' -or
        $_.CommandLine -match 'stock_options_trade_engine' -or
        $_.CommandLine -match 'stock_bullish_reversal_scanner' -or
        $_.CommandLine -match 'stock_bearish_reversal_scanner' -or
        $_.CommandLine -match 'bull_index_trade_engine' -or
        $_.CommandLine -match 'bull_nifty50_scanner_executor' -or
        $_.CommandLine -match 'app_option_Trade' -or
        $_.CommandLine -match 'app_Stock_Trade'
    )
}

if ($targets) {
    $targets | ForEach-Object {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        $name = [System.IO.Path]::GetFileName(($_.CommandLine -replace '"','').Trim().Split()[-1])
        Write-Host "[KILLED] PID $($_.ProcessId) - $name" -ForegroundColor Green
    }
} else {
    Write-Host "No matching processes found." -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Done."
