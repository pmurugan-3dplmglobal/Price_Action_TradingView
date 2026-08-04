@echo off
title Kill Price Action Strategy (Prod_code_01)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0kill_all.ps1"
pause
