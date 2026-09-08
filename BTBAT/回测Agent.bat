@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0agent-backtest.ps1" %*
exit /b %ERRORLEVEL%
