@echo off
title %~nx0
if /I "%PXY_AGENT_CLI%"=="1" goto run_agent
if not defined PXY_FIXED_TAB_TITLE (
    wt.exe -w -1 new-tab --title "%~nx0" --suppressApplicationTitle cmd.exe /d /c "set PXY_FIXED_TAB_TITLE=1&& call ""%~f0"" %*"
    exit /b
)
:run_agent
setlocal EnableExtensions
chcp 65001 >nul 2>&1
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0agent-backtest.ps1" %*
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" echo [FAILED] exit=%EXIT_CODE%
if not defined PXY_AGENT_CLI pause
exit /b %EXIT_CODE%
