@echo off
chcp 65001 >nul 2>&1
setlocal EnableExtensions
title 回测Agent CLI自检
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0Test-PxyBacktestAgentCli.ps1" %*
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" echo [FAILED] exit=%EXIT_CODE%
if not defined PXY_AGENT_CLI pause
exit /b %EXIT_CODE%
