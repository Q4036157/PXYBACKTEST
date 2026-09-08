@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
title PXYBACKTEST 停止本地回测工作台
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop-local-workbench.ps1"
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" pause
exit /b %RC%

