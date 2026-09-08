@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
title PXYBACKTEST 本地回测工作台
set "ARGS="
if /I "%~1"=="--check" set "ARGS=-CheckOnly"
if /I "%~1"=="--no-browser" set "ARGS=-NoBrowser"
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-local-workbench.ps1" %ARGS%
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" pause
exit /b %RC%

