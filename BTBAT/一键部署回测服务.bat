@echo off
setlocal EnableExtensions
title %~nx0
set "PXYOPS_DEPLOY=D:\x1\x2\PXYOPS\deploy\windows\app-win-01\部署工作站项目(无需管理员).bat"
if exist "%PXYOPS_DEPLOY%" goto deploy
echo [ERROR] Missing PXYOPS deployment entry: %PXYOPS_DEPLOY%
if /I not "%PXY_AGENT_CLI%"=="1" pause
exit /b 1

:deploy
call "%PXYOPS_DEPLOY%" -Project PXYBACKTEST %*
exit /b %ERRORLEVEL%
