@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
rem 保留原文件名作为兼容入口；完整回测需要数据、回测 API、本地平台 API 和前端。
call "%~dp0一键启动本地回测工作台.bat" %*
exit /b %ERRORLEVEL%
