@echo off
setlocal EnableExtensions
chcp 65001 >nul 2>&1
title PXYBACKTEST 本地回测前端

rem 回测页面由相邻的 PXYLH 仓库维护；本脚本提供 PXYBACKTEST 的统一本地入口。
for %%I in ("%~dp0..\..\PXYLH") do set "PXYLH_ROOT=%%~fI"
set "FRONTEND_ROOT=%PXYLH_ROOT%\frontend"
set "FRONTEND_URL=http://127.0.0.1:3000/backtest"

echo ============================================================
echo   PXYBACKTEST 本地回测前端
echo   源码：%FRONTEND_ROOT%
echo   地址：%FRONTEND_URL%
echo ============================================================

if not exist "%FRONTEND_ROOT%\package.json" goto :missing_frontend
where node.exe >nul 2>&1 || goto :missing_node
where npm.cmd >nul 2>&1 || goto :missing_node

if /I "%~1"=="--check" goto :check_ok

curl.exe -fsS --max-time 2 "http://127.0.0.1:3000/" >nul 2>&1
if not errorlevel 1 goto :open_page

if not exist "%FRONTEND_ROOT%\node_modules" (
    echo.
    echo [准备] 首次运行，正在安装前端依赖……
    pushd "%FRONTEND_ROOT%" >nul
    call npm.cmd ci
    if errorlevel 1 (
        popd >nul
        goto :install_failed
    )
    popd >nul
)

echo.
echo [启动] 正在打开 Vite 开发服务器……
start "PXYBACKTEST 本地前端" /D "%FRONTEND_ROOT%" "%ComSpec%" /d /k npm.cmd run dev -- --host 127.0.0.1

echo [等待] 正在等待 http://127.0.0.1:3000 ……
for /L %%I in (1,1,60) do (
    curl.exe -fsS --max-time 1 "http://127.0.0.1:3000/" >nul 2>&1
    if not errorlevel 1 goto :open_page
    timeout /t 1 /nobreak >nul
)
goto :start_timeout

:open_page
echo [完成] 前端已启动，正在打开回测页面。
start "" "%FRONTEND_URL%"
exit /b 0

:check_ok
echo [检查通过] PXYLH 前端目录、Node.js 和 npm 均已就绪。
exit /b 0

:missing_frontend
echo.
echo [错误] 未找到 PXYLH 前端：%FRONTEND_ROOT%
echo 请确认 PXYBACKTEST 与 PXYLH 位于同一父目录。
goto :failed

:missing_node
echo.
echo [错误] 未找到 Node.js 或 npm，请先安装 Node.js。
goto :failed

:install_failed
echo.
echo [错误] npm ci 执行失败，请查看上方安装日志。
goto :failed

:start_timeout
echo.
echo [错误] 前端在 60 秒内没有监听 3000 端口，请查看新窗口日志。

:failed
if /I not "%PXY_AGENT_CLI%"=="1" pause
exit /b 1
