@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title PXYBACKTEST restart service

rem ============================================================
rem  one-click restart pxy-backtest service (admin required)
rem ============================================================

rem --- check admin, self-elevate if not ---
powershell.exe -NoLogo -NoProfile -Command "$identity=[Security.Principal.WindowsIdentity]::GetCurrent();$principal=New-Object Security.Principal.WindowsPrincipal($identity);if($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 0}else{exit 1}" >nul 2>&1
if errorlevel 1 (
  echo Requesting administrator privileges...
  powershell.exe -NoLogo -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
  exit /b %ERRORLEVEL%
)

echo ============================================================
echo   PXYBACKTEST service restart
echo   service : pxy-backtest
echo   port    : 127.0.0.1:3024
echo ============================================================

echo.
echo [1/3] Stopping pxy-backtest ...
sc stop pxy-backtest >nul 2>&1
timeout /t 3 /nobreak >nul

echo [2/3] Starting pxy-backtest ...
sc start pxy-backtest >nul 2>&1

echo [3/3] Waiting for health check ...
set "healthy="
for /l %%i in (1,1,30) do (
  timeout /t 1 /nobreak >nul
  set "code="
  for /f "delims=" %%c in ('curl -s -o nul -w "%%{http_code}" --max-time 2 http://127.0.0.1:3024/health 2^>nul') do set "code=%%c"
  if "!code!"=="200" (
    set "healthy=1"
  )
  if defined healthy goto :done
)

:done
if defined healthy (
  echo.
  echo [OK] pxy-backtest restarted, health check passed ^(3024 /health = 200^)
) else (
  echo.
  echo [FAIL] health check did not pass after 30s
  echo  logs: BTBAT\一键实时查看回测日志.bat
)
echo.
pause
exit /b 0
