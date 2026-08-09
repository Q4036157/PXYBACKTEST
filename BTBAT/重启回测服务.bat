@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title PXYBACKTEST restart service

rem ============================================================
rem  one-click restart pxy-backtest service (admin required)
rem ============================================================

rem --- check admin, self-elevate if not ---
if defined PXY_RESTART_ELEVATED goto :admin_check_done
powershell.exe -NoLogo -NoProfile -Command "$identity=[Security.Principal.WindowsIdentity]::GetCurrent();$principal=New-Object Security.Principal.WindowsPrincipal($identity);if($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 0}else{exit 1}" >nul 2>&1
if errorlevel 1 (
  echo Requesting administrator privileges...
  set "PXY_RESTART_BAT=%~f0"
  set "PXY_RESTART_ELEVATED=1"
  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "try { Start-Process -FilePath $env:ComSpec -ArgumentList '/d /k call ""%~f0""' -Verb RunAs -ErrorAction Stop; exit 0 } catch { Write-Host ('[FAIL] Unable to request administrator privileges: ' + $_.Exception.Message); exit 1 }"
  if errorlevel 1 (
    echo.
    echo [FAIL] Administrator window could not be started.
    echo Right-click this file and select ^"Run as administrator^".
    echo.
    pause
    exit /b 1
  )
  exit /b 0
)

:admin_check_done
echo ============================================================
echo   PXYBACKTEST service restart
echo   service : pxy-backtest
echo   port    : 127.0.0.1:3024
echo ============================================================

echo.
echo [1/3] Stopping pxy-backtest ...
sc stop pxy-backtest >nul 2>&1
for /l %%i in (1,1,60) do (
  sc query pxy-backtest | findstr /C:"STOPPED" >nul 2>&1
  if not errorlevel 1 goto :stopped
  timeout /t 1 /nobreak >nul
)

echo.
echo [FAIL] pxy-backtest did not stop within 60s
echo  logs: BTBAT\一键实时查看回测日志.bat
echo.
pause
exit /b 1

:stopped

echo [2/3] Starting pxy-backtest ...
sc start pxy-backtest >nul 2>&1
if errorlevel 1 (
  echo.
  echo [FAIL] sc start pxy-backtest failed
  echo  logs: BTBAT\一键实时查看回测日志.bat
  echo.
  pause
  exit /b 1
)

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
  echo.
  pause
  exit /b 1
)
echo.
pause
exit /b 0
