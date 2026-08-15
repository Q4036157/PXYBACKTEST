@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title PXYBACKTEST one-click deploy

rem ============================================================
rem  PXYBACKTEST deploy: preflight, stop, install, health check
rem ============================================================

if /i "%~1"=="--check" goto :admin_ready

rem Request elevation when the file was started without admin rights.
if defined PXY_BACKTEST_DEPLOY_ELEVATED goto :admin_ready
powershell.exe -NoLogo -NoProfile -Command "$identity=[Security.Principal.WindowsIdentity]::GetCurrent();$principal=New-Object Security.Principal.WindowsPrincipal($identity);if($principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){exit 0}else{exit 1}" >nul 2>&1
if errorlevel 1 (
  echo Requesting administrator privileges...
  set "PXY_BACKTEST_DEPLOY_ELEVATED=1"
  powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "try { Start-Process -FilePath $env:ComSpec -ArgumentList '/d /k call ""%~f0""' -Verb RunAs -ErrorAction Stop; exit 0 } catch { Write-Host ('[FAIL] Unable to request administrator privileges: ' + $_.Exception.Message); exit 1 }"
  if errorlevel 1 (
    echo.
    echo [FAIL] Unable to open an administrator window.
    echo Right-click this BAT file and select Run as administrator.
    pause
    exit /b 1
  )
  exit /b 0
)

:admin_ready
set "SERVICE_NAME=pxy-backtest"
set "SERVICE_PORT=3024"
set "BACKTEST_PYTHON=D:\x1\x2\PXYBACKTEST\.venv\Scripts\python.exe"
set "INSTALLER=D:\x1\x2\PXYOPS\deploy\windows\app-win-01\Install-PxyBacktestService.ps1"
set "DEPLOYED_XML=C:\ProgramData\PXY\services\pxy-backtest\pxy-backtest.xml"

echo ============================================================
echo   PXYBACKTEST one-click deploy
echo   service: %SERVICE_NAME%
echo   port:    127.0.0.1:%SERVICE_PORT%
echo ============================================================

echo.
echo [1/5] Checking deployment files and isolated environment...
if not exist "%BACKTEST_PYTHON%" (
  echo [FAIL] Python not found: %BACKTEST_PYTHON%
  goto :failed
)
if not exist "%INSTALLER%" (
  echo [FAIL] Installer not found: %INSTALLER%
  goto :failed
)
"%BACKTEST_PYTHON%" -c "import fastapi, optuna, pyarrow, uvicorn; print('Environment OK: optuna=' + optuna.__version__ + ', pyarrow=' + pyarrow.__version__)"
if errorlevel 1 (
  echo [FAIL] The PXYBACKTEST environment is missing runtime dependencies.
  goto :failed
)
if /i "%~1"=="--check" (
  echo [OK] Deployment entry check passed. No service changes were made.
  exit /b 0
)

echo.
echo [2/5] Stopping the existing %SERVICE_NAME% service...
sc.exe query "%SERVICE_NAME%" >nul 2>&1
if errorlevel 1 goto :service_stopped
sc.exe query "%SERVICE_NAME%" | findstr /C:"STOPPED" >nul 2>&1
if not errorlevel 1 goto :service_stopped
sc.exe stop "%SERVICE_NAME%" >nul 2>&1
for /l %%i in (1,1,60) do (
  sc.exe query "%SERVICE_NAME%" | findstr /C:"STOPPED" >nul 2>&1
  if not errorlevel 1 goto :service_stopped
  timeout /t 1 /nobreak >nul
)
echo [FAIL] The service did not stop within 60 seconds.
goto :failed

:service_stopped
rem Allow Windows to release the WinSW executable before it is replaced.
timeout /t 2 /nobreak >nul

echo.
echo [3/5] Reinstalling and starting the WinSW service...
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%INSTALLER%"
if errorlevel 1 (
  echo [FAIL] The PXYOPS installer failed.
  goto :failed
)

echo.
echo [4/5] Verifying the deployed Python path...
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$expected='D:\x1\x2\PXYBACKTEST\.venv\Scripts\python.exe';$xml=[xml](Get-Content -LiteralPath '%DEPLOYED_XML%' -Raw);if($xml.service.executable -ne $expected){Write-Host ('[FAIL] Deployed path mismatch: ' + $xml.service.executable) -ForegroundColor Red;exit 1};Write-Host ('Deployed Python: ' + $expected) -ForegroundColor Green"
if errorlevel 1 goto :failed

echo.
echo [5/5] Verifying service state and health endpoint...
sc.exe query "%SERVICE_NAME%" | findstr /C:"RUNNING" >nul 2>&1
if errorlevel 1 (
  echo [FAIL] The service is not RUNNING.
  goto :failed
)
set "HTTP_CODE="
for /f "delims=" %%c in ('curl.exe -s -o nul -w "%%{http_code}" --max-time 5 http://127.0.0.1:%SERVICE_PORT%/health 2^>nul') do set "HTTP_CODE=%%c"
if not "!HTTP_CODE!"=="200" (
  echo [FAIL] Health check returned HTTP !HTTP_CODE!.
  goto :failed
)

echo.
echo [OK] PXYBACKTEST deployment completed and health check passed.
echo Logs: D:\x1\pxy-runtime\PXYBACKTEST\logs
echo.
pause
exit /b 0

:failed
echo.
echo Deployment did not complete. Keep this window open and inspect:
echo   D:\x1\pxy-runtime\PXYBACKTEST\logs
echo.
pause
exit /b 1
