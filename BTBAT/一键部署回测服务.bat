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
set "BACKTEST_PYTHON=D:\x1\x2\PXYLH\venv312\Scripts\python.exe"
set "DAA_PYTHON=D:\x1\x2\DAA\backend\.venv\Scripts\python.exe"
set "INSTALLER=D:\x1\x2\PXYOPS\deploy\windows\app-win-01\Install-PxyBacktestService.ps1"
set "DEPLOYED_XML=C:\ProgramData\PXY\services\pxy-backtest\pxy-backtest.xml"
set "SERVICE_TOKEN_FILE=C:\ProgramData\PXY\secrets\pxy-backtest-service-token"
set "PXYDATA_KEY_FILE=C:\ProgramData\PXY\secrets\pxydata-api-key"
set "PYTHONPATH=D:\x1\x2\PXYBACKTEST;D:\x1\x2\PXYLH;D:\x1\x2\PXYLH\backend"
set "PXYBACKTEST_PXYLH_ROOT=D:\x1\x2\PXYLH"
set "PXYBACKTEST_RUNTIME_ROOT=D:\x1\pxy-runtime\PXYBACKTEST"
set "PXYBACKTEST_SERVICE_TOKEN_FILE=%SERVICE_TOKEN_FILE%"
set "PXYBACKTEST_PXYDATA_BASE_URL=http://127.0.0.1:3020"
set "PXYBACKTEST_PXYDATA_API_KEY_FILE=%PXYDATA_KEY_FILE%"

echo ============================================================
echo   PXYBACKTEST one-click deploy
echo   service: %SERVICE_NAME%
echo   port:    127.0.0.1:%SERVICE_PORT%
echo ============================================================

echo.
echo [1/5] Checking deployment files and target runtime...
if not exist "%BACKTEST_PYTHON%" (
  echo [FAIL] Python not found: %BACKTEST_PYTHON%
  goto :failed
)
if not exist "%INSTALLER%" (
  echo [FAIL] Installer not found: %INSTALLER%
  goto :failed
)
if not exist "%DAA_PYTHON%" (
  echo [FAIL] DAA Python not found: %DAA_PYTHON%
  goto :failed
)
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$token='%SERVICE_TOKEN_FILE%';$data='%PXYDATA_KEY_FILE%';if(-not(Test-Path -LiteralPath $token -PathType Leaf)){Write-Host ('[FAIL] Service token not found: '+$token) -ForegroundColor Red;exit 1};if((Get-Content -LiteralPath $token -Raw).Trim().Length -lt 32){Write-Host '[FAIL] Service token is too short.' -ForegroundColor Red;exit 1};if(-not(Test-Path -LiteralPath $data -PathType Leaf)){Write-Host ('[FAIL] PXYDATA API key not found: '+$data) -ForegroundColor Red;exit 1};if((Get-Content -LiteralPath $data -Raw).Trim().Length -lt 1){Write-Host '[FAIL] PXYDATA API key is empty.' -ForegroundColor Red;exit 1}"
if errorlevel 1 (
  echo [FAIL] Secret file preflight failed.
  goto :failed
)
"%BACKTEST_PYTHON%" -c "import aiosqlite, fastapi, optuna, pyarrow, uvicorn, vnpy; from app.main import create_app; from services.backtest_service.engine_runner import run_backtest_sync; print('Runtime OK: optuna=' + optuna.__version__ + ', pyarrow=' + pyarrow.__version__ + ', aiosqlite=' + aiosqlite.__version__)"
if errorlevel 1 (
  echo [FAIL] The target Python cannot import the PXYLH backtest worker.
  goto :failed
)
"%BACKTEST_PYTHON%" -c "import asyncio; from app.config import Settings; from app.daa_client import DaaAdapterClient; data=asyncio.run(DaaAdapterClient(Settings.from_env(), timeout_seconds=90).get_capabilities()); assert data.get('contract_version') == 'pxybacktest.engine-adapter.a-share.v1'; assert data.get('strategies'); print('DAA capabilities OK: strategies=' + str(len(data['strategies'])))"
if errorlevel 1 (
  echo [FAIL] DAA capabilities preflight failed.
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
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$expected='D:\x1\x2\PXYLH\venv312\Scripts\python.exe';$xml=[xml](Get-Content -LiteralPath '%DEPLOYED_XML%' -Raw);if($xml.service.executable -ne $expected){Write-Host ('[FAIL] Deployed path mismatch: ' + $xml.service.executable) -ForegroundColor Red;exit 1};Write-Host ('Deployed Python: ' + $expected) -ForegroundColor Green"
if errorlevel 1 goto :failed

echo.
echo [5/5] Verifying service state and health endpoint...
sc.exe query "%SERVICE_NAME%" | findstr /C:"RUNNING" >nul 2>&1
if errorlevel 1 (
  echo [FAIL] The service is not RUNNING.
  goto :failed
)
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command "$health=Invoke-RestMethod -Uri 'http://127.0.0.1:%SERVICE_PORT%/health' -TimeoutSec 8;if(-not $health.ok -or -not $health.serviceTokenConfigured -or -not $health.pxydataSnapshotConfigured){Write-Host '[FAIL] Health prerequisites are incomplete.' -ForegroundColor Red;exit 1};$token=(Get-Content -LiteralPath '%SERVICE_TOKEN_FILE%' -Raw).Trim();$headers=@{'X-PXY-Service-Token'=$token;'X-PXY-User-Id'='deployment-check';'X-PXY-Source-Node'='app-win-01'};$cap=Invoke-RestMethod -Uri 'http://127.0.0.1:%SERVICE_PORT%/api/v2/capabilities' -Headers $headers -TimeoutSec 20;$vnpy=$cap.engines|Where-Object id -eq 'vnpy_cta'|Select-Object -First 1;if($cap.task_contract -ne 'pxybacktest.task-result.v2' -or -not $vnpy.available){Write-Host '[FAIL] Capabilities contract check failed.' -ForegroundColor Red;exit 1};Write-Host '[OK] Health, PXYDATA and capabilities checks passed.' -ForegroundColor Green"
if errorlevel 1 goto :failed

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
