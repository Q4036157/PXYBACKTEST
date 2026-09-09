@echo off
setlocal EnableExtensions
set "PXYBACKTEST_ROOT=%~dp0"
if not defined PXYBACKTEST_PYTHON set "PXYBACKTEST_PYTHON=%PXYBACKTEST_ROOT%.venv\Scripts\python.exe"
if not exist "%PXYBACKTEST_PYTHON%" set "PXYBACKTEST_PYTHON=D:\x1\x2\PXYBACKTEST\.venv\Scripts\python.exe"
if not exist "%PXYBACKTEST_PYTHON%" set "PXYBACKTEST_PYTHON=D:\x1\x2\PXYLH\venv312\Scripts\python.exe"
if not exist "%PXYBACKTEST_PYTHON%" (
  echo [ERROR] PXYBACKTEST Python environment not found.
  exit /b 2
)
pushd "%PXYBACKTEST_ROOT%"
set "PYTHONPATH=%PXYBACKTEST_ROOT%;%PYTHONPATH%"
"%PXYBACKTEST_PYTHON%" -X utf8 -m app.cli %*
set "PXYBACKTEST_EXIT=%ERRORLEVEL%"
popd
exit /b %PXYBACKTEST_EXIT%
