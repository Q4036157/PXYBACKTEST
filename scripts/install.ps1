[CmdletBinding()]
param(
    [string]$PxyLhRoot = $env:PXYBACKTEST_PXYLH_ROOT,
    [string]$Python = $env:PXYBACKTEST_PYTHON
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pxylhRoot = if ($PxyLhRoot) {
    $PxyLhRoot
} else {
    Join-Path (Split-Path -Parent $repoRoot) "PXYLH"
}
$backendRoot = Join-Path $pxylhRoot "backend"
$pythonCandidates = @($Python,
    (Join-Path $pxylhRoot "venv312\Scripts\python.exe"),
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    "C:\Users\Work\AppData\Local\Programs\Python\Python312\python.exe",
    "C:\Python312\python.exe"
 ) | Where-Object { $_ }
$python = $pythonCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if (!$python) { throw "No supported Python 3.12 executable was found." }
if (!(Test-Path -LiteralPath $backendRoot -PathType Container)) {
    throw "PXYLH backend was not found: $backendRoot"
}

$env:PYTHONPATH = "$repoRoot;$pxylhRoot;$backendRoot"
& $python -c "import fastapi, pydantic, uvicorn; from app.main import create_app; from services.backtest_service.engine_runner import run_backtest_sync" 2>$null
if ($LASTEXITCODE -ne 0) {
    & $python -m pip install -r (Join-Path $repoRoot "requirements.txt")
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

& $python -c "from app.main import create_app; from services.backtest_service.engine_runner import run_backtest_sync; print('PXYBACKTEST runtime imports OK')"
exit $LASTEXITCODE
