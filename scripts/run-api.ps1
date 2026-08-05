[CmdletBinding()]
param(
    [string]$HostAddress = "127.0.0.1",
    [int]$Port = 3024,
    [string]$PxyLhRoot = $env:PXYBACKTEST_PXYLH_ROOT,
    [string]$Python = $env:PXYBACKTEST_PYTHON
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pxylhRoot = if ($PxyLhRoot) { $PxyLhRoot } else { Join-Path (Split-Path -Parent $repoRoot) "PXYLH" }
$pythonCandidates = @($Python,
    (Join-Path $pxylhRoot "venv312\Scripts\python.exe"),
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    "C:\Users\Work\AppData\Local\Programs\Python\Python312\python.exe",
    "C:\Python312\python.exe"
 ) | Where-Object { $_ }
$python = $pythonCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if (!$python) {
    throw "No supported Python executable was found."
}

$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PXYBACKTEST_PXYLH_ROOT = $pxylhRoot
$env:PYTHONPATH = "$repoRoot;$pxylhRoot;$(Join-Path $pxylhRoot 'backend')"
Push-Location $repoRoot
try {
    & $python -X utf8 -m uvicorn app.main:app --host $HostAddress --port $Port
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
