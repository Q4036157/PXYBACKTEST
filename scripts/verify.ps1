[CmdletBinding()]
param(
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
if (!$python) { throw "No supported Python executable was found." }

Push-Location $repoRoot
try {
    $env:PYTHONPATH = "$repoRoot;$pxylhRoot;$(Join-Path $pxylhRoot 'backend')"
    & $python -m pytest -q
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $python -m compileall -q app tests
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
