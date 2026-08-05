[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pxylhRoot = if ($env:PXYBACKTEST_PXYLH_ROOT) {
    $env:PXYBACKTEST_PXYLH_ROOT
} else {
    "D:\x1\x2\PXYLH"
}
$pxylhRequirements = Join-Path $pxylhRoot "backend\requirements.txt"
$pythonCandidates = @(
    "C:\Users\Work\AppData\Local\Programs\Python\Python312\python.exe",
    "C:\Python312\python.exe"
)
$python = $pythonCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if (!$python) { throw "No supported Python 3.12 executable was found." }
if (!(Test-Path -LiteralPath $pxylhRequirements -PathType Leaf)) {
    throw "PXYLH requirements were not found: $pxylhRequirements"
}

$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (!(Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    & $python -m venv (Join-Path $repoRoot ".venv")
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

& $venvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $venvPython -m pip install -r (Join-Path $repoRoot "requirements.txt") -r $pxylhRequirements
exit $LASTEXITCODE
