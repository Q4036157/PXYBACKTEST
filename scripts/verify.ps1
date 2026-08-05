[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonCandidates = @(
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    "C:\Users\Work\AppData\Local\Programs\Python\Python312\python.exe",
    "C:\Python312\python.exe"
)
$python = $pythonCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if (!$python) { throw "No supported Python executable was found." }

Push-Location $repoRoot
try {
    & $python -m pytest -q
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $python -m compileall -q app tests
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

