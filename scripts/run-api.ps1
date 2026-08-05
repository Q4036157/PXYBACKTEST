[CmdletBinding()]
param(
    [string]$HostAddress = "127.0.0.1",
    [int]$Port = 3024
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonCandidates = @(
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    "C:\Users\Work\AppData\Local\Programs\Python\Python312\python.exe",
    "C:\Python312\python.exe"
)
$python = $pythonCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if (!$python) {
    throw "No supported Python executable was found."
}

$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONPATH = $repoRoot
Push-Location $repoRoot
try {
    & $python -X utf8 -m uvicorn app.main:app --host $HostAddress --port $Port
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

