[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "PXYBACKTEST Python environment is not installed: $python"
}

function Invoke-AgentCommand([string[]]$Arguments, [string]$ExpectedProperty) {
    Push-Location $root
    try {
        $raw = & $python -X utf8 -m app.cli @Arguments
    } finally {
        Pop-Location
    }
    if ($LASTEXITCODE -ne 0) {
        throw "PXYBACKTEST CLI failed: $($Arguments -join ' ') (exit=$LASTEXITCODE)"
    }
    $payload = $raw | ConvertFrom-Json
    if ($ExpectedProperty -and -not $payload.PSObject.Properties[$ExpectedProperty]) {
        throw "PXYBACKTEST CLI response is missing '$ExpectedProperty': $($Arguments -join ' ')"
    }
    Write-Host "[OK] $($Arguments -join ' ')" -ForegroundColor Green
}

Invoke-AgentCommand -Arguments @("--version") -ExpectedProperty "version"
Invoke-AgentCommand -Arguments @("--schema") -ExpectedProperty "commands"
Invoke-AgentCommand -Arguments @("health", "--json") -ExpectedProperty "ok"
Invoke-AgentCommand -Arguments @("capabilities", "--json") -ExpectedProperty "engines"

Write-Host "[OK] PXYBACKTEST Agent CLI self-test passed." -ForegroundColor Green
