[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$cli = Join-Path $root "pxybacktest.cmd"
$agentScript = Join-Path $PSScriptRoot "agent-backtest.ps1"

function Invoke-AgentCommand([string[]]$Arguments, [string]$ExpectedProperty) {
    Push-Location $root
    try {
        $raw = & $cli @Arguments
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

$agentRaw = & powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass `
    -File $agentScript health --json
if ($LASTEXITCODE -ne 0) {
    throw "PXYBACKTEST Agent entry health check failed: exit=$LASTEXITCODE"
}
$agentHealth = $agentRaw | ConvertFrom-Json
if ($agentHealth.ok -ne $true) {
    throw "PXYBACKTEST Agent entry did not return healthy status"
}
Write-Host "[OK] PXYBACKTEST Agent entry token selection passed." -ForegroundColor Green

Write-Host "[OK] PXYBACKTEST Agent CLI self-test passed." -ForegroundColor Green
