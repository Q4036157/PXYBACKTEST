[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CliArgs
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.Encoding]::UTF8

$backtestRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$workspaceRoot = Split-Path -Parent $backtestRoot
$runtimeRoot = Join-Path (Split-Path -Parent $workspaceRoot) "pxy-runtime\PXYBACKTEST\local-workbench"
$tokenFile = Join-Path $runtimeRoot "local-service-token"
$python = Join-Path $backtestRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    $python = Join-Path $workspaceRoot "PXYLH\venv312\Scripts\python.exe"
}
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "本地回测 Python 环境尚未就绪。"
}
if (-not (Test-Path -LiteralPath $tokenFile -PathType Leaf)) {
    throw "请先运行一键启动本地回测工作台。"
}
if (-not $CliArgs -or $CliArgs.Count -eq 0) {
    $CliArgs = @("health")
}

Push-Location $backtestRoot
try {
    & $python -X utf8 -m app.cli `
        --base-url "http://127.0.0.1:3024" `
        --token-file $tokenFile `
        --user-id "local-backtest-user" `
        --source-node "local-agent" `
        @CliArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
