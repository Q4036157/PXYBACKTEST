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
$localTokenFile = Join-Path $runtimeRoot "local-service-token"
$installedTokenFile = "C:\ProgramData\PXY\secrets\pxy-backtest-service-token"
$python = Join-Path $backtestRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    $python = Join-Path $workspaceRoot "PXYLH\venv312\Scripts\python.exe"
}
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "本地回测 Python 环境尚未就绪。"
}
$configuredTokenFile = $env:PXYBACKTEST_SERVICE_TOKEN_FILE
$installedService = Get-Service -Name "pxy-backtest" -ErrorAction SilentlyContinue
if ($configuredTokenFile) {
    $tokenFile = $configuredTokenFile
} elseif (
    $installedService -and
    $installedService.Status -eq [ServiceProcess.ServiceControllerStatus]::Running
) {
    $tokenFile = $installedTokenFile
} elseif (Test-Path -LiteralPath $localTokenFile -PathType Leaf) {
    $tokenFile = $localTokenFile
} else {
    $tokenFile = $installedTokenFile
}
if (-not (Test-Path -LiteralPath $tokenFile -PathType Leaf)) {
    throw "找不到回测服务令牌，请先启动本地工作台或安装 PXYBACKTEST 服务。"
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
