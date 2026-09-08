[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$backtestRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$workspaceRoot = Split-Path -Parent $backtestRoot
$runtimeBase = Join-Path (Split-Path -Parent $workspaceRoot) "pxy-runtime\PXYBACKTEST\local-workbench"
$stateFile = Join-Path $runtimeBase "workbench-state.json"

if (-not (Test-Path -LiteralPath $stateFile -PathType Leaf)) {
    Write-Host "[回测工作台] 当前没有启动状态记录。"
    exit 0
}

$state = Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json
foreach ($record in @($state.processes) | Sort-Object pid -Descending) {
    $process = Get-Process -Id ([int]$record.pid) -ErrorAction SilentlyContinue
    if (-not $process) { continue }
    $expected = [datetime]::Parse([string]$record.start_time_utc).ToUniversalTime()
    $actual = $process.StartTime.ToUniversalTime()
    if ([math]::Abs(($actual - $expected).TotalSeconds) -gt 2) {
        Write-Host "[跳过] PID $($record.pid) 已被其他进程复用。" -ForegroundColor Yellow
        continue
    }
    & taskkill.exe /PID $record.pid /T /F 2>$null | Out-Null
    Write-Host "[停止] $($record.name) (PID $($record.pid))"
}
Remove-Item -LiteralPath $stateFile -Force
Write-Host "[完成] 本地回测工作台已停止。" -ForegroundColor Green

