param(
    [switch]$Check
)

$ErrorActionPreference = "Stop"
$Host.UI.RawUI.WindowTitle = "PXYBACKTEST live logs"

$logDirectory = "D:\x1\pxy-runtime\PXYBACKTEST\logs"
$runtimeLog = Join-Path $logDirectory "pxy-backtest.out.log"
$errorLog = Join-Path $logDirectory "pxy-backtest.err.log"
$pollAccessPattern = 'GET /api/v1/tasks/.*/events\?.* HTTP/1\.1" 200 OK'

Write-Host "============================================================"
Write-Host "  PXYBACKTEST merged live logs [Ctrl+C to exit]"
Write-Host "============================================================"

if (-not (Test-Path -LiteralPath $logDirectory -PathType Container)) {
    Write-Error "Log directory not found: $logDirectory"
}
if (-not (Test-Path -LiteralPath $runtimeLog -PathType Leaf)) {
    Write-Error "Runtime log not found: $runtimeLog"
}

if ($Check) {
    Write-Host "[OK] PXYBACKTEST log paths are ready."
    exit 0
}

$jobs = @()
try {
    $jobs += Start-Job -Name "pxybacktest-runtime" -ArgumentList $runtimeLog, $pollAccessPattern -ScriptBlock {
        param($path, $accessPattern)
        Get-Content -LiteralPath $path -Tail 100 -Wait |
            Where-Object { $_ -notmatch $accessPattern } |
            ForEach-Object { "[RUN] $_" }
    }

    if (Test-Path -LiteralPath $errorLog -PathType Leaf) {
        $jobs += Start-Job -Name "pxybacktest-error" -ArgumentList $errorLog -ScriptBlock {
            param($path)
            Get-Content -LiteralPath $path -Tail 100 -Wait |
                ForEach-Object { "[ERR] $_" }
        }
    }

    Write-Host "[INFO] Runtime and error logs are merged in this window."
    Write-Host "[INFO] Repeated successful /events polling lines are hidden."
    Write-Host "============================================================"

    while ($true) {
        foreach ($job in $jobs) {
            Receive-Job -Job $job
        }
        Start-Sleep -Milliseconds 100
    }
}
finally {
    if ($jobs.Count -gt 0) {
        Stop-Job -Job $jobs -ErrorAction SilentlyContinue
        Remove-Job -Job $jobs -Force -ErrorAction SilentlyContinue
    }
}
