param(
    [switch]$Check
)

$ErrorActionPreference = "Stop"
$Host.UI.RawUI.WindowTitle = "PXYBACKTEST live logs"

$logDirectory = "D:\x1\pxy-runtime\PXYBACKTEST\logs"
$runtimeLog = Join-Path $logDirectory "pxy-backtest.out.log"
$errorLog = Join-Path $logDirectory "pxy-backtest.err.log"
$pollAccessPattern = 'GET /api/v1/tasks/.*/events\?.* HTTP/1\.1" 200 OK'

[Console]::InputEncoding = [System.Text.UTF8Encoding]::new()
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()

function Get-LevelColor {
    param([string]$Level)

    switch -Regex ($Level) {
        '^(CRITICAL|FATAL|ERROR|ERR)$' { return 'Red' }
        '^(WARN|WARNING)$' { return 'Yellow' }
        '^(INFO|INFORMATION)$' { return 'Green' }
        '^(DEBUG|TRACE)$' { return 'DarkGray' }
        default { return 'Gray' }
    }
}

function Get-StatusColor {
    param([int]$Code)

    if ($Code -ge 500) { return 'Red' }
    if ($Code -ge 400) { return 'Yellow' }
    if ($Code -ge 300) { return 'Cyan' }
    if ($Code -ge 200) { return 'Green' }
    return 'Gray'
}

function Get-MessageColor {
    param([string]$Message)

    if ($Message -match '(?i)\b(critical|fatal|error|exception|traceback|failed|failure)\b|失败|异常|错误') {
        return 'Red'
    }
    if ($Message -match '(?i)\b(warn|warning)\b|警告') {
        return 'Yellow'
    }
    if ($Message -match '(?i)\b(info|success|completed|finished)\b|完成|成功|结束') {
        return 'Green'
    }
    if ($Message -match '进度|\d+%') {
        return 'Green'
    }
    return 'Gray'
}

function Write-ColoredSegment {
    param(
        [string]$Text,
        [string]$Color
    )

    if (-not [string]::IsNullOrEmpty($Text)) {
        Write-Host $Text -ForegroundColor $Color -NoNewline
    }
}

function Write-BacktestLogLine {
    param([string]$Line)

    if ($Line -match '^\[(?<source>RUN|ERR|POLL|INFO)\]\s*(?<content>.*)$') {
        $source = $Matches.source
        $content = $Matches.content
        $sourceColor = switch ($source) {
            'ERR' { 'Red' }
            'POLL' { 'Green' }
            default { 'Cyan' }
        }
        Write-ColoredSegment "[$source]" $sourceColor
        Write-ColoredSegment ' ' 'Gray'

        if ($content -match '^(?<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)(?<message>.*)$') {
            Write-ColoredSegment $Matches.timestamp 'DarkGray'
            Write-Host $Matches.message -ForegroundColor (Get-MessageColor $Matches.message)
            return
        }

        if ($content -match '^(?<level>INFO|WARNING|ERROR|DEBUG|CRITICAL):\s+(?<rest>.*)$') {
            $level = $Matches.level
            $rest = $Matches.rest
            Write-ColoredSegment $level (Get-LevelColor $level)
            Write-ColoredSegment ': ' 'DarkGray'
            if ($rest -match '^(?<ip>\S+)\s+-\s+"(?<method>[A-Z]+)\s+(?<path>[^"]+)"\s+(?<code>\d{3})(?<tail>.*)$') {
                Write-ColoredSegment $Matches.ip 'DarkGray'
                Write-ColoredSegment ' - "' 'DarkGray'
                Write-ColoredSegment $Matches.method 'Cyan'
                Write-ColoredSegment ' ' 'Gray'
                Write-ColoredSegment $Matches.path 'White'
                Write-ColoredSegment '" ' 'DarkGray'
                $statusColor = Get-StatusColor ([int]$Matches.code)
                Write-ColoredSegment $Matches.code $statusColor
                Write-Host $Matches.tail -ForegroundColor $statusColor
                return
            }
            Write-Host $rest -ForegroundColor (Get-LevelColor $level)
            return
        }

        if ($source -eq 'ERR') {
            Write-Host $content -ForegroundColor 'Red'
        } elseif ($source -eq 'POLL') {
            Write-Host $content -ForegroundColor 'Green'
        } else {
            Write-Host $content -ForegroundColor (Get-MessageColor $content)
        }
        return
    }

    Write-Host $Line -ForegroundColor (Get-MessageColor $Line)
}

function ConvertFrom-LogTransportLine {
    param([string]$Line)
    if ($Line.StartsWith("B64:")) {
        $bytes = [Convert]::FromBase64String($Line.Substring(4))
        return [Text.Encoding]::UTF8.GetString($bytes)
    }
    return $Line
}

Write-Host "============================================================" -ForegroundColor DarkCyan
Write-Host "  PXYBACKTEST merged live logs [Ctrl+C to exit]" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor DarkCyan

if (-not (Test-Path -LiteralPath $logDirectory -PathType Container)) {
    Write-Error "Log directory not found: $logDirectory"
}
if (-not (Test-Path -LiteralPath $runtimeLog -PathType Leaf)) {
    Write-Error "Runtime log not found: $runtimeLog"
}

if ($Check) {
    $probeJob = Start-Job -ScriptBlock {
        $bytes = [Text.Encoding]::UTF8.GetBytes("回测日志")
        "B64:$([Convert]::ToBase64String($bytes))"
    }
    try {
        Wait-Job -Job $probeJob | Out-Null
        $probe = Receive-Job -Job $probeJob | Select-Object -First 1
        if ((ConvertFrom-LogTransportLine ([string]$probe)) -ne "回测日志") {
            throw "Windows PowerShell UTF-8 log transport check failed."
        }
    }
    finally {
        Remove-Job -Job $probeJob -Force -ErrorAction SilentlyContinue
    }
    Write-Host "[OK] PXYBACKTEST log paths are ready." -ForegroundColor Green
    exit 0
}

$jobs = @()
try {
    $jobs += Start-Job -Name "pxybacktest-runtime" -ArgumentList $runtimeLog, $pollAccessPattern -ScriptBlock {
        param($path, $accessPattern)
        function Write-EncodedLine {
            param([string]$Text)
            $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
            "B64:$([Convert]::ToBase64String($bytes))"
        }
        # 只显示窗口启动后新增的日志；成功轮询按 5 秒汇总，既能确认刷新生效又避免刷屏。
        [long]$lastPollTick = 0
        $pollCount = 0
        Get-Content -LiteralPath $path -Encoding UTF8 -Tail 0 -Wait |
            ForEach-Object {
                $line = [string]$_
                if ($line -match $accessPattern) {
                    $pollCount++
                    [long]$nowTick = [Environment]::TickCount64
                    if ($lastPollTick -eq 0 -or ($nowTick - $lastPollTick) -ge 5000) {
                        Write-EncodedLine "[POLL] events 200 OK count=$pollCount latest=$($line.Trim())"
                        $lastPollTick = $nowTick
                        $pollCount = 0
                    }
                    return
                }
                if (-not [string]::IsNullOrWhiteSpace($line)) {
                    Write-EncodedLine "[RUN] $line"
                }
            }
    }

    if (Test-Path -LiteralPath $errorLog -PathType Leaf) {
        $jobs += Start-Job -Name "pxybacktest-error" -ArgumentList $errorLog -ScriptBlock {
            param($path)
            function Write-EncodedLine {
                param([string]$Text)
                $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
                "B64:$([Convert]::ToBase64String($bytes))"
            }
            # 错误日志同样只看本次启动后的新增内容。
            Get-Content -LiteralPath $path -Encoding UTF8 -Tail 0 -Wait |
                ForEach-Object {
                    $line = [string]$_
                    if (-not [string]::IsNullOrWhiteSpace($line)) {
                        Write-EncodedLine "[ERR] $line"
                    }
                }
        }
    }

    Write-Host "[INFO] Runtime and error logs are merged in this window." -ForegroundColor Cyan
    Write-Host "[INFO] Successful /events polling lines are aggregated every 5 seconds." -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor DarkCyan

    while ($true) {
        foreach ($job in $jobs) {
            Receive-Job -Job $job | ForEach-Object {
                Write-BacktestLogLine (ConvertFrom-LogTransportLine ([string]$_))
            }
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
