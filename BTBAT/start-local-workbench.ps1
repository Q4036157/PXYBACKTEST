[CmdletBinding()]
param(
    [switch]$CheckOnly,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.Encoding]::UTF8

$backtestRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$workspaceRoot = Split-Path -Parent $backtestRoot
$runtimeBase = Join-Path (Split-Path -Parent $workspaceRoot) "pxy-runtime\PXYBACKTEST\local-workbench"
$pxylhRoot = Join-Path $workspaceRoot "PXYLH"
$pxydataRoot = Join-Path $workspaceRoot "PXYDATA"
$frontendRoot = Join-Path $pxylhRoot "frontend"
$pxylhBackend = Join-Path $pxylhRoot "backend"
$pxylhPython = Join-Path $pxylhRoot "venv312\Scripts\python.exe"
$pxylhVenvConfig = Join-Path $pxylhRoot "venv312\pyvenv.cfg"
$dataLauncher = Join-Path $pxydataRoot "DATABAT\启动PXYDATA数据服务(Reload).ps1"
$backtestLauncher = Join-Path $backtestRoot "scripts\run-api.ps1"
$localApiModule = Join-Path $pxylhBackend "backtest_local_app.py"
$dataKeysFile = Join-Path $pxydataRoot "DATABAT\keys.local.env"
$logsRoot = Join-Path $runtimeBase "logs"
$tokenFile = Join-Path $runtimeBase "local-service-token"
$stateFile = Join-Path $runtimeBase "workbench-state.json"
$frontendUrl = "http://127.0.0.1:3000/backtest"

function Write-Step([string]$Message) {
    Write-Host "[回测工作台] $Message" -ForegroundColor Cyan
}

function Test-Http([string]$Url, [int]$TimeoutSeconds = 2) {
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSeconds
        return $response.StatusCode -ge 200 -and $response.StatusCode -lt 400
    } catch {
        return $false
    }
}

function Resolve-Python312Home {
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"),
        "C:\Python312\python.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate -PathType Leaf) {
            return Split-Path -Parent $candidate
        }
    }
    $py = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($py) {
        $resolved = (& $py.Source -3.12 -c "import sys; print(sys.executable)" 2>$null | Select-Object -First 1).Trim()
        if ($resolved -and (Test-Path -LiteralPath $resolved -PathType Leaf)) {
            return Split-Path -Parent $resolved
        }
    }
    throw "未找到 Python 3.12。"
}

function Repair-VenvHome {
    param([string]$ConfigPath, [string]$PythonPath)

    if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) {
        throw "PXYLH Python 3.12 环境不存在：$PythonPath"
    }
    if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
        return
    }
    $text = Get-Content -LiteralPath $ConfigPath -Raw
    $match = [regex]::Match($text, "(?m)^home\s*=\s*(.+?)\s*$")
    $configuredHome = if ($match.Success) { $match.Groups[1].Value.Trim() } else { "" }
    if ($configuredHome -and (Test-Path -LiteralPath (Join-Path $configuredHome "python.exe") -PathType Leaf)) {
        return
    }
    $pythonHome = Resolve-Python312Home
    $updated = if ($match.Success) {
        [regex]::Replace($text, "(?m)^home\s*=.*$", "home = $pythonHome")
    } else {
        "home = $pythonHome`r`n$text"
    }
    [IO.File]::WriteAllText($ConfigPath, ($updated -replace "`r?`n", "`r`n"), [Text.UTF8Encoding]::new($false))
    Write-Step "已修复 Python 3.12 虚拟环境基路径。"
}

function Test-PythonImports([string[]]$Modules) {
    $code = "import " + ($Modules -join ", ")
    & $pxylhPython -c $code 2>$null
    return $LASTEXITCODE -eq 0
}

function Ensure-LocalDependencies {
    $required = @("fastapi", "uvicorn", "wsproto", "empyrical", "peewee")
    if (Test-PythonImports $required) {
        return
    }
    if ($CheckOnly) {
        throw "PXYLH 本地回测依赖不完整。"
    }
    Write-Step "检测到缺失依赖，正在补齐本地回测运行环境……"
    & $pxylhPython -m pip install --disable-pip-version-check "peewee>=3.17.9,<4" wsproto empyrical-reloaded
    if ($LASTEXITCODE -ne 0 -or -not (Test-PythonImports $required)) {
        throw "本地回测依赖安装失败。"
    }
}

function Get-FirstPxyDataKey {
    if (-not (Test-Path -LiteralPath $dataKeysFile -PathType Leaf)) {
        throw "PXYDATA 本地配置不存在：$dataKeysFile"
    }
    $line = Get-Content -LiteralPath $dataKeysFile |
        Where-Object { $_ -match '^\s*PXYDATA_API_KEYS\s*=' } |
        Select-Object -First 1
    if (-not $line) {
        throw "PXYDATA_API_KEYS 尚未配置。"
    }
    $value = (($line -split '=', 2)[1] -split ',')[0].Trim().Trim('"').Trim("'")
    if (-not $value) {
        throw "PXYDATA_API_KEYS 为空。"
    }
    return $value
}

function Start-WorkbenchProcess {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory
    )

    $stdout = Join-Path $logsRoot "$Name.out.log"
    $stderr = Join-Path $logsRoot "$Name.err.log"
    $process = Start-Process -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $WorkingDirectory `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
    $script:startedProcesses.Add([pscustomobject]@{
        name = $Name
        pid = $process.Id
        start_time_utc = $process.StartTime.ToUniversalTime().ToString("O")
        stdout = $stdout
        stderr = $stderr
    }) | Out-Null
    return $process
}

function Wait-Service {
    param(
        [string]$Name,
        [string]$HealthUrl,
        [int]$TimeoutSeconds,
        [System.Diagnostics.Process]$Process
    )

    for ($i = 0; $i -lt $TimeoutSeconds; $i++) {
        if (Test-Http $HealthUrl) {
            Write-Step "$Name 已就绪。"
            return
        }
        if ($Process) {
            $Process.Refresh()
            if ($Process.HasExited) {
                $record = $script:startedProcesses | Where-Object name -eq $Name | Select-Object -Last 1
                $tail = if ($record -and (Test-Path -LiteralPath $record.stderr)) {
                    (Get-Content -LiteralPath $record.stderr -Tail 12) -join "`n"
                } else { "" }
                throw "$Name 提前退出（exit=$($Process.ExitCode)）。`n$tail"
            }
        }
        Start-Sleep -Seconds 1
    }
    throw "$Name 在 $TimeoutSeconds 秒内未通过健康检查：$HealthUrl"
}

$requiredPaths = @($pxylhRoot, $pxydataRoot, $frontendRoot, $dataLauncher, $backtestLauncher, $localApiModule)
foreach ($path in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "本地回测工作台缺少路径：$path"
    }
}
if (-not (Get-Command node.exe -ErrorAction SilentlyContinue) -or -not (Get-Command npm.cmd -ErrorAction SilentlyContinue)) {
    throw "Node.js 或 npm 尚未安装。"
}

Repair-VenvHome -ConfigPath $pxylhVenvConfig -PythonPath $pxylhPython
Ensure-LocalDependencies
$pxydataApiKey = Get-FirstPxyDataKey

if ($CheckOnly) {
    Write-Step "检查通过：PXYDATA、PXYBACKTEST、仅回测 API、前端及依赖均已就绪。"
    exit 0
}

New-Item -ItemType Directory -Force -Path $runtimeBase, $logsRoot | Out-Null
if (-not (Test-Path -LiteralPath $tokenFile) -or (Get-Item -LiteralPath $tokenFile).Length -lt 32) {
    $token = ([guid]::NewGuid().ToString("N") + [guid]::NewGuid().ToString("N"))
    [IO.File]::WriteAllText($tokenFile, $token, [Text.UTF8Encoding]::new($false))
}

$env:PXYBACKTEST_SERVICE_TOKEN_FILE = $tokenFile
$env:PXYBACKTEST_BASE_URL = "http://127.0.0.1:3024"
$env:PXYBACKTEST_SOURCE_NODE = "local-workbench"
$env:PXYBACKTEST_PYTHON = $pxylhPython
$env:PXYBACKTEST_RUNTIME_ROOT = Split-Path -Parent $runtimeBase
$env:PXYBACKTEST_PXYLH_ROOT = $pxylhRoot
$env:PXYBACKTEST_PXYDATA_BASE_URL = "http://127.0.0.1:3020"
$env:PXYBACKTEST_PXYDATA_DATA_ROOT = Join-Path $pxydataRoot "data"
$env:PXYBACKTEST_PXYDATA_API_KEY = $pxydataApiKey
$env:JWT_SECRET = "local-backtest-workbench-only-2026-change"
$env:VITE_API_PROXY_TARGET = "http://127.0.0.1:8000"
$env:VITE_BACKTEST_LOCAL_AUTO_LOGIN = "true"
$script:startedProcesses = [Collections.Generic.List[object]]::new()

try {
    Write-Step "启动顺序：PXYDATA → PXYBACKTEST → 本地回测 API → 前端。"

    $dataProcess = $null
    if (-not (Test-Http "http://127.0.0.1:3020/health")) {
        $dataProcess = Start-WorkbenchProcess -Name "pxydata" -FilePath "powershell.exe" `
            -ArgumentList @("-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $dataLauncher) -WorkingDirectory $pxydataRoot
    }
    Wait-Service -Name "PXYDATA" -HealthUrl "http://127.0.0.1:3020/health" -TimeoutSeconds 180 -Process $dataProcess

    $backtestProcess = $null
    if (-not (Test-Http "http://127.0.0.1:3024/health")) {
        $backtestProcess = Start-WorkbenchProcess -Name "pxybacktest" -FilePath "powershell.exe" `
            -ArgumentList @("-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $backtestLauncher) -WorkingDirectory $backtestRoot
    }
    Wait-Service -Name "PXYBACKTEST" -HealthUrl "http://127.0.0.1:3024/health" -TimeoutSeconds 120 -Process $backtestProcess

    $platformProcess = $null
    if (-not (Test-Http "http://127.0.0.1:8000/api/health")) {
        $platformProcess = Start-WorkbenchProcess -Name "backtest-local-api" -FilePath $pxylhPython `
            -ArgumentList @("-X", "utf8", "-m", "uvicorn", "backtest_local_app:app", "--app-dir", $pxylhBackend, "--host", "127.0.0.1", "--port", "8000") -WorkingDirectory $pxylhBackend
    }
    Wait-Service -Name "本地回测 API" -HealthUrl "http://127.0.0.1:8000/api/health" -TimeoutSeconds 120 -Process $platformProcess

    if (-not (Test-Path -LiteralPath (Join-Path $frontendRoot "node_modules"))) {
        Write-Step "首次运行，安装前端依赖……"
        Push-Location $frontendRoot
        try {
            & npm.cmd ci
            if ($LASTEXITCODE -ne 0) { throw "npm ci 执行失败。" }
        } finally {
            Pop-Location
        }
    }

    $frontendProcess = $null
    if (-not (Test-Http "http://127.0.0.1:3000/")) {
        $frontendProcess = Start-WorkbenchProcess -Name "frontend" -FilePath $env:ComSpec `
            -ArgumentList @("/d", "/c", "npm.cmd run dev -- --host 127.0.0.1") -WorkingDirectory $frontendRoot
    }
    Wait-Service -Name "前端" -HealthUrl "http://127.0.0.1:3000/" -TimeoutSeconds 120 -Process $frontendProcess

    $priorProcesses = @()
    if (Test-Path -LiteralPath $stateFile) {
        try { $priorProcesses = @((Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json).processes) } catch { $priorProcesses = @() }
    }
    $allProcesses = @($priorProcesses) + @($script:startedProcesses)
    $state = [ordered]@{
        contract = "pxybacktest.local-workbench-state.v1"
        started_at = (Get-Date).ToUniversalTime().ToString("O")
        url = $frontendUrl
        agent_user_id = "local-backtest-user"
        token_file = $tokenFile
        processes = @($allProcesses | Group-Object pid | ForEach-Object { $_.Group[-1] })
    }
    $state | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $stateFile -Encoding UTF8

    Write-Host ""
    Write-Host "[完成] 本地回测工作台已启动：$frontendUrl" -ForegroundColor Green
    Write-Host "[日志] $logsRoot"
    Write-Host "[Agent] user_id=local-backtest-user，可通过 PXYBACKTEST CLI/API 提交并由同一前端观察。"
    if (-not $NoBrowser) { Start-Process $frontendUrl }
} catch {
    Write-Host "[启动失败] $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "[日志] $logsRoot"
    foreach ($record in @($script:startedProcesses) | Sort-Object pid -Descending) {
        & taskkill.exe /PID $record.pid /T /F 2>$null | Out-Null
    }
    exit 1
}

