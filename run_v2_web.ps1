param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8080,
    [switch]$NoBrowser,
    [switch]$Background
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Prevent .pyc / __pycache__ generation
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONUTF8 = "1"

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $ProjectDir

$LogDir = Join-Path $ProjectDir "logs"
if (-not (Test-Path -LiteralPath $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

$BrowserHost = if ($BindHost -eq "0.0.0.0") { "127.0.0.1" } else { $BindHost }
$WebLockFile = Join-Path $ProjectDir "v2_web.lock"
if (Test-Path -LiteralPath $WebLockFile) {
    $ExistingPidText = ""
    try {
        $ExistingPidText = (Get-Content -LiteralPath $WebLockFile -ErrorAction Stop | Select-Object -First 1).Trim()
    } catch {
        $ExistingPidText = ""
    }
    $ExistingPid = 0
    if ([int]::TryParse($ExistingPidText, [ref]$ExistingPid) -and $ExistingPid -gt 0 -and $ExistingPid -ne $PID) {
        $RunningProc = Get-Process -Id $ExistingPid -ErrorAction SilentlyContinue
        if ($null -ne $RunningProc -and $RunningProc.ProcessName -match "^(powershell|pwsh|python)") {
            $Url = "http://" + $BrowserHost + ":" + $Port
            Write-Host ("[OK] GMO AI Trader v2 Web Dashboard is already running (PID=" + $ExistingPid + ") at " + $Url)
            if (-not $NoBrowser) {
                Start-Process $Url | Out-Null
            }
            exit 0
        }
    }
    Remove-Item -LiteralPath $WebLockFile -Force -ErrorAction SilentlyContinue
}

# Resolve Python executable
$PythonCandidates = @(
    (Join-Path $ProjectDir ".venv\Scripts\python.exe"),
    "python.exe"
)
$PythonExe = $null
foreach ($cand in $PythonCandidates) {
    if ($cand -eq "python.exe") {
        $cmd = Get-Command "python.exe" -ErrorAction SilentlyContinue
        if ($null -ne $cmd) {
            $PythonExe = $cmd.Source
            break
        }
    } elseif (Test-Path -LiteralPath $cand) {
        $PythonExe = $cand
        break
    }
}

if ($null -eq $PythonExe) {
    throw "Python executable not found."
}

$ArgsList = @("-B", "web_dashboard.py", "--host", $BindHost, "--port", $Port.ToString())
if (-not $NoBrowser -and -not $Background) {
    $ArgsList += "--open-browser"
}

if ($Background) {
    $TaskName = "GMO-AI-Trader-v2-Web"
    $SelfScript = Join-Path $ProjectDir "run_v2_web.ps1"
    $HiddenVbs = Join-Path $ProjectDir "launch_hidden.vbs"
    $TaskAction = New-ScheduledTaskAction `
        -Execute "wscript.exe" `
        -Argument ("//B //NoLogo `"" + $HiddenVbs + "`" `"" + $SelfScript + "`" -BindHost " + $BindHost + " -Port " + $Port.ToString() + " -NoBrowser") `
        -WorkingDirectory $ProjectDir
    $TaskTriggerLogon = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
    $TaskSettings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit (New-TimeSpan -Days 365)
    try {
        Register-ScheduledTask -TaskName $TaskName -Action $TaskAction -Trigger @($TaskTriggerLogon) -Settings $TaskSettings -Description "GMO AI Trader v2 Local Web Dashboard (http://127.0.0.1:8080)" -Force | Out-Null
    } catch {
        Register-ScheduledTask -TaskName $TaskName -Action $TaskAction -Settings $TaskSettings -Description "GMO AI Trader v2 Local Web Dashboard (http://127.0.0.1:8080)" -Force | Out-Null
    }
    Start-ScheduledTask -TaskName $TaskName
    Start-Sleep -Seconds 2
    $Url = "http://" + $BrowserHost + ":" + $Port
    Write-Host ("[OK] Started GMO AI Trader v2 Web Dashboard task (" + $TaskName + ") at " + $Url)
    if (-not $NoBrowser) {
        Start-Process $Url | Out-Null
    }
    exit 0
}

$LockAcquired = $false
try {
    [System.IO.File]::WriteAllText($WebLockFile, $PID.ToString(), [System.Text.Encoding]::ASCII)
    $LockAcquired = $true
    & $PythonExe @ArgsList
    $ExitCode = $LASTEXITCODE
    if ($ExitCode -ne 0) {
        throw ("web_dashboard.py exited with code " + $ExitCode)
    }
} finally {
    if ($LockAcquired -and (Test-Path -LiteralPath $WebLockFile)) {
        Remove-Item -LiteralPath $WebLockFile -Force -ErrorAction SilentlyContinue
    }
}
