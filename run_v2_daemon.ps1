param(
    [switch]$Once,
    [switch]$ForceAi,
    [switch]$DryRunNotify,
    [switch]$ProbeLive,
    [switch]$Backtest,
    [string]$RemoteCmd = ""
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

# Use v2_launcher.lock for the PowerShell wrapper so it does not collide with Python's SingleInstanceLock(v2_daemon.lock)
$LockFile = Join-Path $ProjectDir "v2_launcher.lock"
if (Test-Path -LiteralPath $LockFile) {
    $ExistingPidText = ""
    try {
        $ExistingPidText = (Get-Content -LiteralPath $LockFile -ErrorAction Stop | Select-Object -First 1).Trim()
    } catch {
        $ExistingPidText = ""
    }
    $ExistingPid = 0
    if ([int]::TryParse($ExistingPidText, [ref]$ExistingPid) -and $ExistingPid -gt 0 -and $ExistingPid -ne $PID) {
        $RunningProc = Get-Process -Id $ExistingPid -ErrorAction SilentlyContinue
        if ($null -ne $RunningProc) {
            Write-Host ("[SKIP] GMO AI Trader v2 daemon is already running (PID=" + $ExistingPid + "). Exiting.")
            exit 0
        }
    }
    Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
}

# Resolve Python executable (prefer local .venv, then v1 .venv as read-only runtime, then system python)
$PythonCandidates = @(
    (Join-Path $ProjectDir ".venv\Scripts\python.exe"),
    "e:\gmo-ai-trader\.venv\Scripts\python.exe",
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

try {
    [System.IO.File]::WriteAllText($LockFile, $PID.ToString(), [System.Text.Encoding]::ASCII)

    $ArgsList = @("-B", "auto_trader.py")
    if ($Once) {
        $ArgsList += "--once"
    }
    if ($ForceAi) {
        $ArgsList += "--force-ai"
    }
    if ($DryRunNotify) {
        $ArgsList += "--dry-run-notify"
    }
    if ($ProbeLive) {
        $ArgsList += "--probe-live"
    }
    if ($Backtest) {
        $ArgsList += "--backtest"
    }
    if (-not [string]::IsNullOrWhiteSpace($RemoteCmd)) {
        $ArgsList += "--remote-cmd"
        $ArgsList += $RemoteCmd
    }

    & $PythonExe @ArgsList
    $ExitCode = $LASTEXITCODE
    if ($ExitCode -ne 0) {
        throw ("auto_trader.py exited with code " + $ExitCode)
    }
} finally {
    if (Test-Path -LiteralPath $LockFile) {
        Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
    }
}
