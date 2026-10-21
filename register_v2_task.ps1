param(
    [string]$TaskName = "GMO-AI-Trader-v2-Auto",
    [switch]$Unregister
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONDONTWRITEBYTECODE = "1"

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunnerScript = Join-Path $ProjectDir "run_v2_daemon.ps1"

if ($Unregister) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host ("[OK] Unregistered scheduled task: " + $TaskName)
    exit 0
}

if (-not (Test-Path -LiteralPath $RunnerScript)) {
    throw ("Runner script not found: " + $RunnerScript)
}

# Register v2 daemon to start at user logon and check every 15 minutes (lockfile prevents duplicate instances)
$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument ("-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File `"" + $RunnerScript + "`"") `
    -WorkingDirectory $ProjectDir

$TriggerLogon = New-ScheduledTaskTrigger -AtLogOn
$TriggerRepeat = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddMinutes(5) `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Days 365)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger @($TriggerLogon, $TriggerRepeat) `
    -Settings $Settings `
    -Description "GMO Coin Spot 24/7 Full-Automated AI Trading Daemon (v2)" `
    -Force | Out-Null

Write-Host ("[OK] Registered scheduled task: " + $TaskName)
Write-Host ("Note: Before switching v2 to LIVE mode, disable v1 task (GMO-AI-Trader-Daily) and run migrate_from_v1.py --execute.")
