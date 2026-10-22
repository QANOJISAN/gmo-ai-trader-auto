param(
    [string]$TaskName = "GMO-AI-Trader-v2-Auto",
    [switch]$Unregister
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONDONTWRITEBYTECODE = "1"

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RunnerScript = Join-Path $ProjectDir "run_v2_daemon.ps1"
$HiddenVbs = Join-Path $ProjectDir "launch_hidden.vbs"

if ($Unregister) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host ("[OK] Unregistered scheduled task: " + $TaskName)
    exit 0
}

if (-not (Test-Path -LiteralPath $RunnerScript)) {
    throw ("Runner script not found: " + $RunnerScript)
}
if (-not (Test-Path -LiteralPath $HiddenVbs)) {
    throw ("Hidden launcher VBS not found: " + $HiddenVbs)
}

# Register v2 daemon using wscript.exe + launch_hidden.vbs (SW_HIDE=0) so Windows Terminal never opens a visible window
$Action = New-ScheduledTaskAction `
    -Execute "wscript.exe" `
    -Argument ("//B //NoLogo `"" + $HiddenVbs + "`" `"" + $RunnerScript + "`"") `
    -WorkingDirectory $ProjectDir

$TriggerLogon = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$TriggerRepeat = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddMinutes(5) `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Days 365)

try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $Action `
        -Trigger @($TriggerLogon, $TriggerRepeat) `
        -Settings $Settings `
        -Description "GMO Coin Spot 24/7 Full-Automated AI Trading Daemon (v2)" `
        -Force | Out-Null
} catch {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $Action `
        -Trigger @($TriggerRepeat) `
        -Settings $Settings `
        -Description "GMO Coin Spot 24/7 Full-Automated AI Trading Daemon (v2)" `
        -Force | Out-Null
}

Write-Host ("[OK] Registered scheduled task: " + $TaskName)
Write-Host ("Note: Before switching v2 to LIVE mode, disable v1 task (GMO-AI-Trader-Daily) and run migrate_from_v1.py --execute.")
