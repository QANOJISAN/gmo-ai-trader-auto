Option Explicit
' ============================================================================
' GMO AI Trader v2 - Windowless Stealth Launcher (launch_hidden.vbs)
' Runs PowerShell daemon/web scripts with SW_HIDE (0) via WScript.Shell
' so that neither conhost.exe nor Windows Terminal (wt.exe) ever pops up
' a visible black console window on the desktop or taskbar.
' ============================================================================

Dim shell, fso, scriptDir, targetPs1, extraArgs, i, argVal, cmd, exitCode
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
shell.CurrentDirectory = scriptDir

If WScript.Arguments.Count > 0 Then
    targetPs1 = WScript.Arguments(0)
Else
    targetPs1 = fso.BuildPath(scriptDir, "run_v2_daemon.ps1")
End If

extraArgs = ""
If WScript.Arguments.Count > 1 Then
    For i = 1 To WScript.Arguments.Count - 1
        argVal = WScript.Arguments(i)
        If InStr(argVal, " ") > 0 Then
            extraArgs = extraArgs & " """ & argVal & """"
        Else
            extraArgs = extraArgs & " " & argVal
        End If
    Next
End If

cmd = "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & targetPs1 & """" & extraArgs

' WindowStyle = 0 (SW_HIDE: completely invisible), WaitOnReturn = True
exitCode = shell.Run(cmd, 0, True)
WScript.Quit exitCode
