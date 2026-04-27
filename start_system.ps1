$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $RepoRoot

$RuntimeDir = Join-Path $RepoRoot ".runtime"
$LogDir = Join-Path $RuntimeDir "logs"
New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$PythonExe = if (Test-Path (Join-Path $RepoRoot ".venv\Scripts\python.exe")) {
    Join-Path $RepoRoot ".venv\Scripts\python.exe"
} else {
    "python"
}
$SystemPythonExe = (Get-Command python).Source
$WiredRoot = "C:\Users\Admin\OneDrive\Documents\wired_ai_system"
$WiredAppPath = Join-Path $WiredRoot "app.py"

function Get-ManagedProcess {
    param([string]$Pattern)

    Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -match '^python(\.exe)?$' -and
            $_.CommandLine -and
            $_.CommandLine -like "*$Pattern*"
        }
}

function Stop-ManagedMatches {
    param([string]$Pattern)

    $matches = @(Get-ManagedProcess -Pattern $Pattern)
    foreach ($proc in $matches) {
        try {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
        } catch {
        }
    }
}

function Start-ManagedProcess {
    param(
        [string]$Name,
        [string]$Pattern,
        [string]$ArgumentLine,
        [string]$LogFileName,
        [string]$WorkingDirectory = $RepoRoot,
        [string]$Executable = $PythonExe
    )

    $existing = @(Get-ManagedProcess -Pattern $Pattern)
    if ($existing.Count) {
        Write-Host "Cleaning stale $Name process(es): $($existing.ProcessId -join ', ')" -ForegroundColor Yellow
        Stop-ManagedMatches -Pattern $Pattern
        Start-Sleep -Milliseconds 500
    }

    $stdoutLog = Join-Path $LogDir "$LogFileName.out.log"
    $stderrLog = Join-Path $LogDir "$LogFileName.err.log"

    $proc = Start-Process -FilePath $Executable `
        -ArgumentList $ArgumentLine `
        -WorkingDirectory $WorkingDirectory `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutLog `
        -RedirectStandardError $stderrLog `
        -PassThru

    Set-Content -Path (Join-Path $RuntimeDir "$Name.pid") -Value $proc.Id
    Write-Host "$Name started (PID $($proc.Id))" -ForegroundColor Green
}

Start-ManagedProcess `
    -Name "wired" `
    -Pattern "wired_ai_system\\app.py" `
    -ArgumentLine $WiredAppPath `
    -LogFileName "wired" `
    -WorkingDirectory $WiredRoot `
    -Executable $SystemPythonExe

Start-ManagedProcess `
    -Name "bridge" `
    -Pattern "mq5_bridge_server.py" `
    -ArgumentLine "mq5_bridge_server.py" `
    -LogFileName "bridge"

Start-ManagedProcess `
    -Name "retrain" `
    -Pattern "retrain_scheduler.py" `
    -ArgumentLine "retrain_scheduler.py" `
    -LogFileName "retrain"

Start-ManagedProcess `
    -Name "telegram" `
    -Pattern "telegram_alerts.py" `
    -ArgumentLine "telegram_alerts.py" `
    -LogFileName "telegram"

Start-ManagedProcess `
    -Name "forexsmartbot" `
    -Pattern "ForexSmartBot\app.py --web" `
    -ArgumentLine "ForexSmartBot\app.py --web --host 127.0.0.1 --port 8080 --bridge http://127.0.0.1:5050" `
    -LogFileName "forexsmartbot"

Write-Host ""
Write-Host "Services are launching." -ForegroundColor Cyan
Write-Host "Wired System:  http://127.0.0.1:8000" -ForegroundColor Cyan
Write-Host "Bridge:        http://127.0.0.1:5050" -ForegroundColor Cyan
Write-Host "ForexSmartBot: http://127.0.0.1:8080/forexsmartbot_dashboard.html?bridge=http://127.0.0.1:5050" -ForegroundColor Cyan
Write-Host "Logs:          $LogDir" -ForegroundColor DarkCyan
