$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $RepoRoot

$RuntimeDir = Join-Path $RepoRoot ".runtime"

function Get-ManagedProcess {
    param([string]$Pattern)

    Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -match '^python(\.exe)?$' -and
            $_.CommandLine -and
            $_.CommandLine -like "*$Pattern*"
        }
}

function Stop-ManagedProcess {
    param(
        [string]$Name,
        [string]$Pattern
    )

    $matches = @(Get-ManagedProcess -Pattern $Pattern)
    if (-not $matches.Count) {
        Write-Host "$Name is not running" -ForegroundColor Yellow
        Remove-Item -LiteralPath (Join-Path $RuntimeDir "$Name.pid") -ErrorAction SilentlyContinue
        return
    }

    foreach ($proc in $matches) {
        try {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
            Write-Host "$Name stopped (PID $($proc.ProcessId))" -ForegroundColor Green
        } catch {
            Write-Host "$Name stop failed for PID $($proc.ProcessId)): $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    Remove-Item -LiteralPath (Join-Path $RuntimeDir "$Name.pid") -ErrorAction SilentlyContinue
}

Stop-ManagedProcess -Name "wired" -Pattern "wired_ai_system\\app.py"
Stop-ManagedProcess -Name "retrain" -Pattern "retrain_scheduler.py"
Stop-ManagedProcess -Name "telegram" -Pattern "telegram_alerts.py"
Stop-ManagedProcess -Name "forexsmartbot" -Pattern "ForexSmartBot\app.py --web"
Stop-ManagedProcess -Name "bridge" -Pattern "mq5_bridge_server.py"
