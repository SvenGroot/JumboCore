param(
    [parameter(Mandatory=$true, Position=0)][string]$Server
)

. (Join-Path $PSScriptRoot "Get-JumboConfig.ps1")

$existingServer = &(Join-Path $PSScriptRoot "Get-Server.ps1") $Server
if (-not $existingServer) {
    Write-Warning "$Server is not running."
    exit
}

"Stopping $Server"
Stop-Process $existingServer
$hostname = [System.Net.Dns]::GetHostName()
$pidFile = Join-Path $JUMBO_PID "jumbo-$Server-$hostname.pid"
Remove-Item $pidFile