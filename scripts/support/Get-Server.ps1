param(
    [parameter(Mandatory=$true, Position=0)][string]$Server
)

. (Join-Path $PSScriptRoot "Get-JumboConfig.ps1")

$hostname = [System.Net.Dns]::GetHostName()
$pidFile = Join-Path $JUMBO_PID "jumbo-$Server-$hostname.pid"
Write-Verbose "Pid file is $pidFile"
if (Test-Path $pidFile) {
    $jumboPid = Get-Content $pidFile
    Write-Verbose "Looking for process with id $jumboPid"
    return Get-Process -Id $jumboPid -ErrorAction SilentlyContinue
}

Write-Verbose "$Server is not running."
return $null