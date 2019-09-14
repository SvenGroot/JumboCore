# It's not possible to use Start-Job, since those jobs are tied to the pwsh instance that started
# them.
param(
    [parameter(Mandatory=$true, Position=0)][string]$Server
)

. (Join-Path $PSScriptRoot "Get-JumboConfig.ps1")

$existingServer = &(Join-Path $PSScriptRoot "Get-Server.ps1") $Server
if ($existingServer) {
    Write-Warning "$Server is already running as pid $($existingServer.Id)."
    exit
}

$hostname = [System.Net.Dns]::GetHostName()
$binary = Join-Path $JUMBO_BIN_HOME "$Server.dll"
$arguments = @($binary)
$outLog = Join-Path $JUMBO_LOG "out-$Server-$hostname.log"
$errLog = Join-Path $JUMBO_LOG "err-$Server-$hostname.log"

if ($Server -eq "DfsWeb") {
    $arguments += "--urls=http://*:$JUMBO_DFSWEB_PORT"
}

"Starting $Server"
if ($IsWindows) {
    $process = Start-Process `
        -FilePath $DOTNET_COMMAND `
        -ArgumentList $arguments `
        -WindowStyle "Hidden" `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -WorkingDirectory $JUMBO_BIN_HOME `
        -PassThru

} else {
    $process = Start-Process `
        -FilePath $DOTNET_COMMAND `
        -ArgumentList $arguments `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -WorkingDirectory $JUMBO_BIN_HOME `
        -PassThru
}

$pidFile = Join-Path $JUMBO_PID "jumbo-$Server-$hostname.pid"
Write-Verbose "Started $Server with pid $($process.Id), saving to $pidFile"
Set-Content $pidFile $process.Id
