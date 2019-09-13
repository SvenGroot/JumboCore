param(
    [Parameter(Mandatory=$true, Position=0)][ValidateSet("Start", "Stop")][string]$Action,
    [Parameter(Mandatory=$true, Position=1)][string]$MasterServer,
    [Parameter(Mandatory=$true, Position=2)][string]$MasterHostName,
    [Parameter(Mandatory=$true, Position=3)][string]$SlaveServer,
    [Parameter(Position=4)][string]$WebAdmin = $null
)

. (Join-Path $PSScriptRoot "Get-JumboConfig.ps1")

function Invoke-Server([string]$HostName, [string]$Server)
{
    $runScript = Join-Path $PSScriptRoot "$Action-Server.ps1"
    if (($HostName -eq "localhost") -and (-not $JUMBO_REMOTE_LOCALHOST)) {
        &$runScript $Server | ForEach-Object { "${HostName}: $_" }
        
    } else {
        if ($JUMBO_REMOTE_SSH) {
            Write-Verbose "Using SSH to invoke command on $HostName."
            $job = Invoke-Command -HostName $HostName -AsJob -ScriptBlock {
                &$Using:runScript $Using:Server
            }

        } else {
            Write-Verbose "Using WinRM to invoke command on $HostName."
            $job = Invoke-Command -ComputerName $HostName -AsJob -ScriptBlock {
                &$Using:runScript $Using:Server
            }
        }

        $job | Wait-Job | Receive-Job | ForEach-Object { "$($_.PSComputerName): $_" }
    }
}

if ($Action -eq "Start")
{
    Invoke-Server $MasterHostName $MasterServer
    if ($WebAdmin) {
        Invoke-Server $MasterHostName $WebAdmin
    }
}

$hosts = Get-Content (Join-Path $PSScriptRoot "hosts")
foreach ($hostName in $hosts) {
    Invoke-Server $hostName $SlaveServer
}

if ($Action -eq "Stop")
{
    Invoke-Server $MasterHostName $MasterServer
    if ($WebAdmin) {
        Invoke-Server $MasterHostName $WebAdmin
    }
}
