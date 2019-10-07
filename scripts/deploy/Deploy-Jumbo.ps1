[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

function Get-GroupConfig([string]$configName, [string]$group)
{
    $configFile = "$PSScriptRoot/$configName.$group.config"
    if (Test-Path $configFile) {
        Write-Verbose "${group}: using group config file $configName.$group.config"
        return $configFile

    } else {
        Write-Verbose "${group}: no custom $configName.config for this group"
        return $null
    }
}

$parentDir = Split-Path $PSScriptRoot
. (Join-Path $parentDir "support" "Get-JumboConfig.ps1")

$deployed = @()

Get-Content (Join-Path $PSScriptRoot "groups") | ForEach-Object {
    $group = $_
    "Deploying group '$group'"
    $groupCommonConfig = Get-GroupConfig "common" $group
    $groupDfsConfig = Get-GroupConfig "dfs" $group
    $groupJetConfig = Get-GroupConfig "jet" $group

    $jobs = @()
    Get-Content (Join-Path $PSScriptRoot $group) | ForEach-Object {
        $node = $_
        if ($deployed -contains $node) {
            "Already deployed to node $node; skipping (it may be in more than one group)"
        } else {
            $deployed += $node
            "$group/${node}: deploying all"
            $jobs += Start-Job {
                function Deploy-Config([string]$configFile, [string]$configBaseName, [string]$jumboHome, $session)
                {
                    if ($configFile) {
                        Copy-Item $configFile (Join-Path $jumboHome "bin" "$configBasename.config") -ToSession $session
                    }
                }

                if ($using:JUMBO_REMOTE_SSH) {
                    $session = New-PSSession -HostName $using:node

                } else {
                    $session = New-PSSession -ComputerName $using:node
                }

                Copy-Item (Join-Path $using:parentDir *) $using:JUMBO_HOME -Recurse -Force -ToSession $session
                Deploy-Config $using:groupCommonConfig $using:JUMBO_HOME "common" $session
                Deploy-Config $using:groupDfsConfig $using:JUMBO_HOME "dfs" $session
                Deploy-Config $using:groupJetConfig $using:JUMBO_HOME "jet" $session
                Remove-PSSession $session
                "done"
            }
        }
    }

    $jobs | Wait-Job | Receive-Job | ForEach-Object { "$($_.PSComputerName): $_" }
}