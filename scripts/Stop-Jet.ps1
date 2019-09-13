. (Join-Path $PSScriptRoot "support" "Get-JumboConfig.ps1")

&(Join-Path $PSScriptRoot "support" "Invoke-Cluster.ps1") "Stop" "JobServer" $JUMBO_NAMESERVER "TaskServer"