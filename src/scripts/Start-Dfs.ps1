. (Join-Path $PSScriptRoot "support" "Get-JumboConfig.ps1")

&(Join-Path $PSScriptRoot "support" "Invoke-Cluster.ps1") "Start" "NameServer" $JUMBO_NAMESERVER "DataServer" "DfsWeb"