# This file provides configuration for the various PowerShell scripts used to interact with Jumbo.
# For Jumbo's main configuration, see bin/common.config, bin/dfs.config, and bin/jet.config

# Change the command to use to invoke .Net Core, if it's not in the path, and set $env:DOTNET_HOME
# if not already set.
# N.B. Changing this may be necessary if the path and DOTNET_HOME variable aren't set when using
#      remoting.
$DOTNET_COMMAND = "dotnet"
# $env:DOTNET_HOME = "/dotnet"

# Set JUMBO_HOME to the location of Jumbo's main directory (the directory containing the scripts,
# not the bin directory)
$JUMBO_HOME = Split-Path $PSScriptRoot -Parent
$JUMBO_BIN_HOME = Join-Path $JUMBO_HOME "bin"

# Directory where the process IDs of the various Jumbo processes are stored for use by the run-dfs.ps1 and run-jet.ps1 scripts
$JUMBO_PID=[System.IO.Path]::GetTempPath()

# This controls the location of the "out" and "err" log files. In order for DfsWeb/JetWeb to be able
# to retrieve these files correctly, this value should be set to the same value as the log directory
# in bin/common.config
$JUMBO_LOG = Join-Path $JUMBO_BIN_HOME "log"

# Ports for the admin web sites
$JUMBO_DFSWEB_PORT=35000
$JUMBO_JETWEB_PORT=36000

# Master node names for the run-dfs.ps1 and run-jet.ps1 scripts. Leaving this at localhost is okay
# as long as you only ever invoke those scripts from the respective servers.
$JUMBO_NAMESERVER="localhost"
$JUMBO_JOBSERVER="localhost"

# Set this to $true if you wish to run servers on the local host using Invoke-Command.
$JUMBO_REMOTE_LOCALHOST=$false

# Set this to $false to use WinRM remoting instead of SSH.
# See https://docs.microsoft.com/en-us/powershell/scripting/learn/remoting/ssh-remoting-in-powershell-core?view=powershell-6
$JUMBO_REMOTE_SSH=$true