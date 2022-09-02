# Building Jumbo

## Prerequisites

Jumbo is designed to run on Windows and Linux using Microsoft .Net Core. The
following are the minimum requirements:

- Microsoft .Net 6.0
- Microsoft PowerShell Core 7.2.2

Jumbo was tested using these versions; newer versions are expected to work but
have not been tested.

This is a port of Jumbo to .Net Core; Jumbo was originally written to use both
Microsoft .Net and Mono. Most of the scale and performance testing was done on
Linux using Mono; Windows support was used mainly for debugging. No large-scale
performance testing using .Net Core has been done.

Jumbo includes two administration websites (DfsWeb and JetWeb). These will run
using the Kestrel web server included with .Net Core.

The provided scripts used to start Jumbo on a cluster use [PowerShell remoting over SSH](https://docs.microsoft.com/en-us/powershell/scripting/learn/remoting/ssh-remoting-in-powershell-core?view=powershell-6),
which must be correctly configured on all nodes of the cluster. If you are
running in a single-node environment, no configuration is necessary as the
scripts will directly invoke commands on the local host. On Windows, you can
also configure Jumbo to use WinRM remoting by modifying Get-JumboConfig.ps1.

You can also start Jumbo manually, in which case PowerShell is not required,
but this is not recommended.

## Creating a Jumbo distribution

Jumbo can be built on Linux and Windows by simply running `dotnet build` from
the solution’s root directory. You can run the unit tests by running `dotnet test`
(note: this will take several minutes).

Jumbo can't really be used directly from the build output. It depends on all
of the binaries being available together along with scripts and configuration
files. To get Jumbo ready to run (locally or on a cluster), you must create a
distribution.

You can create a distribution of Jumbo, with all the required binaries and
scripts to run it, by running `./Publish-Release.ps1 <path>`, e.g.
`./Publish-Release.ps1 /jumbodist`.

After doing that, you're ready to [configure Jumbo](Configuration.md).
