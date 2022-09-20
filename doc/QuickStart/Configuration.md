# Configuration

After you have [created a distribution of Jumbo](Building.md) or [downloaded one](https://github.com/SvenGroot/JumboCore/releases),
you must configure it before you can use it.

Jumbo uses several files for configuration. Some configuration needed to start Jumbo is set in
PowerShell scripts, but most of the configuration is done using XML files (Jumbo predates the
proliferation of JSON for this purpose, unfortunately).

The most important configuration files in your distribution's directory are `support/Get-JumboConfig.ps1`,
`bin/common.config`, `bin/dfs.config`, and `bin/jet.config`. These files are pre-created with some
defaults, but you must always edit at least some of them before you can run Jumbo.

All of Jumbo's servers need a local directory on the node they are running on to store some of their
data. These directories must meet the following criteria:

- They must not be the directory of your Jumbo distribution.
- All servers must use a different directory; they cannot share the same one.
- The directories should be empty before you first run Jumbo. If they don't exist, Jumbo will create
  them.
- The directory paths must be the same for every node sharing a particular configuration file (see
  deployment [below](#basic-configuration-and-deployment)).
- For the data server block storage directory, this is where the file data for files on the DFS is
  stored, so these should preferably be on volumes with a lot of space.

## Quick configuration for one node

Want to simply try Jumbo on one node? Here’s what you do:

1. In `bin/dfs.config`, set the NameServer image directory and the DataServer block directory to
   local directories.
2. In `bin/jet.config`, set the JobServer archive directory and TaskServer task directory.

Here's an example of what these files would look like for local running.

`bin/dfs.config`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<ookii.jumbo.dfs>
  <fileSystem url="jdfs://localhost:9000" />
  <nameServer blockSize="64MB"
              replicationFactor="1"
              imageDirectory="/jumbo/nameserver" />
  <dataServer port="9001"
              blockStorageDirectory="/jumbo/dataserver" />
</ookii.jumbo.dfs>
```

`bin/jet.config`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<ookii.jumbo.jet>
  <jobServer hostName="localhost"
             port="9500"
             archiveDirectory="/jumbo/jobarchive" />
  <taskServer taskDirectory="/jumbo/taskserver"
              port="9501"
              taskSlots="2"
              fileServerPort="9502" />
</ookii.jumbo.jet>
```

In this example, `/jumbo` is used as the base directory for all of Jumbo's paths. Replace them with
your own directories as desired. These should all be absolute paths, and on Windows should include
a drive letter (e.g. `D:\jumbo\nameserver`).

That’s it, you can now run Jumbo on one node, so you can skip ahead to [running Jumbo](Running.md).

## Basic configuration and deployment

If you want to run Jumbo on a cluster, more configuration is needed.

First, you must configure some values in [`support/Get-JumboConfig.ps1`](../../src/scripts/support/Get-JumboConfig.ps1).
You must set the `$JUMBO_HOME` variable to the directory where Jumbo's binaries are to be deployed
(this path must be the same on all nodes). If you change the `$JUMBO_LOG` directory, also make the
corresponding modifications in `bin/common.config`. Check the comments in the file for further available configuration options.

Using [`bin/common.config`](../../src/common.config), you can modify the log directory and configure
rack-awareness (used by the task scheduler to get data locality). If you don't need either of these
things, you don't need to touch this file. See the [common.config XML documentation](https://www.ookii.org/Link/JumboDocCommonConfig)
for all the options.

In order to start Jumbo using the provided scripts, you must also specify which nodes it will run
on. Jumbo uses a two-level configuration system for this: the `deploy/groups` file specifies groups
of nodes, and files matching the group names contain the actual node names. By default, there are
two groups defined: `deploy/masters`, and `deploy/nodes`.

Groups are used only when Jumbo is deployed to your cluster using the provided `deploy/Deploy-Jumbo.ps1`
script. Dividing nodes into multiple groups allows you specify different configuration settings for
each group. When running Jumbo, it simply starts the servers on all nodes in all groups, without
distinguishing the groups.

The `masters` group is special and is only used during deployment. You only need to modify the
`masters` file if you intend to use the deployment script (see below). This group should contain
the node(s) that run the NameServer and JobServer. The contents of the `masters` file are skipped
when running Jumbo; the `Get-JumboConfig.ps1` file specifies which node should run the NameServer,
and which node the JobServer, and that is what's used when starting Jumbo.

Specify all nodes that should run DataServers and TaskServers in the `nodes` file. Alternatively,
you can define your own groups. If you only wish to evaluate Jumbo on a single node, you can leave
the content of these files at “localhost”.

You can deploy Jumbo to multiple nodes using the `Deploy-Jumbo.ps1` script, which will copy all
Jumbo files including the configuration to `$JUMBO_HOME` on all the nodes. This way, `$JUMBO_HOME`
does not need to be a network path available on all the nodes.

When deploying, you can use different configuration files for each group. To do this, create files
named `common.<groupname>.config`, `dfs.<groupname>.config` and `jet.<groupname>.config` (replace
`<groupname>` with the name of the group) in the same directory as `Deploy-Jumbo.ps1`. If any of
those files does not exist for a group, that group uses the default configuration file.

## Configuring the Jumbo file system

Jumbo can use the provided DFS (recommended), or can be configured to use the local file system
for storage (for testing purposes).

### Configuring the DFS

There are four values that you must specify in `bin/dfs.config` to use the DFS:

1. Set the file system URL to the host name and port of the NameServer.
2. Set the NameServer image directory to a local directory where the NameServer’s metadata will be
   stored.
3. Set the replicationFactor to an appropriate value. It’s recommended to use 3 replicas unless you
   have fewer than 3 data servers (in which case set it to the number of data servers).
4. Set the DataServer block directory to a local directory where the file data for each node will be
   stored.

See the [dfs.config XML documentation](https://www.ookii.org/Link/JumboDocDfsConfig) for information
on the other options that are available.

The below is an example of a typical `bin/dfs.config`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<ookii.jumbo.dfs>
  <fileSystem url="jdfs://nameservernodename:9000"/>
  <nameServer blockSize="128MB"
              replicationFactor="3"
              imageDirectory="/jumbo/nameserver" />
  <dataServer port="9001"
              blockStoragePath="/jumbo/dataserver"/>
</ookii.jumbo.dfs>
```

### Running without the DFS

It is not strictly required to use the Jumbo DFS to run data processing jobs. You can also use the
local file system. To do this, modify the `bin/dfs.config` file and set the file system URL to
`file:///root/path`, where /root/path is the root of the file system as visible by Jumbo. If this
path is a file share accessible on every node, you can even use this when running Jumbo on multiple
nodes.

Certain features are not available when the Jumbo DFS is not used. It’s generally recommended to use
the Jumbo DFS instead of the local file system except for debugging purposes.

## Configuring Jumbo Jet

There are two values that you must specify in `bin/jet.config`:

1. Set the JobServer host name and port.
2. Set the JobServer's job archive directory to a local directory where information about completed
   jobs is archived so it can be accessed after the Jumbo Jet server restarts.
3. Set the TaskServer task directory to a local directory where configuration, task log and
   intermediate data files will be stored on each node.

See the [jet.config XML documentation](https://www.ookii.org/Link/JumboDocJetConfig) for information
on the other options that are available.

The below is an example of a typical `bin/jet.config`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<ookii.jumbo.jet>
  <jobServer hostName="jobserverhostname"
             port="9500"
             archiveDirectory="/jumbo/jobarchive"
             broadcastAddress="192.168.0.255"
             broadcastPort="9550" />
  <taskServer port="9501"
              taskDirectory="/jumbo/taskserver"
              taskSlots="2"
              fileServerPort="9502"
              fileServerMaxConnections="4"
              immediateCompletedTaskNotification="true"/>
  <fileChannel memoryStorageSize="2GB"
               spillBufferSize="100MB"/>
  <mergeRecordReader maxFileInputs="10"/>
</ookii.jumbo.jet>
```

Now, you are ready to [run Jumbo](Running.md) on your cluster.
