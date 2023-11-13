# Jumbo Components

In this section, we'll take a look at the components that make up Jumbo and the basics of how they
work. Jumbo consists of a distributed file system and the Jumbo Jet data processing engine.

## Jumbo assemblies

Jumbo is spread over a number of class libraries and applications.

- **Ookii.Jumbo:** Class library containing functionality used by all Jumbo clients and servers.
- **Ookii.Jumbo.Dfs:** Class library containing functionality used by the DFS servers and
  clients.
- **Ookii.Jumbo.Jet:** Class library containing functionality used by Jumbo Jet's servers, clients,
  and jobs.
- **Ookii.Jumbo.Jet.Samples:** Class library with sample jobs for Jumbo Jet.
- **NameServer:** The primary server for Jumbo's DFS, handling the file system namespace and fault
  tolerance.
- **DataServer:** DFS server that stores file data.
- **JobServer:** The primary server for Jumbo Jet, handling job submission, scheduling and fault
  tolerance.
- **TaskServer:** Jumbo Jet server that executes tasks in a job.
- **TaskHost:** Host application used by the TaskServer to run tasks out-of-process.
- **DfsShell:** Client application to interact with the DFS (list directories, upload/download files,
  query DFS status, etc.).
- **JetShell:** Client application to interace with Jumbo Jet (submit jobs, query cluster status, etc.).
- **DfsWeb:** Administration web portal for the DFS, allowing you to view the DFS health and status,
  browse the namespace, and view server logs.
- **JetWeb:** Administration web portal for Jumbo Jet, allowing to view the Jet cluster health and
  status, job and task status, and view server and task logs.
- **Ookii.Jumbo.Test:** Unit and functional tests for Jumbo.
- **Ookii.Jumbo.Test.Tasks:** Task types used by Ookii.Jumbo.Test when testing Jumbo Jet.

For more details about the class libraries, you can view [their documentation](https://www.ookii.org/Link/JumboDoc).

## Jumbo DFS

Jumbo’s distributed file system uses a setup similar to the Google DFS and Hadoop DFS.

A single _NameServer_ maintains the file system namespace. All clients interact with the NameServer
to query or modify the namespace (e.g. list a directory, create a file). The files on the DFS are
read-only after creation, so can only be written once.

A file is divided into one or more _blocks_. Blocks are typically quite large; 64MB is a normal
size. Blocks are stored on _DataServers_, which store the file data but don’t know anything about
the namespace. They only have a list of blocks, and don't know which files they belong to. For
fault tolerance, every block is replicated on multiple DataServers (typically three, unless you
have less than three DataServers).

The NameServer keeps track of which blocks belong to which files, and also which DataServers have
which blocks. If a DataServer crashes or is otherwise removed from the network, the NameServer
makes sure all affected blocks are re-replicated to other DataServers.

To create a new file, a client application takes the following steps:

1. The client asks the NameServer to create a file.
2. The NameServer adds the file to the namespace, and assigns it a new block.
3. The NameServer chooses which DataServers will hold the block, and sends the client the block ID
   and a list of DataServers.
4. The client connects to the first DataServer in the list, and sends it the block ID and a list of
   the remaining servers.
5. The client sends file data to the DataServer, in increments called _packets_. Packets are 64KB
   and each have a CRC32 checksum.
6. The DataServer validates the CRC of each packet, writes it to disk, and sends an acknowledgement
   to the client. If the CRC is invalid, it asks the client to retransmit.
7. The DataServer also connects to the next DataServer in the list, and forwards all the packets,
   following the same protocol. This ensures maximum write throughput because the client doesn't
   have to send data to all the DataServers.
8. Once the final packet for a block is sent (either because the end of the block was reached or
   the end of the file was reached), the DataServer informs the NameServer it has this block.
9. If the file is longer than one block, once a block is finished, the client asks the NameServer
   to append a block. The process repeats from step 3.
10. When the final block is completed, the client tells the NameServer to close the file. The
    changes are committed, and the file is now read-only.

To ensure the best fault tolerance and performance, Jumbo tries to place two replicas of each block
on the current rack, and one on a different rack, in case an entire rack fails (if using three
replicas and rack topology has been configured). If the client is in the cluster (for example,
Jumbo Jet's TaskServers and tasks often run on the same nodes as the DataServers), it also tries
to place the first replica on the same node as the client (unless the client request's otherwise).

When the NameServer returns the list of replicas to the client, it orders them by the _distance_
from the client. Since the client will only write directly to the first server in the list, this
will be the closest server, allowing for maximum performance. Per the above, there are three
possible distances:

- Data local: the DataServer is on the same node as the client.
- Rack local: the DataServer is in the same rack as the client.
- Non-local: the DataServer is in a different rack.

Using rack locality requires configuring a network topology using `common.config`. That way, Jumbo
can know which nodes are in which rack.

When reading a file, the process is simpler:

1. The client queries the NameServer for information about the file.
2. The NameServer sends the information, which includes the list of blocks that make up the file.
3. For each block, the client asks the NameServer for a list of DataServers that have this block.
4. The NameServer orders the list by distance from the client, and sends it to the client.
5. The client connects to a DataServer to read the block (typically the closest one).
6. During reading, the client verifies the CRC of each packet. If it's corrupt, it'll move on to the
   next DataServer (there is currently no mechanism to remove the corrupted block and re-replicate
   it; see, this is why Jumbo is not production quality code).

Client applications interact with the DFS using the [`FileSystemClient`][] class, which provides an API
for performing these operations. Using it, a client simply creates a file, gets a [`DfsOutputStream`][]
for it, and writes to that stream like any normal file. All the steps above are taken care of under
the hood.

It's probably rare that you'll ever write a DFS client application yourself. If you write Jumbo Jet
jobs, most of this is abstracted through its data input and output models. And as an end user, you
use `DfsShell` to interact with the DFS, as we've already seen in the quick start guide.

## Jumbo Jet

Jumbo Jet is Jumbo’s data processing system. It is very loosely based on Hadoop 1.0 and MapReduce,
but offers more flexibility.

A data processing job in Jumbo Jet consists of a linear sequence of _stages_, connected by
_channels_. A stage performs some processing operation on the data, and the channels control how
data is transferred between stages. Stages are responsible for the bulk of the data processing
work, while channels transfer data and can do some additional intermediate processing such as
sorting and partitioning.

If you are familiar with MapReduce, you can consider MapReduce to be a two-stage job where the
first stage runs the map function, the second runs the reduce function, and the channel between the
two stages sorts and partitions the data. In fact, that is exactly how you create MapReduce jobs in
Jumbo. But Jumbo is not limited to MapReduce; it can use different kind of job structures as well.

A stage reads data either from a _data input_ (typically a file or multiple files on the DFS) or a
channel, and writes data either to a channel or a _data output_ (typically a directory on the DFS).
A stage can also have multiple channels as input, for example to perform a join.

Stages are divided into _tasks_, where each task processes a part of the stage's input data. Tasks
are run in parallel on multiple systems in a cluster. For stages reading a data input (file), the
input is divided linearly across tasks (these are called _splits_). For stages reading a channel
input, the channel _partitions_ the data across the tasks.

Tasks run a user-defined piece of code to do their processing. This code doesn’t need to be aware of
most of the details. Regardless of whether the task is reading from or writing to a file or a
channel, the code is the same. Input is provided via a [`RecordReader<T>`][] and output is written to a
[`RecordWriter<T>`][], which take care of the details. Since partitioning (and optionally, things like
sorting) are handled by the Jumbo Jet infrastructure, you don’t have to worry about how to perform
those operations; you simply need to specify you want them to happen.

The typical way to create a job in Jumbo is to use the [`JobBuilder`][], which allows you to specify a
sequence of operations which are then translated into a job configuration of stages and channels.
This means you can create jobs without worrying too much about their actual structure during
execution (although of course this is available if you want to do more complex processing).

We will go into more details about how jobs are executed [later](JobExecution.md). First, it's time
to learn how to [write your own jobs for Jumbo Jet](Tutorial1.md).

[`DfsOutputStream`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Dfs_DfsOutputStream.htm
[`FileSystemClient`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Dfs_FileSystem_FileSystemClient.htm
[`JobBuilder`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder.htm
[`RecordReader<T>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_RecordReader_1.htm
[`RecordWriter<T>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_RecordWriter_1.htm
