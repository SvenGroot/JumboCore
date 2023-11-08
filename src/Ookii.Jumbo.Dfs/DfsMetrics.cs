// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Represents information about the current state of the distributed file system.
/// </summary>
[Serializable]
public class DfsMetrics
{
    private readonly Collection<DataServerMetrics> _dataServers = new Collection<DataServerMetrics>();

    /// <summary>
    /// Initializes a new instance of the <see cref="DfsMetrics"/> class.
    /// </summary>
    /// <param name="nameServer">The address of the name server.</param>
    public DfsMetrics(ServerAddress nameServer) 
    {
        ArgumentNullException.ThrowIfNull(nameServer);
        NameServer = nameServer;
    }


    /// <summary>
    /// Gets the address of the name server.
    /// </summary>
    /// <value>The address of the name server.</value>
    public ServerAddress NameServer { get; }

    /// <summary>
    /// Gets or sets the total size of all files.
    /// </summary>
    /// <value>
    /// The size of all files in the DFS added together; note that the actual space used on the
    /// data servers will be higher due to replication.
    /// </value>
    public long TotalSize { get; set; }

    /// <summary>
    /// Gets the total storage capacity of the DFS.
    /// </summary>
    /// <value>The total capacity. This is the total disk space of all the data servers combined.</value>
    public long TotalCapacity
    {
        get
        {
            return (from server in DataServers
                    select server.DiskSpaceTotal).Sum();
        }
    }

    /// <summary>
    /// Gets or sets the storage capacity that is used by files on the DFS.
    /// </summary>
    /// <value>The storage capacity that is used by files on the DFS; this includes space used by replicated blocks.</value>
    public long DfsCapacityUsed
    {
        get
        {
            return (from server in DataServers
                    select server.DiskSpaceUsed).Sum();
        }
    }

    /// <summary>
    /// Gets or sets the storage capacity that is available.
    /// </summary>
    /// <value>The storage capacity that is available, which is the value of <see cref="TotalCapacity"/>
    /// minus <see cref="DfsCapacityUsed"/> minus the capacity used by other files on the same disks.</value>
    public long AvailableCapacity
    {
        get
        {
            return (from server in DataServers
                    select server.DiskSpaceFree).Sum();
        }
    }

    /// <summary>
    /// Gets or sets the total number of blocks. This does not include pending blocks.
    /// </summary>
    /// <value>
    /// The the total number of non-pending blocks.
    /// </value>
    public int TotalBlockCount { get; set; }

    /// <summary>
    /// Gets or sets the total number of blocks that are not fully replicated.
    /// </summary>
    /// <value>
    /// The total number of blocks that are not fully replicated.
    /// </value>
    public int UnderReplicatedBlockCount { get; set; }

    /// <summary>
    /// Gets or sets the total number of blocks that have not yet been committed.
    /// </summary>
    /// <value>
    /// The total number of blocks that have not yet been committed.
    /// </value>
    public int PendingBlockCount { get; set; }

    /// <summary>
    /// Gets metrics for all data servers registered with the system.
    /// </summary>
    /// <value>
    /// A list of <see cref="DataServerMetrics"/> objects for each data server.
    /// </value>
    public Collection<DataServerMetrics> DataServers
    {
        get { return _dataServers; }
    }

    /// <summary>
    /// Prints the metrics.
    /// </summary>
    /// <param name="writer">The <see cref="TextWriter"/> to print the metrics to.</param>
    public void PrintMetrics(TextWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteLine("Name server:      {0}", NameServer);
        writer.WriteLine("Total size:       {0:#,0} bytes", TotalSize);
        writer.WriteLine("Blocks:           {0} (excl. pending blocks)", TotalBlockCount);
        writer.WriteLine("Under-replicated: {0}", UnderReplicatedBlockCount);
        writer.WriteLine("Pending blocks:   {0}", PendingBlockCount);
        writer.WriteLine("Data servers:     {0}", DataServers.Count);
        foreach (ServerMetrics server in DataServers)
            writer.WriteLine("  {0}", server);
    }
}
