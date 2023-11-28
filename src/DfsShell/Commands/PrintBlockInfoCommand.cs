// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("blockinfo"), Description("Prints the data server list for the specified block.")]
partial class PrintBlockInfoCommand : DfsShellCommand
{
    [CommandLineArgument(IsPositional = true, IsRequired = true)]
    [Description("The path of the new directory to create.")]
    public Guid BlockId { get; set; }

    public override int Run()
    {
        var dfsClient = Client as DfsClient;
        if (dfsClient == null)
        {
            Console.WriteLine("The configured file system doesn't support blocks.");
        }
        else
        {
            var file = dfsClient.NameServer.GetFileForBlock(BlockId);
            if (file == null)
            {
                Console.Error.WriteLine("Unknown block ID.");
            }
            else
            {
                Console.WriteLine("Block ID: {0:B}", BlockId);
                Console.WriteLine("File: {0}", file);
                var servers = dfsClient.NameServer.GetDataServersForBlock(BlockId);
                Console.WriteLine("Data server list for block {0:B}:", BlockId);
                foreach (var server in servers)
                {
                    Console.WriteLine(server);
                }

                return 0;
            }
        }

        return 1;
    }
}
