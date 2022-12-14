// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [Command("blockinfo"), Description("Prints the data server list for the specified block.")]
    class PrintBlockInfoCommand : DfsShellCommand
    {
        private readonly Guid _blockId;

        public PrintBlockInfoCommand([Description("The block ID."), ArgumentName("BlockId")] Guid blockId)
        {
            _blockId = blockId;
        }

        public override int Run()
        {
            var dfsClient = Client as DfsClient;
            if (dfsClient == null)
                Console.WriteLine("The configured file system doesn't support blocks.");
            else
            {
                var file = dfsClient.NameServer.GetFileForBlock(_blockId);
                if (file == null)
                    Console.Error.WriteLine("Unknown block ID.");
                else
                {
                    Console.WriteLine("Block ID: {0:B}", _blockId);
                    Console.WriteLine("File: {0}", file);
                    var servers = dfsClient.NameServer.GetDataServersForBlock(_blockId);
                    Console.WriteLine("Data server list for block {0:B}:", _blockId);
                    foreach (var server in servers)
                        Console.WriteLine(server);

                    return 0;
                }
            }

            return 1;
        }
    }
}
