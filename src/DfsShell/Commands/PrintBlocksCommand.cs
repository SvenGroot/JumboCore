// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.Globalization;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("blocks"), Description("Prints a list of blocks.")]
sealed partial class PrintBlocksCommand : DfsShellCommand
{
    [CommandLineArgument(IsPositional = true)]
    [Description("The kind of blocks to include in the results: Normal, Pending, or UnderReplicated.")]
    public BlockKind Kind { get; set; } = BlockKind.Normal;

    [CommandLineArgument, Description("Show the path of the file that each block belongs to.")]
    public bool ShowFiles { get; set; }

    public override int Run()
    {
        var dfsClient = Client as DfsClient;
        if (dfsClient == null)
        {
            Console.WriteLine("The configured file system doesn't support blocks.");
        }
        else
        {
            var blocks = dfsClient.NameServer.GetBlocks(Kind);
            foreach (var blockId in blocks)
            {
                if (ShowFiles)
                {
                    Console.WriteLine("{0:B}: {1}", blockId, dfsClient.NameServer.GetFileForBlock(blockId));
                }
                else
                {
                    Console.WriteLine(blockId.ToString("B", CultureInfo.CurrentCulture));
                }
            }

            return 0;
        }

        return 1;
    }
}
