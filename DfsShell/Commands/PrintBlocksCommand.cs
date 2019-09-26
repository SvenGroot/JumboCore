// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using Ookii.Jumbo.Dfs;
using System.Runtime.InteropServices;
using Ookii.Jumbo.Dfs.FileSystem;
using System.Globalization;

namespace DfsShell.Commands
{
    [ShellCommand("blocks"), Description("Prints a list of blocks.")]
    sealed class PrintBlocksCommand : DfsShellCommand
    {
        private readonly BlockKind _kind;

        public PrintBlocksCommand([Description("The kind of blocks to include in the results: Normal, Pending, or UnderReplicated. The default is Normal."), ArgumentName("Kind")] BlockKind kind = BlockKind.Normal)
        {
            _kind = kind;
        }

        [CommandLineArgument, Description("Show the path of the file that each block belongs to.")]
        public bool ShowFiles { get; set; }

        public override void Run()
        {
            DfsClient dfsClient = Client as DfsClient;
            if( dfsClient == null )
                Console.WriteLine("The configured file system doesn't support blocks.");
            else
            {
                Guid[] blocks = dfsClient.NameServer.GetBlocks(_kind);
                foreach( Guid blockId in blocks )
                {
                    if( ShowFiles )
                        Console.WriteLine("{0:B}: {1}", blockId, dfsClient.NameServer.GetFileForBlock(blockId));
                    else
                        Console.WriteLine(blockId.ToString("B", CultureInfo.CurrentCulture));
                }
            }
        }
    }
}
