// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [GeneratedParser]
    [Command("get"), Description("Retrieves a file or directory from the DFS.")]
    partial class GetCommand : DfsShellCommandWithProgress
    {
        [CommandLineArgument(IsPositional = true, IsRequired = true)]
        [Description("The path of the DFS file or directory to retrieve.")]
        public string DfsPath { get; set; }

        [CommandLineArgument(IsPositional = true)]
        [Description("The local path where the file should be stored. The default value is the current directory.")]
        public string LocalPath { get; set; } = ".";

        [CommandLineArgument, Description("Suppress progress information output.")]
        public bool Quiet { get; set; }

        public override int Run()
        {
            var entry = Client.GetFileSystemEntryInfo(DfsPath);
            if (entry == null)
            {
                Console.Error.WriteLine("Path {0} does not exist on the DFS.", DfsPath);
                return 1;
            }

            var localPath = LocalPath == "." ? Environment.CurrentDirectory : Path.Combine(Environment.CurrentDirectory, LocalPath);
            var progressCallback = Quiet ? null : new ProgressCallback(PrintProgress);

            try
            {
                if (entry is JumboFile)
                {
                    if (Directory.Exists(localPath))
                    {
                        // It's a directory, so append the file name
                        localPath = Path.Combine(localPath, entry.Name);
                    }
                    if (!Quiet)
                        Console.WriteLine("Copying DFS file \"{0}\" to local file \"{1}\"...", entry.FullPath, localPath);
                    Client.DownloadFile(DfsPath, localPath, progressCallback);
                }
                else
                {
                    if (!Quiet)
                        Console.WriteLine("Copying DFS directory \"{0}\" to local directory \"{1}\"...", entry.FullPath, localPath);
                    Client.DownloadDirectory(DfsPath, localPath, progressCallback);
                }
                if (!Quiet)
                    Console.WriteLine();
            }
            catch (UnauthorizedAccessException ex)
            {
                Console.Error.WriteLine("Unable to open local file:");
                Console.Error.WriteLine(ex.Message);
                return 1;
            }
            catch (IOException ex)
            {
                Console.Error.WriteLine("Unable to get file:");
                Console.Error.WriteLine(ex.Message);
                return 1;
            }

            return 0;
        }
    }
}
