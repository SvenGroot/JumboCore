// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("get"), Description("Retrieves a file or directory from the DFS.")]
    class GetCommand : DfsShellCommandWithProgress
    {
        private readonly string _localPath;
        private readonly string _dfsPath;

        public GetCommand([Description("The path of the DFS file or directory to retrieve."), ArgumentName("DfsPath")] string dfsPath,
                          [Optional, DefaultParameterValue("."), Description("The local path where the file should be stored. The default value is the current directory."), ArgumentName("LocalPath")] string localPath)
        {
            ArgumentNullException.ThrowIfNull(dfsPath);
            ArgumentNullException.ThrowIfNull(localPath);

            _dfsPath = dfsPath;
            _localPath = localPath;
        }

        [CommandLineArgument, Description("Suppress progress information output.")]
        public bool Quiet { get; set; }

        public override void Run()
        {
            var entry = Client.GetFileSystemEntryInfo(_dfsPath);
            if (entry == null)
            {
                Console.Error.WriteLine("Path {0} does not exist on the DFS.", _dfsPath);
                return;
            }

            var localPath = _localPath == "." ? Environment.CurrentDirectory : Path.Combine(Environment.CurrentDirectory, _localPath);
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
                    Client.DownloadFile(_dfsPath, localPath, progressCallback);
                }
                else
                {
                    if (!Quiet)
                        Console.WriteLine("Copying DFS directory \"{0}\" to local directory \"{1}\"...", entry.FullPath, localPath);
                    Client.DownloadDirectory(_dfsPath, localPath, progressCallback);
                }
                if (!Quiet)
                    Console.WriteLine();
            }
            catch (UnauthorizedAccessException ex)
            {
                Console.Error.WriteLine("Unable to open local file:");
                Console.Error.WriteLine(ex.Message);
            }
            catch (IOException ex)
            {
                Console.Error.WriteLine("Unable to get file:");
                Console.Error.WriteLine(ex.Message);
            }
        }
    }
}
