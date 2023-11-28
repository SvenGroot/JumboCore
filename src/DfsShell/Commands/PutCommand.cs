// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.IO;
using Ookii;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.IO;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("put"), Description("Stores a file or directory on the DFS.")]
partial class PutCommand : DfsShellCommandWithProgress
{

    [CommandLineArgument(IsPositional = true, IsRequired = true)]
    [Description("The path of the local file or directory to upload.")]
    public string LocalPath { get; set; }

    [CommandLineArgument(IsPositional = true, IsRequired = true)]
    [Description("The path of the DFS file or directory to upload to.")]
    public string DfsPath { get; set; }

    [CommandLineArgument, Description("The block size of the DFS file.")]
    public BinarySize BlockSize { get; set; }

    [CommandLineArgument, Description("The replication factor of the DFS file.")]
    public int ReplicationFactor { get; set; }

    [CommandLineArgument, Description("Suppress progress information output.")]
    public bool Quiet { get; set; }

    [CommandLineArgument, Description("The record reader used to read the file(s). This must be the assembly-qualified name of the type. If this argument is specified, you must also specify a record writer using the same record type.")]
    public string RecordReaderType { get; set; }

    [CommandLineArgument, Description("The record writer used to write the file(s) to the DFS. This must be the assembly-qualified name of the type. If this argument is specified, you must also specify a record writer using the same record type.")]
    public string RecordWriterType { get; set; }

    [CommandLineArgument, Description("The record options for the file. Must be a comma-separated list of the values of the RecordStreamOptions enumeration. If this option is anything other than None, you must specify a record reader and record writer.")]
    public RecordStreamOptions RecordOptions { get; set; }

    [CommandLineArgument("Text"), Description("Treat the file as line-separated text. This is equivalent to specifying LineRecordReader as the record reader and TextRecordReader<Utf8String> as the record writer.")]
    public bool TextFile { get; set; }

    [CommandLineArgument, Description("The first replica should not be put on the local node if that node is part of the DFS. Note that the first replica might still be placed on the local node; it is just no longer guaranteed.")]
    public bool NoLocalReplica { get; set; }

    public override int Run()
    {
        if (!File.Exists(LocalPath) && !Directory.Exists(LocalPath))
        {
            Console.Error.WriteLine("Local path {0} does not exist.", LocalPath);
        }
        else if (BlockSize.Value < 0 || BlockSize.Value >= Int32.MaxValue)
        {
            Console.Error.WriteLine("Invalid block size.");
        }
        else if (CheckRecordOptions(out var recordReaderType, out var recordWriterType))
        {
            var progressCallback = Quiet ? null : new ProgressCallback(PrintProgress);
            try
            {
                var isDirectory = Directory.Exists(LocalPath);
                if (isDirectory)
                {
                    if (!Quiet)
                    {
                        Console.WriteLine("Copying local directory \"{0}\" to DFS directory \"{1}\"...", LocalPath, DfsPath);
                    }

                    if (recordReaderType != null)
                    {
                        UploadDirectoryRecords(LocalPath, DfsPath, recordReaderType, recordWriterType);
                    }
                    else
                    {
                        Client.UploadDirectory(LocalPath, DfsPath, (int)BlockSize.Value, ReplicationFactor, !NoLocalReplica, progressCallback);
                    }
                }
                else
                {
                    var dir = Client.GetDirectoryInfo(DfsPath);
                    var dfsPath = DfsPath;
                    if (dir != null)
                    {
                        var fileName = Path.GetFileName(LocalPath);
                        dfsPath = Client.Path.Combine(dfsPath, fileName);
                    }
                    if (!Quiet)
                    {
                        Console.WriteLine("Copying local file \"{0}\" to DFS file \"{1}\"...", LocalPath, dfsPath);
                    }

                    if (recordReaderType != null)
                    {
                        UploadFileRecords(LocalPath, dfsPath, recordReaderType, recordWriterType);
                    }
                    else
                    {
                        Client.UploadFile(LocalPath, dfsPath, (int)BlockSize.Value, ReplicationFactor, !NoLocalReplica, progressCallback);
                    }
                }
                if (!Quiet)
                {
                    Console.WriteLine();
                }

                return 0;
            }
            catch (UnauthorizedAccessException ex)
            {
                Console.Error.WriteLine("Unable to open local file:");
                Console.Error.WriteLine(ex.Message);
            }
            catch (IOException ex)
            {
                Console.Error.WriteLine("Unable to read local file:");
                Console.Error.WriteLine(ex.Message);
            }
        }

        return 1;
    }

    private void UploadFileRecords(string localPath, string dfsPath, Type recordReaderType, Type recordWriterType)
    {
        var previousPercentage = -1;
        using (var inputStream = File.OpenRead(localPath))
        using (var reader = (IRecordReader)Activator.CreateInstance(recordReaderType, inputStream))
        using (var outputStream = Client.CreateFile(dfsPath, (int)BlockSize.Value, ReplicationFactor, !NoLocalReplica, RecordOptions))
        using (var writer = (IRecordWriter)Activator.CreateInstance(recordWriterType, outputStream))
        {
            while (reader.ReadRecord())
            {
                writer.WriteRecord(reader.CurrentRecord);
                if (!Quiet)
                {
                    var percentage = (int)(reader.Progress * 100);
                    if (percentage != previousPercentage)
                    {
                        previousPercentage = percentage;
                        PrintProgress(dfsPath, percentage, inputStream.Position);
                    }
                }
            }
        }
    }

    private void UploadDirectoryRecords(string localPath, string dfsPath, Type recordReaderType, Type recordWriterType)
    {
        var files = System.IO.Directory.GetFiles(localPath);

        var directory = Client.GetDirectoryInfo(dfsPath);
        if (directory != null)
        {
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Directory {0} already exists on the DFS.", dfsPath), nameof(dfsPath));
        }

        Client.CreateDirectory(dfsPath);

        foreach (var file in files)
        {
            var targetFile = Client.Path.Combine(dfsPath, System.IO.Path.GetFileName(file));
            UploadFileRecords(file, targetFile, recordReaderType, recordWriterType);
        }
    }

    private bool CheckRecordOptions(out Type recordReaderType, out Type recordWriterType)
    {
        recordReaderType = null;
        recordWriterType = null;
        if (TextFile)
        {
            if (!(RecordReaderType == null && RecordWriterType == null))
            {
                Console.Error.WriteLine("You may not specify a record reader or record writer if the -text option is specified.");
                return false;
            }
            recordReaderType = typeof(LineRecordReader);
            recordWriterType = typeof(TextRecordWriter<Utf8String>);
            return true;
        }
        else if (RecordReaderType != null || RecordWriterType != null)
        {
            if (RecordReaderType == null || RecordWriterType == null)
            {
                Console.Error.WriteLine("You must specify both a record reader and a record writer.");
                return false;
            }
            recordReaderType = Type.GetType(RecordReaderType, true);
            recordWriterType = Type.GetType(RecordWriterType, true);

            var recordReaderRecordType = recordReaderType.FindGenericBaseType(typeof(RecordReader<>), true).GetGenericArguments()[0];
            var recordWriterRecordType = recordWriterType.FindGenericBaseType(typeof(RecordWriter<>), true).GetGenericArguments()[0];
            if (recordReaderRecordType != recordWriterRecordType)
            {
                Console.Error.WriteLine("The record reader and writer must have the same record types.");
                return false;
            }

            return true;
        }
        else if (RecordOptions != RecordStreamOptions.None)
        {
            Console.Error.WriteLine("You must specify a record reader and writer if the -ro option is set to anything other than None.");
            return false;
        }

        return true;
    }
}
