// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Ookii.CommandLine;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace DfsShell.Commands
{
    [ShellCommand("put"), Description("Stores a file or directory on the DFS.")]
    class PutCommand : DfsShellCommandWithProgress
    {
        private readonly string _localPath;
        private readonly string _dfsPath;

        public PutCommand([Description("The path of the local file or directory to upload."), ArgumentName("LocalPath")] string localPath,
                              [Description("The path of the DFS file or directory to upload to."), ArgumentName("DfsPath")] string dfsPath)
        {
            if (localPath == null)
                throw new ArgumentNullException(nameof(localPath));
            if (dfsPath == null)
                throw new ArgumentNullException(nameof(dfsPath));

            _localPath = localPath;
            _dfsPath = dfsPath;
        }

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

        public override void Run()
        {
            Type recordReaderType;
            Type recordWriterType;
            if (!File.Exists(_localPath) && !Directory.Exists(_localPath))
                Console.Error.WriteLine("Local path {0} does not exist.", _localPath);
            else if (BlockSize.Value < 0 || BlockSize.Value >= Int32.MaxValue)
                Console.Error.WriteLine("Invalid block size.");
            else if (CheckRecordOptions(out recordReaderType, out recordWriterType))
            {
                ProgressCallback progressCallback = Quiet ? null : new ProgressCallback(PrintProgress);
                try
                {
                    bool isDirectory = Directory.Exists(_localPath);
                    if (isDirectory)
                    {
                        if (!Quiet)
                            Console.WriteLine("Copying local directory \"{0}\" to DFS directory \"{1}\"...", _localPath, _dfsPath);
                        if (recordReaderType != null)
                            UploadDirectoryRecords(_localPath, _dfsPath, recordReaderType, recordWriterType);
                        else
                            Client.UploadDirectory(_localPath, _dfsPath, (int)BlockSize.Value, ReplicationFactor, !NoLocalReplica, progressCallback);
                    }
                    else
                    {
                        JumboDirectory dir = Client.GetDirectoryInfo(_dfsPath);
                        string dfsPath = _dfsPath;
                        if (dir != null)
                        {
                            string fileName = Path.GetFileName(_localPath);
                            dfsPath = Client.Path.Combine(dfsPath, fileName);
                        }
                        if (!Quiet)
                            Console.WriteLine("Copying local file \"{0}\" to DFS file \"{1}\"...", _localPath, dfsPath);
                        if (recordReaderType != null)
                            UploadFileRecords(_localPath, dfsPath, recordReaderType, recordWriterType);
                        else
                            Client.UploadFile(_localPath, dfsPath, (int)BlockSize.Value, ReplicationFactor, !NoLocalReplica, progressCallback);
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
                    Console.Error.WriteLine("Unable to read local file:");
                    Console.Error.WriteLine(ex.Message);
                }
            }
        }

        private void UploadFileRecords(string localPath, string dfsPath, Type recordReaderType, Type recordWriterType)
        {
            int previousPercentage = -1;
            using (FileStream inputStream = File.OpenRead(localPath))
            using (IRecordReader reader = (IRecordReader)Activator.CreateInstance(recordReaderType, inputStream))
            using (Stream outputStream = Client.CreateFile(dfsPath, (int)BlockSize.Value, ReplicationFactor, !NoLocalReplica, RecordOptions))
            using (IRecordWriter writer = (IRecordWriter)Activator.CreateInstance(recordWriterType, outputStream))
            {
                while (reader.ReadRecord())
                {
                    writer.WriteRecord(reader.CurrentRecord);
                    if (!Quiet)
                    {
                        int percentage = (int)(reader.Progress * 100);
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
            string[] files = System.IO.Directory.GetFiles(localPath);

            JumboDirectory directory = Client.GetDirectoryInfo(dfsPath);
            if (directory != null)
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Directory {0} already exists on the DFS.", dfsPath), nameof(dfsPath));
            Client.CreateDirectory(dfsPath);

            foreach (string file in files)
            {
                string targetFile = Client.Path.Combine(dfsPath, System.IO.Path.GetFileName(file));
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

                Type recordReaderRecordType = recordReaderType.FindGenericBaseType(typeof(RecordReader<>), true).GetGenericArguments()[0];
                Type recordWriterRecordType = recordWriterType.FindGenericBaseType(typeof(RecordWriter<>), true).GetGenericArguments()[0];
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
}
