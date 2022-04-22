// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.IO;

namespace NameServerApplication
{
    /// <summary>
    /// Represents an edit log file for the file system.
    /// </summary>
    sealed class EditLog : IDisposable
    {
        #region Nested types

        private abstract class EditLogEntry : IWritable
        {
            protected EditLogEntry(FileSystemMutation mutation)
            {
                Mutation = mutation;
            }

            protected EditLogEntry(FileSystemMutation mutation, DateTime date, string path)
            {
                Mutation = mutation;
                Date = date;
                Path = path;
            }

            public FileSystemMutation Mutation { get; private set; }

            public DateTime Date { get; private set; }

            public string Path { get; private set; }

            public static EditLogEntry ReadEntry(BinaryReader reader)
            {
                var mutation = reader.ReadInt32();

                var result = _entryTypeMap[mutation]();
                result.Read(reader);
                return result;
            }

            public abstract void Replay(FileSystem fileSystem);

            #region IWritable Members

            public virtual void Write(BinaryWriter writer)
            {
                writer.Write((int)Mutation);
                writer.Write(Date.Ticks);
                writer.Write(Path);
            }

            public virtual void Read(BinaryReader reader)
            {
                // Mutation is not read from the reader here because it has to be read up front to determine what type of class to create.
                Date = new DateTime(reader.ReadInt64());
                Path = reader.ReadString();
            }

            #endregion
        }

        private sealed class CreateDirectoryEditLogEntry : EditLogEntry
        {
            public CreateDirectoryEditLogEntry()
                : base(FileSystemMutation.CreateDirectory)
            {
            }

            public CreateDirectoryEditLogEntry(DateTime date, string path)
                : base(FileSystemMutation.CreateDirectory, date, path)
            {
            }

            public override void Replay(FileSystem fileSystem)
            {
                fileSystem.CreateDirectory(Path, Date);
            }
        }

        private sealed class CreateFileEditLogEntry : EditLogEntry
        {
            public CreateFileEditLogEntry()
                : base(FileSystemMutation.CreateFile)
            {
            }

            public CreateFileEditLogEntry(DateTime date, string path, int blockSize, int replicationFactor, RecordStreamOptions recordOptions)
                : base(FileSystemMutation.CreateFile, date, path)
            {
                BlockSize = blockSize;
                ReplicationFactor = replicationFactor;
                RecordOptions = recordOptions;
            }

            public int BlockSize { get; private set; }

            public int ReplicationFactor { get; private set; }

            public RecordStreamOptions RecordOptions { get; private set; }

            public override void Replay(FileSystem fileSystem)
            {
                fileSystem.CreateFile(Path, Date, BlockSize, ReplicationFactor, RecordOptions, false);
            }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(BlockSize);
                writer.Write(ReplicationFactor);
                writer.Write((int)RecordOptions);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                BlockSize = reader.ReadInt32();
                ReplicationFactor = reader.ReadInt32();
                RecordOptions = (RecordStreamOptions)reader.ReadInt32();
            }
        }

        private sealed class AppendBlockEditLogEntry : EditLogEntry
        {
            public AppendBlockEditLogEntry()
                : base(FileSystemMutation.AppendBlock)
            {
            }

            public AppendBlockEditLogEntry(DateTime date, string path, Guid blockId)
                : base(FileSystemMutation.AppendBlock, date, path)
            {
                BlockId = blockId;
            }

            public Guid BlockId { get; private set; }

            public override void Replay(FileSystem fileSystem)
            {
                fileSystem.AppendBlock(Path, BlockId, -1);
            }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(BlockId.ToByteArray());
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                BlockId = new Guid(reader.ReadBytes(16));
            }
        }

        private sealed class CommitBlockEditLogEntry : EditLogEntry
        {
            public CommitBlockEditLogEntry()
                : base(FileSystemMutation.CommitBlock)
            {
            }

            public CommitBlockEditLogEntry(DateTime date, string path, Guid blockId, int size)
                : base(FileSystemMutation.CommitBlock, date, path)
            {
                BlockId = blockId;
                Size = size;
            }

            public Guid BlockId { get; private set; }

            public int Size { get; private set; }

            public override void Replay(FileSystem fileSystem)
            {
                fileSystem.CommitBlock(Path, BlockId, Size);
            }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(BlockId.ToByteArray());
                writer.Write(Size);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                BlockId = new Guid(reader.ReadBytes(16));
                Size = reader.ReadInt32();
            }
        }

        private sealed class CommitFileEditLogEntry : EditLogEntry
        {
            public CommitFileEditLogEntry()
                : base(FileSystemMutation.CommitFile)
            {
            }

            public CommitFileEditLogEntry(DateTime date, string path)
                : base(FileSystemMutation.CommitFile, date, path)
            {
            }

            public override void Replay(FileSystem fileSystem)
            {
                fileSystem.CloseFile(Path);
            }
        }

        private sealed class DeleteEditLogEntry : EditLogEntry
        {
            public DeleteEditLogEntry()
                : base(FileSystemMutation.Delete)
            {
            }

            public DeleteEditLogEntry(DateTime date, string path, bool recursive)
                : base(FileSystemMutation.Delete, date, path)
            {
                IsRecursive = recursive;
            }

            public bool IsRecursive { get; private set; }

            public override void Replay(FileSystem fileSystem)
            {
                fileSystem.Delete(Path, IsRecursive);
            }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(IsRecursive);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                IsRecursive = reader.ReadBoolean();
            }
        }

        private sealed class MoveEditLogEntry : EditLogEntry
        {
            public MoveEditLogEntry()
                : base(FileSystemMutation.Move)
            {
            }

            public MoveEditLogEntry(DateTime date, string path, string targetPath)
                : base(FileSystemMutation.Move, date, path)
            {
                TargetPath = targetPath;
            }

            public string TargetPath { get; private set; }

            public override void Replay(FileSystem fileSystem)
            {
                fileSystem.Move(Path, TargetPath);
            }

            public override void Write(BinaryWriter writer)
            {
                base.Write(writer);
                writer.Write(TargetPath);
            }

            public override void Read(BinaryReader reader)
            {
                base.Read(reader);
                TargetPath = reader.ReadString();
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(EditLog));

        private static readonly Func<EditLogEntry>[] _entryTypeMap = CreateEntryTypeMap();
        private const string _logFileName = "EditLog";
        private const string _newLogFileName = "EditLog.new";
        private readonly object _logFileLock = new object();
        private bool _loggingEnabled = true;
        private readonly string _logFileDirectory;
        private string _logFilePath;
        private ChecksumOutputStream _logFileStream;
        private BinaryWriter _logFileWriter;

        public EditLog(string logFileDirectory)
        {
            if (logFileDirectory == null)
                logFileDirectory = string.Empty;
            if (logFileDirectory.Length > 0)
                System.IO.Directory.CreateDirectory(logFileDirectory);
            _logFileDirectory = logFileDirectory;
            _logFilePath = Path.Combine(logFileDirectory, _logFileName);
        }

        public bool IsUsingNewLogFile
        {
            get { return _logFilePath == Path.Combine(_logFileDirectory, _newLogFileName); }
        }

        public void LoadFileSystemFromLog(bool readOnly, FileSystem fileSystem)
        {
            if (!File.Exists(_logFilePath))
                throw new DfsException("The file system edit log file is missing.");
            _log.Info("Replaying log file.");

            var oldCrc = ChecksumOutputStream.CheckCrc(_logFilePath);

            ReplayLog(fileSystem);
            // A read only file system is used to create a checkpoint, and doesn't need to read the new log file.
            if (!readOnly)
            {
                var newLogFilePath = Path.Combine(_logFileDirectory, _newLogFileName);
                // We don't need to check for this if _logFilePath doesn't exist; if _logFilePath doesn't exist and newLogFilePath does,
                // it means that there's also a temp image file which the name server will catch while restarting.
                if (File.Exists(newLogFilePath))
                {
                    _logFilePath = newLogFilePath;
                    _log.Info("Replaying new log file.");

                    oldCrc = ChecksumOutputStream.CheckCrc(_logFilePath);

                    ReplayLog(fileSystem);
                }
            }
            _log.Info("Replaying log file finished.");
            _loggingEnabled = !readOnly;
            if (!readOnly)
                OpenExistingLogFile(oldCrc);
        }

        public void CreateLog(bool readOnly, FileSystem fileSystem)
        {
            var crc = CreateLogFile(_logFilePath);
            LoadFileSystemFromLog(readOnly, fileSystem);
        }

        public void LogCreateDirectory(string path, DateTime date)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            LogMutation(new CreateDirectoryEditLogEntry(date, path));
        }

        public void LogCreateFile(string path, DateTime date, int blockSize, int replicationFactor, RecordStreamOptions recordOptions)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            LogMutation(new CreateFileEditLogEntry(date, path, blockSize, replicationFactor, recordOptions));
        }

        public void LogAppendBlock(string path, DateTime date, Guid blockId)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            LogMutation(new AppendBlockEditLogEntry(date, path, blockId));
        }

        public void LogCommitBlock(string path, DateTime date, Guid blockId, int size)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            LogMutation(new CommitBlockEditLogEntry(date, path, blockId, size));
        }

        public void LogCommitFile(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            LogMutation(new CommitFileEditLogEntry(DateTime.UtcNow, path));
        }

        public void LogDelete(string path, bool recursive)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            LogMutation(new DeleteEditLogEntry(DateTime.UtcNow, path, recursive));
        }

        public void LogMove(string from, string to)
        {
            if (from == null)
                throw new ArgumentNullException(nameof(from));
            if (to == null)
                throw new ArgumentNullException(nameof(to));

            LogMutation(new MoveEditLogEntry(DateTime.UtcNow, from, to));
        }

        public void SwitchToNewLogFile()
        {
            lock (_logFileLock)
            {
                var newLogFileName = Path.Combine(_logFileDirectory, _newLogFileName);
                if (_logFilePath == newLogFileName)
                    _log.Warn("The edit log was already using the new log file.");
                else
                {
                    _log.Info("Switching to new edit log file.");
                    CloseLogFile();
                    var crc = CreateLogFile(newLogFileName);
                    _logFilePath = newLogFileName;
                    OpenExistingLogFile(crc);
                }
            }
        }

        public void DiscardOldLogFile()
        {
            lock (_logFileLock)
            {
                var newLogFileName = Path.Combine(_logFileDirectory, _newLogFileName);
                var logFileName = Path.Combine(_logFileDirectory, _logFileName);
                if (_logFilePath != newLogFileName)
                    _log.Warn("No old edit log file to discard; no action taken.");
                else
                {
                    _log.Info("Discarding old edit log file, and renaming new log file.");
                    var crc = _logFileStream.Crc;
                    CloseLogFile();

                    File.Delete(logFileName);
                    File.Delete(logFileName + ".crc");
                    File.Move(newLogFileName, logFileName);
                    File.Move(newLogFileName + ".crc", logFileName + ".crc");

                    _logFilePath = logFileName;
                    OpenExistingLogFile(crc);
                }
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            CloseLogFile();
        }

        #endregion

        private static long CreateLogFile(string logFilePath)
        {
            _log.InfoFormat("Initializing new edit log file at '{0}'.", logFilePath);
            using (Stream stream = File.Open(logFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            using (var logFileStream = new ChecksumOutputStream(stream, logFilePath + ".crc", 0L))
            using (var logFileWriter = new BinaryWriter(logFileStream))
            {
                logFileWriter.Write(FileSystem.FileSystemFormatVersion);
                logFileWriter.Flush();
                return logFileStream.Crc;
            }
        }

        private void ReplayLog(FileSystem fileSystem)
        {
            try
            {
                _loggingEnabled = false;
                using (var stream = File.OpenRead(_logFilePath))
                using (var reader = new BinaryReader(stream))
                {
                    var version = reader.ReadInt32();
                    if (version != FileSystem.FileSystemFormatVersion)
                        throw new NotSupportedException("The log file uses an unsupported file system version.");

                    var length = stream.Length;
                    while (stream.Position < length)
                    {
                        var entry = EditLogEntry.ReadEntry(reader);
                        entry.Replay(fileSystem);
                    }
                }
            }
            finally
            {
                _loggingEnabled = true;
            }
        }

        private static void HandleLoggingError(Exception ex)
        {
            _log.Error("Unable to log file system mutation.", ex);
        }

        private void LogMutation(EditLogEntry entry)
        {
            if (_loggingEnabled)
            {
                try
                {
                    lock (_logFileLock)
                    {
                        entry.Write(_logFileWriter);
                        _logFileWriter.Flush();
                    }
                }
                catch (IOException ex)
                {
                    HandleLoggingError(ex);
                    throw;
                }
            }
        }

        private void OpenExistingLogFile(long oldCrc)
        {
            CloseLogFile();
            _log.InfoFormat("Opening existing edit log file '{0}' for writing.", _logFilePath);
            _logFileStream = new ChecksumOutputStream(File.Open(_logFilePath, FileMode.Append, FileAccess.Write, FileShare.None), _logFilePath + ".crc", oldCrc);
            _logFileWriter = new BinaryWriter(_logFileStream);
        }

        private void CloseLogFile()
        {
            if (_logFileWriter != null)
                ((IDisposable)_logFileWriter).Dispose();
            if (_logFileStream != null)
                _logFileStream.Dispose();
            _logFileWriter = null;
            _logFileStream = null;
        }

        private static Func<EditLogEntry>[] CreateEntryTypeMap()
        {
            var result = new Func<EditLogEntry>[(int)FileSystemMutation.MaxValue + 1];

            result[(int)FileSystemMutation.CreateDirectory] = () => new CreateDirectoryEditLogEntry();
            result[(int)FileSystemMutation.CreateFile] = () => new CreateFileEditLogEntry();
            result[(int)FileSystemMutation.AppendBlock] = () => new AppendBlockEditLogEntry();
            result[(int)FileSystemMutation.CommitBlock] = () => new CommitBlockEditLogEntry();
            result[(int)FileSystemMutation.CommitFile] = () => new CommitFileEditLogEntry();
            result[(int)FileSystemMutation.Delete] = () => new DeleteEditLogEntry();
            result[(int)FileSystemMutation.Move] = () => new MoveEditLogEntry();

            return result;
        }
    }
}
