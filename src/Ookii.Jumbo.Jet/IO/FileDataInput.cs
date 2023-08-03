// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.IO
{
    /// <summary>
    /// Provides a stage with input from a file system.
    /// </summary>
    /// <para>
    ///   This type inherits from <see cref="Configurable"/>, but the configuration is only used during task execution.
    ///   If you are creating a job configuration, there is no need to configure this type other than specifying
    ///   the <see cref="DfsConfiguration"/> to the <see cref="FileDataOutput.FileDataOutput(DfsConfiguration,Type,string,int,int,RecordStreamOptions)"/> constructor.
    /// </para>
    public class FileDataInput : Configurable, IDataInput
    {
        /// <summary>
        /// The key of the setting in the stage settings that holds the input path.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   The input path setting is informational only; it is not used by the <see cref="FileDataInput"/> class. Changing this setting does not affect the job.
        /// </para>
        /// <para>
        ///   This setting will only be set if the <see cref="FileDataInput"/> was created from a single file or directory.
        /// </para>
        /// </remarks>
        public const string InputPathSettingKey = "FileDataInput.InputPath";

        /// <summary>
        /// The key of the setting in the stage settings that holds the record reader type.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "TypeSetting")]
        public const string RecordReaderTypeSettingKey = "FileDataInput.RecordReader";

        private readonly List<ITaskInput>? _taskInputs;
        private const double _splitSlack = 1.1;
        private readonly string? _inputPath;
        private Type? _recordReaderType;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDataInput"/> class.
        /// </summary>
        public FileDataInput()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDataInput" /> class.
        /// </summary>
        /// <param name="dfsConfiguration">The DFS configuration.</param>
        /// <param name="recordReaderType">Type of the record reader.</param>
        /// <param name="fileOrDirectory">The input file or directory.</param>
        /// <param name="minSplitSize">The minimum split size.</param>
        /// <param name="maxSplitSize">The maximum split size.</param>
        /// <exception cref="System.ArgumentNullException">fileOrDirectory</exception>
        public FileDataInput(DfsConfiguration dfsConfiguration, Type recordReaderType, JumboFileSystemEntry fileOrDirectory, int minSplitSize = 1, int maxSplitSize = Int32.MaxValue)
            : this(dfsConfiguration, recordReaderType, EnumerateFiles(fileOrDirectory), minSplitSize, maxSplitSize)
        {
            ArgumentNullException.ThrowIfNull(fileOrDirectory);
            _inputPath = fileOrDirectory.FullPath;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDataInput"/> class.
        /// </summary>
        /// <param name="dfsConfiguration">The DFS configuration.</param>
        /// <param name="recordReaderType">Type of the record reader.</param>
        /// <param name="inputFiles">The input files.</param>
        /// <param name="minSplitSize">The minimum split size.</param>
        /// <param name="maxSplitSize">The maximum split size.</param>
        public FileDataInput(DfsConfiguration dfsConfiguration, Type recordReaderType, IEnumerable<JumboFile> inputFiles, int minSplitSize = 1, int maxSplitSize = Int32.MaxValue)
        {
            ArgumentNullException.ThrowIfNull(dfsConfiguration);
            ArgumentNullException.ThrowIfNull(recordReaderType);
            ArgumentNullException.ThrowIfNull(inputFiles);
            if (maxSplitSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxSplitSize));
            if (minSplitSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(minSplitSize));
            if (minSplitSize > maxSplitSize)
                throw new ArgumentException("Minimum split size must be less than or equal to maximum split size.");
            if (recordReaderType.FindGenericBaseType(typeof(RecordReader<>), false) == null)
                throw new ArgumentException("The type is not a record reader.", nameof(recordReaderType));

            var fileSystem = FileSystemClient.Create(dfsConfiguration);
            var localityFileSystem = fileSystem as IFileSystemWithLocality;
            var taskInputs = new List<FileTaskInput>();
            foreach (var file in inputFiles)
            {
                if (file.Size > 0) // Don't create splits for zero-length files
                {
                    var splitSize = Math.Max(minSplitSize, (int)Math.Min(maxSplitSize, file.BlockSize));

                    long offset;
                    for (offset = 0; offset + (splitSize * _splitSlack) < file.Size; offset += splitSize)
                    {
                        taskInputs.Add(new FileTaskInput(file.FullPath, offset, splitSize, GetSplitLocations(localityFileSystem, file, offset)));
                    }

                    taskInputs.Add(new FileTaskInput(file.FullPath, offset, file.Size - offset, GetSplitLocations(localityFileSystem, file, offset)));
                }
            }

            if (taskInputs.Count == 0)
                throw new ArgumentException("The specified input path contains no non-empty splits.", nameof(inputFiles));
            // Sort by descending split size, so biggest splits are done first. Using OrderBy because that does a stable sort.
            _taskInputs = taskInputs.OrderByDescending(input => input.Size).Cast<ITaskInput>().ToList();
            _recordReaderType = recordReaderType;
            DfsConfiguration = dfsConfiguration;
        }

        /// <summary>
        /// Gets the type of the records of this input.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        public Type RecordType
        {
            get { return RecordReader.GetRecordType(_recordReaderType!); }
        }

        /// <summary>
        /// Gets the inputs for each task.
        /// </summary>
        /// <value>
        /// A list of task inputs, or <see langword="null"/> if the job is not being constructed. The returned collection may be read-only.
        /// </value>
        public IList<ITaskInput>? TaskInputs
        {
            get { return _taskInputs == null ? null : _taskInputs.AsReadOnly(); }
        }

        /// <summary>
        /// Creates the record reader for the specified task.
        /// </summary>
        /// <param name="input">The task input.</param>
        /// <returns>
        /// The record reader.
        /// </returns>
        public IRecordReader CreateRecordReader(ITaskInput input)
        {
            ArgumentNullException.ThrowIfNull(input);

            var fileInput = (FileTaskInput)input;
            return (IRecordReader)JetActivator.CreateInstance(_recordReaderType!, DfsConfiguration, JetConfiguration, TaskContext, FileSystemClient.Create(DfsConfiguration!).OpenFile(fileInput.Path), fileInput.Offset, fileInput.Size, TaskContext == null ? false : TaskContext.StageConfiguration.AllowRecordReuse);
        }

        /// <summary>
        /// Notifies the data input that it has been added to a stage.
        /// </summary>
        /// <param name="stage">The stage configuration of the stage.</param>
        public void NotifyAddedToStage(Jobs.StageConfiguration stage)
        {
            ArgumentNullException.ThrowIfNull(stage);

            stage.AddSetting(RecordReaderTypeSettingKey, _recordReaderType!.AssemblyQualifiedName!);
            // This setting is added for informational purposes only (so someone reading the job config can see what the input path was).
            // It is not used at all after setting it.
            if (_inputPath != null)
                stage.AddSetting(InputPathSettingKey, _inputPath);
        }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration" /> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();
            if (TaskContext != null && TaskContext.StageConfiguration.TryGetSetting(RecordReaderTypeSettingKey, out string? typeName))
            {
                _recordReaderType = Type.GetType(typeName, true)!;
            }
        }

        private static IEnumerable<string>? GetSplitLocations(IFileSystemWithLocality? localityFileSystem, JumboFile file, long offset)
        {
            if (localityFileSystem != null)
            {
                return localityFileSystem.GetLocationsForOffset(file, offset);
            }

            return null;
        }

        private static IEnumerable<JumboFile> EnumerateFiles(JumboFileSystemEntry entry)
        {
            ArgumentNullException.ThrowIfNull(entry);

            var directory = entry as JumboDirectory;
            if (directory != null)
            {
                return from child in directory.Children
                       let file = child as JumboFile
                       where file != null
                       select file;
            }
            else
            {
                return new[] { (JumboFile)entry };
            }
        }
    }
}
