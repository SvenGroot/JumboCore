// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.Reflection;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Provides methods for constructing Jumbo Jet jobs as a sequence of operations.
    /// </summary>
    public sealed partial class JobBuilder
    {
        private static HashSet<string> _dependencyAssemblies = GetDependencies();

        private static HashSet<string> GetDependencies()
        {
            HashSet<string> result = new HashSet<string>();
            GetDependencies(result, Assembly.GetExecutingAssembly());
            return result;
        }

        private static void GetDependencies(HashSet<string> dependencies, Assembly assembly)
        {
            if (!dependencies.Add(assembly.FullName))
            {
                return;
            }

            foreach (var name in assembly.GetReferencedAssemblies())
            {
                GetDependencies(dependencies, Assembly.Load(name));
            }
        }

        private readonly List<IJobBuilderOperation> _operations = new List<IJobBuilderOperation>();
        private readonly HashSet<Assembly> _assemblies = new HashSet<Assembly>();
        private readonly FileSystemClient _fileSystemClient;
        private readonly JetClient _jetClient;
        private readonly DynamicTaskBuilder _taskBuilder = new DynamicTaskBuilder();
        private SettingsDictionary _settings;

        /// <summary>
        /// Initializes a new instance of the <see cref="JobBuilder"/> class.
        /// </summary>
        /// <param name="fileSystemClient">The DFS client. May be <see langword="null"/>.</param>
        /// <param name="jetClient">The Jet client. May be <see langword="null"/>.</param>
        public JobBuilder(FileSystemClient fileSystemClient, JetClient jetClient)
        {
            _fileSystemClient = fileSystemClient ?? FileSystemClient.Create();
            _jetClient = jetClient ?? new JetClient();
        }

        /// <summary>
        /// Gets the <see cref="DynamicTaskBuilder"/> used to create task classes from methods.
        /// </summary>
        /// <value>
        /// The task builder.
        /// </value>
        /// <remarks>
        /// You only need to use this property if you are extending the <see cref="JobBuilder"/>.
        /// </remarks>
        public DynamicTaskBuilder TaskBuilder
        {
            get { return _taskBuilder; }
        }

        /// <summary>
        /// Gets the full paths of all the non-GAC and non-Jumbo assemblies used by this job.
        /// </summary>
        /// <value>
        /// The assembly paths.
        /// </value>
        public IEnumerable<string> AssemblyLocations
        {
            get
            {
                var files = from a in _assemblies
                            select a.Location;
                if( _taskBuilder.IsDynamicAssemblyCreated )
                {
                    files = files.Concat(new[] { _taskBuilder.DynamicAssemblyPath });
                }
                return files;
            }
        }

        /// <summary>
        /// Reads input records from the specified path on the DFS.
        /// </summary>
        /// <param name="path">The path of a directory or file on the DFS.</param>
        /// <param name="recordReaderType">Type of the record reader.</param>
        /// <returns>A <see cref="FileInput"/> instance representing this input.</returns>
        public FileInput Read(string path, Type recordReaderType)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            FileInput input = new FileInput(path, recordReaderType);
            AddAssembly(recordReaderType.Assembly);
            return input;
        }

        /// <summary>
        /// Gets or sets the descriptive name of the job.
        /// </summary>
        /// <value>
        /// The name of the job.
        /// </value>
        public string JobName { get; set; }

        /// <summary>
        /// Gets the settings for the job.
        /// </summary>
        /// <value>
        /// A <see cref="SettingsDictionary"/> containing the settings.
        /// </value>
        public SettingsDictionary Settings
        {
            get { return _settings ?? (_settings = new SettingsDictionary()); }
        }

        /// <summary>
        /// Writes the result of the specified operation to the DFS.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="path">The path of a directory on the DFS.</param>
        /// <param name="recordWriterType">Type of the record writer. This may be a generic type definition.</param>
        /// <returns>A <see cref="FileOutput"/> instance representing the output.</returns>
        /// <remarks>
        /// <para>
        ///   If <paramref name="recordWriterType"/> is a generic type definition, it will be constructed using the output record type of the operation.
        /// </para>
        /// </remarks>
        public FileOutput Write(IJobBuilderOperation operation, string path, Type recordWriterType)
        {
            if( operation == null )
                throw new ArgumentNullException("operation");
            if( path == null )
                throw new ArgumentNullException("path");
            if( recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");

            if( recordWriterType.IsGenericTypeDefinition )
                recordWriterType = recordWriterType.MakeGenericType(operation.RecordType);

            FileOutput output = new FileOutput(path, recordWriterType);
            operation.SetOutput(output);

            AddAssembly(recordWriterType.Assembly);

            return output;
        }

        /// <summary>
        /// Processes the specified input using the specified task.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="taskType">Type of the task.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        public StageOperation Process(IOperationInput input, Type taskType)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            CheckIfInputBelongsToJobBuilder(input);
            return new StageOperation(this, input, taskType);
        }

        /// <summary>
        /// Processes the specified input using the specified delegate.
        /// </summary>
        /// <typeparam name="TInput">The type of the input.</typeparam>
        /// <typeparam name="TOutput">The type of the output.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="processor">The processing function to use to create the task.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class implementing <see cref="ITask{TInput, TOutput}"/> which calls the target method of the <paramref name="processor"/> delegate
        ///   from the <see cref="ITask{TInput, TOutput}.Run"/> method.
        /// </para>
        /// <note>
        ///   The task method will be called from a completely different process than the one that is using <see cref="JobBuilder"/>, so it should not really
        ///   on any external state.
        /// </note>
        /// <para>
        ///   If <paramref name="recordReuse"/> is <see cref="RecordReuseMode.Default"/> and the target method has the <see cref="AllowRecordReuseAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class. If the target method has the <see cref="ProcessAllInputPartitionsAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class.
        /// </para>
        /// <para>
        ///   If the target method is a <see langword="public" /> <see langword="static"/> method, it will be called directly by the generated task class. Otherwise, the supplied
        ///   delegate will be serialized to the task settings and used to call the method. If the target method is an instance method, the instance it belongs to will be
        ///   serialized as well (this class must have the <see cref="SerializableAttribute"/> attribute).
        /// </para>
        /// </remarks>
        public StageOperation Process<TInput, TOutput>(IOperationInput input, Action<RecordReader<TInput>, RecordWriter<TOutput>, TaskContext> processor, RecordReuseMode recordReuse = RecordReuseMode.Default)
        {
            return ProcessCore<TInput, TOutput>(input, processor, recordReuse);
        }

        /// <summary>
        /// Processes the specified input using the specified delegate.
        /// </summary>
        /// <typeparam name="TInput">The type of the input.</typeparam>
        /// <typeparam name="TOutput">The type of the output.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="processor">The processing function to use to create the task.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class implementing <see cref="ITask{TInput, TOutput}"/> which calls the target method of the <paramref name="processor"/> delegate
        ///   from the <see cref="ITask{TInput, TOutput}.Run"/> method.
        /// </para>
        /// <note>
        ///   The task method will be called from a completely different process than the one that is using <see cref="JobBuilder"/>, so it should not really
        ///   on any external state.
        /// </note>
        /// <para>
        ///   If <paramref name="recordReuse"/> is <see cref="RecordReuseMode.Default"/> and the target method has the <see cref="AllowRecordReuseAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class. If the target method has the <see cref="ProcessAllInputPartitionsAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class.
        /// </para>
        /// <para>
        ///   If the target method is a <see langword="public" /> <see langword="static"/> method, it will be called directly by the generated task class. Otherwise, the supplied
        ///   delegate will be serialized to the task settings and used to call the method. If the target method is an instance method, the instance it belongs to will be
        ///   serialized as well (this class must have the <see cref="SerializableAttribute"/> attribute).
        /// </para>
        /// </remarks>
        public StageOperation Process<TInput, TOutput>(IOperationInput input, Action<RecordReader<TInput>, RecordWriter<TOutput>> processor, RecordReuseMode recordReuse = RecordReuseMode.Default)
        {
            return ProcessCore<TInput, TOutput>(input, processor, recordReuse);
        }

        /// <summary>
        /// Creates the job configuration.
        /// </summary>
        /// <returns>The job configuration.</returns>
        public JobConfiguration CreateJob()
        {
            JobBuilderCompiler compiler = new JobBuilderCompiler(_assemblies, _fileSystemClient, _jetClient);
            foreach( var operation in _operations )
                operation.CreateConfiguration(compiler);
            compiler.Job.JobName = JobName;
            if( _taskBuilder.IsDynamicAssemblyCreated )
            {
                _taskBuilder.SaveAssembly();
                compiler.Job.AssemblyFileNames.Add(_taskBuilder.DynamicAssemblyFileName);
            }

            compiler.Job.AddSettings(_settings);
            return compiler.Job;
        }

        /// <summary>
        /// Adds the specified operation.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <remarks>
        /// Normally, you should only use this method if you are extending the <see cref="JobBuilder"/>.
        /// </remarks>
        public void AddOperation(IJobBuilderOperation operation)
        {
            if( operation == null )
                throw new ArgumentNullException("operation");
            if( operation.JobBuilder != this )
                throw new ArgumentException("The specified operation doesn't belong to this job builder.", "operation");
            _operations.Add(operation);
        }

        /// <summary>
        /// Adds an assembly and all its referenced assemblies to the list of required assemblies for this job.
        /// </summary>
        /// <param name="assembly">The assembly.</param>
        /// <remarks>
        /// <para>
        ///   You only need to call this method if you're extending the <see cref="JobBuilder"/>.
        /// </para>
        /// <para>
        ///   GAC assemblies and assemblies belonging to Jumbo are automatically excluded.
        /// </para>
        /// </remarks>
        public void AddAssembly(Assembly assembly)
        {
            if( assembly == null )
                throw new ArgumentNullException("assembly");

            if( !(assembly.GlobalAssemblyCache || _dependencyAssemblies.Contains(assembly.FullName)) &&
                (_taskBuilder.IsDynamicAssembly(assembly) || _assemblies.Add(assembly)) )
            {
                foreach( AssemblyName reference in assembly.GetReferencedAssemblies() )
                {
                    AddAssembly(Assembly.Load(reference));
                }
            }
        }

        /// <summary>
        /// Checks if the specified input belongs to this job builder.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <exception cref="ArgumentException">
        ///   The input is an operation that does not belong to this job builder.
        /// </exception>
        public void CheckIfInputBelongsToJobBuilder(IOperationInput input)
        {
            IJobBuilderOperation operation = input as IJobBuilderOperation;
            if( !(operation == null || operation.JobBuilder == this) )
                throw new ArgumentException("The specified input doesn't belong to this job builder.", "input");
        }

        private StageOperation ProcessCore<TInput, TOutput>(IOperationInput input, Delegate processor, RecordReuseMode recordReuse)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( processor == null )
                throw new ArgumentNullException("processor");
            CheckIfInputBelongsToJobBuilder(input);
            Type taskType = _taskBuilder.CreateDynamicTask(typeof(ITask<TInput, TOutput>).GetMethod("Run"), processor, 0, recordReuse);
            StageOperation result = new StageOperation(this, input, taskType);
            AddAssemblyAndSerializeDelegateIfNeeded(processor, result);
            return result;
        }

        private void AddAssemblyAndSerializeDelegateIfNeeded(Delegate processor, StageOperation operation)
        {
            if( !DynamicTaskBuilder.CanCallTargetMethodDirectly(processor) )
                DynamicTaskBuilder.SerializeDelegate(operation.Settings, processor);
            AddAssembly(processor.Method.DeclaringType.Assembly);
        }
    }
}
