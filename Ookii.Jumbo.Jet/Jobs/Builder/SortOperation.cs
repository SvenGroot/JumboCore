using System;
using System.Collections.Generic;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents a sorting operation.
    /// </summary>
    public class SortOperation : TwoStepOperation
    {
        private readonly Type _comparerType;
        private readonly Type _combinerType;
        private readonly bool _useSpillSort;

        private SortOperation(JobBuilder builder, IOperationInput input, Type comparerType, Type combinerType, bool useSpillSort)
            : base(builder, input, typeof(SortTask<>), typeof(EmptyTask<>), true)
        {
            if (comparerType != null)
            {
                if (comparerType.IsGenericTypeDefinition)
                    comparerType = comparerType.MakeGenericType(input.RecordType);
                if (comparerType.ContainsGenericParameters)
                    throw new ArgumentException("The comparer type must be a closed constructed generic type.", nameof(comparerType));

                var interfaceType = comparerType.FindGenericInterfaceType(typeof(IComparer<>));
                if (input.RecordType.IsSubclassOf(interfaceType.GetGenericArguments()[0]))
                    throw new ArgumentException("The specified comparer cannot compare the record type.");
                builder.AddAssembly(comparerType.Assembly);
            }

            if (combinerType != null)
            {
                if (!useSpillSort)
                    throw new NotSupportedException("Combiners can only be used with spill sort.");

                if (combinerType.IsGenericTypeDefinition)
                    combinerType = combinerType.MakeGenericType(input.RecordType);
                var info = new TaskTypeInfo(combinerType);
                if (!(info.InputRecordType == input.RecordType && info.OutputRecordType == input.RecordType))
                    throw new ArgumentException("The combiner's input or output record type doesn't match the sort operation's input record type.");

                builder.AddAssembly(combinerType.Assembly);
            }

            _comparerType = comparerType;
            _combinerType = combinerType;
            InputChannel.MultiInputRecordReaderType = typeof(MergeRecordReader<>);
            StageId = "SortStage";
            SecondStepStageId = "MergeStage";
            _useSpillSort = useSpillSort;
        }

        /// <summary>
        /// Gets the type of the combiner.
        /// </summary>
        /// <value>
        /// The type of the combiner.
        /// </value>
        public Type CombinerType
        {
            get { return _combinerType; }
        }

        /// <summary>
        /// Gets the type of the comparer.
        /// </summary>
        /// <value>
        /// The type of the comparer.
        /// </value>
        public Type ComparerType
        {
            get { return _comparerType; }
        }

        /// <summary>
        /// Creates a new instance of the <see cref="SortOperation"/> class for regular sorting.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="input">The input for this operation.</param>
        /// <param name="comparerType">The type of <see cref="IComparer{T}"/> to use for this operation, or <see langword="null"/> to use the default comparer. May be a generic type definition with a single type parameter.</param>
        /// <returns>A <see cref="SortOperation"/> instance.</returns>
        /// <remarks>
        /// If <paramref name="comparerType"/> is a generic type definition with a singe type parameter, it will be constructed using the input's record type.
        /// </remarks>
        public static SortOperation CreateMemorySortOperation(JobBuilder builder, IOperationInput input, Type comparerType)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(input);

            return new SortOperation(builder, input, comparerType, null, false);
        }

        /// <summary>
        /// Creates a new instance of the <see cref="SortOperation" /> class for regular sorting.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="input">The input for this operation.</param>
        /// <param name="comparerType">Type of the comparer to use, or <see langword="null"/> to use the default. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="Ookii.Jumbo.IO.IRawComparer{T}"/> are supported, but using <see cref="Ookii.Jumbo.IO.IRawComparer{T}"/> is strongly recommended.</param>
        /// <param name="combinerType">The type of the combiner task to use, or <see langword="null" /> to use no combiner. May be a generic type definition with a single type parameter.</param>
        /// <returns>
        /// A <see cref="SortOperation" /> instance.
        /// </returns>
        /// <remarks>
        /// If <paramref name="combinerType" /> is a generic type definition with a singe type parameter, it will be constructed using the input's record type.
        /// </remarks>
        public static SortOperation CreateSpillSortOperation(JobBuilder builder, IOperationInput input, Type comparerType, Type combinerType)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(input);

            return new SortOperation(builder, input, comparerType, combinerType, true);
        }

        /// <summary>
        /// Creates the configuration for this stage.
        /// </summary>
        /// <param name="compiler">The <see cref="JobBuilderCompiler"/>.</param>
        /// <returns>
        /// The <see cref="StageConfiguration"/> for the stage.
        /// </returns>
        protected override StageConfiguration CreateConfiguration(JobBuilderCompiler compiler)
        {
            ArgumentNullException.ThrowIfNull(compiler);
            if (_useSpillSort)
            {
                if (InputChannel.ChannelType == null)
                    InputChannel.ChannelType = ChannelType.File; // Spill sort requires file channel, so make sure it doesn't default to anything else

                var input = InputChannel.CreateInput();
                if (input.ChannelType != ChannelType.File)
                    throw new NotSupportedException("Spill sort can only be used on file channels.");

                input.InputStage.AddSetting(JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill);
                if (_comparerType != null)
                    input.InputStage.AddSetting(JumboSettings.FileChannel.Stage.SpillSortComparerType, _comparerType.AssemblyQualifiedName);
                if (_combinerType != null)
                    input.InputStage.AddSetting(JumboSettings.FileChannel.Stage.SpillSortCombinerType, _combinerType.AssemblyQualifiedName);
                return compiler.CreateStage("MergeStage", SecondStepTaskType.TaskType, InputChannel.TaskCount, input, Output, true, InputChannel.Settings);
            }
            else
            {
                var result = base.CreateConfiguration(compiler);
                if (_comparerType != null)
                    FirstStepStage.AddSetting(TaskConstants.SortTaskComparerSettingKey, _comparerType.AssemblyQualifiedName);
                return result;
            }
        }
    }
}
