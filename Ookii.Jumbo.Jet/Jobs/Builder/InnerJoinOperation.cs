// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents an inner join operation.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This operation joins two inputs by first sorting them (by using a spill sort on the input channel for each input) and
    ///   then joining them using an inner equi-join using the <see cref="InnerJoinRecordReader{TOuter,TInner,TResult}"/>.
    /// </para>
    /// <para>
    ///   In order for the join to be performed correctly, both the outer and inner input must be sorted on the join attribute.
    ///   Please set the outer and inner comparer types accordingly.
    /// </para>
    /// <para>
    ///   If the join uses more than one task, both the outer and inner input must be partitioned on the join attribute. For
    ///   this purpose, the outerComparerType or innerComparerType should also implement <see cref="IEqualityComparer{T}"/>,
    ///   or you should manually set a different <see cref="Channel.PartitionerType"/>.
    /// </para>
    /// </remarks>
    public class InnerJoinOperation : StageOperationBase
    {
        private readonly Channel _outerInputChannel;
        private readonly Channel _innerInputChannel;
        private readonly Type _innerJoinRecordReaderType;

        /// <summary>
        /// Initializes a new instance of the <see cref="InnerJoinOperation"/> class.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="outerInput">The outer input for the join operation.</param>
        /// <param name="innerInput">The inner input for the join operation.</param>
        /// <param name="innerJoinRecordReaderType">Type of the inner join record reader.</param>
        /// <param name="outerComparerType">Type of the comparer used to sort the outer relation. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="IRawComparer{T}"/> are supported, but using <see cref="IRawComparer{T}"/> is strongly recommended.</param>
        /// <param name="innerComparerType">Type of the comparer used to sort the inner relation. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="IRawComparer{T}"/> are supported, but using <see cref="IRawComparer{T}"/> is strongly recommended.</param>
        public InnerJoinOperation(JobBuilder builder, IOperationInput outerInput, IOperationInput innerInput, Type innerJoinRecordReaderType, Type outerComparerType, Type innerComparerType)
            : base(builder, GetEmptyTaskTypeForRecord(innerJoinRecordReaderType))
        {
            if (outerInput == null)
                throw new ArgumentNullException(nameof(outerInput));
            if (innerInput == null)
                throw new ArgumentNullException(nameof(innerInput));
            if (innerJoinRecordReaderType == null)
                throw new ArgumentNullException(nameof(innerJoinRecordReaderType));

            Type baseType = innerJoinRecordReaderType.FindGenericBaseType(typeof(InnerJoinRecordReader<,,>), true);
            Type outerRecordType = baseType.GetGenericArguments()[0];
            Type innerRecordType = baseType.GetGenericArguments()[1];
            InputTypeAttribute[] inputTypeAttributes = (InputTypeAttribute[])Attribute.GetCustomAttributes(innerJoinRecordReaderType, typeof(InputTypeAttribute));
            if (!(inputTypeAttributes.Any(a => a.AcceptedType == outerRecordType) && inputTypeAttributes.Any(a => a.AcceptedType == innerRecordType)))
                throw new ArgumentException("The inner join record reader type does not declare the required InputType attributes.", nameof(innerJoinRecordReaderType));
            if (outerInput.RecordType != outerRecordType)
                throw new ArgumentException("The record type of the outer input does not match the join's outer type.");
            if (innerInput.RecordType != innerRecordType)
                throw new ArgumentException("The record type of the inner input does not match the join's inner type.");

            IJobBuilderOperation outer = CreateExtraStepForDataInput(builder, outerInput, "OuterReadStage");
            IJobBuilderOperation inner = CreateExtraStepForDataInput(builder, innerInput, "InnerReadStage");

            _outerInputChannel = CreateChannel(builder, outer, outerComparerType);
            _innerInputChannel = CreateChannel(builder, inner, innerComparerType);

            _innerJoinRecordReaderType = innerJoinRecordReaderType;
            StageId = "JoinStage";

            builder.AddOperation(this);
            builder.AddAssembly(innerJoinRecordReaderType.Assembly);
        }

        /// <summary>
        /// Creates the configuration for this stage.
        /// </summary>
        /// <param name="compiler">The <see cref="JobBuilderCompiler" />.</param>
        /// <returns>
        /// The <see cref="StageConfiguration" /> for the stage.
        /// </returns>
        protected override StageConfiguration CreateConfiguration(JobBuilderCompiler compiler)
        {
            if (compiler == null)
                throw new ArgumentNullException(nameof(compiler));

            if (_innerInputChannel.TaskCount != _outerInputChannel.TaskCount)
                throw new InvalidOperationException("Outer and inner input channels for a join operation must use the same number of tasks.");
            if (_innerInputChannel.PartitionsPerTask != 1 || _outerInputChannel.PartitionsPerTask != 1)
                throw new InvalidOperationException("Cannot use multiple partitions per task for a join operation.");

            return compiler.CreateStage(StageId, TaskType.TaskType, _outerInputChannel.TaskCount, new[] { _outerInputChannel.CreateInput(), _innerInputChannel.CreateInput() }, Output, new[] { _outerInputChannel.Settings, _innerInputChannel.Settings }, _innerJoinRecordReaderType);
        }

        private static Type GetEmptyTaskTypeForRecord(Type innerJoinRecordReaderType)
        {
            if (innerJoinRecordReaderType == null)
                throw new ArgumentNullException(nameof(innerJoinRecordReaderType));
            return typeof(EmptyTask<>).MakeGenericType(RecordReader.GetRecordType(innerJoinRecordReaderType));
        }

        private static IJobBuilderOperation CreateExtraStepForDataInput(JobBuilder builder, IOperationInput input, string stageId)
        {
            IJobBuilderOperation operation = input as IJobBuilderOperation;
            if (operation == null)
                return new StageOperation(builder, input, typeof(EmptyTask<>)) { StageId = stageId };

            return operation;
        }

        private static Type MakeComparerType(Type comparerType, Type recordType)
        {
            if (comparerType != null)
            {
                if (comparerType.IsGenericTypeDefinition)
                    comparerType = comparerType.MakeGenericType(recordType);
                Type interfaceType = comparerType.FindGenericInterfaceType(typeof(IComparer<>), true);
                if (interfaceType.GetGenericArguments()[0] != recordType)
                    throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Comparer {0} is not valid for type {1}.", comparerType, recordType));
            }

            return comparerType;
        }

        private Channel CreateChannel(JobBuilder builder, IJobBuilderOperation input, Type comparerType)
        {
            Channel channel = new Channel(input, this);
            channel.ChannelType = ChannelType.File;
            channel.MultiInputRecordReaderType = typeof(MergeRecordReader<>);
            channel.Settings.AddSetting(JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill);
            if (comparerType != null)
            {
                comparerType = MakeComparerType(comparerType, input.RecordType);
                channel.Settings.Add(JumboSettings.FileChannel.Stage.SpillSortComparerType, comparerType.AssemblyQualifiedName);
                if (comparerType.GetInterfaces().Contains(typeof(IEqualityComparer<>).MakeGenericType(input.RecordType)))
                    channel.Settings.Add(PartitionerConstants.EqualityComparerSetting, comparerType.AssemblyQualifiedName);
                builder.AddAssembly(comparerType.Assembly);
            }

            return channel;
        }
    }
}
