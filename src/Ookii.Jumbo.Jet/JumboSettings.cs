// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

#pragma warning disable CA1034 // Nested types should not be visible - nesting done for organization.

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Provides constants for the keys of job and stage settings used by components of Jumbo.
/// </summary>
public static class JumboSettings
{
    /// <summary>
    /// Provides constants for the keys of job and stage settings used by the <see cref="FileOutputChannel"/> and <see cref="FileInputChannel"/>.
    /// </summary>
    public static class FileChannel
    {
        /// <summary>
        /// Provides constants for the keys of settings used by the <see cref="Ookii.Jumbo.Jet.Channels.FileOutputChannel"/> and <see cref="FileInputChannel"/> that can appear in both the job and stage settings.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   These settings are read by the <see cref="FileOutputChannel"/> and <see cref="FileInputChannel"/>. using the <see cref="TaskContext.GetSetting"/> method, which first checks the stage settings
        ///   and then the job settings. This means that if one of these settings appears in the stage settings, it overrides the job settings. If it appears in the job
        ///   settings, this overrides the global default.
        /// </para>
        /// </remarks>
        public static class StageOrJob
        {
            /// <summary>
            /// The key to use in the stage or job settings to override the default write buffer size specified in <see cref="FileChannelConfigurationElement.WriteBufferSize"/>.
            /// Stage settings take precedence over job settings. The setting should have type <see cref="BinarySize"/>.
            /// </summary>
            public const string WriteBufferSize = "FileOutputChannel.WriteBufferSize";

            /// <summary>
            /// The key to use in the job or stage settings to select between a sorting or non-sorting channel.
            /// Stage settings take precedence over job settings. The setting should have type <see cref="FileChannelOutputType"/>.
            /// </summary>
            public const string ChannelOutputType = "FileOutputChannel.OutputType";

            /// <summary>
            /// The key to use in the job or stage settings to override the default spill buffer size specified in <see cref="FileChannelConfigurationElement.SpillBufferSize"/>.
            /// Stage settings take precedence over job settings. The setting should have type <see cref="BinarySize"/>.
            /// </summary>
            public const string SpillBufferSize = "FileOutputChannel.SpillBufferSize";

            /// <summary>
            /// The key to use in the job or stage settings to override the default spill output buffer limit specified in <see cref="FileChannelConfigurationElement.SpillBufferLimit"/>.
            /// Stage settings take precedence over job settings. The setting should have type <see cref="Single"/>.
            /// </summary>
            public const string SpillBufferLimit = "FileOutputChannel.SpillBufferLimit";

            /// <summary>
            /// The key to use in the job or stage settings to override the minimum number of spills needed for the combiner to be run during the merge specified in 
            /// <see cref="FileChannelConfigurationElement.SpillSortMinSpillsForCombineDuringMerge"/>. This value is only used when the output type is <see cref="FileChannelOutputType.SortSpill"/>
            /// and a combiner is specified. Stage settings take precedence over job settings. The setting should have type <see cref="Int32"/>.
            /// </summary>
            public const string SpillSortMinSpillsForCombineDuringMerge = "FileOutputChannel.SpillSortMinSpillsForCombineDuringMerge";

            /// <summary>
            /// The key to use in the job or stage settings to override the global memory storage size setting specified in
            /// <see cref="FileChannelConfigurationElement.MemoryStorageSize"/>. Stage settings take
            /// precedence over job settings. The setting should have type <see cref="BinarySize"/>.
            /// </summary>
            public const string MemoryStorageSize = "FileInputChannel.MemoryStorageSize";

            /// <summary>
            /// The key to use in the job or stage settings to override the global memory storage wait timeout setting specified
            /// in <see cref="FileChannelConfigurationElement.MemoryStorageWaitTimeout"/>. Stage settings take precedence over job
            /// settings. The setting should have type <see cref="Int32"/>.
            /// </summary>
            public const string MemoryStorageWaitTimeout = "FileInputChannel.MemoryStorageWaitTimeout";

        }

        /// <summary>
        /// Provides constants for the keys of settings used by the <see cref="Ookii.Jumbo.Jet.Channels.FileOutputChannel"/> that can appear only in the stage settings.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   These settings are read by the <see cref="FileOutputChannel"/> and <see cref="FileInputChannel"/> using the <see cref="Ookii.Jumbo.Jet.Jobs.StageConfiguration.GetSetting"/> method, so if these
        ///   settings appear in the job settings they are ignored.
        /// </para>
        /// </remarks>
        public static class Stage
        {
            /// <summary>
            /// The key to use in the stage settings to specify the type of a <see cref="IRawComparer{T}"/> or <see cref="IComparer{T}"/> to use when the output type is <see cref="FileChannelOutputType.SortSpill"/>. It's ignored
            /// for other output types. The setting should be an assembly-qualified type name of a type implementing <see cref="IRawComparer{T}"/> or <see cref="IComparer{T}"/>. Using a <see cref="IRawComparer{T}"/> is strongly recommended.
            /// </summary>
            public const string SpillSortComparerType = "FileOutputChannel.SpillSortComparer";

            /// <summary>
            /// The key to use in the stage settings to specify the type of a combiner to use when the output type is <see cref="FileChannelOutputType.SortSpill"/>. It's ignored
            /// for other output types. The setting should be an assembly-qualified type name of a type implementing <see cref="ITask{TInput,TOutput}"/>.
            /// </summary>
            public const string SpillSortCombinerType = "FileOutputChannel.SpillSortCombiner";
        }
    }
}

#pragma warning restore CA1034 // Nested types should not be visible
