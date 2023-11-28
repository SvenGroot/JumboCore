// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;

namespace Ookii.Jumbo.Jet.Tasks;

/// <summary>
/// Provides constants for use with the built-in tasks.
/// </summary>
public static class TaskConstants
{
    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the <see cref="IComparer{T}"/>
    /// to use for the <see cref="SortTask{T}"/>. The value of the setting is a <see cref="String"/> that specifies the assembly-qualified type name of the comparer.
    /// The default value is <see langword="null"/>, indicating the <see cref="Comparer{T}.Default"/> will be used.
    /// </summary>
    public const string SortTaskComparerSettingKey = "SortTask.Comparer";

    /// <summary>
    /// The name of the setting in the <see cref="Jobs.StageConfiguration.StageSettings"/> or <see cref="Jobs.JobConfiguration.JobSettings"/> that specifies whether to use
    /// parallel sorting in the <see cref="SortTask{T}"/>. The type of the setting is <see cref="Boolean"/>. The default value is <see langword="true"/>. Stage settings take precedence over
    /// job settings.
    /// </summary>
    public const string SortTaskUseParallelSortSettingKey = "SortTask.UseParallelSort";

    /// <summary>
    /// The name of the setting in the <see cref="Jobs.StageConfiguration.StageSettings"/> that determines the default value assigned to every key/value pair by
    /// the <see cref="GenerateInt32PairTask{TKey}"/>. The type of the setting is <see cref="Int32"/>. This setting can only be
    /// specified in the stage settings, not in the job settings.
    /// </summary>
    public const string GeneratePairTaskDefaultValueKey = "GeneratePairTask.DefaultValue";

    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the <see cref="IEqualityComparer{T}"/>
    /// to use for the <see cref="ReduceTask{TKey,TValue,TOutput}"/>. The value of the setting is a <see cref="String"/> that specifies the assembly-qualified type name of the comparer.
    /// The default value is <see langword="null"/>, indicating the <see cref="EqualityComparer{T}.Default"/> will be used.
    /// </summary>
    public const string ReduceTaskKeyComparerSettingKey = "ReduceTask.KeyComparer";

    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the <see cref="IEqualityComparer{T}"/>
    /// to use for the <see cref="AccumulatorTask{TKey,TValue}"/>. The value of the setting is a <see cref="String"/> that specifies the assembly-qualified type name of the comparer.
    /// The default value is <see langword="null"/>, indicating the <see cref="EqualityComparer{T}.Default"/> will be used.
    /// </summary>
    public const string AccumulatorTaskKeyComparerSettingKey = "AccumulatorTask.KeyComparer";

    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the delegate type
    /// used in a task type that uses a delegate. The value of the setting is a  <see cref="String"/> that
    /// specifies the assembly-qualified type name. This setting is used by the
    /// <see cref="Ookii.Jumbo.Jet.Jobs.Builder.JobBuilder"/> and should not normally be used by your code.
    /// </summary>
    public const string JobBuilderDelegateTypeSettingKey = "JobBuilder.DelegateType";

    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the type that declares
    /// a method to be called using a delegate in a task type that uses a delegate. The value of the setting is a 
    /// <see cref="String"/> that specifies the assembly-qualified type name. This setting is used by the
    /// <see cref="Ookii.Jumbo.Jet.Jobs.Builder.JobBuilder"/> and should not normally be used by your code.
    /// </summary>
    public const string JobBuilderDelegateMethodTypeSettingKey = "JobBuilder.DelegateMethodType";

    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the name of
    /// a method to be called using a delegate in a task type that uses a delegate. The value of the setting is a 
    /// <see cref="String"/> that specifies the method name. This setting is used by the
    /// <see cref="Ookii.Jumbo.Jet.Jobs.Builder.JobBuilder"/> and should not normally be used by your code.
    /// </summary>
    public const string JobBuilderDelegateMethodSettingKey = "JobBuilder.DelegateMethodSetting";

    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the target
    /// for invoking a delegate in a task type that uses a delegate. The value of the setting is a 
    /// <see cref="String"/> that specifies the base64-encoded binary serialization of the object. This setting is used by the
    /// <see cref="Ookii.Jumbo.Jet.Jobs.Builder.JobBuilder"/> and should not normally be used by your code.
    /// </summary>
    public const string JobBuilderDelegateTargetSettingKey = "JobBuilder.DelegateTargetSetting";
}
