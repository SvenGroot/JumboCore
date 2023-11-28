// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet.Jobs.Builder;

/// <summary>
/// Provides methods for the <see cref="JobBuilder"/> to transform an operation into stages in a <see cref="JobConfiguration"/>.
/// </summary>
/// <remarks>
/// <note>
///   It's recommended to explicitly implement this interface, as the user of the <see cref="JobBuilder"/> shouldn't use these methods.
/// </note>
/// </remarks>
public interface IJobBuilderOperation : IOperationInput
{
    /// <summary>
    /// Gets the job builder that this operation belongs to.
    /// </summary>
    /// <value>
    /// The job builder.
    /// </value>
    JobBuilder JobBuilder { get; }

    /// <summary>
    /// Gets the <see cref="StageConfiguration"/> for this operation after the <see cref="CreateConfiguration"/> method has been called.
    /// </summary>
    /// <value>
    /// The <see cref="StageConfiguration"/>, or <see langword="null"/> if the <see cref="CreateConfiguration"/> method hasn't been called yet.
    /// </value>
    /// <remarks>
    /// <para>
    ///   Any other <see cref="IJobBuilderOperation"/> that has an input channel connected to this operation can use this
    ///   to specify their input when building the job.
    /// </para>
    /// </remarks>
    StageConfiguration Stage { get; }

    /// <summary>
    /// Creates the settings in the job configuration for this operation.
    /// </summary>
    /// <param name="compiler">The compiler.</param>
    void CreateConfiguration(JobBuilderCompiler compiler);

    /// <summary>
    /// Sets the output for this operation.
    /// </summary>
    /// <param name="output">The output.</param>
    /// <exception cref="InvalidOperationException">This operation already has output.</exception>
    void SetOutput(IOperationOutput output);
}
