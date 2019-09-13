// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides object activation services for Jumbo Jet.
    /// </summary>
    public static class JetActivator
    {
        /// <summary>
        /// Creates an instance of the specified type and configures it.
        /// </summary>
        /// <param name="type">The type to instantiate.</param>
        /// <param name="dfsConfiguration">The configuration used to access the distributed file system.</param>
        /// <param name="jetConfiguration">The configuration used to access Jet.</param>
        /// <param name="taskAttemptConfiguration">The configuration for the task attempt.</param>
        /// <param name="args">The arguments to pass to the object's constructor.</param>
        /// <returns>An instance of the specified type with the configuration applied to it.</returns>
        /// <remarks>
        /// <para>
        ///   This function instantiates the type specified in <paramref name="type"/>, then checks if the object
        ///   implements <see cref="IConfigurable"/> and if so, applies the configuration to it.
        /// </para>
        /// </remarks>
        public static object CreateInstance(Type type, DfsConfiguration dfsConfiguration, JetConfiguration jetConfiguration, TaskContext taskAttemptConfiguration, params object[] args)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            object instance = Activator.CreateInstance(type, args);

            ApplyConfiguration(instance, dfsConfiguration, jetConfiguration, taskAttemptConfiguration);

            return instance;
        }

        /// <summary>
        /// Creates an instance of the specified type and configures it.
        /// </summary>
        /// <param name="type">The type to instantiate.</param>
        /// <param name="taskExecution">A <see cref="TaskExecutionUtility"/> instance that contains the configuration to use.</param>
        /// <param name="args">The arguments to pass to the object's constructor.</param>
        /// <returns>An instance of the specified type with the configuration applied to it.</returns>
        /// <remarks>
        /// <para>
        ///   This function instantiates the type specified in <paramref name="type"/>, then checks if the object
        ///   implements <see cref="IConfigurable"/> and if so, applies the configuration to it.
        /// </para>
        /// </remarks>
        public static object CreateInstance(Type type, TaskExecutionUtility taskExecution, params object[] args)
        {
            if( taskExecution == null )
                return CreateInstance(type, (DfsConfiguration)null, null, null, args);
            else
                return CreateInstance(type, taskExecution.FileSystemClient.Configuration, taskExecution.JetClient.Configuration, taskExecution.Context, args);
        }

        /// <summary>
        /// Applies the specified configuration to the specified cobject.
        /// </summary>
        /// <param name="target">The object to configure.</param>
        /// <param name="dfsConfiguration">The configuration used to access the distributed file system.</param>
        /// <param name="jetConfiguration">The configuration used to access Jet.</param>
        /// <param name="taskAttemptConfiguration">The configuration for the task attempt.</param>
        /// <remarks>
        /// <para>
        ///   This function checks if the object implements <see cref="IConfigurable"/> and if so, applies the configuration to it.
        /// </para>
        /// </remarks>
        public static void ApplyConfiguration(object target, DfsConfiguration dfsConfiguration, JetConfiguration jetConfiguration, TaskContext taskAttemptConfiguration)
        {
            if( target == null )
                throw new ArgumentNullException("target");

            IConfigurable configurable = target as IConfigurable;
            if( configurable != null )
            {
                configurable.DfsConfiguration = dfsConfiguration;
                configurable.JetConfiguration = jetConfiguration;
                configurable.TaskContext = taskAttemptConfiguration;
                configurable.NotifyConfigurationChanged();
            }
        }
    }
}
