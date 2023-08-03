// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides a basic implementation of <see cref="IConfigurable"/>.
    /// </summary>
    public abstract class Configurable : IConfigurable
    {
        #region IConfigurable Members

        /// <summary>
        /// Gets or sets the configuration used to access the Distributed File System.
        /// </summary>
        public Ookii.Jumbo.Dfs.DfsConfiguration? DfsConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration used to access the Jet servers.
        /// </summary>
        public JetConfiguration? JetConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration for the task attempt.
        /// </summary>
        public TaskContext? TaskContext { get; set; }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public virtual void NotifyConfigurationChanged()
        {
        }

        #endregion
    }
}
