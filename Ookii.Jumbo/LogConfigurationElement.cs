using System.Configuration;
using System.IO;
using System.Reflection;
using Ookii.Jumbo.Rpc;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides global logging configuration.
    /// </summary>
    public class LogConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the directory where log files are stored.
        /// </summary>
        /// <value>The directory where log files are stored. This value should end in a directory separator character.</value>
        /// <remarks>
        /// <para>
        ///   If using the run-dfs.sh and run-jet.sh scripts on Unix, the JUMBO_LOG value in jumbo-config.sh should be set to the same value.
        /// </para>
        /// </remarks>
        [ConfigurationProperty("directory", DefaultValue = "./log/", IsRequired = false, IsKey = false)]
        public string Directory
        {
            get { return (string)this["directory"]; }
            set { this["directory"] = value; }
        }

        /// <summary>
        /// Initializes the logger based on the configuration.
        /// </summary>
        public void ConfigureLogger()
        {
            var dir = Directory;

            if (!string.IsNullOrEmpty(dir) && !(dir[dir.Length - 1] == Path.DirectorySeparatorChar || dir[dir.Length - 1] == Path.AltDirectorySeparatorChar))
                dir += Path.DirectorySeparatorChar;
            dir = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), dir);
            log4net.GlobalContext.Properties["LogDirectory"] = dir;
            log4net.GlobalContext.Properties["LocalHostName"] = ServerContext.LocalHostName;
            log4net.Config.XmlConfigurator.Configure(log4net.LogManager.GetRepository(Assembly.GetEntryAssembly()),
                new FileInfo(ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).FilePath));
        }
    }
}
