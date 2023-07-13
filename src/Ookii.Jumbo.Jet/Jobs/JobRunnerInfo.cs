// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides information about a job runner.
    /// </summary>
    public sealed class JobRunnerInfo
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobRunnerInfo));

        private readonly Type _jobRunnerType;

        /// <summary>
        /// Initializes a new instance of the <see cref="JobRunnerInfo"/> class.
        /// </summary>
        /// <param name="type">The type of the job runner.</param>
        public JobRunnerInfo(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            if (!type.GetInterfaces().Contains(typeof(IJobRunner)))
                throw new ArgumentException("Specified type is not a job runner.", nameof(type));

            _jobRunnerType = type;
        }

        /// <summary>
        /// Gets the name of the job runner.
        /// </summary>
        public string Name
        {
            get { return _jobRunnerType.Name; }
        }

        /// <summary>
        /// Gets a description of the job runner.
        /// </summary>
        public string Description
        {
            get
            {
                var description = (DescriptionAttribute)Attribute.GetCustomAttribute(_jobRunnerType, typeof(DescriptionAttribute));
                return description == null ? "" : description.Description;
            }
        }

        /// <summary>
        /// Gets all the job runners defined in the specified assembly.
        /// </summary>
        /// <param name="assembly">The assembly to check for job runners.</param>
        /// <returns>An array holding the job runners in the assembly.</returns>
        public static JobRunnerInfo[] GetJobRunners(Assembly assembly)
        {
            ArgumentNullException.ThrowIfNull(assembly);

            var types = assembly.GetTypes();
            return (from type in types
                    where type.IsPublic && type.IsClass && !type.IsAbstract && type.GetInterfaces().Contains(typeof(IJobRunner))
                    orderby type.Name
                    select new JobRunnerInfo(type)).ToArray();
        }

        /// <summary>
        /// Gets the specified job runner from the specified assembly.
        /// </summary>
        /// <param name="assembly">The assembly to check for the job runner.</param>
        /// <param name="name">The name of the job runner.</param>
        /// <returns>The <see cref="JobRunnerInfo"/> for the specified job runner, or <see langword="null" /> if it was not found.</returns>
        public static JobRunnerInfo GetJobRunner(Assembly assembly, string name)
        {
            ArgumentNullException.ThrowIfNull(assembly);
            ArgumentNullException.ThrowIfNull(name);

            var types = assembly.GetTypes();
            return (from type in types
                    where type.IsPublic && type.IsClass && !type.IsAbstract && type.GetInterfaces().Contains(typeof(IJobRunner)) && string.Equals(type.Name, name, StringComparison.OrdinalIgnoreCase)
                    select new JobRunnerInfo(type)).SingleOrDefault();
        }

        /// <summary>
        /// Creates an instance of the job runner.
        /// </summary>
        /// <param name="dfsConfiguration">The Jumbo DFS configuration for the job.</param>
        /// <param name="jetConfiguration">The Jumbo Jet configuration for the job.</param>
        /// <param name="args">The arguments for the job.</param>
        /// <param name="parseOptions">The options that control parsing.</param>
        /// <returns>An instance of the job runner, or <see langword="null" /> if the incorrect number of arguments was specified.</returns>
        public IJobRunner CreateInstance(DfsConfiguration dfsConfiguration, JetConfiguration jetConfiguration, ReadOnlyMemory<string> args, ParseOptions parseOptions)
        {
            ArgumentNullException.ThrowIfNull(dfsConfiguration);
            ArgumentNullException.ThrowIfNull(jetConfiguration);

            var parser = new CommandLineParser(_jobRunnerType, parseOptions);
            IJobRunner jobRunner = null;
            try
            {
                jobRunner = (IJobRunner)parser.Parse(args);
            }
            catch (CommandLineArgumentException ex)
            {
                Console.Error.WriteLine(ex.Message);
            }

            if (parser.HelpRequested)
            {
                parser.WriteUsage();
            }

            if (jobRunner != null)
            {
                var logMessage = new StringBuilder("Created job runner for job ");
                logMessage.Append(Name);

                if (_log.IsInfoEnabled)
                {
                    foreach (var argument in parser.Arguments)
                    {
                        if (argument.HasValue)
                        {
                            logMessage.Append(", ");
                            logMessage.Append(argument.MemberName);
                            logMessage.Append(" = ");
                            if (argument.Kind == ArgumentKind.Dictionary)
                                AppendDictionayArgument(logMessage, (IDictionary)argument.Value);
                            else if (argument.Kind == ArgumentKind.MultiValue)
                                AppendMultiValueArgument(logMessage, (IEnumerable)argument.Value);
                            else
                                logMessage.Append(argument.Value);
                        }
                    }

                    _log.Info(logMessage.ToString());
                }

                JetActivator.ApplyConfiguration(jobRunner, dfsConfiguration, jetConfiguration, null);
            }

            return jobRunner;
        }

        /// <summary>
        /// Creates an instance of the job runner with the configuration from the app.config file.
        /// </summary>
        /// <param name="args">The arguments for the job.</param>
        /// <param name="parseOptions">The options that control parsing.</param>
        /// <returns>An instance of the job runner, or <see langword="null" /> if the incorrect number of arguments was specified.</returns>
        public IJobRunner CreateInstance(ReadOnlyMemory<string> args, ParseOptions parseOptions = null)
        {
            return CreateInstance(DfsConfiguration.GetConfiguration(), JetConfiguration.GetConfiguration(), args, parseOptions);
        }

        private static void AppendDictionayArgument(StringBuilder logMessage, IDictionary values)
        {
            logMessage.Append("{ ");
            var first = true;
            foreach (DictionaryEntry entry in values)
            {
                if (first)
                    first = false;
                else
                    logMessage.Append(", ");
                logMessage.AppendFormat(CultureInfo.InvariantCulture, "{0}={1}", entry.Key, entry.Value);
            }
            logMessage.Append(" }");
        }

        private static void AppendMultiValueArgument(StringBuilder logMessage, IEnumerable values)
        {
            logMessage.Append("{ ");
            logMessage.Append(string.Join(", ", values.Cast<object>()));
            logMessage.Append(" }");
        }
    }
}
