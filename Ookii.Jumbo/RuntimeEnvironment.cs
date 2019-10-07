﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Management;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides information about the runtime environment of the application.
    /// </summary>
    public static class RuntimeEnvironment
    {
        private static string _operatingSystemDescription;
        private static string _processorName;

        /// <summary>
        /// Gets a description of the runtime environment, including the version number.
        /// </summary>
        /// <value>
        /// A string value that describes the runtime environment, including the version number.
        /// </value>
        public static string Description
        {
            get
            {
                return RuntimeInformation.FrameworkDescription;
            }
        }

        /// <summary>
        /// Gets a description of the operating system, including the version number.
        /// </summary>
        /// <value>
        /// A string value that describes the operating system, including the version number.
        /// </value>
        public static string OperatingSystemDescription
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                if( _operatingSystemDescription == null )
                {
                    string description = null;
                    switch( Environment.OSVersion.Platform )
                    {
                    case PlatformID.Win32NT:
                        description = GetOSDescriptionWindows();
                        break;
                    case PlatformID.Unix:
                        description = GetOSDescriptionUnix();
                        break;
                    }

                    if( description == null )
                        _operatingSystemDescription = Environment.OSVersion.ToString();
                    else
                        _operatingSystemDescription = string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0} ({1})", description, Environment.OSVersion);
                }
                return _operatingSystemDescription;
            }
        }

        /// <summary>
        /// Gets the name of the system's processor.
        /// </summary>
        /// <value>
        /// A string value containing the name of the system's processor.
        /// </value>
        public static string ProcessorName
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                if( _processorName == null )
                {
                    switch( Environment.OSVersion.Platform )
                    {
                    case PlatformID.Win32NT:
                        _processorName = GetProcessorNameWindows();
                        break;
                    case PlatformID.Unix:
                        _processorName = GetProcessorNameUnix();
                        break;
                    }
                    if( _processorName == null )
                        _processorName = "unknown";
                }
                return _processorName;
            }
        }

        /// <summary>
        /// Gets the version of Jumbo.
        /// </summary>
        /// <value>
        /// A <see cref="Version"/> value for the Jumbo version.
        /// </value>
        /// <remarks>
        /// The value returned is actually the version of the Ookii.Jumbo assembly.
        /// </remarks>
        public static Version JumboVersion
        {
            get
            {
                AssemblyFileVersionAttribute config = (AssemblyFileVersionAttribute)Attribute.GetCustomAttribute(Assembly.GetExecutingAssembly(), typeof(AssemblyFileVersionAttribute));
                if( config != null )
                    return new Version(config.Version);
                else
                    return Assembly.GetExecutingAssembly().GetName().Version;
            }
        }

        /// <summary>
        /// Gets the Jumbo build configuration, typically the branch name.
        /// </summary>
        /// <value>
        /// A string describing the build configuration.
        /// </value>
        /// <remarks>
        /// This is typically the subversion branch from which Jumbo was built.
        /// </remarks>
        public static string JumboConfiguration
        {
            get
            {
                AssemblyConfigurationAttribute config = (AssemblyConfigurationAttribute)Attribute.GetCustomAttribute(Assembly.GetExecutingAssembly(), typeof(AssemblyConfigurationAttribute));
                if( config != null )
                    return config.Configuration;
                else
                    return null;
            }
        }

        /// <summary>
        /// Writes environemnt information to the specified log.
        /// </summary>
        /// <param name="log">The log to write the information to.</param>
        public static void LogEnvironmentInformation(this log4net.ILog log)
        {
            if( log == null )
                throw new ArgumentNullException(nameof(log));

            if( log.IsInfoEnabled )
            {
                log.InfoFormat("Jumbo Version: {0} ({1})", JumboVersion, JumboConfiguration);
                Assembly entry = Assembly.GetEntryAssembly();
                if( entry != null ) // entry is null when running under nunit.
                    log.InfoFormat("{0} Version: {1}", entry.GetName().Name, entry.GetName().Version);
                log.InfoFormat("   OS Version: {0}", OperatingSystemDescription);
                log.InfoFormat("  CLR Version: {0} ({1} bit runtime)", Description, IntPtr.Size * 8);
                log.InfoFormat("          CPU: {0} CPUs ({1})", Environment.ProcessorCount, ProcessorName);
                using( MemoryStatus status = new MemoryStatus() )
                {
                    log.InfoFormat("       Memory: {0}", status);
                }
            }
        }

        private static string GetOSDescriptionWindows()
        {
            // Use WMI to get the OS name.
            SelectQuery query = new SelectQuery("Win32_OperatingSystem", null, new[] { "Caption" });
            using( ManagementObjectSearcher searcher = new ManagementObjectSearcher(query) )
            {

                foreach( ManagementBaseObject obj in searcher.Get() )
                {
                    return (string)obj["Caption"];
                }
            }

            return null;
        }

        private static string GetOSDescriptionUnix()
        {
            try
            {
                const string lsbReleasePath = "/etc/lsb-release";
                const string descriptionPrefix = "DISTRIB_DESCRIPTION=";
                if (File.Exists(lsbReleasePath))
                {
                    foreach (var line in File.ReadLines(lsbReleasePath))
                    {
                        if (line.StartsWith(descriptionPrefix, StringComparison.Ordinal))
                        {
                            return line.Substring(descriptionPrefix.Length).Trim('"');
                        }
                    }
                }
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }

            return null;
        }

        private static string GetProcessorNameWindows()
        {
            SelectQuery query = new SelectQuery("Win32_Processor", null, new[] { "Name" });
            using( ManagementObjectSearcher searcher = new ManagementObjectSearcher(query) )
            {

                // We assume all CPUs are identical, which should be true in an SMP system.
                foreach( ManagementBaseObject obj in searcher.Get() )
                {
                    return (string)obj["Name"];
                }
            }
            return null;            
        }

        private static string GetProcessorNameUnix()
        {
            if( File.Exists("/proc/cpuinfo") )
            {
                using( StreamReader reader = File.OpenText("/proc/cpuinfo") )
                {
                    string line;
                    while( (line = reader.ReadLine()) != null )
                    {
                        // We assume all CPUs are identical, which should be true in an SMP system.
                        if( line.StartsWith("model name", StringComparison.Ordinal) )
                        {
                            return line.Substring(line.IndexOf(":", StringComparison.Ordinal) + 1).Trim();
                        }
                    }
                }
            }
            return null;
        }
    }

}
