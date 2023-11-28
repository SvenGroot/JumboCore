using System;
using System.IO;
using Ookii.Jumbo.Rpc;

namespace Ookii.Jumbo;

/// <summary>
/// Provides methods for reading the log file. For Jumbo internal use only.
/// </summary>
public static class LogFileHelper
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LogFileHelper));

    /// <summary>
    /// Gets the specified log file stream. For Jumbo internal use only.
    /// </summary>
    /// <param name="serverName">Name of the server whose log file to return..</param>
    /// <param name="kind">The kind of log file.</param>
    /// <param name="maxSize">The maximum size, or zero to return the entire log file..</param>
    /// <returns>A <see cref="Stream"/> for the log file whose position is set so that no more than <paramref name="maxSize"/> bytes will be read if you read until the end,
    /// or <see langword="null"/> if the log file doesn't exist.</returns>
    public static Stream? GetLogFileStream(string serverName, LogFileKind kind, int maxSize)
    {
        ArgumentNullException.ThrowIfNull(serverName);
        if (maxSize < 0)
        {
            throw new ArgumentException("maxSize must be zero or higher positive.", nameof(maxSize));
        }

        string? fileName = null;
        switch (kind)
        {
        case LogFileKind.Log:
            foreach (var appender in log4net.LogManager.GetRepository(System.Reflection.Assembly.GetEntryAssembly()).GetAppenders())
            {
                var fileAppender = appender as log4net.Appender.FileAppender;
                if (fileAppender != null)
                {
                    fileName = fileAppender.File;
                }
            }
            break;
        case LogFileKind.StdOut:
            fileName = Path.Combine(JumboConfiguration.GetConfiguration().Log.Directory, "out-" + serverName + "-" + ServerContext.LocalHostName + ".txt");
            break;
        case LogFileKind.StdErr:
            fileName = Path.Combine(JumboConfiguration.GetConfiguration().Log.Directory, "err-" + serverName + "-" + ServerContext.LocalHostName + ".txt");
            break;
        }

        if (fileName != null && File.Exists(fileName))
        {
            _log.InfoFormat("Retrieving log file {0}", fileName);
            var logStream = System.IO.File.Open(fileName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite);
            try
            {
                if (maxSize > 0 && logStream.Length > maxSize)
                {
                    logStream.Position = logStream.Length - maxSize;
                    while (logStream.ReadByte() != '\n')
                    {
                    }
                }
                return logStream;
            }
            catch
            {
                logStream.Dispose();
                throw;
            }
        }
        else
        {
            if (fileName != null)
            {
                _log.WarnFormat("Log file {0} not found.", fileName);
            }

            return null;
        }
    }

    /// <summary>
    /// Gets the contents of the specified log file. For Jumbo internal use only.
    /// </summary>
    /// <param name="serverName">Name of the server whose log file to return.</param>
    /// <param name="kind">The kind of log file.</param>
    /// <param name="maxSize">The maximum size, or zero to return the entire log file..</param>
    /// <returns>The contents of the log file, or <see langword="null"/> if the log file doesn't exist.</returns>
    public static string? GetLogFileContents(string serverName, LogFileKind kind, int maxSize)
    {
        using (var logStream = GetLogFileStream(serverName, kind, maxSize))
        {
            if (logStream != null)
            {
                using (var reader = new StreamReader(logStream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        return null;
    }
}
