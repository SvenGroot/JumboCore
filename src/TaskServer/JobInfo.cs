﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Ookii.Jumbo.Dfs.FileSystem;

namespace TaskServerApplication;

sealed class JobInfo
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobInfo));

    private readonly Guid _jobId;
    private Dictionary<string, string> _downloadedFiles;

    public JobInfo(Guid jobId)
    {
        _jobId = jobId;
    }

    public string DownloadDfsFile(string dfsPath)
    {
        string localPath;
        if (_downloadedFiles == null)
        {
            _downloadedFiles = new Dictionary<string, string>();
        }
        else
        {
            // Did we already download this file?
            if (_downloadedFiles.TryGetValue(dfsPath, out localPath))
            {
                return localPath;
            }
        }

        var localJobDirectory = TaskServer.Instance.GetJobDirectory(_jobId);
        var downloadDirectory = Path.Combine(localJobDirectory, "dfs");
        Directory.CreateDirectory(downloadDirectory);

        localPath = Path.Combine(downloadDirectory, "file" + _downloadedFiles.Count.ToString(CultureInfo.InvariantCulture));

        _log.DebugFormat("Downloading DFS file '{0}' to local file '{1}'.", dfsPath, localPath);
        var client = FileSystemClient.Create(TaskServer.Instance.DfsConfiguration);
        client.DownloadFile(dfsPath, localPath);

        _downloadedFiles.Add(dfsPath, localPath);
        return localPath;
    }
}
