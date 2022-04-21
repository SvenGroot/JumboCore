// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using DataServerApplication;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;
using NameServerApplication;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs
{
    class TestDfsCluster
    {
        //private AppDomain _clusterDomain;
        private ClusterRunner _clusterRunner;

        public const int NameServerPort = 10000;
        public const int FirstDataServerPort = 10001;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TestDfsCluster));

        private class DataServerInfo
        {
            public Thread Thread { get; set; }
            public DataServer Server { get; set; }
        }

        private class ClusterRunner : MarshalByRefObject
        {
            private int _nextDataServerPort = FirstDataServerPort;
            private string _path;
            List<DataServerInfo> _dataServers = new List<DataServerInfo>();

            public void Run(string imagePath, int replicationFactor, int dataServers, int? blockSize, bool format)
            {
                Utilities.ConfigureLogging();
                DfsConfiguration config = new DfsConfiguration();
                config.FileSystem.Url = new Uri("jdfs://localhost:" + NameServerPort);
                config.NameServer.ReplicationFactor = replicationFactor;
                config.NameServer.ImageDirectory = imagePath;
                if (blockSize != null)
                    config.NameServer.BlockSize = blockSize.Value;

                if (format)
                    FileSystem.Format(config);

                NameServer.Run(new JumboConfiguration(), config);
                _path = imagePath;
                StartDataServers(dataServers);
            }

            public void StartDataServers(int dataServers)
            {
                if (dataServers > 0)
                {
                    for (int x = 0; x < dataServers; ++x, ++_nextDataServerPort)
                    {
                        string blocksPath = System.IO.Path.Combine(_path, "blocks" + x.ToString(System.Globalization.CultureInfo.InvariantCulture));
                        System.IO.Directory.CreateDirectory(blocksPath);
                        RunDataServer(blocksPath, _nextDataServerPort);
                    }
                }
            }

            public void Shutdown()
            {
                _log.Info("Shutting down cluster.");
                lock (_dataServers)
                {
                    foreach (var info in _dataServers)
                    {
                        _log.Info("Shutting down data server.");
                        info.Server.Abort();
                        info.Thread.Join();
                    }
                    _dataServers.Clear();
                }

                _log.Info("Shutting down name server.");
                NameServer.Shutdown();
            }

            public ServerAddress ShutdownDataServer(int index)
            {
                _log.Info("Shutting down data server.");
                lock (_dataServers)
                {
                    var info = _dataServers[index];
                    ServerAddress address = info.Server.LocalAddress;
                    info.Server.Abort();
                    info.Thread.Join();
                    _dataServers.RemoveAt(index);
                    return address;
                }
            }

            private void RunDataServer(string path, int port)
            {
                DfsConfiguration config = new DfsConfiguration();
                config.FileSystem.Url = new Uri("jdfs://localhost:" + NameServerPort);
                config.DataServer.Port = port;
                config.DataServer.BlockStorageDirectory = path;
                Thread t = new Thread(RunDataServerThread);
                t.Name = "DataServer";
                t.IsBackground = true;
                t.Start(config);
            }

            private void RunDataServerThread(object parameter)
            {
                DfsConfiguration config = (DfsConfiguration)parameter;
                DataServer server = new DataServer(config);
                lock (_dataServers)
                {
                    _dataServers.Add(new DataServerInfo() { Thread = Thread.CurrentThread, Server = server });
                }
                server.Run();
            }
        }

        private readonly DfsClient _client;

        public TestDfsCluster(int dataServers, int replicationFactor)
            : this(dataServers, replicationFactor, null)
        {
        }

        public TestDfsCluster(int dataServers, int replicationFactor, int? blockSize)
            : this(dataServers, replicationFactor, blockSize, true)
        {
        }

        public TestDfsCluster(int dataServers, int replicationFactor, int? blockSize, bool eraseExistingData)
        {
            string path = Utilities.TestOutputPath;
            if (eraseExistingData && System.IO.Directory.Exists(path))
                System.IO.Directory.Delete(path, true);
            System.IO.Directory.CreateDirectory(path);

            _clusterRunner = new ClusterRunner();
            _clusterRunner.Run(path, replicationFactor, dataServers, blockSize, eraseExistingData);
            _client = (DfsClient)FileSystemClient.Create(CreateClientConfig());
        }

        public DfsClient Client
        {
            get { return _client; }
        }

        public void Shutdown()
        {
            Thread.Sleep(1000);
            _clusterRunner.Shutdown();
            _clusterRunner = null;
            Trace.WriteLine("Shutting down now.");
            Trace.Flush();
            //AppDomain.Unload(_clusterDomain);
            //_clusterDomain = null;
        }

        public ServerAddress ShutdownDataServer(int index)
        {
            return _clusterRunner.ShutdownDataServer(index);
        }



        public void StartDataServers(int dataServers)
        {
            _clusterRunner.StartDataServers(dataServers);
        }

        public static DfsConfiguration CreateClientConfig()
        {
            DfsConfiguration config = new DfsConfiguration();
            config.FileSystem.Url = new Uri("jdfs://localhost:" + NameServerPort);
            return config;
        }

    }
}
