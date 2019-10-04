using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using ICSharpCode.SharpZipLib.Zip;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace JetWeb.Api
{
    [Route("api/[controller]")]
    public class JobInfoController : Controller
    {
        private readonly string _basePath;

        public JobInfoController(IWebHostEnvironment env)
        {
            _basePath = env.WebRootPath;
        }

        // GET: api/<controller>
        [HttpGet]
        public async Task<IActionResult> Get(Guid id, bool archived, CancellationToken token)
        {
            JetClient client = new JetClient();
            FileSystemClient fileSystemClient = FileSystemClient.Create();
            JobStatus job;
            if (archived)
                job = client.JobServer.GetArchivedJobStatus(id);
            else
                job = client.JobServer.GetJobStatus(id);

            string fileName = string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0} ({1}).zip", job.JobName, job.JobId);
            MemoryStream memoryStream = new MemoryStream();
            ZipOutputStream stream = new ZipOutputStream(memoryStream);
            stream.SetLevel(9);

            stream.PutNextEntry(new ZipEntry("config.xml"));
            if (archived)
            {
                byte[] buffer = System.Text.Encoding.UTF8.GetBytes(client.JobServer.GetJobConfigurationFile(id, true));
                await stream.WriteAsync(buffer, 0, buffer.Length, token);
            }
            else
            {
                string configFilePath = fileSystemClient.Path.Combine(fileSystemClient.Path.Combine(client.Configuration.JobServer.JetDfsPath, "job_" + job.JobId.ToString("B")), Job.JobConfigFileName);
                using (Stream configStream = fileSystemClient.OpenFile(configFilePath))
                {
                    await configStream.CopyToAsync(stream, token);
                }
            }

            stream.PutNextEntry(new ZipEntry("config.xslt"));
            using (FileStream configXsltStream = System.IO.File.OpenRead(Path.Combine(_basePath, "Api", "config.xslt")))
            {
                await configXsltStream.CopyToAsync(stream, token);
            }

            stream.PutNextEntry(new ZipEntry("summary.xml"));
            using (MemoryStream xmlStream = new MemoryStream())
            {
                using (XmlWriter writer = XmlWriter.Create(xmlStream, new XmlWriterSettings() { Async = true }))
                {
                    await job.ToXml().SaveAsync(writer, token);
                }
                xmlStream.Position = 0;
                await xmlStream.CopyToAsync(stream, token);
            }

            stream.PutNextEntry(new ZipEntry("summary.xslt"));
            using (FileStream configXsltStream = System.IO.File.OpenRead(Path.Combine(_basePath, "Api", "summary.xslt")))
            {
                await configXsltStream.CopyToAsync(stream, token);
            }

            var servers = (from stage in job.Stages
                           from task in stage.Tasks
                           select task.TaskServer).Distinct();


            var semaphore = new SemaphoreSlim(1, 1);
            var downloadTasks = servers.Select(server => DownloadLogFiles(semaphore, server, stream, id, token));

            await Task.WhenAll(downloadTasks);
            stream.Finish();
            memoryStream.Position = 0;
            return File(memoryStream, "application/zip", fileName);
        }

        private async Task DownloadLogFiles(SemaphoreSlim semaphore, ServerAddress server, ZipOutputStream zipStream, Guid jobId, CancellationToken token)
        {
            ITaskServerClientProtocol taskServer = JetClient.CreateTaskServerClient(server);
            byte[] logBytes = taskServer.GetCompressedTaskLogFiles(jobId);
            await semaphore.WaitAsync(token);
            try
            {
                zipStream.PutNextEntry(new ZipEntry(string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}-{1}.zip", server.HostName, server.Port)));
                await zipStream.WriteAsync(logBytes, 0, logBytes.Length, token);
            }
            finally
            {
                semaphore.Release();
            }
        }

    }
}
