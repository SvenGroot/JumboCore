using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Api
{
    [Route("api/[controller]")]
    [ApiController]
    public class DownloadController : ControllerBase
    {
        // GET: api/<controller>
        [HttpGet]
        public IActionResult Download(string path)
        {
            FileSystemClient client = FileSystemClient.Create();
            Stream stream = client.OpenFile(path);
            return File(stream, "application/octet-stream", client.Path.GetFileName(path));
        }
    }
}
