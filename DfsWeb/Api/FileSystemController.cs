using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Ookii.Jumbo.Dfs.FileSystem;

#pragma warning disable CA1822 // Mark members as static

namespace DfsWeb.Api
{
    [Route("api/[controller]")]
    public class FileSystemController : Controller
    {
        // GET: api/<controller>
        [HttpGet]
        public FileSystemEntryInfo Get(string path = "/")
        {
            FileSystemClient client = FileSystemClient.Create();
            return new FileSystemEntryInfo(client.GetDirectoryInfo(path), true);
        }
    }
}

#pragma warning restore CA1822 // Mark members as static
