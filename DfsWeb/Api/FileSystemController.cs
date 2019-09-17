using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Ookii.Jumbo.Dfs.FileSystem;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

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
