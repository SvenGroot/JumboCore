using Microsoft.AspNetCore.Mvc;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Api;

[Route("api/[controller]")]
public class FileSystemController : Controller
{
    // GET: api/<controller>
    [HttpGet]
    public FileSystemEntryInfo Get(string path = "/")
    {
        var client = FileSystemClient.Create();
        return new FileSystemEntryInfo(client.GetDirectoryInfo(path), true);
    }
}