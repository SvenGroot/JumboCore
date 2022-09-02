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
            var client = FileSystemClient.Create();
            var stream = client.OpenFile(path);
            return File(stream, "application/octet-stream", client.Path.GetFileName(path));
        }
    }
}
