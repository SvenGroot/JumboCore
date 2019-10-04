using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Ookii.Jumbo.Jet;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace JetWeb.Api
{
    [Route("api/[controller]")]
    public class JobConfigController : Controller
    {
        // GET: api/<controller>
        [HttpGet]
        public IActionResult Get(Guid id, bool archived)
        {
            JetClient client = new JetClient();
            var config = client.JobServer.GetJobConfigurationFile(id, archived);

            Response.ContentType = "text/xml; charset=utf-8";

            return File(Encoding.UTF8.GetBytes(config), "text/xml; charset=utf-8");
        }
    }
}
