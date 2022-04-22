using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo.Jet;

namespace JetWeb.Pages
{
    public class ArchiveModel : PageModel
    {
        public IEnumerable<ArchivedJob> Jobs { get; set; }

        public void OnGet()
        {
            var client = new JetClient();
            Jobs = client.JobServer.GetArchivedJobs().Reverse();
        }
    }
}
