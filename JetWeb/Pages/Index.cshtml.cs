using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo.Jet;

namespace JetWeb.Pages
{
    public class IndexModel : PageModel
    {
        public JetMetrics Metrics { get; set; }

        public string ErrorMessage { get; set; }

        public JobStatus[] RunningJobs { get; set; }
        public JobStatus[] FinishedJobs { get; set; }
        public JobStatus[] FailedJobs { get; set; }


        public void OnGet()
        {
            try
            {
                JetClient client = new JetClient();
                Metrics = client.JobServer.GetMetrics();
                ViewData["Title"] = Metrics.JobServer.ToString();

                RunningJobs = Metrics.RunningJobs.Select(id => client.JobServer.GetJobStatus(id)).ToArray();
                FinishedJobs = Metrics.FinishedJobs.Select(id => client.JobServer.GetJobStatus(id)).ToArray();
                FailedJobs = Metrics.FailedJobs.Select(id => client.JobServer.GetJobStatus(id)).ToArray();
            }
            catch (SocketException ex)
            {
                ErrorMessage = string.Format("Unable to connect to job server at {0}:{1}. Note: if you changed the job server port in jet.config, you must also modify web.config. Error message: {2}", JetConfiguration.GetConfiguration().JobServer.HostName, JetConfiguration.GetConfiguration().JobServer.Port, ex.Message);
            }
        }
    }
}