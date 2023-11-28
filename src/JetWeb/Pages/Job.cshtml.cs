using System;
using System.Globalization;
using JetWeb.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo.Jet;

namespace JetWeb.Pages;

public class JobModel : PageModel
{
    [BindProperty(Name = "id", SupportsGet = true)]
    public Guid JobId { get; set; }

    [BindProperty(SupportsGet = true)]
    public bool Archived { get; set; }

    [BindProperty(SupportsGet = true)]
    public int Refresh { get; set; } = 5;

    public JobStatus Job { get; set; }

    public JobTableModel TableModel { get; set; }

    public StageTableModel Stages { get; set; }

    public void OnGet()
    {
        var client = new JetClient();
        if (Archived)
        {
            Job = client.JobServer.GetArchivedJobStatus(JobId);
        }
        else
        {
            Job = client.JobServer.GetJobStatus(JobId);
        }

        if (Job == null)
        {
            ViewData["Title"] = "Job not found";
        }
        else
        {
            ViewData["Title"] = $"Job {Job.JobName} ({Job.JobId:B})";
            TableModel = new JobTableModel()
            {
                RunningJobs = true,
                SummaryTable = true,
                Jobs = new[] { Job }
            };

            Stages = new StageTableModel()
            {
                Job = Job,
                Archived = Archived,
                Stages = Job.Stages
            };

            if (!Job.IsFinished && Refresh > 0)
            {
                Response.Headers["Refresh"] = new Microsoft.Extensions.Primitives.StringValues(Refresh.ToString(CultureInfo.InvariantCulture));
            }
        }
    }
}
