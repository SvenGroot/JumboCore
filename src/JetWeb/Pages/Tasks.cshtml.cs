using System;
using System.Collections.Generic;
using System.Linq;
using JetWeb.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo.Jet;

namespace JetWeb.Pages;

public class TasksModel : PageModel
{
    [BindProperty(Name = "job", SupportsGet = true)]
    public Guid JobId { get; set; }

    [BindProperty(Name = "stage", SupportsGet = true)]
    public string StageId { get; set; }

    [BindProperty(SupportsGet = true)]
    public bool Archived { get; set; }

    [BindProperty(SupportsGet = true)]
    public TaskState? State { get; set; }

    [BindProperty(SupportsGet = true)]
    public int? DataDistance { get; set; }

    public JobStatus Job { get; set; }

    public StageTableModel Stages { get; set; }

    public IEnumerable<TaskStatus> Tasks { get; set; }

    public TaskProgress ComplexProgressTemplate { get; set; }

    public void OnGet()
    {
        var client = new JetClient();
        JobStatus job;

        if (Archived)
        {
            job = client.JobServer.GetArchivedJobStatus(JobId);
        }
        else
        {
            job = client.JobServer.GetJobStatus(JobId);
        }

        if (job == null)
        {
            ViewData["Title"] = "Job not found.";
            return;
        }

        Job = job;
        IEnumerable<TaskStatus> tasks;
        if (string.IsNullOrEmpty(StageId))
        {
            Stages = new StageTableModel
            {
                Job = job,
                Archived = Archived,
                Stages = job.Stages
            };

            tasks = (from s in job.Stages
                     from t in s.Tasks
                     select t).Concat(job.FailedTaskAttempts);
        }
        else
        {
            var stage = (from s in job.Stages
                         where s.StageId == StageId
                         select s).SingleOrDefault();

            if (stage == null)
            {
                ViewData["Title"] = "Stage not found";
                return;
            }

            tasks = stage.Tasks;
            Stages = new StageTableModel
            {
                Job = job,
                Archived = Archived,
                Stages = new[] { stage }
            };

            // If the stage returns complex progress, all tasks will be the same. This isn't
            // done if showing tasks from multiple stages since they might not be the same.
            ComplexProgressTemplate = (from task in tasks
                                       where task.TaskProgress?.AdditionalProgressValues != null
                                       select task.TaskProgress).FirstOrDefault();
        }

        tasks = FilterTasksByState(tasks);
        tasks = FilterTasksByDistance(tasks);

        ViewData["Title"] = $"Job {job.JobName} ({job.JobId}) tasks";
        Tasks = tasks;
    }

    private IEnumerable<TaskStatus> FilterTasksByState(IEnumerable<TaskStatus> tasks)
    {
        if (State.HasValue)
        {
            return from t in tasks
                   where t.State == State
                   select t;
        }

        return tasks;
    }

    private IEnumerable<TaskStatus> FilterTasksByDistance(IEnumerable<TaskStatus> tasks)
    {
        if (DataDistance.HasValue)
        {
            return from t in tasks
                   where t.DataDistance == DataDistance.Value
                   select t;
        }

        return tasks;
    }

}
