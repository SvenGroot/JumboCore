using System.Collections.Generic;
using Ookii.Jumbo.Jet;

namespace JetWeb.Models;

public class JobTableModel
{
    public bool RunningJobs { get; set; }

    public bool SummaryTable { get; set; }

    public IEnumerable<JobStatus> Jobs { get; set; }
}
