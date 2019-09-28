using Ookii.Jumbo.Jet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace JetWeb.Models
{
    public class JobTableModel
    {
        public bool RunningJobs { get; set; }

        public bool SummaryTable { get; set; }

        public IEnumerable<JobStatus> Jobs { get; set; }
    }
}
