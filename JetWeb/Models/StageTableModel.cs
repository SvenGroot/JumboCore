using Ookii.Jumbo.Jet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace JetWeb.Models
{
    public class StageTableModel
    {
        public JobStatus Job { get; set; }

        public bool Archived { get; set; }

        public IEnumerable<StageStatus> Stages { get; set; }
    }
}
