using System.Collections.Generic;
using Ookii.Jumbo.Jet;

namespace JetWeb.Models;

public class StageTableModel
{
    public JobStatus Job { get; set; }

    public bool Archived { get; set; }

    public IEnumerable<StageStatus> Stages { get; set; }
}
