// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;

namespace JetShell.Commands
{
    [ShellCommand("abort"), Description("Aborts a running job.")]
    class AbortJobCommand : JetShellCommand
    {
        private readonly Guid _jobId;

        public AbortJobCommand([Description("The job ID of the job to abort."), ArgumentName("JobId")] Guid jobId)
        {
            _jobId = jobId;
        }

        public override void Run()
        {
            if( JetClient.JobServer.AbortJob(_jobId) )
            {
                Console.WriteLine("Aborted job {0:B}.", _jobId);
            }
            else
            {
                Console.WriteLine("Job {0:B} was not found or not running.", _jobId);
                ExitCode = 1;
            }
        }
    }
}
