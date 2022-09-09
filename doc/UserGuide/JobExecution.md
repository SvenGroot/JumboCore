# Job execution life cycle

If you've made it this far, you've executed a few jobs on Jumbo, and even wrote one of your own.
But what actually happens when you run a job?

In this section, we’ll look at how JetShell submits a job, and how it gets executed by Jumbo Jet.
This example assumes you’re using a `JobBuilderJob`; for other types of job runners, the first few
steps may be slightly different, though the concept is the same.

1. When you invoke JetShell with the job argument, it will first load the specified assembly and
search for a class matching the specified name. If that class is a job runner (it implements
`IJobRunner`), JetShell instantiates it, and calles the `IJobRunner.RunJob` method.
2. For a `JobBuilderJob`, the `RunJob` method calls the `BuildJob` method, which the job's author
will have overridden. The `BuildJob` method specifies a sequence of operations by calling methods
on the `JobBuilder` (as we saw in the [tutorial](Tutorial1.md)). The `JobBuilder` compiles this
sequence of operations into a job configuration.
3. JetShell calls `JetClient.JobServer.CreateJob`, which tells the JobServer to assign a new job ID and
creates a directory on the DFS for the job’s files. It then calls `JobBuilderJob.OnJobCreated` to
give the job runner a chance to modify the job configuration and upload additional files.
4. It calls `JetClient.RunJob`, passing the job ID, configuration, and list of assemblies. This
method saves the configuration to the DFS, uploads the assembly files, and instructs the JobServer
to start the job.
5. The job ID is returned to JetShell, which calls `JetClient.WaitForJobCompletion` to wait until
the job finishes, printing progress to the console occasionally.
6. When starting a job, the `JobServer` loads the job configuration and constructs a list of all
tasks, adding input data locality information if applicable. The list is ordered based on
dependencies between the stages.
7. The JobServer runs the task scheduler, which assigns tasks to TaskServers. If possible, it prefers
to run tasks on nodes that have the input data for that task stored locally, using the locality
information from the previous step.
8. When a TaskServer sends a heartbeat to the JobServer, it receives as response all new tasks that
have been assigned to it.
9. When the TaskServer receives a new task and it hasn't run any previous tasks for that job,
it downloads all of the job’s files (configuration, assemblies, and any additional files that the
user uploaded to the job’s directory on the DFS) to a local cache.
10. The TaskServer launches a new TaskHost process for the task (the original version of Jumbo
supported running tasks in AppDomains for testing and debugging purposes, but this is no longer
possible in JumboCore).
11. The task host loads the job configuration from the local cache, and finds the configuration for
the task it has been instructed to run.
12. The task host instantiates the task’s class (implementing `ITask`), opens input and output files
on the DFS and sets up channels to other tasks. Note that DFS output is written to a temporary file,
not the final output path. If the stage for this task has a child stage connected via a pipeline
channel, these are also instantiated.
13. The task host calls `ITask.Run` for the task.
14. Periodically, the task host informs the TaskServer of the task’s progress. The TaskServer
forwards this information to the JobServer during heartbeats. If a task does not send progress
updates for a configurable time-out, it is killed by the TaskServer.
15. When the task finishes, the task host finalizes any output and closes all input and output
files and channels. If the output is a DFS file, the temporary file is renamed to the final output
file.
16. The task host notifies the TaskServer of task completion, and terminates.
17. When the TaskServer gets the completion notification, it notifies the JobServer on the next
heartbeat (if immediate completed task notification is enabled, a heartbeat is sent immediately
without waiting for the timeout).
18. If a task encounters an error or terminated without notifying the TaskServer of success, the
TaskServer notifies the JobServer of task failure, which will then reset the task to schedule it
again. If too many task failures occur, the job is failed.
19. When a TaskServer notifies the JobServer it has finished a task, the JobServer runs the
scheduler again to find new tasks to run on that TaskServer.
20. The JobServer updates the state of the job, so that other tasks that depend on this task (for
example, if the completed task had a file channel output the tasks of the receiving stage of that
channel will periodically check which tasks are finished to retrieve their output). If enabled and
applicable, a UDP task completion broadcast message is sent, which allows receiving stage tasks
to immediately pick up the newly available output without having to poll.
21. When all tasks in a job have finished, a job cleanup command is sent to all TaskServers that
ran tasks for the job so they can delete any temporary and intermediate files that still remain.
22. The `JetClient.WaitForJobCompletion` method returns once the job is finished, and JetShell
terminates.

That seems like a lot to run only the few lines of code you wrote for the WordCount job. But, all
this stuff makes it so you can process huge amounts of data on a large cluster of machines, while
only needing to write a few lines of code.

Next, let's look at some of [Jumbo's features](DfsFeatures.md) in more detail.
