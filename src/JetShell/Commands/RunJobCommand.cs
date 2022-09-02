// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Reflection;
using Ookii.CommandLine;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;

namespace JetShell.Commands
{
    // Note: this class needs special handling, don't create it using CommandLineParser.
    [ShellCommand("job", CustomArgumentParsing = true), Description("Runs a job on the Jumbo Jet cluster.")]
    class RunJobCommand : JetShellCommand
    {
        private readonly string[] _args;
        private readonly int _argIndex;
        private readonly CreateShellCommandOptions _options;

        public RunJobCommand(string[] args, int argIndex, CreateShellCommandOptions options)
        {
            _args = args;
            _argIndex = argIndex;
            _options = options;
        }

        public override void Run()
        {
            ExitCode = 1; // Assume failure unless we can successfully run a job.
            if (_args.Length - _argIndex == 0)
                _options.Out.WriteLine(_options.UsageOptions.UsagePrefix + " job <assemblyName> <jobName> [job arguments...]");
            else
            {
                var assemblyFileName = _args[_argIndex];
                var assembly = Assembly.LoadFrom(assemblyFileName);
                if (_args.Length - _argIndex == 1)
                {
                    _options.Out.WriteLine(_options.UsageOptions.UsagePrefix + " job <assemblyName> <jobName> [job arguments...]");
                    _options.Out.WriteLine();
                    PrintAssemblyJobList(_options.Out, assembly);
                }
                else
                {
                    var jobName = _args[_argIndex + 1];
                    var jobRunnerInfo = JobRunnerInfo.GetJobRunner(assembly, jobName);
                    if (jobRunnerInfo == null)
                    {
                        _options.Error.WriteLine("Job {0} does not exist in the assembly {1}.", jobName, Path.GetFileName(assemblyFileName));
                        PrintAssemblyJobList(_options.Out, assembly);
                    }
                    else
                    {
                        var jobRunner = jobRunnerInfo.CreateInstance(_args, _argIndex + 2);
                        if (jobRunner == null)
                        {
                            _options.UsageOptions.UsagePrefix = string.Format(CultureInfo.InvariantCulture, "{0} job {1} {2} ", _options.UsageOptions.UsagePrefix, Path.GetFileName(assemblyFileName), jobRunnerInfo.Name);
                            jobRunnerInfo.CommandLineParser.WriteUsageToConsole(_options.UsageOptions);
                        }
                        else
                        {
                            var jobId = jobRunner.RunJob();
                            if (jobId != Guid.Empty)
                            {
                                var success = JetClient.WaitForJobCompletion(jobId);
                                jobRunner.FinishJob(success);
                                ExitCode = success ? 0 : 1;
                            }
                            else
                                ExitCode = 2;
                        }
                    }
                }
            }
        }

        private void PrintAssemblyJobList(TextWriter writer, Assembly assembly)
        {
            var lineWriter = writer as LineWrappingTextWriter;
            var jobs = JobRunnerInfo.GetJobRunners(assembly);
            writer.WriteLine("The assembly {0} defines the following jobs:", assembly.GetName().Name);
            writer.WriteLine();
            if (lineWriter != null)
                lineWriter.Indent = _options.CommandDescriptionIndent;
            foreach (var job in jobs)
            {
                if (lineWriter != null)
                    lineWriter.ResetIndent();
                writer.WriteLine(_options.CommandDescriptionFormat, job.Name, job.Description);
            }
        }
    }
}
