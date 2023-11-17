// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Reflection;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.CommandLine.Terminal;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;

namespace JetShell.Commands
{
    // Note: this class needs special handling, don't create it using CommandLineParser.
    [Command("job"), Description("Runs a job on the Jumbo Jet cluster.")]
    class RunJobCommand : JetShellCommand, ICommandWithCustomParsing
    {
        private IJobRunner _jobRunner;

        public void Parse(ReadOnlyMemory<string> args, CommandManager manager)
        {
            if (args.Length == 0)
            {
                WriteUsage(null, manager.Options);
                return;
            }

            var assemblyFileName = args.Span[0];
            var assembly = Assembly.LoadFrom(assemblyFileName);
            if (args.Length == 1)
            {
                WriteUsage(assembly, manager.Options);
                return;
            }

            var jobName = args.Span[1];
            var jobRunnerInfo = JobRunnerInfo.GetJobRunner(assembly, jobName);
            if (jobRunnerInfo == null)
            {
                WriteUsage(assembly, manager.Options);
                return;
            }

            manager.Options.UsageWriter.CommandName += $" {Path.GetFileName(assemblyFileName)} {jobRunnerInfo.Name}";
            manager.Options.AutoVersionArgument = false;
            _jobRunner = jobRunnerInfo.CreateInstance(args[2..], manager.Options);
        }

        public override int Run()
        {
            if (_jobRunner == null)
            {
                return 1;
            }

            var jobId = _jobRunner.RunJob();
            if (jobId == Guid.Empty)
            {
                return 2;
            }

            var success = JetClient.WaitForJobCompletion(jobId);
            _jobRunner.FinishJob(success);
            return success ? 0 : 1;
        }

        private static void WriteUsage(Assembly assembly, CommandOptions options)
        {
            using var writer = LineWrappingTextWriter.ForConsoleOut();
            using var support = VirtualTerminal.EnableColor(StandardStream.Output);
            WriteColor(options.UsageWriter.UsagePrefixColor, writer, support);
            writer.Write("Usage:");
            WriteColor(options.UsageWriter.ColorReset, writer, support);
            writer.Write($" {CommandLineParser.GetExecutableName()} job {Path.GetFileName(assembly?.Location) ?? "<assembly>"} [<job>] [job arguments...]");
            writer.WriteLine();
            if (assembly != null) 
            {
                PrintAssemblyJobList(writer, support, assembly, options);
            }
        }

        private static void WriteColor(TextFormat color, TextWriter writer, VirtualTerminalSupport support)
        {
            if (support.IsSupported)
            {
                writer.Write(color);
            }
        }

        private static void PrintAssemblyJobList(LineWrappingTextWriter writer, VirtualTerminalSupport support, Assembly assembly, CommandOptions options)
        {
            var jobs = JobRunnerInfo.GetJobRunners(assembly);
            writer.WriteLine();
            writer.WriteLine("The assembly {0} defines the following jobs:", assembly.GetName().Name);
            writer.WriteLine();
            writer.Indent = options.UsageWriter.CommandDescriptionIndent;
            foreach (var job in jobs)
            {
                writer.ResetIndent();
                WriteColor(options.UsageWriter.CommandDescriptionColor, writer, support);
                writer.Write($"    {job.Name}");
                WriteColor(options.UsageWriter.ColorReset, writer, support);
                writer.WriteLine();
                writer.WriteLine(job.Description);
                writer.WriteLine();
            }
        }
    }
}
