# Tutorial 1: creating a data processing job

In this section, we'll walk you through writing your own distributed data processing application
that runs on Jumbo. Sounds complicated? Actually, it's not. Most of the complicated stuff is done
for you by the [`JobBuilder`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder.htm),
a helper that makes it easy to write code for your tasks, define a job configuration, and submit
the job to the cluster.

## Setting up your project

In order to introduce data processing with Jumbo, we are going to look how to create a job that
does the same thing as the WordCount sample job. WordCount reads an input file and counts how often
each word occurs in the input. It’s simple, and a pretty standard example for MapReduce-style data
processing.

First, let’s get a project set up for the sample. Create a directory called JumboSample, and run
the following command in that directory:

```text
dotnet new classlib -f net6.0
```

Next, we need to add references to the core Jumbo class libraries. Unfortunately, this can’t be
done using the command line. Open the file JumboSample.csproj that was just created, and add the
following section inside the `<Project>` element:

```xml
<ItemGroup>
  <Reference Include="Ookii.Jumbo">
    <HintPath>/jumbo_home/bin/Ookii.Jumbo.dll</HintPath>
  </Reference>
  <Reference Include="Ookii.Jumbo.Dfs">
    <HintPath>/jumbo_home/bin/Ookii.Jumbo.Dfs.dll</HintPath>
  </Reference>
  <Reference Include="Ookii.Jumbo.Jet">
    <HintPath>/jumbo_home/bin/Ookii.Jumbo.Jet.dll</HintPath>
  </Reference>
</ItemGroup>
```

Make sure to replace `/jumbo_home` with the path where you deployed Jumbo. If you are using Visual
Studio, you can also do this using the “Add Reference” dialog by browsing to the DLLs.

While you're in there, set the `<Nullable>` element to `disable`; Jumbo's class libraries aren't aware
of nullable reference types (they were written long before that was a thing), and the sample code
here also assumes it's off.

We also need to add a single package reference to the project, using the following command:

```text
dotnet add package Ookii.CommandLine -v 4.0.0
```

Finally, we’ll remove the Class1.cs file from the template, and create a new file called
WordCount.cs.

## Creating a JobRunner

Although you could write your own client application that submits jobs to Jumbo, the easiest way is
to create a job runner which can be used with JetShell.

A job runner is a class that creates the job configuration and specifies command line arguments
for the job’s invocation. These job runners are invoked using the `JetShell job` command that we
used in the quick start guide.

A job runner is any class that implements the [`IJobRunner`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_IJobRunner.htm)
interface, although typically you’ll probably want to inherit from the [`BaseJobRunner`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_BaseJobRunner.htm)
class, which defines a number of standard command line arguments and behaviors for job runners.
Because we’re going to use the `JobBuilder` to build our job, we’ll use the [`JobBuilderJob`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilderJob.htm)
class as the base class for our job runner, which itself is derived from `BaseJobRunner`.

So, start out a new C# file as follows:

```csharp
using System;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs.Builder;

namespace JumboSample
{
    public class WordCount : JobBuilderJob
    {
    }
}
```

That’s our job runner class. The first thing we need to add is the arguments. This job is going to
need to know where its input data and where to place its output, so we’ll have to create command
line arguments for that. Jumbo uses [Ookii.CommandLine](https://www.ookii.org/Link/CommandLineGitHub)
for defining command line arguments, so we need to add properties and mark them as command line
arguments:

```csharp
[CommandLineArgument(IsRequired = true, Position = 0)]
public string InputPath { get; set; }

[CommandLineArgument(IsRequired = true, Position = 1)]
public string OutputPath { get; set; }
```

This creates two required positional arguments, so it is now possible to specify the input and
output path on the command line when invoking this job.

## Data processing functions

In order to create a job, we need to write functions that process the data for each of the stages.
WordCount is a distributed counting operation, which consists of two operations: the first
operation extracts the words and initializes their count to 1. The second step adds up all
the counts for each word.

For this job, we're going to use a method of reading text files that's built-in to Jumbo. This reads
the input line-by-line, so the function for the first operation should split that line into words,
and then generate key/value pairs with the word as the key and a count of 1 as the value. For that
purpose, we’ll write a map function, much like you’ll see in Hadoop MapReduce jobs (only here it’s
just a function; unlike in Hadoop there is no need to create full classes for each task if you’re
using the `JobBuilder`):

```csharp
public static void MapWords(Utf8String line, RecordWriter<Pair<Utf8String, int>> output)
{
    string[] words = line.ToString().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
    foreach( string word in words )
        output.WriteRecord(Pair.MakePair(new Utf8String(word), 1));
}
```

First of all, notice that this is a public static method; this is preferred for processing
functions. Although you can use private methods or even lambdas, these must be called through a
delegate which is slower than the direct call that’s possible with public static methods. Keep in
mind that this code will be executed in an entirely different process than the one that is building
the job, so none of the state that’s available during job creation will still be intact during
execution. Preferably, processing functions should not use any external state.

Let’s see how this function works. The function will be called for each record in the input, which
in this case are the lines of text in the input (unlike Hadoop, records don’t have to be key/value
pairs, so the input record in this case is not). That line is passed in the first parameter, `line`.

The type of this parameter is [`Utf8String`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_Utf8String.htm).
This is a special string type used by Jumbo; unlike the regular `String` class, it is mutable
and also uses a more compact in-memory representation for most strings (as the name suggests, text
is stored in utf-8 encoding). These two features make `Utf8String` more efficient for Jumbo’s
purposes. Although Jumbo can use regular strings, it’s recommended to use `Utf8String` unless you
have a good reason not to. In this case, we’ll be reading the input using the [`LineRecordReader`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_LineRecordReader.htm),
which returns `Utf8String` records, so we have to use it for the input.

The second parameter is the `RecordWriter` to which the output should be written. This record
writer can be connected to a channel or a file, and can use a host of different output serialization
options. The processing function doesn’t need to care; it just writes records to the writer.

The type of our output records is [`Pair<Utf8String, int>`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_Pair_2.htm).
Note that you _cannot_ use the regular `KeyValuePair` structure with Jumbo; `Pair` provides a number
of additional features that Jumbo needs.

The function body simply splits the input on spaces, and creates a key/value pair for each word
with the key as the word, and a value of 1.

For the second step, we’re going to use Jumbo’s support for hash table aggregation (rather than a
typical reduce task). This means we need to define a function that takes the current value of a
key, a new value, and returns the updated value. In this case, we need to sum the values:

```csharp
public static int AggregateCounts(Utf8String key, int oldValue, int newValue)
{
    return oldValue + newValue;
}
```

Note that we’re not using the `key` parameter; this function just adds the values, and doesn’t need
the key. However, this is the signature that the `JobBuilder` requires, so the parameter must be
included.

In this case, it’s not necessary to write output. Jumbo has a built-in task type
([`AccumulatorTask`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Tasks_AccumulatorTask_2.htm))
that performs aggregation which takes care of all of that. This function is called by that task to
update the values, so that’s all the code we need to write for aggregation functions.

## Creating the job

Now that we have the functions that will process the data, we need to tell Jumbo which order to
apply them in. This is where the `JobBuilder` comes in. It provides a number of methods to
construct a sequence of various types of data processing operations. The resulting job
configuration can be customized to specify things like the number of partitions or channel types,
or you can just use `JobBuilder`’s defaults, which automatically decide on a number of partitions
based on the input data size and task capacity of your cluster.

In order to create the job with a job runner derived from `JobBuilderJob`, you need to override
the [`BuildJob`](https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilderJob_BuildJob.htm)
function. Here’s that function for our word count sample:

```csharp
protected override void BuildJob(JobBuilder job)
{
    var input = job.Read(InputPath, typeof(LineRecordReader));
    var words = job.Map<Utf8String, Pair<Utf8String, int>>(input, MapWords);
    var aggregated = job.GroupAggregate<Utf8String, int>(words, AggregateCounts);
    WriteOutput(aggregated, OutputPath, typeof(TextRecordWriter<>));
}
```

Let’s see what this function does. The first thing to remember is that none of the operations are
actually executed here. Each of these functions doesn’t actually perform the specified operation,
but adds a stage to the job configuration that will perform that operation. Only when the job is
submitted to the cluster will this be executed.

The first line tells Jumbo to [read](https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_Read.htm)
input from the specified path (which we get from the command line argument) using the
`LineRecordReader`. This record reader reads utf-8 text input and provides a `Utf8String` record
for each line. Since we didn’t specify any options, Jumbo will split the input file based on the
DFS block size for the file (if the input is being read from the Jumbo DFS), and create a single
task for each block. The `LineRecordReader` handles those splits and makes sure that all records
are read by exactly one task (no records are missed and none are read by two tasks), even when those
records cross a block boundary. Your code doesn’t need to worry about that.

It is possible to use the properties on the returned `input` variable to customize the split sizes
to create more or fewer tasks, but in this simple example, we just use the defaults.

The second line of the `BuildJob` function tells Jumbo to invoke a map function (the `MapWords`
function we defined earlier) for each record in the input. Unfortunately, limitations in C#’s type
argument inference when it comes to delegates means that it’s necessary to explicitly specify the
generic arguments for the [`JobBuilder.Map`](https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_Map__2.htm)
function. This is true for all JobBuilder functions that use delegates.

The `Map` function creates a stage that reads from the input, and executes the specified function
on each record. What to do with the output is specified later. As indicated before, Jumbo will
automatically create multiple tasks in the stage based on the input split size.

The third line calls the [`GroupAggregate`](https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_GroupAggregate__2.htm)
function, which tells Jumbo to group data by key and run the specified aggregation function (the
`AggregateCounts` function we wrote earlier). Because the input here is the output from another
operation, the `JobBuilder` will create a channel and set defaults for the number of partitions,
which you can of course override if you want.

`GroupAggregate` actually creates two stages, both performing the aggregation operation. The first
does it locally for each task in the input stage, and the second aggregates the data from all input
tasks. This is similar to using a combiner with MapReduce in Hadoop, and helps reduce the amount of
data that needs to be transferred over the network.

The final line tells Jumbo to write the output to the specified path using the specified
`RecordWriter` (in this case, we’re writing the output as text). Note that we’re not calling the
[`JobBuilder.Write`](https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_Write.htm)
method directly, but instead use the [`JobBuilderJob.WriteOutput`](https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilderJob_WriteOutput.htm)
method. This method applies some settings from command line arguments that are common to each
`JobBuilderJob` to the output. These command line arguments allow the user to specify things like
the block size and replication factor for the output.

In this case, we're using the [`TextRecordWriter`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_TextRecordWriter_1.htm),
which converts each record to a string and writes it to the output file. We specify the open generic
type `TextRecordWriter<>` to let Jumbo automatically instantiate it with the type of records being
written (`TextRecordWriter<Pair<Utf8String, int>>`) in this case.

And that’s it. Putting it all together gives us this:

```csharp
using System;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs.Builder;

namespace JumboSample
{
    public class WordCount : JobBuilderJob
    {
        [CommandLineArgument(IsRequired = true, Position = 0)]
        public string InputPath { get; set; }

        [CommandLineArgument(IsRequired = true, Position = 1)]
        public string OutputPath { get; set; }

        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));
            var words = job.Map<Utf8String, Pair<Utf8String, int>>(input, MapWords);
            var aggregated = job.GroupAggregate<Utf8String, int>(words, AggregateCounts);
            WriteOutput(aggregated, OutputPath, typeof(TextRecordWriter<>));
        }

        public static void MapWords(Utf8String line, RecordWriter<Pair<Utf8String, int>> output)
        {
            string[] words = line.ToString().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            foreach( string word in words )
                output.WriteRecord(Pair.MakePair(new Utf8String(word), 1));
        }

        public static int AggregateCounts(Utf8String key, int oldValue, int newValue)
        {
            return oldValue + newValue;
        }
    }
}
```

That's all the code you need to write for this simple version of WordCount.

## Compiling and running your job

To build your project, run the following command:

```text
dotnet build
```

To run a job, use the JetShell command line utility, which is in the directory where you published
your Jumbo distribution using the `Publish-Release.ps1` script in the quick start guide.

To work with jobs, we use JetShell’s `job` command. The first argument of this command is the path
of the assembly containing the job. Next, you specify the job name (which is the name of your job
runner class), and then any arguments for the job itself. To see which jobs are defined by a DLL,
omit the job name:

```text
> ./JetShell.ps1 job ~/JumboSample/bin/Debug/net6.0/JumboSample.dll
Usage: JetShell job <assemblyName> <jobName> [job arguments...]

The assembly JumboSample defines the following jobs:

    WordCount
```

Replace the path with the path to the JumboSample.dll you created above.

There is no description for the job, because we didn’t specify any. To add a description to a job,
apply the `System.ComponentModel.DescriptionAttribute` to the job runner class.

To see which arguments are accepted by a job, specify the job name but no arguments (note: this
only works if the job has at least one required argument; otherwise, just specify a non-existing
argument):

```text
> ./JetShell.ps1 job ~/JumboSample/bin/Debug/net6.0/JumboSample.dll wordcount
The required argument 'InputPath' was not supplied.
Usage: JetShell job JumboSample.dll WordCount  [-InputPath] <String> [-OutputPath]
   <String> [-BlockSize <BinarySize>] [-ConfigOnly <FileName>] [-Interactive]
   [-OverwriteOutput] [-Property <[Stage:]Property=Value>...] [-ReplicationFactor
   <Int32>] [-Setting <[Stage:]Setting=Value>...]
   
    -BlockSize <BinarySize>
        Block size of the job's output files.
        
    -ConfigOnly <FileName>
        Don't run the job, but only create the configuration and write it to the
        specified file. Use this to test if your job builder job is creating the correct
        configuration without running the job. Note there can still be side-effects such
        as output directories on the file system being created. If the OverwriteOutput
        switch is specified, the output directory will still be erased!
        
    -Interactive [<Boolean>]
        Wait for user confirmation before starting the job and before exiting.
        
    -OverwriteOutput [<Boolean>]
        Delete the output directory before running the job, if it exists.
        
    -Property <[Stage:]Property=Value>
        Modifies the value of one of the properties in the job configuration after the
        job has been created. Uses the format "PropertyName=value" or
        "CompoundStageId:PropertyName=value". You can access properties more than one
        level deep, e.g. "MyStage:OutputChannel.PartitionsPerTask=2". Can be specified
        more than once.
        
    -ReplicationFactor <Int32>
        Replication factor of the job's output files.
        
    -Setting <[Stage:]Setting=Value>
        Defines or overrides a job or stage setting in the job configuration. Uses the
        format "SettingName=value" or "CompoundStageId:SettingName=value". Can be
        specified more than once. 
```

As you can see, the WordCount job accepts far more arguments than the two we defined in the sample.
These arguments are defined by `JobBuilderJob`, and are common to all jobs that inherit from that
class. Note that the two properties we defined (InputPath and OutputPath) are not in the long list
of arguments because they have no descriptions. Apply the
`System.ComponentModel.DescriptionAttribute` to the property that defines the argument to add a
description.

Running the job is done the same way as with the built-in WordCount sample in the quick start guide:

```text
> ./JetShell.ps1 job ~/JumboSample/bin/Debug/net5.0/JumboSample.dll wordcount /mobydick.txt /sampleoutput
228 [1] INFO Ookii.Jumbo.Jet.Jobs.JobRunnerInfo (null) - Created job runner for job WordCount, InputPath = /mobydick.txt, OutputPath = /sampleoutput
474 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Saving job configuration to DFS file /JumboJet/job_{9b832d24-fc89-4dbf-8f7f-0ae44f94bcec}/job.xml.
703 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /home/sgroot/JumboSample/bin/Debug/net5.0/JumboSample.dll to DFS directory /JumboJet/job_{9b832d24-fc89-4dbf-8f7f-0ae44f94bcec}.
751 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /tmp/Ookii.Jumbo.Jet.Generated.fb08f0e1a18f419a879c8dfd3a22d935.dll to DFS directory /JumboJet/job_{9b832d24-fc89-4dbf-8f7f-0ae44f94bcec}.
835 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Running job 9b832d24-fc89-4dbf-8f7f-0ae44f94bcec.
0.0 %; finished: 0/1 tasks; MapWordsTaskStage: 0.0 %
100.0 %; finished: 1/1 tasks; MapWordsTaskStage: 100.0 %

Job completed.
Start time: 2013-06-03 03:45:39.555
End time:   2013-06-03 03:45:42.851
Duration:   00:00:03.2967750 (3.296775s)
```

You may have noted that the JetClient class uploaded two DLLs to the DFS: your JumboSample.dll, but
also a generated file. This is because the `JobBuilder` generates task classes to invoke the task
functions (`MapWords` and `AggregateWordCounts`) that we used.

Despite having two stages in the job, this sample execution had only 1 task. This is because the
input file has only one block, so there is only one task for the MapWords stage. Because of this,
the JobBuilder realizes that there is no need to aggregate data across tasks, and only performs
local aggregation. If you use larger input data with more than one input split, you will see more
tasks and two distinct stages in the job execution.

## What the JobBuilder does for you

At its core, any Jumbo Jet needs at least two things: a job configuration, and one or more
assemblies containing the code for the tasks. The former is an XML file that specifies the stages,
the channels, and the input `RecordReader` and output `RecordWriter` types. Each stage specifies
the type name of a class implementing [`ITask<TInput, TOutput>`](https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_ITask_2.htm)
which is what will be run during the tasks.

You could manually write classes that implement `ITask<TInput, TOutput>` (this may still be useful,
even when using the `JobBuilder`, for more complicated tasks). You could also manually write a
configuration file, upload it and the other required files to the DFS, and write your own client
that talks to the JobServer to submit and start the job.

Thanks to JetShell and the `JobBuilder`, you don't need to do any of that. The `JobBuilder` generates
classes implementing `ITask<TInput, TOutput>` that invoke the data processing functions you
provided. It transforms the sequence of steps you used in `BuildJob` into a job configuration,
and creates the XML file. And JetShell handles uploading everything and running your job, printing
progress until it finishes.

If you wanted to, you could derive from `BaseJobRunner` and build the configuration and task classes
yourself, while still using JetShell. But for most jobs, `JobBuilder` will be the easiest option.

The job we used here is not a typical MapReduce job (since it used hash-table aggregation). What
if we do want to write a traditional MapReduce job? [Keep reading](MapReduce.md) and find out.
