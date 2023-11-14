# Tutorial 2: advanced WordCount

Now that you have a better insight into how Jumbo works and some of its features, let’s create a
more complicated job. We’ll create a new version of WordCount that:

- Uses a custom comparer for aggregation to allow case insensitive word count.
- Creates a job with more stages that sorts the result by descending frequency (something which
  using Hadoop would require more than one job).
- Customizes channel and stage configuration.
- Uses job settings to specify a list of patterns to ignore while counting.

To start off, create a new file called AdvancedWordCount.cs in the project we created in
[the first tutorial](Tutorial1.md) (or create a new project using the same method, if you prefer).
We’ll create a class called AdvancedWordCount in this file:

```csharp
using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;

namespace JumboSample
{
    [Description("Alternative version of WordCount that demonstrates some more advanced features of Jumbo.")]
    public class AdvancedWordCount : JobBuilderJob
    {
    }
}
```

Note that I’ve added a description to the class, which will be displayed by JetShell.

This version of WordCount will have four command line parameters:

```csharp
[CommandLineArgument(IsRequired = true, Position = 0)]
[Description("The input file or directory containing the input text (must be utf-8).")]
public string InputPath { get; set; }

[CommandLineArgument(IsRequired = true, Position = 1)]
[Description("The directory where the output will be written.")]
public string OutputPath { get; set; }

[CommandLineArgument]
[Description("Perform a case-insensitive comparison on the words.")]
[JobSetting]
public bool CaseInsensitive { get; set; }

[CommandLineArgument]
[Description("The DFS path of a file containing regular expression patterns that define text that should be ignored while counting.")]
[JobSetting]
public string IgnorePatternsFile { get; set; }
```

Besides the input and output path, we also have a switch argument that indicates whether or not to
use case-insensitive comparisons on the words, and finally a parameter that specifies a text file
containing a list of patterns to ignore. Note that I’ve added descriptions to all of these, which
will be used by JetShell when displaying command line usage information for the job.

The CaseInsensitive and IgnorePatternsFile properties also have the [`JobSettingAttribute`][]
applied. While you can manually add job settings via the JobBuilder.Settings property, for
convenience [`JobBuilderJob`][] will add the value of every property marked with the
[`JobSettingAttribute`][] to the job settings, using `ClassName.PropertyName` as the setting’s key.
This allows our tasks to get the value of these arguments during job execution.

## Data processing functions

Next, we have to specify the task functions. This time, we need to keep some state in between
records (the list of ignored patterns), so instead of using a map function (which processes a
single record), we use a function that will process all records:

```csharp
[AllowRecordReuse]
public static void MapWords(RecordReader<Utf8String> input, RecordWriter<Pair<string, int>> output, TaskContext context)
{
```

This function signature takes a [`RecordReader<T>`][] from which the input is read, instead of a
record instance. It also has a [`TaskContext`][] parameter, which we’ll need to access the job
settings. Note that I’ve applied the [`AllowRecordReuseAttribute`][] attribute to the method, to
tell Jumbo it’s okay to reuse record object instances for the input, which improves performance by
reducing GC pressure.

One interesting thing to note is that for the output record type, we’re using [`Pair<string,
int>`][], so we’re using [`String`][] instead of [`Utf8String`][]. This is because we want to be
able to use a case-insensitive string comparer, and there is none for [`Utf8String`][]. Of course,
you could write one, but since the .Net [`String`][] class already has one we’ll use that instead.
This limits our ability to use record reuse, but since we'll be converting records to string anyway
to split the words, it doesn't really matter.

The first thing the method should do is read the list of ignore patterns:

```csharp
Regex ignorePattern = GetIgnorePattern(context);
```

We’ll get back to the details of the `GetIgnorePattern` function in a bit.

Since we’re keeping state between the records, we might as well reuse the output record object
instance as well, and the array containing the separator for [`String.Split`][].

```csharp
Pair<string, int> outputRecord = Pair.MakePair((string)null, 1);
char[] separator = new char[] { ' ' };
```

In this case we know that output record reuse is safe without checking
[`TaskContext.StageConfiguration.AllowOutputRecordReuse`][] because the output of this stage will be
a pipeline channel to an aggregation task, which we know also supports record reuse.

The only thing remaining is to process the records:

```csharp
foreach( Utf8String record in input.EnumerateRecords() )
{
    string line = record.ToString();
    if( ignorePattern != null )
        line = ignorePattern.Replace(line, " ");

    string[] words = line.Split(separator, StringSplitOptions.RemoveEmptyEntries);
    foreach( string word in words )
    {
        outputRecord.Key = word;
        output.WriteRecord(outputRecord);
    }
}
```

This basically does the same thing as the map function from our first version of WordCount, except
it removes words from the line that match the ignore pattern, and reuses the same instance of
[`Pair<TKey, TValue>`][] for every record.

Let’s look at that `GetIgnorePattern` function, which loads the ignore patterns file:

```csharp
private static Regex GetIgnorePattern(TaskContext context)
{
    string dfsPath = context.JobConfiguration.GetSetting("AdvancedWordCount.IgnorePatternsFile", null);
    if( dfsPath == null )
        return null;
    bool caseInsensitive = context.JobConfiguration.GetTypedSetting("AdvancedWordCount.CaseInsensitive", false);

    string path = context.DownloadDfsFile(dfsPath);
    var patterns = File.ReadLines(path)
        .Where(line => !string.IsNullOrWhiteSpace(line))
        .Select(line => "(" + line.Trim() + ")");

    return new Regex(string.Join("|", patterns), caseInsensitive ? RegexOptions.IgnoreCase : RegexOptions.None);
}
```

The function checks the job configuration to get the value of the setting that was added by our
`IgnorePatternsFile` property. That file is then loaded by using the
[`TaskContext.DownloadDfsFile`][] helper function. The task could of course use
[`FileSystemClient`][] directly to read the file from the DFS, but this method will cache the file
locally on the task server so that if multiple tasks on that server need the file it doesn’t need to
read it from the DFS every time. This function returns a local path where the cached file is stored.
The method then reads that file and constructs a regular expression for the ignored patterns,
optionally making it case-insensitive.

Note that in this case it would probably have made more sense to add the ignore patterns themselves
to the job configuration, but I wanted to demonstrate the [`DownloadDfsFile`][DownloadDfsFile_1]
function, so there you are.

We also need an aggregation function, which is the same as before:

```csharp
[AllowRecordReuse]
public static int AggregateCounts(string key, int oldValue, int newValue)
{
    return oldValue + newValue;
}
```

The only difference is the key type ([`String`][] instead of [`Utf8String`][]), and the
[`AllowRecordReuseAttribute`][] attribute. Allowing record reuse for an aggregation function is safe
as long as the types of the key and value are either value types or implement [`ICloneable`][].
Since [`String`][] implements [`ICloneable`][] and `int` is a value type, we can do it here.

In this version of WordCount, we want to sort the result by descending word frequency. However,
word frequency is the value of the key/value pair, and the default comparer for Pair sorts by key.
We could write a custom comparer, but it’s easier to add an additional stage that inverts the key
and value:

```csharp
[AllowRecordReuse]
public static void ReversePair<TKey, TValue>(Pair<TKey, TValue> record, RecordWriter<Pair<TValue, TKey>> output)
{
    output.WriteRecord(Pair.MakePair(record.Value, record.Key));
}
```

We’re going to use this function twice, first to put the frequency as the key, and after sorting to
swap the key and value back. Therefore, I’ve made the function generic so we can use the same
function both times.

Because this task will be used in a child stage, we want the [`JobBuilder`][] to generate a task
type that derives from [`PushTask<TInput, TOutput>`][]. This is not the case if we use a loop-style
function like `MapWords` above, so we use the style that takes a single output record. This prevents
us from reusing the output [`Pair<TKey, TValue>`][] instance, but in this case the performance gain
from using a push task is greater than the loss from not using output record reuse.

We could get around that by implementing our own task class which keeps the reused instance as a
member, but that's beyond the scope of this tutorial.

## Creating the job

Next, we have to implement the BuildJob function:

```csharp
protected override void BuildJob(JobBuilder job)
{
    var input = job.Read(InputPath, typeof(LineRecordReader));

    var words = job.Process<Utf8String, Pair<string, int>>(input, MapWords);
    words.StageId = "WordCount";
```

As before, we read the input using a [`LineRecordReader`][]. Because we’re using a function that
processes all records rather than a map function, we call [`JobBuilder.Process`][] rather than
[`JobBuilder.Map`][] for the first operation. We’re also assigning an explicit stage ID, which makes
the job progress in JetShell and the JetWeb administration portal look a bit nicer than using the
auto-generated stage ID (which you may have noticed was MapWordsTaskStage for this stage in the
previous tutorial).

Since we want to support case-insensitive comparisons, we need to select which comparer to use for
aggregation based on the `CaseInsensitive` property:

```csharp
Type comparerType = CaseInsensitive ? typeof(OrdinalIgnoreCaseStringComparer) : null;
```

Now add the aggregation step to the JobBuilder:

```csharp
var aggregated = job.GroupAggregate<string, int>(words, AggregateCounts, comparerType);
words.StageId = "WordCountAggregation";
```

Again, we’re assigning an explicit stage ID just to make it look nice. We’re also passing the
custom comparer type.

Next, we need to change the [`Pair<string, int>`][] records into [`Pair<int, string>`][], so we can
sort them by frequency.

```csharp
var reversed = job.Map<Pair<string, int>, Pair<int, string>>(aggregated, ReversePair<string, int>);
reversed.InputChannel.ChannelType = ChannelType.Pipeline;
```

Because this is a simple map function applied to each of the output records of the
WordCountAggregation stage, there really is no sense in re-partitioning and re-shuffling the
records. Therefore, we tell Jumbo to use a pipeline channel so that this step is performed
immediately for each record in the same process that’s running the WordCountAggregation task.

Next, we need to sort the records:

```csharp
var sorted = job.SpillSort(reversed, typeof(InvertedRawComparer<>));
sorted.InputChannel.TaskCount = 1;
```

We use the [`InvertedRawComparer<T>`][],
which inverts the default raw comparer for a type so we can sort by descending rather than ascending
frequency.

Normally, a file channel partitions the data over multiple tasks, but that would give us multiple
output files that are each individually sorted by frequency, while what we want is a single sorted
list. Therefore, we indicate explicitly that we want only one task (and thus one partition). This
is probably not a good idea for very large amounts of data, but for this sample it shouldn’t be a
problem.

Finally, we turn the records back into [`Pair<string, int>`][] (again using a pipelined task), and
write them to the output:

```csharp
var output = job.Map<Pair<int, string>, Pair<string, int>>(sorted, ReversePair<int, string>);
output.StageId = "WordCountOutput";
output.InputChannel.ChannelType = ChannelType.Pipeline;

WriteOutput(output, OutputPath, typeof(TextRecordWriter<>));
```

One additional thing to note is the `OrdinalIgnoreCaseStringComparer`, which is not a standard type.
Basically, we want to use [`StringComparer.OrdinalIgnoreCase`][], but that’s a property, and the
type of that property is internal so we can’t use that. So we create a type that wraps it:

```csharp
private class OrdinalIgnoreCaseStringComparer : StringComparer
{
    public override int Compare(string x, string y)
    {
        return OrdinalIgnoreCase.Compare(x, y);
    }

    public override bool Equals(string x, string y)
    {
        return OrdinalIgnoreCase.Equals(x, y);
    }

    public override int GetHashCode(string obj)
    {
        return OrdinalIgnoreCase.GetHashCode(obj);
    }
}
```

Putting everything together, we now have the following file:

```csharp
using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;

namespace JumboSample
{
    [Description("Alternative version of WordCount that demonstrates some more advanced features of Jumbo.")]
    public class AdvancedWordCount : JobBuilderJob
    {
        private class OrdinalIgnoreCaseStringComparer : StringComparer
        {
            public override int Compare(string x, string y)
            {
                return OrdinalIgnoreCase.Compare(x, y);
            }

            public override bool Equals(string x, string y)
            {
                return OrdinalIgnoreCase.Equals(x, y);
            }

            public override int GetHashCode(string obj)
            {
                return OrdinalIgnoreCase.GetHashCode(obj);
            }
        }

        [CommandLineArgument(IsRequired = true, Position = 0)]
        [Description("The input file or directory containing the input text (must be utf-8).")]
        public string InputPath { get; set; }

        [CommandLineArgument(IsRequired = true, Position = 1)]
        [Description("The directory where the output will be written.")]
        public string OutputPath { get; set; }

        [CommandLineArgument]
        [Description("Perform a case-insensitive comparison on the words.")]
        [JobSetting]
        public bool CaseInsensitive { get; set; }

        [CommandLineArgument]
        [Description("The DFS path of a file containing regular expression patterns that define text that should be ignored while counting.")]
        [JobSetting]
        public string IgnorePatternsFile { get; set; }

        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));

            var words = job.Process<Utf8String, Pair<string, int>>(input, MapWords);
            words.StageId = "WordCount";

            Type comparerType = CaseInsensitive ? typeof(OrdinalIgnoreCaseStringComparer) : null;

            var aggregated = job.GroupAggregate<string, int>(words, AggregateCounts, comparerType);
            words.StageId = "WordCountAggregation";

            var reversed = job.Map<Pair<string, int>, Pair<int, string>>(aggregated, ReversePair<string, int>);
            reversed.InputChannel.ChannelType = ChannelType.Pipeline;

            var sorted = job.SpillSort(reversed, typeof(InvertedRawComparer<>));
            sorted.InputChannel.TaskCount = 1;

            var output = job.Map<Pair<int, string>, Pair<string, int>>(sorted, ReversePair<int, string>);
            output.StageId = "WordCountOutput";
            output.InputChannel.ChannelType = ChannelType.Pipeline;

            WriteOutput(output, OutputPath, typeof(TextRecordWriter<>));
        }

        [AllowRecordReuse]
        public static void MapWords(RecordReader<Utf8String> input, RecordWriter<Pair<string, int>> output, TaskContext context)
        {
            Regex ignorePattern = GetIgnorePattern(context);

            Pair<string, int> outputRecord = Pair.MakePair((string)null, 1);
            char[] separator = new char[] { ' ' };
            foreach( Utf8String record in input.EnumerateRecords() )
            {
                string line = record.ToString();
                if( ignorePattern != null )
                    line = ignorePattern.Replace(line, " ");

                string[] words = line.Split(separator, StringSplitOptions.RemoveEmptyEntries);
                foreach( string word in words )
                {
                    outputRecord.Key = word;
                    output.WriteRecord(outputRecord);
                }
            }
        }

        [AllowRecordReuse]
        public static int AggregateCounts(string key, int oldValue, int newValue)
        {
            return oldValue + newValue;
        }

        [AllowRecordReuse]
        public static void ReversePair<TKey, TValue>(Pair<TKey, TValue> record, RecordWriter<Pair<TValue, TKey>> output)
        {
            output.WriteRecord(Pair.MakePair(record.Value, record.Key));
        }

        private static Regex GetIgnorePattern(TaskContext context)
        {
            string dfsPath = context.JobConfiguration.GetSetting("AdvancedWordCount.IgnorePatternsFile", null);
            if( dfsPath == null )
                return null;
            bool caseInsensitive = context.JobConfiguration.GetTypedSetting("AdvancedWordCount.CaseInsensitive", false);

            string path = context.DownloadDfsFile(dfsPath);
            var patterns = File.ReadLines(path)
                .Where(line => !string.IsNullOrWhiteSpace(line))
                .Select(line => "(" + line.Trim() + ")");

            return new Regex(string.Join("|", patterns), caseInsensitive ? RegexOptions.IgnoreCase : RegexOptions.None);
        }
    }
}
```

### Compiling and running the job

Compiling the job is the same as with the first sample:

```text
> dotnet build
```

Now, when we inspect the assembly using JetShell, you should see the following:

```text
> ./JetShell.ps1 job ~/JumboSample/bin/Debug/net5.0/JumboSample.dll 
Usage: JetShell job <assemblyName> <jobName> [job arguments...]

The assembly JumboSample defines the following jobs:

    AdvancedWordCount
        Alternative version of WordCount that demonstrates some more advanced
        features of Jumbo.

    WordCount
```

Note how the description of the job was included in the output.

We can also check the parameters for our job:

```text
> ./JetShell.ps1 job ~/JumboSample/bin/Debug/net5.0/JumboSample.dll advancedwordcount
The required argument 'InputPath' was not supplied.
Alternative version of WordCount that demonstrates some more advanced features
of Jumbo.

Usage: JetShell job JumboSample.dll AdvancedWordCount  [-InputPath] <String>
   [-OutputPath] <String> [-BlockSize <BinarySize>] [-CaseInsensitive]
   [-ConfigOnly <FileName>] [-IgnorePatternsFile <String>] [-Interactive]
   [-OverwriteOutput] [-Property <[Stage:]Property=Value>...]
   [-ReplicationFactor <Int32>] [-Setting <[Stage:]Setting=Value>...]
   
    -InputPath <String>
        The input file or directory containing the input text (must be utf-8).
        
    -OutputPath <String>
        The directory where the output will be written.
        
    -BlockSize <BinarySize>
        Block size of the job's output files.
        
    -CaseInsensitive [<Boolean>]
        Perform a case-insensitive comparison on the words.
        
    -ConfigOnly <FileName>
        Don't run the job, but only create the configuration and write it to
        the specified file. Use this to test if your job builder job is
        creating the correct configuration without running the job. Note there
        can still be side-effects such as output directories on the file
        system being created. If the OverwriteOutput switch is specified, the
        output directory will still be erased!
        
    -IgnorePatternsFile <String>
        The path of a file containing regular expression patterns that define
        text that should be ignored while counting.
        
    -Interactive [<Boolean>]
        Wait for user confirmation before starting the job and before exiting.
        
    -OverwriteOutput [<Boolean>]
        Delete the output directory before running the job, if it exists.
        
    -Property <[Stage:]Property=Value>
        Modifies the value of one of the properties in the job configuration
        after the job has been created. Uses the format "PropertyName=value"
        or "CompoundStageId:PropertyName=value". You can access properties
        more than one level deep, e.g.
        "MyStage:OutputChannel.PartitionsPerTask=2". Can be specified more
        than once.
        
    -ReplicationFactor <Int32>
        Replication factor of the job's output files.
        
    -Setting <[Stage:]Setting=Value>
        Defines or overrides a job or stage setting in the job configuration.
        Uses the format "SettingName=value" or
        "CompoundStageId:SettingName=value". Can be specified more than once.
```

Notice that our custom parameters are now listed, along with their description, in the long list of
arguments.

Now we can run the job, but before doing that, let’s create a file with ignore patterns, and store
it on the DFS as /ignore.txt (`./DfsShell.ps1 put ignore.txt /`):

```text
\bIshmael\b
\bwh.*\b
```

This will ignore the word “Ishmael”, and any word starting with “wh” (like “whale”).

```text
> ./JetShell.ps1 job ~/JumboSample/bin/Debug/net5.0/JumboSample.dll advancedwordcount /mobydick.txt /sampleoutput -ignorepatternsfile /ignore.txt -caseinsensitive
237 [1] INFO Ookii.Jumbo.Jet.Jobs.JobRunnerInfo (null) - Created job runner for job AdvancedWordCount, InputPath = /mobydick.txt, OutputPath = /sampleoutput, CaseInsensitive = True, IgnorePatternsFile = /ignore.txt, OverwriteOutput = False
430 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Saving job configuration to DFS file /JumboJet/job_{c44c00b5-5168-49ea-beb7-b8b68eb8374e}/job.xml.
665 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /home/sgroot/JumboSample/bin/Debug/net5.0/JumboSample.dll to DFS directory /JumboJet/job_{c44c00b5-5168-49ea-beb7-b8b68eb8374e}.
713 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /tmp/Ookii.Jumbo.Jet.Generated.8feaf9721e2a462490336d9a7891163a.dll to DFS directory /JumboJet/job_{c44c00b5-5168-49ea-beb7-b8b68eb8374e}.
782 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Running job c44c00b5-5168-49ea-beb7-b8b68eb8374e.
0.0 %; finished: 0/2 tasks; WordCountAggregation: 0.0 %; WordCountOutput: 0.0 %
50.0 %; finished: 1/2 tasks; WordCountAggregation: 100.0 %; WordCountOutput: 0.0 %
100.0 %; finished: 2/2 tasks; WordCountAggregation: 100.0 %; WordCountOutput: 100.0 %

Job completed.
Start time: 2013-06-03 08:10:35.023
End time:   2013-06-03 08:10:38.695
Duration:   00:00:03.6723330 (3.672333s)
```

Note that this job had two stages despite there being only one block in the input, which is because
the [`SpillSort`][] operation cannot be rolled into one stage. With more input blocks, the
[`JobBuilder`][] would create a three-stage job in this example.

If you view the output, you can see that it did indeed ignore case (words will be listed with the
case of their first occurrence), is sorted by frequency, and the patterns we specified were ignored:

```text
> ./DfsShell.ps1 cat /sampleoutput/WordCountOutput-00001
[The, 12465]
[of, 5870]
[and, 5605]
[a, 3979]
[to, 3970]
[In, 3536]
[that, 2410]
[his, 2164]
[with, 1530]
[it, 1511]
[but, 1493]
[As, 1491]
…
```

If you want to look at some jobs that are more complex than WordCount, take a look at some of the
[included samples](Samples.md).

[`AllowRecordReuseAttribute`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_AllowRecordReuseAttribute.htm
[`FileSystemClient`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Dfs_FileSystem_FileSystemClient.htm
[`ICloneable`]: https://learn.microsoft.com/dotnet/api/system.icloneable
[`InvertedRawComparer<T>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_InvertedRawComparer_1.htm
[`JobBuilder.Map`]: https://www.ookii.org/docs/jumbo-2.0/html/Overload_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_Map.htm
[`JobBuilder`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder.htm
[`JobBuilder.Process`]: https://www.ookii.org/docs/jumbo-2.0/html/Overload_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_Process.htm
[`JobBuilderJob`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilderJob.htm
[`JobSettingAttribute`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_JobSettingAttribute.htm
[`LineRecordReader`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_LineRecordReader.htm
[`Pair<int, string>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_Pair_2.htm
[`Pair<string, int>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_Pair_2.htm
[`Pair<TKey, TValue>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_Pair_2.htm
[`PushTask<TInput, TOutput>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_PushTask_2.htm
[`RecordReader<T>`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_RecordReader_1.htm
[`SpillSort`]: https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_SpillSort.htm
[`String.Split`]: https://learn.microsoft.com/dotnet/api/system.string.split
[`String`]: https://learn.microsoft.com/dotnet/api/system.string
[`StringComparer.OrdinalIgnoreCase`]: https://learn.microsoft.com/dotnet/api/system.stringcomparer.ordinalignorecase
[`TaskContext.DownloadDfsFile`]: https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_TaskContext_DownloadDfsFile.htm
[`TaskContext.StageConfiguration.AllowOutputRecordReuse`]: https://www.ookii.org/docs/jumbo-2.0/html/P_Ookii_Jumbo_Jet_Jobs_StageConfiguration_AllowOutputRecordReuse.htm
[`TaskContext`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_TaskContext.htm
[`Utf8String`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_IO_Utf8String.htm
[DownloadDfsFile_1]: https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_TaskContext_DownloadDfsFile.htm
