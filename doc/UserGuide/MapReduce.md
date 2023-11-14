# MapReduce jobs

The WordCount job we [created in the tutorial](Tutorial1.md) uses some features that aren't
part of regular MapReduce, like hash table aggregation. There are other features too, like the
ability to have jobs with more than two stages, that don't fit neatly into MapReduce.

It is however possible to create a normal MapReduce job. Essentially, all that’s needed is two-stage
job where the first stage runs a map function, the second stage runs a reduce function, and the
channel between them sorts the data by key. The [`JobBuilder`][] provides methods for all these
operations.

For example, to convert the word count sample to MapReduce, all we need to do is replace the
`AggregateCounts` function with a reduce function:

```C#
public static void ReduceWordCount(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
{
    output.WriteRecord(Pair.MakePair(key, values.Sum()));
}
```

This is pretty much exactly like the reduce function you’d write in Hadoop (except for convenience
I used the [`Sum`][] method provided by LINQ in .Net, rather than summing the values manually).

The [`BuildJob`][] function for the MapReduce version would look as follows:

```C#
var input = job.Read(InputPath, typeof(LineRecordReader));
var words = job.Map<Utf8String, Pair<Utf8String, int>>(input, MapWords);
var sorted = job.SpillSortCombine<Utf8String, int>(words, ReduceWordCount);
var counted = job.Reduce<Utf8String, int, Pair<Utf8String, int>>(sorted, ReduceWordCount);
WriteOutput(counted, OutputPath, typeof(TextRecordWriter<>));
```

It starts off the same as the previous version: reads the input, and runs the map function. It then
calls [`SpillSortCombine`][], which performs an external merge sort using multiple passes on the
sending stage’s side, and merging the data on the receiving stage’s side. This is identical to the
sorting method used by Hadoop 1.0, and can handle very large amounts of data without putting too
much pressure on memory usage. Like Hadoop, it’s possible to run a combiner during the sort, for
which in this case we use the `ReduceWordCount` function. Note that [`SpillSortCombine`][] doesn’t
add an extra stage, but configures the channel to perform the sorting operation.

After sorting, we call the [`Reduce`][] function to add a stage that runs the `ReduceWordCount`
function, and finally we write the output as in the previous sample.

The end result is a job that runs almost exactly like a Hadoop job would. Which in the case of this
WordCount example is likely slower than the version we built earlier.

Next, it's time to look at [how Jumbo Jet executes jobs](JobExecution.md).

[`BuildJob`]: https://www.ookii.org/docs/jumbo-2.0/html/M_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilderJob_BuildJob.htm
[`JobBuilder`]: https://www.ookii.org/docs/jumbo-2.0/html/T_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder.htm
[`Reduce`]: https://www.ookii.org/docs/jumbo-2.0/html/Overload_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_Reduce.htm
[`SpillSortCombine`]: https://www.ookii.org/docs/jumbo-2.0/html/Overload_Ookii_Jumbo_Jet_Jobs_Builder_JobBuilder_SpillSortCombine.htm
[`Sum`]: https://learn.microsoft.com/dotnet/api/system.linq.enumerable.sum
