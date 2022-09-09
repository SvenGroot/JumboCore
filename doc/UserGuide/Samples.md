# Jumbo Jet samples

Jumbo Jet comes with a number of sample jobs, all provided in the [Ookii.Jumbo.Jet.Samples.dll](../../src/Ookii.Jumbo.Jet.Samples/)
assembly. The following samples are provided:

## WordCount

A simple job that counts the frequency of each word in the input file(s). This is similar to the
WordCount job created in [the first tutorial](Tutorial1.md).

This job lets you choose between the "optimized" version (identical to the tutorial), a version
implemented using lambdas (which is less efficient because it needs to use delegates to call the
lambdas), and a version using MapReduce.

## AdvancedWordCount

A version of WordCount that uses more advanced features of Jumbo, identical to the job created in
[the second tutorial](Tutorial2.md).

## GenerateText

A job that generates random text files. This can be used as input for the WordCount job, although
the nature of the distribution used means that every word will appear in roughly equal amounts.

## LineCount

Just when you thought jobs couldn't be simpler than WordCount, there's LineCount, which just counts
the number of lines in the input. Consists of two stages, one to count lines in each input split,
and one to sum the counts.

## TeraSort

A distributed sorting algorithm using the data format and rules described by the
[GraySort benchmark](http://sortbenchmark.org/). This job uses `SpillSort` directly on the input
data and writes the result directly to the output, without any actual "stages" specified. This
causes the `JobBuilder` to generate two no-op stages using the `EmptyTask<T>` utility type.

This job also provides a custom record type (`GenSortRecord`), raw comparer
(`GenSortRecordRawComparer`), and record reader and writer (`GenSortRecordReader` and
`GenSortRecordWriter`).

## GenSort

A job that generates random data that can be used with the TeraSort job.

## ValSort

A job that validates whether the output of TeraSort is correctly sorted. Also demonstrates how
to perform additional steps in JetShell after job completion.

## Parallel Frequent Pattern Growth (Parallel FP-Growth)

A job that implements the Parallel FP-Growth algorithm described in the paper "PFP: Parallel
FP-Growth for Query Recommendation" by Li et al., 2008.

This algorithm calculates the top-K frequent patterns for each item in the database, only
regarding patterns that have the specified minimum support.

The algorithm has three steps: first, it counts how often each item occurs in the input database,
filters out the infrequent features, and divides the resulting feature list into groups. Next,
it generates group-dependent transactions from the input and runs the FP-Growth algorithm on
each group. Finally, the results from each group are aggregated to form the final result.

The number of groups should be carefully selected so that the number of items per group it
not too large. Ideally, each group should have 5-10 items at most for a large database.

The input for this job should be a plain text file (or files) where each line represents
a transaction containing a space-delimited list of transactions.

This example demonstrates a more complicated Jumbo job, with several stages including
more than one stage with file input. Several of its tasks are implemented as task type classes,
rather than using methods with `JobBuilder`. It also uses scheduling dependencies, group aggregation,
partition-based grouping using multiple partitions per task, dynamic partition assignment,
and custom progress providers.

And now, by studying these samples, you've learned everything there is to know about Jumbo!

Seriously though, there's much more to learn, if you're so inclined. Begin by checking out the
[class library documentation](http://www.ookii.org/Link/JumboDoc) and of course, the Jumbo source
code. If you have any questions, contact me [on GitHub](https://github.com/SvenGroot/JumboCore/discussions).
