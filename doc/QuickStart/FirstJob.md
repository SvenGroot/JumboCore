# Running your first data processing job

Once Jumbo is [up and running](Running.md), you can start using Jumbo Jet to run data processing jobs.

To help you get started, this article demonstrates how to upload a text file to the DFS and run
the WordCount job on the file. WordCount is an included sample job that counts the frequency of
each word in the input.

To run this job, you need some utf-8 plain text files to use as input. You can generate some random
text using another Jumbo sample job (which we’ll do below), but if you want some non-random text,
why not use [Project Gutenberg](http://www.gutenberg.org/)? For example, you could use
[Moby Dick](http://www.gutenberg.org/cache/epub/2701/pg2701.txt).

## Uploading input data

First, you must upload a text file. Any plain text file stored as utf-8 will do. If your file is
named mobydick.txt, use the following:

```text
> ./DfsShell.ps1 put /local/path/to/mobydick.txt /
```

This will store the file somefile.txt in the root of the DFS. Verify it by running `./DfsShell.ps1 ls`.
The output should look something like this:

```text
> ./DfsShell.ps1 ls
Directory listing for /

2013-02-16 20:53        1,257,260  mobydick.txt
```

DfsShell is your key to interacting with the DFS. Use it to upload and download files, manipulate
the namespace (rename, delete, etc.), view status, and more. You can also use the “Browse file system
namespace” option in the DFS administration website to view the contents of the file system.

DfsShell commands operate mostly on DFS paths, but sometimes also take local paths. For example,
the first argument to the `put` command above a path on the local file system, while the second
argument is a DFS path (the root, in this case).

> Note: the `DfsShell.ps1` script is just a wrapper that invokes `dotnet bin/DfsShell.dll` so you
> don't have to type that every time.

If you want to use multiple files as input for a job, upload all those files to the same
directory. Do not use  the root in this case; use `./DfsShell.ps1 mkdir` to create a directory.

## Running the job

Where DfsShell is used to interact with the DFS, you use JetShell to use Jumbo Jet. Let's use it
to run the WordCount job on the file uploaded above (change the first path if your file is not
called mobydick.txt).

```text
> ./JetShell.ps1 job bin/Ookii.Jumbo.Jet.Samples.dll wordcount /mobydick.txt /wcoutput
236 [1] INFO Ookii.Jumbo.Jet.Jobs.JobRunnerInfo (null) - Created job runner for job WordCount, InputPath = /mobydick.txt, OutputPath = /wcoutput
427 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Saving job configuration to DFS file /JumboJet/job_{56f57c95-f6e4-445a-b87d-8fd1ce408db5}/job.xml.
977 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /home/sgroot/jumbo/build/bin/Ookii.Jumbo.Jet.Samples.dll to DFS directory /JumboJet/job_{56f57c95-f6e4-445a-b87d-8fd1ce408db5}.
1051 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Running job 56f57c95-f6e4-445a-b87d-8fd1ce408db5.
0.0 %; finished: 0/1 tasks; WordCount: 0.0 %
100.0 %; finished: 1/1 tasks; WordCount: 100.0 %

Job completed.
Start time: 2013-02-16 20:56:42.397
End time:   2013-02-16 20:56:44.016
Duration:   00:00:01.6193970 (1.619397s)
```

Let’s examine what we did here. JetShell has multiple functions, but the main one is to launch jobs,
which is done with the `job` command. The first argument to that command specifies the local (not DFS)
path to the assembly file containing the job (in this case, the included assembly with sample jobs),
followed by the name of the job. The remaining arguments are specific to the job; the WordCount job
expects at least the input and output DFS paths (as indeed do most sample jobs). We tell it to read
from `/mobydick.txt`, and to place the output in a directory on the DFS called `/wcoutput` (this
directory will be created by the job).

> Tip: want to see what jobs are available in an assembly? Simply run `./JetShell.ps1 job <assembly>`
> without any other arguments. Similarly, run `./JetShell.ps1 job bin/Ookii.Jumbo.Jet.Samples.dll wordcount`
> to see all the arguments for the WordCount job; this should work for most jobs. Jumbo uses
> [Ookii.CommandLine](https://www.ookii.org/Link/CommandLineGitHub), another project of mine, to do
> all this.

Let’s see what running the job did to the file system:

```text
> ./DfsShell.ps1 ls
Directory listing for /

2012-07-11 17:15            <DIR>  JumboJet
2013-02-16 20:53        1,257,260  mobydick.txt
2013-02-16 20:56            <DIR>  wcoutput
```

You can see there are two new directories. `JumboJet` is a working directory for the Jet execution
engine; it’s not important for the user. The `wcoutput` directory contains the output of the job.
Let’s check it out:

```text
> ./DfsShell.ps1 ls /wcoutput
Directory listing for /wcoutput

2013-02-16 20:56          468,008  WordCountAggregation-00001
```

As you can see, there is one file. The files are named after the tasks that produced them, and in
this example there was only one task because the input file was quite small. You can view the results
using DfsShell as well:

```text
> ./DfsShell.ps1 cat /wcoutput/WordCountAggregation-00001
[The, 549]
[Project, 79]
[Gutenberg, 20]
[EBook, 1]
[of, 6587]
[Moby, 79]
[Dick;, 9]
[or, 758]
[Whale,, 39]
[by, 1113]
[Herman, 4]
[Melville, 4]
[This, 102]
[eBook, 5]
[is, 1586]
...
```

That probably kept going for a while, depending on the size of the file. That tells you exactly how
often each word occurred in your text file.

> Note: the WordCount sample just splits the text on spaces, so if you uploaded something that isn’t
> just plain text (like an HTML file), the results might be a bit weird. Even in this case you’ll
> notice that some of the “words” include punctuation marks, and that different capitalizations of
> the same word are counted separately. That’s a limitation of the sample, not of Jumbo. The
> [user guide](../UserGuide.md) will introduce a more advanced version of WordCount that overcomes
> some of these limitations.

Don’t forget to go to the Jet administration website (`http://localhost:36000` by default). You can
see lots of cool statistics about your job there.

And that’s your very first job! But wait a second? Isn’t Jumbo for distributed processing? But
unless you uploaded a very large text file (larger than the block size for the file system), the job
probably only had one task, which ran on only one node.

## Running a bigger job

We can make this more interesting by using another sample job included with Jumbo, GenerateText, to
generate some larger input for the WordCount job (read ahead before running this, especially if
you're using Jumbo on only a single node!):

```text
> ./JetShell.ps1 job bin/Ookii.Jumbo.Jet.Samples.dll generatetext /bigtext 64 256MB
256 [1] INFO Ookii.Jumbo.Jet.Jobs.JobRunnerInfo (null) - Created job runner for job GenerateText, OutputPath = /bigtext, SizePerTask = 256MB, TaskCount = 64
411 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Saving job configuration to DFS file /JumboJet/job_{b4c04385-df32-458b-8b74-f41e0364e05e}/job.xml.
518 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /home/sgroot/jumbo/build/bin/Ookii.Jumbo.Jet.Samples.dll to DFS directory /JumboJet/job_{b4c04385-df32-458b-8b74-f41e0364e05e}.
596 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /tmp/Ookii.Jumbo.Jet.Generated.9ee75f0181f24f6691303f8106e79503.dll to DFS directory /JumboJet/job_{b4c04385-df32-458b-8b74-f41e0364e05e}.
647 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Running job b4c04385-df32-458b-8b74-f41e0364e05e.
0.0 %; finished: 0/64 tasks; GenerateTaskStage: 0.0 %
3.1 %; finished: 2/64 tasks; GenerateTaskStage: 3.1 %
24.8 %; finished: 15/64 tasks; GenerateTaskStage: 24.8 %
94.5 %; finished: 58/64 tasks; GenerateTaskStage: 94.5 %
100.0 %; finished: 64/64 tasks; GenerateTaskStage: 100.0 %

Job completed.
Start time: 2013-02-16 21:13:10.875
End time:   2013-02-16 21:13:20.624
Duration:   00:00:09.7494140 (9.749414s)
```

The parameters for this job indicate the output path (`/bigtext`), the number of generator tasks (64),
and the size of the data to generate per task (256MB).

In total, this job generated 16GB of random text, using 64 tasks each generating 256MB. I’m running
this example on 32 nodes; if you’re using a smaller cluster, you may want to scale down the size
accordingly. Just make sure you use more than one generator task or a total size that’s larger than
the DFS block size.

Okay, now you can go ahead and run the above command, adjusting the size so it's reasonable for your
environment. You can see the files the job created by running `./DfsShell ls /bigtext`.

Then, we can simply run WordCount as before:

```text
> ./JetShell.ps1 job bin/Ookii.Jumbo.Jet.Samples.dll wordcount /bigtext /wcoutput
236 [1] INFO Ookii.Jumbo.Jet.Jobs.JobRunnerInfo (null) - Created job runner for job WordCount, InputPath = /bigtext, OutputPath = /wcoutput
496 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Saving job configuration to DFS file /JumboJet/job_{db8b43d7-6446-4d88-b2a9-6647031d98a9}/job.xml.
665 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Uploading local file /home/sgroot/jumbo/build/bin/Ookii.Jumbo.Jet.Samples.dll to DFS directory /JumboJet/job_{db8b43d7-6446-4d88-b2a9-6647031d98a9}.
710 [1] INFO Ookii.Jumbo.Jet.JetClient (null) - Running job db8b43d7-6446-4d88-b2a9-6647031d98a9.
0.0 %; finished: 0/320 tasks; WordCount: 0.0 %; WordCountAggregation: 0.0 %
3.8 %; finished: 12/320 tasks; WordCount: 4.7 %; WordCountAggregation: 0.0 %
15.9 %; finished: 51/320 tasks; WordCount: 19.9 %; WordCountAggregation: 0.0 %
20.0 %; finished: 64/320 tasks; WordCount: 25.0 %; WordCountAggregation: 0.0 %
28.8 %; finished: 92/320 tasks; WordCount: 35.9 %; WordCountAggregation: 0.0 %
38.8 %; finished: 124/320 tasks; WordCount: 48.4 %; WordCountAggregation: 0.0 %
40.0 %; finished: 128/320 tasks; WordCount: 50.0 %; WordCountAggregation: 0.0 %
41.9 %; finished: 134/320 tasks; WordCount: 52.3 %; WordCountAggregation: 0.0 %
52.2 %; finished: 167/320 tasks; WordCount: 65.2 %; WordCountAggregation: 0.0 %
59.7 %; finished: 191/320 tasks; WordCount: 74.6 %; WordCountAggregation: 0.0 %
60.0 %; finished: 192/320 tasks; WordCount: 75.0 %; WordCountAggregation: 0.0 %
67.8 %; finished: 217/320 tasks; WordCount: 84.8 %; WordCountAggregation: 0.0 %
76.3 %; finished: 244/320 tasks; WordCount: 95.3 %; WordCountAggregation: 0.0 %
80.0 %; finished: 256/320 tasks; WordCount: 100.0 %; WordCountAggregation: 0.0 %
85.3 %; finished: 272/320 tasks; WordCount: 100.0 %; WordCountAggregation: 26.7 %
94.1 %; finished: 301/320 tasks; WordCount: 100.0 %; WordCountAggregation: 70.4 %
100.0 %; finished: 320/320 tasks; WordCount: 100.0 %; WordCountAggregation: 100.0 %

Job completed.
Start time: 2013-02-16 21:18:57.619
End time:   2013-02-16 21:19:18.695
Duration:   00:00:21.0754110 (21.075411s)
```

Now we had quite a few more tasks, with two stages: WordCount, which reads a piece of the input and
counts the words locally, and WordCountAggregation, which aggregates all the pieces of the first
stage. You could compare the WordCount stage with a map stage, and the WordCountAggregation stage
with a reduce stage (except that this version of WordCount actually uses hash table aggregation,
which is not possible with Hadoop 1.0).

If you look at the /wcoutput directory now, you'll probably also see more than one output file,
because each task in the WordCountAggregation stage creates its own output file. How many tasks
are used depends on the cluster configuration.

Want to know more about how this example works and how to create your own processing jobs? Move on
to the [user guide](../UserGuide.md)!
