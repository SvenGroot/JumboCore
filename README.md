# Jumbo for .Net 6+

Jumbo is a distributed data processing system for Microsoft .Net 6.0 and later. It was created as
a way for me to learn more about MapReduce and [Apache Hadoop](https://hadoop.apache.org/) during
my time as a Ph.D. candidate at [Kitsuregawa Lab](http://www.tkl.iis.u-tokyo.ac.jp/new/?lang=en) at
the University of Tokyo's Institute of Industrial Science.

Jumbo offers a similar Distributed File System and distributed data processing engine as Hadoop, and
its design sometimes heavily borrows from Hadoop (pre-1.0).

It was created as a research and learning project, and is not intended for production use.

## Overview

Jumbo consists of two components:

- **Jumbo DFS**, a distributed file system similar to Google's DFS and Hadoop DFS.
- **Jumbo Jet**, a distributed data processing engine that can run MapReduce and other large scale
  data processing workloads on a cluster.

Jumbo allows the user to create a pipeline of processing steps (stages), where each stage is broken
into tasks that are executed in parallel on a cluster of machines. Jumbo supports MapReduce, but has
more flexibility and also allows for alternate processing pipelines.

Jumbo was originally created between 2008 and 2013 for Mono (to run on Linux clusters) and Microsoft
.Net Framework (for development/debugging). JumboCore is a port of the original Jumbo to run on on
.Net Core, and now .Net 6+. If you want to play around with Jumbo, this is the version you want;
it's much easier to run than the original.

If you're interested, there is a repository for the [original Jumbo](https://github.com/SvenGroot/Jumbo).
It's only provided to preserve the history of the project, since this repository only contains
the history of the port. There's no reason to try and use that version.

Because this is a port of an older project, it mostly does not use any fancy new C# language
features that were introduced after .Net Framework 4.0, and I'm sure the code could be improved in
many ways.

This project is not really maintained, however I do still sometimes like to tinker, so you may
randomly see some updates. Here's some updates that were made after bringing the project to GitHub:

- **Nullable annotations**: all the class libraries have been updated to support Nullable Reference
  Types (at this time, this isn't true for the executables). I was curious to see what that would
  uncover. What I found was that, especially in Jumbo Jet, I have a lot of places where I assume stuff
  is not null because of what part of execution I'm in. I mostly just left those, using the
  null-forgiving operator, since I didn't want to rearchitect the code too much.
- **Removal of BinaryFormatter**: the `BinaryFormatter` class is [unsafe](https://aka.ms/binaryformatter),
  cannot be made safe, and is deprecated. I believe it's slated for removal in .Net 9. However,
  Jumbo's RPC mechanism relied on it. With some modifications, I was able to transition to using
  Jumbo's own `IWritiable`/`IValueWriter<T>` serialization mechanism for RPC, eliminating the use of
  this dangerous class.

## Limitations

:warning: **Jumbo is not production quality code!** :warning:

Jumbo was created as a case study on the design of MapReduce-style systems. It is not a full-fledged
alternative for Hadoop and lacks many of the features that would be required in a real environment.
Features were added as I needed them or if I felt like adding them. Some features were added but
rarely used, so may not perform well or have bugs.

Jumbo should not be relied upon to store or process important data. The original Jumbo using Mono
has only received limited testing at scale, and JumboCore was *never* tested at scale. By all means,
try it out, but I am not responsible if you lose data.

Jumbo’s primary purpose was to help me understand how Hadoop works. It is loosely based on Hadoop,
and blatantly borrows a number of design elements from Hadoop, while differing in some other areas.
It was not designed to compete with Hadoop, and not originally intended to be released. I am
releasing it now because I want to preserve it, and some people may think it's interesting to
play around with.

The purpose of this release is for distributed systems enthusiasts who would like to experiment with
a data processing system besides Hadoop. Check the source to see what design alternatives I used. Or
maybe you’d like to see how .Net’s capabilities over Java (such as proper generics or LINQ)
influenced the code.

That said, Jumbo should work pretty well. It has some interesting additions over Hadoop (1.0; I'm
not up to date on the current development of Hadoop), and some things that I just think are cool
(such as how runtime assembly generation is used to make defining tasks and jobs easy).

I hope you enjoy looking at this project. Despite its age and shortcomings, I'm still pretty proud
of it. It's still by far the largest project I've done where all of the work was done by me
individually.

If you have any questions or feedback, feel free to [use the discussions tab](https://github.com/SvenGroot/JumboCore/discussions).
I will try to help you if I can, but please understand that I’m not officially supporting this project.

## Why is it called Jumbo?

Hadoop, in case you didn't know, is named after the stuffed elephant of one of the original developer's
children. Jumbo is a real elephant, made famous by P.T. Barnum, and since his name has become
synonymous with things that are huge, it seemed appropriate for *large* scale data processing.

Additionally, it was consistent with my penchant for using words meaning "big" (my last name is "big"
in Dutch, and my website is [Ookii.org](https://www.ookii.org), "big" in Japanese).

As for Jumbo Jet, that was an obvious pun that I couldn't resist once the name Jumbo was selected.

## Getting started

Want to give Jumbo a try? Head over to the [Quick Start Guide](doc/QuickStart.md)! Or if you're
eager to learn how it all works, skip straight to the [User Guide](doc/UserGuide.md). You can
also check out the [class library documentation](http://www.ookii.org/Link/JumboDoc).
