# Running Jumbo

Once you have [configured Jumbo](Configuration.md), you are almost ready to run it.

## Preparing the DFS

Before you can run Jumbo, you must format the distributed file system (you can skip this step if
you’re not using the DFS). To do this, run `dotnet bin/NameServer.dll -format` on the node that will
run the NameServer. This will create the necessary files for an empty DFS in the image directory
you configured for the NameServer.

## Starting Jumbo

Now, you can run Jumbo by executing `./Start-Dfs.ps1` and `./Start-Jet.ps1`.

If all went well, Jumbo should be running in the background. If you are running on a cluster, it
should be running on each node. If anything is wrong, go to the log directory (`bin/log` under your
distribution if you didn't change it in the configuration) and check the log files to see what went
wrong.

Open your browser to [http://localhost:35000](http://localhost:35000/) to see the DFS administration
page, and [http://localhost:36000](http://localhost:36000/) for the Jet administration page. If you
changed the configuration, are not hosting the administration sites using the default method, or are
accessing the pages from a computer other than the one running the NameServer and JobServer
respectively you may need to use different URLs.

If everything's working, let's try running [your first data processing job](FirstJob.md).

## Stopping Jumbo

To stop Jumbo, simply run `./Stop-Jet.ps1` and `./Stop-Dfs.ps1`. Jumbo has no "clean shutdown," so
all this does is kill Jumbo's processes on all the nodes. All of Jumbo's executables are designed
to be stopped this way and to recover if stopped in the middle of something important.
