using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Pages;

public class BlockListModel : PageModel
{
    public List<string> Blocks { get; } = new List<string>();

    public string NewQueryString { get; private set; }

    [BindProperty(SupportsGet = true)]
    public string DataServer { get; set; }

    [BindProperty(SupportsGet = true)]
    public int Port { get; set; }

    [BindProperty(SupportsGet = true)]
    public BlockKind Kind { get; set; }

    [BindProperty(SupportsGet = true, Name = "Files")]
    public bool IncludeFiles { get; set; }

    public void OnGet()
    {
        // DFS web only applicable when file system is a DFS.
        var client = (DfsClient)FileSystemClient.Create();
        Guid[] blocks;
        if (DataServer == null)
        {
            ViewData["Title"] = $"Block list ({Kind})";
            blocks = client.NameServer.GetBlocks(Kind);
            NewQueryString = "kind=" + Kind.ToString();
        }
        else
        {
            var address = new ServerAddress(DataServer, Port);
            ViewData["Title"] = $"Block list for {address}";
            blocks = client.NameServer.GetDataServerBlocks(address);
            NewQueryString = FormattableString.Invariant($"dataServer={DataServer}&port={Port}");
        }

        if (blocks != null)
        {
            foreach (var blockId in blocks)
            {
                if (IncludeFiles)
                {
                    Blocks.Add($"{blockId:B}: {client.NameServer.GetFileForBlock(blockId)}");
                }
                else
                {
                    Blocks.Add(blockId.ToString("B", CultureInfo.CurrentCulture));
                }
            }
        }

    }
}
