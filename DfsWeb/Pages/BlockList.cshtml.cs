using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Pages
{
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
            DfsClient client = (DfsClient)FileSystemClient.Create();
            Guid[] blocks;
            if (DataServer == null)
            {
                ViewData["Title"] = string.Format("Block list ({0})", Kind);
                blocks = client.NameServer.GetBlocks(Kind);
                NewQueryString = "kind=" + Kind.ToString();
            }
            else
            {
                ServerAddress address = new ServerAddress(DataServer, Port);
                ViewData["Title"] = string.Format("Block list for {0}", address);
                blocks = client.NameServer.GetDataServerBlocks(address);
                NewQueryString = string.Format("dataServer={0}&port={1}", DataServer, Port);
            }

            if (blocks != null)
            {
                foreach (Guid blockId in blocks)
                {
                    if (IncludeFiles)
                        Blocks.Add(string.Format("{0:B}: {1}", blockId, client.NameServer.GetFileForBlock(blockId)));
                    else
                        Blocks.Add(blockId.ToString("B"));
                }
            }

        }
    }
}