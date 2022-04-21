using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Pages
{
    public class SetSafeModeModel : PageModel
    {
        public bool SafeMode { get; set; }

        [BindProperty]
        public bool NewSafeMode { get; set; }

        public string ErrorMessage { get; set; }

        public void OnGet()
        {
            DfsClient client = (DfsClient)FileSystemClient.Create();
            SafeMode = client.NameServer.SafeMode;
        }

        public void OnPost()
        {
            try
            {
                DfsClient client = (DfsClient)FileSystemClient.Create();
                SafeMode = client.NameServer.SafeMode;
                client.NameServer.SafeMode = NewSafeMode;
                SafeMode = NewSafeMode;
            }
            catch (InvalidOperationException ex)
            {
                ErrorMessage = ex.Message;
            }
        }
    }
}
