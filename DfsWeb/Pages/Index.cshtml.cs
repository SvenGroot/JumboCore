using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo;

namespace DfsWeb.Pages
{
    public class IndexModel : PageModel
    {
        public void OnGet()
        {

        }

        private static string FormatSize(long bytes)
        {
            if (bytes < BinarySize.Kilobyte)
            {
                return string.Format("{0:#,0} bytes", bytes);
            }
            return string.Format("<abbr title=\"{1:#,0} bytes\">{0:#,0.# SB}</abbr>", (BinarySize)bytes, bytes);
        }

    }
}