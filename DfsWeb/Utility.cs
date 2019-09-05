using Microsoft.AspNetCore.Html;
using Ookii.Jumbo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DfsWeb
{
    public static class Utility
    {
        public static HtmlString FormatSize(long bytes)
        {
            if (bytes < BinarySize.Kilobyte)
            {
                return new HtmlString(string.Format("{0:#,0} bytes", bytes));
            }

            return new HtmlString(string.Format("<abbr title=\"{1:#,0} bytes\">{0:#,0.# SB}</abbr>", (BinarySize)bytes, bytes));
        }
    }
}
