using Microsoft.AspNetCore.Html;
using Ookii;
using Ookii.Jumbo;

namespace DfsWeb
{
    public static class Utility
    {
        public static HtmlString FormatSize(long bytes)
        {
            if (bytes < BinarySize.Kibi)
            {
                return new HtmlString($"{bytes:#,0} bytes");
            }

            return new HtmlString($"<abbr title=\"{bytes:#,0} bytes\">{(BinarySize)bytes:#,0.# SB}</abbr>");
        }
    }
}
