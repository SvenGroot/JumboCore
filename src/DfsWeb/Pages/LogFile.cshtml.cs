using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Web;
using Microsoft.AspNetCore.Html;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Pages;

public class LogFileModel : PageModel
{
    [BindProperty(SupportsGet = true)]
    public string DataServer { get; set; }

    [BindProperty(SupportsGet = true)]
    public int Port { get; set; }

    [BindProperty(SupportsGet = true)]
    public string MaxSize { get; set; }

    [BindProperty(SupportsGet = true)]
    public string Kind { get; set; }

    public HtmlString LogFileContents { get; set; }

    public void OnGet()
    {
        LogFileKind logKind;
        switch (Kind)
        {
        case "out":
            logKind = LogFileKind.StdOut;
            break;

        case "err":
            logKind = LogFileKind.StdErr;
            break;

        default:
            logKind = LogFileKind.Log;
            break;
        }

        var maxSize = Int32.MaxValue;
        if (MaxSize != null)
        {
            maxSize = (int)BinarySize.Parse(MaxSize, CultureInfo.InvariantCulture);
        }

        if (DataServer == null)
        {
            var client = (DfsClient)FileSystemClient.Create();
            var metrics = client.NameServer.GetMetrics();
            ViewData["Title"] = $"Name server {metrics.NameServer} log file";
            var log = client.NameServer.GetLogFileContents(logKind, maxSize);
            LogFileContents = FormatLogFile(log);
        }
        else
        {
            ViewData["Title"] = $"Data server {DataServer}:{Port} log file";
            LogFileContents = FormatLogFile(DfsClient.GetDataServerLogFileContents(DataServer, Port, logKind, maxSize));
        }
    }

    private static HtmlString FormatLogFile(string log)
    {
        var result = new StringBuilder(log.Length);
        using (var reader = new StringReader(log))
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                if (line.Contains(" WARN ", StringComparison.Ordinal))
                {
                    result.AppendFormat(CultureInfo.CurrentCulture, "<span class=\"logWarning\">{0}</span>", HttpUtility.HtmlEncode(line));
                }
                else if (line.Contains(" ERROR ", StringComparison.Ordinal))
                {
                    result.AppendFormat(CultureInfo.CurrentCulture, "<span class=\"logError\">{0}</span>", HttpUtility.HtmlEncode(line));
                }
                else
                {
                    result.Append(HttpUtility.HtmlEncode(line));
                }

                result.AppendLine();
            }
        }

        return new HtmlString(result.ToString());
    }
}
