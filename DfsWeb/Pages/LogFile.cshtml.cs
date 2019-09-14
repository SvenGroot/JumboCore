using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Html;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Pages
{
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

            int maxSize = Int32.MaxValue;
            if (MaxSize != null)
                maxSize = (int)BinarySize.Parse(MaxSize);

            if (DataServer == null)
            {
                DfsClient client = (DfsClient)FileSystemClient.Create();
                DfsMetrics metrics = client.NameServer.GetMetrics();
                ViewData["Title"] = string.Format("Name server {0} log file", metrics.NameServer);
                string log = client.NameServer.GetLogFileContents(logKind, maxSize);
                LogFileContents = FormatLogFile(log);
            }
            else
            {
                ViewData["Title"] = string.Format("Data server {0}:{1} log file", DataServer, Port);
                LogFileContents = FormatLogFile(DfsClient.GetDataServerLogFileContents(DataServer, Port, logKind, maxSize));
            }
        }

        private HtmlString FormatLogFile(string log)
        {
            StringBuilder result = new StringBuilder(log.Length);
            using (StringReader reader = new StringReader(log))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.Contains(" WARN "))
                        result.AppendFormat("<span class=\"logWarning\">{0}</span>", HttpUtility.HtmlEncode(line));
                    else if (line.Contains(" ERROR "))
                        result.AppendFormat("<span class=\"logError\">{0}</span>", HttpUtility.HtmlEncode(line));
                    else
                        result.Append(HttpUtility.HtmlEncode(line));

                    result.AppendLine();
                }
            }

            return new HtmlString(result.ToString());
        }
    }
}