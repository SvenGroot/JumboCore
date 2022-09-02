using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Web;
using Microsoft.AspNetCore.Html;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet;

namespace JetWeb.Pages
{
    public class LogFileModel : PageModel
    {
        [BindProperty(SupportsGet = true)]
        public string TaskServer { get; set; }

        [BindProperty(SupportsGet = true)]
        public int Port { get; set; }

        [BindProperty(SupportsGet = true)]
        public string MaxSize { get; set; }

        [BindProperty(SupportsGet = true)]
        public string Kind { get; set; }

        [BindProperty(SupportsGet = true)]
        public string TaskId { get; set; }

        [BindProperty(SupportsGet = true)]
        public Guid JobId { get; set; }

        [BindProperty(SupportsGet = true)]
        public int Attempt { get; set; }

        [BindProperty(SupportsGet = true)]
        public bool Profile { get; set; }

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
                maxSize = (int)BinarySize.Parse(MaxSize, CultureInfo.InvariantCulture);

            string log;
            if (TaskServer == null)
            {
                var client = new JetClient();
                var metrics = client.JobServer.GetMetrics();
                ViewData["Title"] = $"Job server {metrics.JobServer} log file";
                log = client.JobServer.GetLogFileContents(logKind, maxSize);
            }
            else
            {
                var client = JetClient.CreateTaskServerClient(new ServerAddress(TaskServer, Port));
                if (TaskId == null)
                {
                    ViewData["Title"] = $"Task server {TaskServer}:{Port} log file";
                    log = client.GetLogFileContents(logKind, maxSize);
                }
                else
                {
                    if (Profile)
                    {
                        log = client.GetTaskProfileOutput(JobId, new TaskAttemptId(new TaskId(TaskId), Attempt));
                        ViewData["Title"] = $"Task {JobId:B}_{TaskId}_{Attempt} profile output (on {TaskServer}) - Jumbo Jet";
                    }
                    else
                    {
                        log = client.GetTaskLogFileContents(JobId, new TaskAttemptId(new TaskId(TaskId), Attempt), maxSize);
                        ViewData["Title"] = $"Task {JobId:B}_{TaskId}_{Attempt} log file (on {TaskServer}) - Jumbo Jet";
                    }
                }
            }

            LogFileContents = FormatLogFile(log);
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
                        result.AppendFormat(CultureInfo.CurrentCulture, "<span class=\"logWarning\">{0}</span>", HttpUtility.HtmlEncode(line));
                    else if (line.Contains(" ERROR ", StringComparison.Ordinal))
                        result.AppendFormat(CultureInfo.CurrentCulture, "<span class=\"logError\">{0}</span>", HttpUtility.HtmlEncode(line));
                    else
                        result.Append(HttpUtility.HtmlEncode(line));

                    result.AppendLine();
                }
            }

            return new HtmlString(result.ToString());
        }
    }
}
