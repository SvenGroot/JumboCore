// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace DfsShell.Commands
{
    abstract class DfsShellCommandWithProgress : DfsShellCommand
    {
        private string _previousFileName;

        protected void PrintProgress(string fileName, int progressPercentage, long progressBytes)
        {
            if (_previousFileName != fileName)
            {
                Console.WriteLine();
                Console.WriteLine("{0}:", fileName);
                _previousFileName = fileName;
            }
            var progressBytesString = progressBytes.ToString("#,0", System.Globalization.CultureInfo.CurrentCulture);
            var width = Console.WindowWidth - 9 - Math.Max(15, progressBytesString.Length);
            if (width < 0)
                width = 0; // mainly useful is console.windowwidth couldn't be determined.
            var progressWidth = (int)(progressPercentage / 100.0f * width);
            var progressBar = new string('=', progressWidth); ;
            if (progressWidth < width)
            {
                progressBar += ">" + new string(' ', width - progressWidth - 1);
            }
            Console.Write("\r{0,3}% [{1}] {2}", progressPercentage, progressBar, progressBytesString);
        }
    }
}
