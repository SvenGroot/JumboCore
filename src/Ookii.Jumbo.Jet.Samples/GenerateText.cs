using System;
using System.ComponentModel;
using System.Linq;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Samples;

/// <summary>
/// Generates random text data.
/// </summary>
[GeneratedParser]
[Description("Generates random text data of the specified size.")]
public partial class GenerateText : JobBuilderJob
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenerateText));

    private static readonly Utf8String _space = new Utf8String(" ");

    /// <summary>
    /// Gets or sets the output path.
    /// </summary>
    /// <value>
    /// The output path.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The output directory on the Jumbo DFS where the generated data will be written.")]
    public required string OutputPath { get; set; }

    /// <summary>
    /// Gets or sets the task count.
    /// </summary>
    /// <value>
    /// The task count.
    /// </value>
    [CommandLineArgument(IsPositional = true, DefaultValue = 1), Description("The number of generator tasks to use.")]
    public int TaskCount { get; set; }

    /// <summary>
    /// Gets or sets the size per task.
    /// </summary>
    /// <value>
    /// The size per task.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The size of the data to generate per task. Specify zero to use the DFS block size."), JobSetting]
    public BinarySize SizePerTask { get; set; }

    /// <summary>
    /// Gets or sets the number of words per line.
    /// </summary>
    /// <value>
    /// The words per line.
    /// </value>
    [CommandLineArgument(DefaultValue = 10), Description("The number of words per line."), JobSetting]
    public int WordsPerLine { get; set; }

    /// <summary>
    /// Gets or sets the amount by which the number of words by line can be varied.
    /// </summary>
    /// <value>
    /// The words per line randomization.
    /// </value>
    [CommandLineArgument(DefaultValue = 5), Description("The amount by which the number of words per line will be varied."), JobSetting]
    public int WordsPerLineRandomization { get; set; }

    /// <summary>
    /// Constructs the job configuration using the specified job builder.
    /// </summary>
    /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
    protected override void BuildJob(JobBuilder job)
    {
        if (SizePerTask == BinarySize.Zero)
        {
            SizePerTask = BlockSize;
            if (SizePerTask == BinarySize.Zero)
            {
                SizePerTask = (BinarySize?)FileSystemClient.DefaultBlockSize ?? BinarySize.FromMebi(64); // Default to 64MB if the file system doesn't support blocks
            }
        }

        // This is an example of how you can use an instance method of a serializable type to communicate state, instead of using individual settings.
        var generated = job.Generate<Utf8String>(TaskCount, Generate);
        WriteOutput(generated, OutputPath, typeof(TextRecordWriter<>));
    }

    /// <summary>
    /// Generates the text.
    /// </summary>
    /// <param name="output">The output.</param>
    /// <param name="context">The context.</param>
    public static void Generate(RecordWriter<Utf8String> output, ProgressContext context)
    {
        long sizePerTask = context.TaskContext!.GetSetting("GenerateText.SizePerTask", BinarySize.Zero).Value;
        int wordsPerLine = context.TaskContext.GetSetting("GenerateText.WordsPerLine", 10);
        int wordsPerLineRandomization = context.TaskContext.GetSetting("GenerateText.WordsPerLineRandomization", 5);

        Utf8String[] words = LoadWords();

        Random rnd = new Random();
        Utf8String line = new Utf8String();
        GenerateLine(rnd, line, words, wordsPerLine + rnd.Next(wordsPerLineRandomization));
        int lines = 0;
        while (output.OutputBytes + line.ByteLength + Environment.NewLine.Length < sizePerTask)
        {
            context.Progress = (float)output.OutputBytes / (float)sizePerTask;
            output.WriteRecord(line);
            ++lines;
            GenerateLine(rnd, line, words, wordsPerLine + rnd.Next(wordsPerLineRandomization));
        }

        _log.InfoFormat("Written {0} lines of text, size {1}", lines, output.OutputBytes);
    }

    private static void GenerateLine(Random rnd, Utf8String line, Utf8String[] words, int numWords)
    {
        line.ByteLength = 0;
        for (int x = 0; x < numWords; ++x)
        {
            if (x > 0)
            {
                line.Append(_space);
            }

            Utf8String word = words[rnd.Next(words.Length)];
            line.Append(word);
        }
    }

    private static Utf8String[] LoadWords()
    {
        return Properties.Resources.Words.Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries).Select(w => new Utf8String(w)).ToArray();
    }
}
