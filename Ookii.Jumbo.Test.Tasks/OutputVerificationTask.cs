// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Test.Tasks
{
    public class OutputVerificationTask : Configurable, ITask<int, bool>
    {
        public const string StageToVerifySettingName = "Test.StageToVerify";

        public void Run(RecordReader<int> input, RecordWriter<bool> output)
        {
            FileSystemClient client = FileSystemClient.Create(DfsConfiguration);
            string stageId = TaskContext.GetSetting(StageToVerifySettingName, (string)null);
            StageConfiguration stage = TaskContext.JobConfiguration.GetStage(stageId);
            bool result = true;
            for (int x = 0; x < stage.TaskCount; ++x)
            {
                if (client.GetFileInfo(FileDataOutput.GetOutputPath(stage, x + 1)) == null)
                {
                    result = false;
                    break;
                }
            }
            output.WriteRecord(result);
        }
    }
}
