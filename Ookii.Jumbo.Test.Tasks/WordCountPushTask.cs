// $Id$
//
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class WordCountPushTask : PushTask<Utf8String, Pair<Utf8String, int>>
    {
        private Pair<Utf8String, int> _record = Pair.MakePair(new Utf8String(), 1);
        private char[] _separator = new[] { ' ' };

        public override void ProcessRecord(Utf8String record, RecordWriter<Pair<Utf8String, int>> output)
        {
            string[] words = record.ToString().Split(_separator, StringSplitOptions.RemoveEmptyEntries);
            foreach( string word in words )
            {
                _record.Key.Set(word);
                output.WriteRecord(_record);
            }
        }

        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();
            if( !TaskContext.StageConfiguration.AllowOutputRecordReuse )
                throw new NotSupportedException("Output record reuse required.");
        }
    }
}
