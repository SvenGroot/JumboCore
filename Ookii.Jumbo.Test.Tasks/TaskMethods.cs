using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Tasks;
using System;
using System.Collections.Generic;
using System.Text;

namespace Ookii.Jumbo.Test.Tasks
{
    public static class TaskMethods
    {
        public static void ProcessRecords(RecordReader<Utf8String> input, RecordWriter<int> output, TaskContext context)
        {
        }

        public static void ProcessRecordsNoContext(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
        }

        public static int AccumulateRecords(Utf8String key, int value, int newValue, TaskContext context)
        {
            return value + newValue;
        }

        public static int AccumulateRecordsNoContext(Utf8String key, int value, int newValue)
        {
            return value + newValue;
        }

        public static void MapRecords(Utf8String record, RecordWriter<Pair<Utf8String, int>> output, TaskContext context)
        {
        }

        public static void MapRecordsNoContext(Utf8String record, RecordWriter<Pair<Utf8String, int>> output)
        {
        }

        public static void ReduceRecords(Utf8String key, IEnumerable<int> values, RecordWriter<int> output, TaskContext context)
        {
        }

        public static void ReduceRecordsNoContext(Utf8String key, IEnumerable<int> values, RecordWriter<int> output)
        {
        }

        public static void GenerateRecords(RecordWriter<int> output, TaskContext context)
        {
        }

        public static void GenerateRecordsProgressContext(RecordWriter<int> output, ProgressContext context)
        {
        }

        public static void GenerateRecordsNoContext(RecordWriter<int> output)
        {
        }

        public static void CombineRecords(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output, TaskContext context)
        {
        }

        public static void CombineRecordsNoContext(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
        {
        }
    }
}
