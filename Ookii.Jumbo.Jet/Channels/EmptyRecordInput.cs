﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class EmptyRecordInput : RecordInput
    {
        #region Nested Types

        private sealed class EmptyRecordReader<T> : RecordReader<T>
        {
            public override float Progress
            {
                get { return 1.0f; }
            }

            protected override bool ReadRecordInternal()
            {
                return false;
            }
        }
        
        #endregion

        private readonly Type _recordReaderType;
        private readonly string _sourceName;

        public EmptyRecordInput(Type recordType, string sourceName)
        {
            if( recordType == null )
                throw new ArgumentNullException("recordType");
            _recordReaderType = typeof(EmptyRecordReader<>).MakeGenericType(recordType);
            _sourceName = sourceName;
        }

        public override bool IsMemoryBased
        {
            get { return true; }
        }

        public override bool IsRawReaderSupported
        {
            get { return true; }
        }

        protected override IRecordReader CreateReader()
        {
            IRecordReader reader = (IRecordReader)Activator.CreateInstance(_recordReaderType);
            reader.SourceName = _sourceName;
            return reader;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Setting SourceName cannot throw.")]
        protected override RecordReader<RawRecord> CreateRawReader()
        {
            return new EmptyRecordReader<RawRecord>() { SourceName = _sourceName };
        }
    }
}
