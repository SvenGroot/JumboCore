// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Abstract base class for the data sent by a DataServer to a NameServer during a heartbeat
    /// </summary>
    [ValueWriter(typeof(PolymorphicValueWriter<HeartbeatData>))]
    [WritableDerivedType(typeof(InitialHeartbeatData))]
    [WritableDerivedType(typeof(StatusHeartbeatData))]
    [WritableDerivedType(typeof(BlockReportHeartbeatData))]
    [WritableDerivedType(typeof(NewBlockHeartbeatData))]
    public abstract class HeartbeatData : IWritable
    {
        /// <inheritdoc/>
        public abstract void Read(BinaryReader reader);

        /// <inheritdoc/>
        public abstract void Write(BinaryWriter writer);
    }
}
