// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Interface used by multi input record readers that read data from a channel.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This interface can be used by multi input record readers that need to know what input channel
    ///   they are reading from.
    /// </para>
    /// <para>
    ///   If a multi input record reader implements this interface, Jumbo Jet will set the <see cref="Channel"/>
    ///   property after the record reader is created. The <see cref="Channel"/> property will only
    ///   be set if the reader is reading from exactly one channel.
    /// </para>
    /// <para>
    ///   If the record reader also implements <see cref="IConfigurable"/>, <see cref="IConfigurable.NotifyConfigurationChanged"/>
    ///   will be called after the <see cref="Channel"/> property is set.
    /// </para>
    /// </remarks>
    public interface IChannelMultiInputRecordReader
    {
        /// <summary>
        /// Gets or sets the input channel that this reader is reading from.
        /// </summary>
        /// <value>The channel.</value>
        IInputChannel Channel { get; set; }
    }
}
