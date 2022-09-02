using System.ComponentModel;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Provides data for the <see cref="MultiInputRecordReader{T}.CurrentPartitionChanging"/> event.
    /// </summary>
    public sealed class CurrentPartitionChangingEventArgs : CancelEventArgs
    {
        private readonly int _newPartitionNumber;

        /// <summary>
        /// Initializes a new instance of the <see cref="CurrentPartitionChangingEventArgs"/> class.
        /// </summary>
        /// <param name="newPartitionNumber">The new partition number.</param>
        public CurrentPartitionChangingEventArgs(int newPartitionNumber)
        {
            _newPartitionNumber = newPartitionNumber;
        }

        /// <summary>
        /// Gets the value that <see cref="MultiInputRecordReader{T}.CurrentPartition"/> will be changed to.
        /// </summary>
        /// <value>The new value of <see cref="MultiInputRecordReader{T}.CurrentPartition"/>.</value>
        public int NewPartitionNumber
        {
            get { return _newPartitionNumber; }
        }
    }
}
