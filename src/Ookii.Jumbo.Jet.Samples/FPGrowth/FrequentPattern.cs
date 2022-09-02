// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// A frequent pattern found by the <see cref="PFPGrowth"/> job.
    /// </summary>
    public class FrequentPattern : IWritable
    {
        private WritableCollection<Utf8String> _items = new WritableCollection<Utf8String>();

        /// <summary>
        /// Initializes a new instance of the <see cref="FrequentPattern"/> class.
        /// </summary>
        /// <param name="items">The items. May be <see langword="null" />.</param>
        /// <param name="support">The support.</param>
        public FrequentPattern(IEnumerable<Utf8String> items, int support)
        {
            if (items != null)
                _items.AddRange(items);

            Support = support;
        }

        /// <summary>
        /// Gets the items of the pattern.
        /// </summary>
        /// <value>The items.</value>
        public WritableCollection<Utf8String> Items
        {
            get { return _items; }
        }

        /// <summary>
        /// Gets or sets the support of the pattern.
        /// </summary>
        /// <value>The support.</value>
        public int Support { get; set; }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format("{{{0}:{1}}}", Items.ToDelimitedString(","), Support);
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            _items.Write(writer);
            writer.Write(Support);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            if (_items == null)
                _items = (WritableCollection<Utf8String>)FormatterServices.GetUninitializedObject(typeof(WritableCollection<Utf8String>));
            _items.Read(reader);
            Support = reader.ReadInt32();
        }
    }
}
