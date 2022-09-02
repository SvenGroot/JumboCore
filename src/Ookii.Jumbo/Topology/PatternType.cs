// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// The type of patterns used by a <see cref="PatternTopologyResolver"/>.
    /// </summary>
    public enum PatternType
    {
        /// <summary>
        /// The patterns are regular expressions.
        /// </summary>
        RegularExpression,
        /// <summary>
        /// The pattersn are range expressions (see <see cref="RangeExpression"/>).
        /// </summary>
        RangeExpression
    }
}
