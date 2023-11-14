using System;

namespace Ookii.Jumbo;

/// <summary>
/// Extension methods for the <see cref="Int128"/> and <see cref="UInt128"/> structures.
/// </summary>
public static class Int128Extensions
{
    /// <summary>
    /// Gets the high 64 bits of a <see cref="UInt128"/> value.
    /// </summary>
    /// <param name="self">The <see cref="UInt128"/> value.</param>
    /// <returns>The high 64 bits.</returns>
    [CLSCompliant(false)]
    public static ulong High64(this UInt128 self) => (ulong)(self >> 64);

    /// <summary>
    /// Gets the low 64 bits of a <see cref="UInt128"/> value.
    /// </summary>
    /// <param name="self">The <see cref="UInt128"/> value.</param>
    /// <returns>The low 64 bits.</returns>
    [CLSCompliant(false)]
    public static ulong Low64(this UInt128 self) => (ulong)(self & ulong.MaxValue);

    /// <summary>
    /// Gets the high 64 bits of a <see cref="Int128"/> value.
    /// </summary>
    /// <param name="self">The <see cref="Int128"/> value.</param>
    /// <returns>The high 64 bits.</returns>
    [CLSCompliant(false)]
    public static ulong High64(this Int128 self) => (ulong)(self >> 64);

    /// <summary>
    /// Gets the low 64 bits of a <see cref="Int128"/> value.
    /// </summary>
    /// <param name="self">The <see cref="Int128"/> value.</param>
    /// <returns>The low 64 bits.</returns>
    [CLSCompliant(false)]
    public static ulong Low64(this Int128 self) => (ulong)(self & ulong.MaxValue);
}
