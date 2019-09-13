// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;

namespace Ookii.Jumbo
{
    static class BinarySizeFormatter
    {
        private static Regex _formatRegex = new Regex(@"(?<before>\s*)(?<prefix>[ASKMGTP])?(?<iec>i?)(?<after>B?\s*)$", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

        public static string Format(BinarySize value, string format, IFormatProvider provider)
        {
            string before = null;
            string realPrefix;
            string after = null;
            string numberFormat = null;
            long factor;

            // Must support the "G" specifier, required by IFormattable
            if( string.IsNullOrEmpty(format) || string.Equals(format, "g", StringComparison.OrdinalIgnoreCase) )
            {
                factor = DetermineAutomaticScalingFactor(value, false, out realPrefix);
                after = "B";
            }
            else
            {
                Match m = _formatRegex.Match(format);
                if( !m.Success )
                    throw new FormatException("Invalid format string.");

                before = m.Groups["before"].Value;
                string prefix = m.Groups["prefix"].Success ? m.Groups["prefix"].Value : null;
                string iec = m.Groups["iec"].Value;
                after = m.Groups["after"].Value;
                numberFormat = format.Substring(0, m.Index);

                if( prefix == null )
                {
                    realPrefix = null;
                    factor = BinarySize.Byte;
                }
                else if( prefix == "A" || prefix == "a" )
                    factor = DetermineAutomaticScalingFactor(value, false, out realPrefix);
                else if( prefix == "S" || prefix == "s" )
                    factor = DetermineAutomaticScalingFactor(value, true, out realPrefix);
                else
                {
                    realPrefix = prefix;
                    factor = BinarySize.GetUnitScalingFactor(prefix);
                }

                if( prefix != null && char.IsLower(prefix, 0) )
                    realPrefix = realPrefix.ToLower(CultureInfo.CurrentCulture);

                if( factor > 1 )
                    realPrefix += iec;
            }

            return (value.Value / (decimal)factor).ToString(numberFormat, provider) + before + realPrefix + after;
        }

        private static long DetermineAutomaticScalingFactor(BinarySize value, bool allowRounding, out string prefix)
        {
            if( value >= BinarySize.Petabyte && (allowRounding || value.Value % BinarySize.Petabyte == 0) )
            {
                prefix = "P";
                return BinarySize.Petabyte;
            }
            else if( value >= BinarySize.Terabyte && (allowRounding || value.Value % BinarySize.Terabyte == 0) )
            {
                prefix = "T";
                return BinarySize.Terabyte;
            }
            else if( value >= BinarySize.Gigabyte && (allowRounding || value.Value % BinarySize.Gigabyte == 0) )
            {
                prefix = "G";
                return BinarySize.Gigabyte;
            }
            else if( value >= BinarySize.Megabyte && (allowRounding || value.Value % BinarySize.Megabyte == 0) )
            {
                prefix = "M";
                return BinarySize.Megabyte;
            }
            else if( value >= BinarySize.Kilobyte && (allowRounding || value.Value % BinarySize.Kilobyte == 0) )
            {
                prefix = "K";
                return BinarySize.Kilobyte;
            }
            else
            {
                prefix = "";
                return BinarySize.Byte;
            }
        }
    }
}
