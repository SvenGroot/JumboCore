// Copyright (c) Sven Groot (Ookii.org)
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// A 128 bit unsigned integer, based on the code provided with gensort, see http://www.hpl.hp.com/hosted/sortbenchmark/.
    /// </summary>
    [ValueWriter(typeof(UInt128Writer))]
    public struct UInt128
    {
        private readonly ulong _high64;
        private readonly ulong _low64;

        private static ulong[] _hi2loQuot = new ulong[] {
            0UL,
            1844674407370955161UL,
            3689348814741910323UL,
            5534023222112865484UL,
            7378697629483820646UL,
            9223372036854775808UL,
            11068046444225730969UL,
            12912720851596686131UL,
            14757395258967641292UL,
            16602069666338596454UL
        };

        private static int[] _hi2loMod = new int[] {
            0,
            6,
            2,
            8,
            4,
            0,
            6,
            2,
            8,
            4
        };

        /// <summary>
        /// A <see cref="UInt128"/> with the value zero.
        /// </summary>
        public static readonly UInt128 Zero = new UInt128();

        /// <summary>
        /// Initializes a new instance of the <see cref="UInt128"/> struct with the specified high and low bits.
        /// </summary>
        /// <param name="high64">The high 64 bits of the value.</param>
        /// <param name="low64">The low 64 bits of the value.</param>
        public UInt128(ulong high64, ulong low64)
        {
            _high64 = high64;
            _low64 = low64;
        }

        /// <summary>
        /// Gets the high 64 bits of the value.
        /// </summary>
        public ulong High64
        {
            get { return _high64; }
        }

        /// <summary>
        /// Gets the low 64 bits of the value.
        /// </summary>
        public ulong Low64
        {
            get { return _low64; }
        }

        /// <summary>
        /// Tests this instance for equality with the specified object.
        /// </summary>
        /// <param name="obj">The object to test for equality.</param>
        /// <returns><see langword="true"/> if this instance is equal to <paramref name="obj"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object? obj)
        {
            if (!(obj is UInt128))
                return false;
            UInt128 other = (UInt128)obj;
            return this == other;
        }

        /// <summary>
        /// Returns a decimal string representation of the <see cref="UInt128"/>.
        /// </summary>
        /// <returns>A decimal string representation of the <see cref="UInt128"/>.</returns>
        public override string ToString()
        {
            ulong hi8 = High64;
            ulong lo8 = Low64;
            int himod;
            int lomod;
            char[] temp = new char[39];
            int digit = 0;

            while (hi8 != 0)
            {
                himod = (int)(hi8 % 10);
                hi8 /= 10;
                lomod = (int)(lo8 % 10);
                lo8 /= 10;

                lo8 += _hi2loQuot[himod];
                lomod += _hi2loMod[himod];

                if (lomod >= 10)       /* if adding to 2 mods caused a "carry" */
                {
                    lomod -= 10;
                    lo8 += 1;
                }
                temp[digit++] = (char)('0' + lomod);
            }
            string lowString = lo8.ToString();
            StringBuilder result = new StringBuilder(lowString.Length + digit);
            result.Append(lowString);
            /* concatenate low order digits computed before hi8 was reduced to 0 */
            while (digit > 0)
                result.Append(temp[--digit]);
            return result.ToString();
        }

        /// <summary>
        /// Returns a hexadecimal string representation of the <see cref="UInt128"/>.
        /// </summary>
        /// <returns>A hexadecimal string representation of the <see cref="UInt128"/>.</returns>
        public string ToHexString()
        {
            if (High64 != 0)
                return High64.ToString("x") + Low64.ToString("x");
            else
                return Low64.ToString("x");
        }

        /// <summary>
        /// Returns a 32 bit hash code for this <see cref="UInt128"/>.
        /// </summary>
        /// <returns>A 32 bit hash code for this <see cref="UInt128"/>.</returns>
        public override int GetHashCode()
        {
            return High64.GetHashCode() ^ Low64.GetHashCode();
        }

        /// <summary>
        /// Tests two instances of <see cref="UInt128"/> for equality
        /// </summary>
        /// <param name="left">The first <see cref="UInt128"/>.</param>
        /// <param name="right">The second <see cref="UInt128"/>.</param>
        /// <returns><see langword="true"/> if the two instances are equal; otherwise, <see langword="false"/>.</returns>
        public static bool operator ==(UInt128 left, UInt128 right)
        {
            return left.High64 == right.High64 && left.Low64 == right.Low64;
        }

        /// <summary>
        /// Tests two instances of <see cref="UInt128"/> for inequality
        /// </summary>
        /// <param name="left">The first <see cref="UInt128"/>.</param>
        /// <param name="right">The second <see cref="UInt128"/>.</param>
        /// <returns><see langword="true"/> if the two instances are not equal; otherwise, <see langword="false"/>.</returns>
        public static bool operator !=(UInt128 left, UInt128 right)
        {
            return !(left.High64 == right.High64 && left.Low64 == right.Low64);
        }

        /// <summary>
        /// Increments the value of the specified instance by one.
        /// </summary>
        /// <param name="value">The <see cref="UInt128"/> to increment.</param>
        /// <returns>The incremented value.</returns>
        public static UInt128 operator ++(UInt128 value)
        {
            ulong sumLow = value.Low64 + 1;
            ulong sumHigh = sumLow == 0 ? value.High64 + 1 : value.High64;
            return new UInt128(sumHigh, sumLow);
        }

        /// <summary>
        /// Adds two <see cref="UInt128"/> values.
        /// </summary>
        /// <param name="left">The first <see cref="UInt128"/>.</param>
        /// <param name="right">The second <see cref="UInt128"/>.</param>
        /// <returns>The result of the addition.</returns>
        public static UInt128 operator +(UInt128 left, UInt128 right)
        {
            ulong sumLow;
            ulong sumHigh;

            ulong resultHighBit;
            ulong highBit0;
            ulong highBit1;

            sumHigh = left.High64 + right.High64;

            highBit0 = (left.Low64 & 0x8000000000000000L);
            highBit1 = (right.Low64 & 0x8000000000000000L);
            sumLow = left.Low64 + right.Low64;
            resultHighBit = (sumLow & 0x8000000000000000L);
            if ((highBit0 & highBit1) != 0L || ((highBit0 ^ highBit1) != 0L && resultHighBit == 0L))
                ++sumHigh; // add carry
            return new UInt128(sumHigh, sumLow);
        }

        /// <summary>
        /// Multiplies two <see cref="UInt128"/> values.
        /// </summary>
        /// <param name="left">The first <see cref="UInt128"/>.</param>
        /// <param name="right">The second <see cref="UInt128"/>.</param>
        /// <returns>The result of the multiplication.</returns>
        public static UInt128 operator *(UInt128 left, UInt128 right)
        {
            ulong productHigh, productLow;
            ulong ahi4, alow4, bhi4, blow4, temp;
            ulong reshibit, hibit0, hibit1;

            productHigh = 0;

            ahi4 = left.Low64 >> 32;        /* get hi 4 bytes of the low 8 bytes */
            alow4 = (left.Low64 & 0xFFFFFFFFL);  /* get low 4 bytes of the low 8 bytes */
            bhi4 = right.Low64 >> 32;        /* get hi 4 bytes of the low 8 bytes */
            blow4 = (right.Low64 & 0xFFFFFFFFL);  /* get low 4 bytes of the low 8 bytes */

            /* assign 8-byte product of the lower 4 bytes of "a" and the lower 4 bytes
             * of "b" to the lower 8 bytes of the result product.
             */
            productLow = alow4 * blow4;

            temp = ahi4 * blow4; /* mult high 4 bytes of "a" by low 4 bytes of "b" */
            productHigh += temp >> 32; /* add high 4 bytes to high 8 result bytes*/
            temp <<= 32;     /* get lower half ready to add to lower 8 result bytes */
            hibit0 = (temp & 0x8000000000000000L);
            hibit1 = (productLow & 0x8000000000000000L);
            productLow += temp;
            reshibit = (productLow & 0x8000000000000000L);
            if ((hibit0 & hibit1) != 0L || ((hibit0 ^ hibit1) != 0L && reshibit == 0L))
                productHigh++;  /* add carry bit */

            temp = alow4 * bhi4; /* mult low 4 bytes of "a" by high 4 bytes of "b" */
            productHigh += temp >> 32; /* add high 4 bytes to high 8 result bytes*/
            temp <<= 32;     /* get lower half ready to add to lower 8 result bytes */
            hibit0 = (temp & 0x8000000000000000L);
            hibit1 = (productLow & 0x8000000000000000L);
            productLow += temp;
            reshibit = (productLow & 0x8000000000000000L);
            if ((hibit0 & hibit1) != 0L || ((hibit0 ^ hibit1) != 0L && reshibit == 0L))
                productHigh++;  /* add carry bit */

            productHigh += ahi4 * bhi4;
            productHigh += left.Low64 * right.High64;
            productHigh += left.High64 * right.Low64;
            return new UInt128(productHigh, productLow);
        }
    }
}
