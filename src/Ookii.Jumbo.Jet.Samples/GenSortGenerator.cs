// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.Jet.Samples.IO;

namespace Ookii.Jumbo.Jet.Samples
{
    class GenSortGenerator
    {
        private const int _recordSize = 100;
        private GenSortRecord _record = new GenSortRecord();

        public IEnumerable<GenSortRecord> GenerateRecords(UInt128 startRecord, ulong count)
        {
            Random128 rnd = new Random128(startRecord);
            UInt128 recordNumber = startRecord;
            for (ulong x = 0; x < count; ++x)
            {
                GenerateAsciiRecord(rnd.Next(), recordNumber);
                yield return _record;
                ++recordNumber;
            }
        }

        private void GenerateAsciiRecord(UInt128 random, UInt128 recordNumber)
        {
            int i;
            ulong temp;

            /* generate the 10-byte ascii key using mostly the high 64 bits.
             */
            temp = random.High64();
            _record.RecordBuffer[0] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[1] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[2] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[3] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[4] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[5] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[6] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[7] = (byte)(' ' + (temp % 95));
            temp = random.Low64();
            _record.RecordBuffer[8] = (byte)(' ' + (temp % 95));
            temp /= 95;
            _record.RecordBuffer[9] = (byte)(' ' + (temp % 95));
            temp /= 95;

            /* add 2 bytes of "break" */
            _record.RecordBuffer[10] = (byte)' ';
            _record.RecordBuffer[11] = (byte)' ';

            /* convert the 128-bit record number to 32 bits of ascii hexadecimal
             * as the next 32 bytes of the record.
             */
            for (i = 0; i < 16; i++)
                _record.RecordBuffer[12 + i] = HexDigit((recordNumber.High64() >> (60 - 4 * i)) & 0xF);
            for (i = 0; i < 16; i++)
                _record.RecordBuffer[28 + i] = HexDigit((recordNumber.Low64() >> (60 - 4 * i)) & 0xF);

            /* add 2 bytes of "break" data */
            _record.RecordBuffer[44] = (byte)' ';
            _record.RecordBuffer[45] = (byte)' ';

            /* add 52 bytes of filler based on low 48 bits of randomom number */
            _record.RecordBuffer[46] = _record.RecordBuffer[47] = _record.RecordBuffer[48] = _record.RecordBuffer[49] =
                HexDigit((random.Low64() >> 48) & 0xF);
            _record.RecordBuffer[50] = _record.RecordBuffer[51] = _record.RecordBuffer[52] = _record.RecordBuffer[53] =
                HexDigit((random.Low64() >> 44) & 0xF);
            _record.RecordBuffer[54] = _record.RecordBuffer[55] = _record.RecordBuffer[56] = _record.RecordBuffer[57] =
                HexDigit((random.Low64() >> 40) & 0xF);
            _record.RecordBuffer[58] = _record.RecordBuffer[59] = _record.RecordBuffer[60] = _record.RecordBuffer[61] =
                HexDigit((random.Low64() >> 36) & 0xF);
            _record.RecordBuffer[62] = _record.RecordBuffer[63] = _record.RecordBuffer[64] = _record.RecordBuffer[65] =
                HexDigit((random.Low64() >> 32) & 0xF);
            _record.RecordBuffer[66] = _record.RecordBuffer[67] = _record.RecordBuffer[68] = _record.RecordBuffer[69] =
                HexDigit((random.Low64() >> 28) & 0xF);
            _record.RecordBuffer[70] = _record.RecordBuffer[71] = _record.RecordBuffer[72] = _record.RecordBuffer[73] =
                HexDigit((random.Low64() >> 24) & 0xF);
            _record.RecordBuffer[74] = _record.RecordBuffer[75] = _record.RecordBuffer[76] = _record.RecordBuffer[77] =
                HexDigit((random.Low64() >> 20) & 0xF);
            _record.RecordBuffer[78] = _record.RecordBuffer[79] = _record.RecordBuffer[80] = _record.RecordBuffer[81] =
                HexDigit((random.Low64() >> 16) & 0xF);
            _record.RecordBuffer[82] = _record.RecordBuffer[83] = _record.RecordBuffer[84] = _record.RecordBuffer[85] =
                HexDigit((random.Low64() >> 12) & 0xF);
            _record.RecordBuffer[86] = _record.RecordBuffer[87] = _record.RecordBuffer[88] = _record.RecordBuffer[89] =
                HexDigit((random.Low64() >> 8) & 0xF);
            _record.RecordBuffer[90] = _record.RecordBuffer[91] = _record.RecordBuffer[92] = _record.RecordBuffer[93] =
                HexDigit((random.Low64() >> 4) & 0xF);
            _record.RecordBuffer[94] = _record.RecordBuffer[95] = _record.RecordBuffer[96] = _record.RecordBuffer[97] =
                HexDigit((random.Low64() >> 0) & 0xF);

            /* add 2 bytes of "break" data */
            _record.RecordBuffer[98] = (byte)'\r';	/* nice for Windows */
            _record.RecordBuffer[99] = (byte)'\n';
        }

        private static byte HexDigit(ulong value)
        {
            return (byte)(value >= 10 ? 'A' + value - 10 : '0' + value);
        }
    }
}
