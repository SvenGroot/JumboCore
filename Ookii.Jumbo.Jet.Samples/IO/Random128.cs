// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// A 128-bit random generator based on the one included with gensort, see http://www.hpl.hp.com/hosted/sortbenchmark/
    /// </summary>
    class Random128
    {
        #region Nested types

        private struct GenItem
        {
            private readonly UInt128 _a;
            private readonly UInt128 _c;

            public GenItem(UInt128 a, UInt128 c)
            {
                _a = a;
                _c = c;
            }

            public UInt128 A
            {
                get { return _a; }
            }

            public UInt128 C
            {
                get { return _c; }
            }
        }

        #endregion

        #region Generator array

        private static readonly GenItem[] _gen = new[]
        {
            new GenItem(new UInt128(0x2360ed051fc65da4L, 0x4385df649fccf645L), new UInt128(0x4a696d4772617952L, 0x4950202020202001L)),
            new GenItem(new UInt128(0x17bce35bdf69743cL, 0x529ed9eb20e0ae99L), new UInt128(0x95e0e48262b3edfeL, 0x04479485c755b646L)),
            new GenItem(new UInt128(0xf4dd417327db7a9bL, 0xd194dfbe42d45771L), new UInt128(0x882a02c315362b60L, 0x765f100068b33a1cL)),
            new GenItem(new UInt128(0x6347af777a7898f6L, 0xd1a2d6f33505ffe1L), new UInt128(0x5efc4abfaca23e8cL, 0xa8edb1f2dfbf6478L)),
            new GenItem(new UInt128(0xb6a4239f3b315f84L, 0xf6ef6d3d288c03c1L), new UInt128(0xf25bd15439d16af5L, 0x94c1b1bafa6239f0L)),
            new GenItem(new UInt128(0x2c82901ad1cb0cd1L, 0x82b631ba6b261781L), new UInt128(0x89ca67c29c9397d5L, 0x9c612596145db7e0L)),
            new GenItem(new UInt128(0xdab03f988288676eL, 0xe49e66c4d2746f01L), new UInt128(0x8b6ae036713bd578L, 0xa8093c8eae5c7fc0L)),
            new GenItem(new UInt128(0x602167331d86cf56L, 0x84fe009a6d09de01L), new UInt128(0x98a2542fd23d0dbdL, 0xff3b886cdb1d3f80L)),
            new GenItem(new UInt128(0x61ecb5c24d95b058L, 0xf04c80a23697bc01L), new UInt128(0x954db923fdb7933eL, 0x947cd1edcecb7f00L)),
            new GenItem(new UInt128(0x4a5c31e0654c28aaL, 0x60474e83bf3f7801L), new UInt128(0x00be4a36657c98cdL, 0x204e8c8af7dafe00L)),
            new GenItem(new UInt128(0xae4f079d54fbece1L, 0x478331d3c6bef001L), new UInt128(0x991965329dccb28dL, 0x581199ab18c5fc00L)),
            new GenItem(new UInt128(0x101b8cb830c7cb92L, 0x7ff1ed50ae7de001L), new UInt128(0xe1a8705b63ad5b8cL, 0xd6c3d268d5cbf800L)),
            new GenItem(new UInt128(0xf54a27fc056b00e7L, 0x563f3505e0fbc001L), new UInt128(0x2b657bbfd6ed9d63L, 0x2079e70c3c97f000L)),
            new GenItem(new UInt128(0xdf8a6fc1a833d201L, 0xf98d719dd1f78001L), new UInt128(0x59b60ee4c52fa49eL, 0x9fe90682bd2fe000L)),
            new GenItem(new UInt128(0x5480a5015f101a4eL, 0xa7e3f183e3ef0001L), new UInt128(0xcc099c8803067946L, 0x4fe86aae8a5fc000L)),
            new GenItem(new UInt128(0xa498509e76e5d792L, 0x5f539c28c7de0001L), new UInt128(0x06b9abff9f9f33ddL, 0x30362c0154bf8000L)),
            new GenItem(new UInt128(0x0798a3d8b10dc72eL, 0x60121cd58fbc0001L), new UInt128(0xe296707121688d5aL, 0x0260b293a97f0000L)),
            new GenItem(new UInt128(0x1647d1e78ec02e66L, 0x5fafcbbb1f780001L), new UInt128(0x189ffc4701ff23cbL, 0x8f8acf6b52fe0000L)),
            new GenItem(new UInt128(0xa7c982285e72bf8cL, 0x0c8ddfb63ef00001L), new UInt128(0x5141110ab208fb9dL, 0x61fb47e6a5fc0000L)),
            new GenItem(new UInt128(0x3eb78ee8fb8c56dbL, 0xc5d4e06c7de00001L), new UInt128(0x3c97caa62540f294L, 0x8d8d340d4bf80000L)),
            new GenItem(new UInt128(0x72d03b6f4681f2f9L, 0xfe8e44d8fbc00001L), new UInt128(0x1b25cb9cfe5a0c96L, 0x3174f91a97f00000L)),
            new GenItem(new UInt128(0xea85f81e4f502c9bL, 0xc8ae99b1f7800001L), new UInt128(0x0c644570b4a48710L, 0x3c5436352fe00000L)),
            new GenItem(new UInt128(0x629c320db08b00c6L, 0xbfa57363ef000001L), new UInt128(0x3d0589c28869472bL, 0xde517c6a5fc00000L)),
            new GenItem(new UInt128(0xc5c4b9ce268d074aL, 0x386be6c7de000001L), new UInt128(0xbc95e5ab36477e65L, 0x534738d4bf800000L)),
            new GenItem(new UInt128(0xf30bbbbed1596187L, 0x555bcd8fbc000001L), new UInt128(0xddb02ff72a031c01L, 0x011f71a97f000000L)),
            new GenItem(new UInt128(0x4a1000fb26c9eedaL, 0x3cc79b1f78000001L), new UInt128(0x2561426086d9acdbL, 0x6c82e352fe000000L)),
            new GenItem(new UInt128(0x89fb5307f6bf8ce2L, 0xc1cf363ef0000001L), new UInt128(0x64a788e3c118ed1cL, 0x8215c6a5fc000000L)),
            new GenItem(new UInt128(0x830b7b3358a5d67eL, 0xa49e6c7de0000001L), new UInt128(0xe65ea321908627cfL, 0xa86b8d4bf8000000L)),
            new GenItem(new UInt128(0xfd8a51da91a69fe1L, 0xcd3cd8fbc0000001L), new UInt128(0x53d27225604d85f9L, 0xe1d71a97f0000000L)),
            new GenItem(new UInt128(0x901a48b642b90b55L, 0xaa79b1f780000001L), new UInt128(0xca5ec7a3ed1fe55eL, 0x07ae352fe0000000L)),
            new GenItem(new UInt128(0x118cdefdf32144f3L, 0x94f363ef00000001L), new UInt128(0x4daebb2e08533065L, 0x1f5c6a5fc0000000L)),
            new GenItem(new UInt128(0x0a88c0a91cff4308L, 0x29e6c7de00000001L), new UInt128(0x9d6f1a00a8f3f76eL, 0x7eb8d4bf80000000L)),
            new GenItem(new UInt128(0x433bef4314f16a94L, 0x53cd8fbc00000001L), new UInt128(0x158c62f2b31e496dL, 0xfd71a97f00000000L)),
            new GenItem(new UInt128(0xc294b02995ae6738L, 0xa79b1f7800000001L), new UInt128(0x290e84a2eb15fd1fL, 0xfae352fe00000000L)),
            new GenItem(new UInt128(0x913575e0da8b16b1L, 0x4f363ef000000001L), new UInt128(0xe3dc1bfbe991a34fL, 0xf5c6a5fc00000000L)),
            new GenItem(new UInt128(0x2f61b9f871cf4e62L, 0x9e6c7de000000001L), new UInt128(0xddf540d020b9eadfL, 0xeb8d4bf800000000L)),
            new GenItem(new UInt128(0x78d26ccbd68320c5L, 0x3cd8fbc000000001L), new UInt128(0x8ee4950177ce66bfL, 0xd71a97f000000000L)),
            new GenItem(new UInt128(0x8b7ebd037898518aL, 0x79b1f78000000001L), new UInt128(0x39e0f787c907117fL, 0xae352fe000000000L)),
            new GenItem(new UInt128(0x0b5507b61f78e314L, 0xf363ef0000000001L), new UInt128(0x659d2522f7b732ffL, 0x5c6a5fc000000000L)),
            new GenItem(new UInt128(0x4f884628f812c629L, 0xe6c7de0000000001L), new UInt128(0x9e8722938612a5feL, 0xb8d4bf8000000000L)),
            new GenItem(new UInt128(0xbe896744d4a98c53L, 0xcd8fbc0000000001L), new UInt128(0xe941a65d66b64bfdL, 0x71a97f0000000000L)),
            new GenItem(new UInt128(0xdaf63a553b6318a7L, 0x9b1f780000000001L), new UInt128(0x7b50d19437b097faL, 0xe352fe0000000000L)),
            new GenItem(new UInt128(0x2d7a23d8bf06314fL, 0x363ef00000000001L), new UInt128(0x59d7b68e18712ff5L, 0xc6a5fc0000000000L)),
            new GenItem(new UInt128(0x392b046a9f0c629eL, 0x6c7de00000000001L), new UInt128(0x4087bab2d5225febL, 0x8d4bf80000000000L)),
            new GenItem(new UInt128(0xeb30fbb9c218c53cL, 0xd8fbc00000000001L), new UInt128(0xb470abc03b44bfd7L, 0x1a97f00000000000L)),
            new GenItem(new UInt128(0xb9cdc30594318a79L, 0xb1f7800000000001L), new UInt128(0x366630eaba897faeL, 0x352fe00000000000L)),
            new GenItem(new UInt128(0x014ab453686314f3L, 0x63ef000000000001L), new UInt128(0xa2dfc77e8512ff5cL, 0x6a5fc00000000000L)),
            new GenItem(new UInt128(0x395221c7d0c629e6L, 0xc7de000000000001L), new UInt128(0x1e0d25a14a25feb8L, 0xd4bf800000000000L)),
            new GenItem(new UInt128(0x4d972813a18c53cdL, 0x8fbc000000000001L), new UInt128(0x9d50a5d3944bfd71L, 0xa97f000000000000L)),
            new GenItem(new UInt128(0x06f9e2374318a79bL, 0x1f78000000000001L), new UInt128(0xbf7ab5eb2897fae3L, 0x52fe000000000000L)),
            new GenItem(new UInt128(0xbd220cae86314f36L, 0x3ef0000000000001L), new UInt128(0x925b14e6512ff5c6L, 0xa5fc000000000000L)),
            new GenItem(new UInt128(0x36fd3a5d0c629e6cL, 0x7de0000000000001L), new UInt128(0x724cce0ca25feb8dL, 0x4bf8000000000000L)),
            new GenItem(new UInt128(0x60def8ba18c53cd8L, 0xfbc0000000000001L), new UInt128(0x1af42d1944bfd71aL, 0x97f0000000000000L)),
            new GenItem(new UInt128(0x8d500174318a79b1L, 0xf780000000000001L), new UInt128(0x0f529e32897fae35L, 0x2fe0000000000000L)),
            new GenItem(new UInt128(0x48e842e86314f363L, 0xef00000000000001L), new UInt128(0x844e4c6512ff5c6aL, 0x5fc0000000000000L)),
            new GenItem(new UInt128(0x4af185d0c629e6c7L, 0xde00000000000001L), new UInt128(0x9f40d8ca25feb8d4L, 0xbf80000000000000L)),
            new GenItem(new UInt128(0x7a670ba18c53cd8fL, 0xbc00000000000001L), new UInt128(0x9912b1944bfd71a9L, 0x7f00000000000000L)),
            new GenItem(new UInt128(0x86de174318a79b1fL, 0x7800000000000001L), new UInt128(0x9c69632897fae352L, 0xfe00000000000000L)),
            new GenItem(new UInt128(0x55fc2e86314f363eL, 0xf000000000000001L), new UInt128(0xe1e2c6512ff5c6a5L, 0xfc00000000000000L)),
            new GenItem(new UInt128(0xccf85d0c629e6c7dL, 0xe000000000000001L), new UInt128(0x68058ca25feb8d4bL, 0xf800000000000000L)),
            new GenItem(new UInt128(0x1df0ba18c53cd8fbL, 0xc000000000000001L), new UInt128(0x610b1944bfd71a97L, 0xf000000000000000L)),
            new GenItem(new UInt128(0x4be174318a79b1f7L, 0x8000000000000001L), new UInt128(0x061632897fae352fL, 0xe000000000000000L)),
            new GenItem(new UInt128(0xd7c2e86314f363efL, 0x0000000000000001L), new UInt128(0x1c2c6512ff5c6a5fL, 0xc000000000000000L)),
            new GenItem(new UInt128(0xaf85d0c629e6c7deL, 0x0000000000000001L), new UInt128(0x7858ca25feb8d4bfL, 0x8000000000000000L)),
            new GenItem(new UInt128(0x5f0ba18c53cd8fbcL, 0x0000000000000001L), new UInt128(0xf0b1944bfd71a97fL, 0x0000000000000000L)),
            new GenItem(new UInt128(0xbe174318a79b1f78L, 0x0000000000000001L), new UInt128(0xe1632897fae352feL, 0x0000000000000000L)),
            new GenItem(new UInt128(0x7c2e86314f363ef0L, 0x0000000000000001L), new UInt128(0xc2c6512ff5c6a5fcL, 0x0000000000000000L)),
            new GenItem(new UInt128(0xf85d0c629e6c7de0L, 0x0000000000000001L), new UInt128(0x858ca25feb8d4bf8L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xf0ba18c53cd8fbc0L, 0x0000000000000001L), new UInt128(0x0b1944bfd71a97f0L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xe174318a79b1f780L, 0x0000000000000001L), new UInt128(0x1632897fae352fe0L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xc2e86314f363ef00L, 0x0000000000000001L), new UInt128(0x2c6512ff5c6a5fc0L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x85d0c629e6c7de00L, 0x0000000000000001L), new UInt128(0x58ca25feb8d4bf80L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x0ba18c53cd8fbc00L, 0x0000000000000001L), new UInt128(0xb1944bfd71a97f00L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x174318a79b1f7800L, 0x0000000000000001L), new UInt128(0x632897fae352fe00L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x2e86314f363ef000L, 0x0000000000000001L), new UInt128(0xc6512ff5c6a5fc00L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x5d0c629e6c7de000L, 0x0000000000000001L), new UInt128(0x8ca25feb8d4bf800L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xba18c53cd8fbc000L, 0x0000000000000001L), new UInt128(0x1944bfd71a97f000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x74318a79b1f78000L, 0x0000000000000001L), new UInt128(0x32897fae352fe000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xe86314f363ef0000L, 0x0000000000000001L), new UInt128(0x6512ff5c6a5fc000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xd0c629e6c7de0000L, 0x0000000000000001L), new UInt128(0xca25feb8d4bf8000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xa18c53cd8fbc0000L, 0x0000000000000001L), new UInt128(0x944bfd71a97f0000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x4318a79b1f780000L, 0x0000000000000001L), new UInt128(0x2897fae352fe0000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x86314f363ef00000L, 0x0000000000000001L), new UInt128(0x512ff5c6a5fc0000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x0c629e6c7de00000L, 0x0000000000000001L), new UInt128(0xa25feb8d4bf80000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x18c53cd8fbc00000L, 0x0000000000000001L), new UInt128(0x44bfd71a97f00000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x318a79b1f7800000L, 0x0000000000000001L), new UInt128(0x897fae352fe00000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x6314f363ef000000L, 0x0000000000000001L), new UInt128(0x12ff5c6a5fc00000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xc629e6c7de000000L, 0x0000000000000001L), new UInt128(0x25feb8d4bf800000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x8c53cd8fbc000000L, 0x0000000000000001L), new UInt128(0x4bfd71a97f000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x18a79b1f78000000L, 0x0000000000000001L), new UInt128(0x97fae352fe000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x314f363ef0000000L, 0x0000000000000001L), new UInt128(0x2ff5c6a5fc000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x629e6c7de0000000L, 0x0000000000000001L), new UInt128(0x5feb8d4bf8000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xc53cd8fbc0000000L, 0x0000000000000001L), new UInt128(0xbfd71a97f0000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x8a79b1f780000000L, 0x0000000000000001L), new UInt128(0x7fae352fe0000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x14f363ef00000000L, 0x0000000000000001L), new UInt128(0xff5c6a5fc0000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x29e6c7de00000000L, 0x0000000000000001L), new UInt128(0xfeb8d4bf80000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x53cd8fbc00000000L, 0x0000000000000001L), new UInt128(0xfd71a97f00000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xa79b1f7800000000L, 0x0000000000000001L), new UInt128(0xfae352fe00000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x4f363ef000000000L, 0x0000000000000001L), new UInt128(0xf5c6a5fc00000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x9e6c7de000000000L, 0x0000000000000001L), new UInt128(0xeb8d4bf800000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x3cd8fbc000000000L, 0x0000000000000001L), new UInt128(0xd71a97f000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x79b1f78000000000L, 0x0000000000000001L), new UInt128(0xae352fe000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xf363ef0000000000L, 0x0000000000000001L), new UInt128(0x5c6a5fc000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xe6c7de0000000000L, 0x0000000000000001L), new UInt128(0xb8d4bf8000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xcd8fbc0000000000L, 0x0000000000000001L), new UInt128(0x71a97f0000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x9b1f780000000000L, 0x0000000000000001L), new UInt128(0xe352fe0000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x363ef00000000000L, 0x0000000000000001L), new UInt128(0xc6a5fc0000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x6c7de00000000000L, 0x0000000000000001L), new UInt128(0x8d4bf80000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xd8fbc00000000000L, 0x0000000000000001L), new UInt128(0x1a97f00000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xb1f7800000000000L, 0x0000000000000001L), new UInt128(0x352fe00000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x63ef000000000000L, 0x0000000000000001L), new UInt128(0x6a5fc00000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xc7de000000000000L, 0x0000000000000001L), new UInt128(0xd4bf800000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x8fbc000000000000L, 0x0000000000000001L), new UInt128(0xa97f000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x1f78000000000000L, 0x0000000000000001L), new UInt128(0x52fe000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x3ef0000000000000L, 0x0000000000000001L), new UInt128(0xa5fc000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x7de0000000000000L, 0x0000000000000001L), new UInt128(0x4bf8000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xfbc0000000000000L, 0x0000000000000001L), new UInt128(0x97f0000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xf780000000000000L, 0x0000000000000001L), new UInt128(0x2fe0000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xef00000000000000L, 0x0000000000000001L), new UInt128(0x5fc0000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xde00000000000000L, 0x0000000000000001L), new UInt128(0xbf80000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xbc00000000000000L, 0x0000000000000001L), new UInt128(0x7f00000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x7800000000000000L, 0x0000000000000001L), new UInt128(0xfe00000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xf000000000000000L, 0x0000000000000001L), new UInt128(0xfc00000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xe000000000000000L, 0x0000000000000001L), new UInt128(0xf800000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0xc000000000000000L, 0x0000000000000001L), new UInt128(0xf000000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x8000000000000000L, 0x0000000000000001L), new UInt128(0xe000000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x0000000000000000L, 0x0000000000000001L), new UInt128(0xc000000000000000L, 0x0000000000000000L)),
            new GenItem(new UInt128(0x0000000000000000L, 0x0000000000000001L), new UInt128(0x8000000000000000L, 0x0000000000000000L))
        };

        #endregion

        private UInt128 _value;

        public Random128()
            : this(UInt128.Zero)
        {
        }

        public Random128(UInt128 advance)
        {
            _value = SkipAhead(advance);
        }

        public UInt128 Next()
        {
            _value = (_value * _gen[0].A) + _gen[0].C;
            return _value;
        }

        private UInt128 SkipAhead(UInt128 advance)
        {
            UInt128 newValue = UInt128.Zero;
            ulong bitmap;

            bitmap = advance.Low64;
            for( int i = 0; bitmap != 0 && i < 64; i++ )
            {
                if( (bitmap & ((ulong)1 << i)) != 0 )
                {
                    /* advance random number by f**(2**i) (x)
                     */
                    newValue = (newValue * _gen[i].A) + _gen[i].C;
                    bitmap &= ~((ulong)1 << i);
                }
            }
            bitmap = advance.High64;
            for( int i = 0; bitmap != 0 && i < 64; i++ )
            {
                if( (bitmap & ((ulong)1 << i)) != 0 )
                {
                    /* advance random number by f**(2**(i + 64)) (x)
                     */
                    newValue = (newValue * _gen[i + 64].A) + _gen[i + 64].C;
                    bitmap &= ~((ulong)1 << i);
                }
            }
            return newValue;
        }
    }
}
