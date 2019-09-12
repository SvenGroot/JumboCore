// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Test.Tasks;
using Ookii.Jumbo.IO;
using log4net.Layout;
using log4net.Appender;
using log4net.Core;
using NUnit.Framework;
using System.Reflection;

namespace Ookii.Jumbo.Test
{
    static class Utilities
    {
        private static readonly string _testOutputPath = GetOutputPath();

        class TestAppender : AppenderSkeleton
        {
            protected override void Append(LoggingEvent loggingEvent)
            {
                TestContext.Progress.Write(RenderLoggingEvent(loggingEvent));
            }

            protected override bool RequiresLayout
            {
                get { return true; }
            }
        }

        public static void ConfigureLogging()
        {
            PatternLayout layout = new PatternLayout();
            layout.ConversionPattern = PatternLayout.DetailConversionPattern;
            layout.ActivateOptions();
            TestAppender appender = new TestAppender();
            appender.Layout = layout;
            appender.Threshold = Level.All;
            appender.ActivateOptions();
            log4net.LogManager.ResetConfiguration(Assembly.GetCallingAssembly());
            log4net.Config.BasicConfigurator.Configure(log4net.LogManager.GetRepository(Assembly.GetCallingAssembly()), appender);
        }

        private static string GetOutputPath()
        {
            string path = Environment.GetEnvironmentVariable("JUMBO_TESTOUTPUT");
            if( string.IsNullOrEmpty(path) )
                path = System.IO.Path.Combine(Environment.CurrentDirectory, "TestOutput");
            return path;
        }

        public static string TestOutputPath
        {
            get { return _testOutputPath; }
        }

        public static string GenerateFile(string name, int size)
        {
            string path = System.IO.Path.Combine(TestOutputPath, name);
            using( FileStream stream = System.IO.File.Create(path) )
            {
                GenerateData(stream, size);
            }
            return path;
        }

        public static void GenerateData(Stream stream, int size)
        {
            Random rnd = new Random();
            int sizeRemaining = size;
            byte[] buffer = new byte[4096];
            while( sizeRemaining > 0 )
            {
                int writeSize = Math.Min(buffer.Length, sizeRemaining);
                rnd.NextBytes(buffer);
                stream.Write(buffer, 0, writeSize);
                sizeRemaining -= writeSize;
            }
        }

        public static IEnumerable<Utf8String> GenerateUtf8TextData(int records, int recordSize)
        {
            Random rnd = new Random();
            byte[] data = new byte[recordSize];
            for( int x = 0; x < records; ++x )
            {
                for( int i = 0; i < recordSize; ++i )
                    data[i] = (byte)rnd.Next('a', 'z');
                yield return new Utf8String(data);
            }
        }

        public static int GenerateDataLines(Stream stream, int size)
        {
            Random rnd = new Random();
            int sizeRemaining = size;
            int lines = 0;
            while( sizeRemaining > 0 )
            {
                stream.WriteByte((byte)rnd.Next('a', 'z'));
                if( sizeRemaining % 80 == 0 )
                {
                    stream.WriteByte((byte)'\n');
                    --sizeRemaining;
                    ++lines;
                }
                --sizeRemaining;
            }
            return lines + 1;
        }

        public static List<string> GenerateDataWords(StreamWriter writer, int lines, int wordsPerLine)
        {
            List<string> result = new List<string>();
            Random rnd = new Random();
            for( int line = 0; line < lines; ++line )
            {
                for( int word = 0; word < wordsPerLine; ++word )
                {
                    if( writer != null && word != 0 )
                        writer.Write(" ");
                    string wordToWrite = _words[rnd.Next(_words.Length)];
                    result.Add(wordToWrite);
                    if( writer != null )
                        writer.Write(wordToWrite);
                }
                if( writer != null )
                    writer.WriteLine();
            }
            return result;
        }

        public static List<string> GenerateTextData(int length, int count)
        {
            List<string> result = new List<string>(count);
            StringBuilder sb = new StringBuilder(length);
            Random rnd = new Random();
            for( int x = 0; x < count; ++x )
            {
                sb.Length = 0;
                for( int l = 0; l < length; ++l )
                {
                    sb.Append((char)rnd.Next('a', 'z'));
                }
                result.Add(sb.ToString());
            }
            return result;
        }

        public static List<int> GenerateNumberData(int count)
        {
            return GenerateNumberData(count, new Random());
        }

        public static List<int> GenerateNumberData(int count, Random rnd)
        {
            List<int> result = new List<int>();
            for( int x = 0; x < count; ++x )
                result.Add(rnd.Next());
            return result;
        }

        public static byte[] GenerateData(int size)
        {
            Random rnd = new Random();
            byte[] data = new byte[size];
            rnd.NextBytes(data);
            return data;
        }

        public static Packet GeneratePacket(int size, bool isLastPacket)
        {
            Random rnd = new Random();
            byte[] data = new byte[size];
            rnd.NextBytes(data);
            return new Packet(data, size, 0, isLastPacket);
        }

        public static void CopyStream(Stream src, Stream dest)
        {
            CopyStream(src, dest, 4096);
        }

        public static void CopyStream(Stream src, Stream dest, int bufferSize)
        {
            byte[] buffer = new byte[bufferSize];
            int bytesRead = 0;
            do
            {
                bytesRead = src.Read(buffer, 0, buffer.Length);
                if( bytesRead > 0 )
                {
                    dest.Write(buffer, 0, bytesRead);
                }
            } while( bytesRead > 0 );
        }

        public static bool CompareStream(Stream stream1, Stream stream2)
        {
            return CompareStream(stream1, stream2, 4096);
        }

        public static bool CompareStream(Stream stream1, Stream stream2, int bufferSize)
        {
            byte[] buffer1 = new byte[bufferSize];
            byte[] buffer2 = new byte[bufferSize];
            int bytesRead1 = 0;
            int bytesRead2 = 0;
            do
            {
                bytesRead1 = stream1.Read(buffer1, 0, buffer1.Length);
                bytesRead2 = stream2.Read(buffer2, 0, buffer2.Length);
                if( bytesRead1 > 0 && bytesRead1 == bytesRead2 )
                {
                    if( !CompareArray(buffer1, 0, buffer2, 0, bytesRead1) )
                        return false;
                }
            } while( bytesRead1 > 0 && bytesRead1 == bytesRead2 );
            return bytesRead1 == bytesRead2;
        }

        public static bool CompareArray<T>(T[] array1, int offset1, T[] array2, int offset2, int count)
        {
            return CompareList(array1, offset1, array2, offset2, count);
        }

        public static bool CompareList<T>(IList<T> list1, int offset1, IList<T> list2, int offset2, int count)
        {
            int end = offset1 + count;
            for( int i1 = offset1, i2 = offset2; i1 < end; ++i1, ++i2 )
            {
                if( !object.Equals(list1[i1], list2[i2]) )
                    return false;
            }
            return true;
        }

        public static bool CompareList<T>(IList<T> list1, IList<T> list2)
        {
            if( list1.Count != list2.Count )
                return false;
            else
                return CompareList(list1, 0, list2, 0, list1.Count);
        }

        public static void TraceLineAndFlush(string message)
        {
            Trace.WriteLine(message);
            Trace.Flush();
        }

        public static void GenerateJoinData(IList<Customer> customers, IList<Order> orders, int customerCount, int perCustomerRecordMax, int ordersPerCustomerMax)
        {
            Random rnd = new Random();
            string[] words = File.ReadAllLines("english-words.10");
            int orderId = 0;

            for (int x = 1; x <= customerCount; ++x)
            {
                int records = rnd.Next(1, perCustomerRecordMax);
                for (int y = 0; y < records; ++y)
                {
                    customers.Add(new Customer() { Id = x, Name = words[rnd.Next(words.Length)] });
                }
                int orderCount = rnd.Next(0, ordersPerCustomerMax + 1);
                for (int y = 0; y < orderCount; ++y)
                {
                    orders.Add(new Order() { Id = ++orderId, CustomerId = x, ItemId = rnd.Next(100) });
                }
            }
        }

        private static readonly String[] _words = {
                                   "diurnalness", "Homoiousian",
                                   "spiranthic", "tetragynian",
                                   "silverhead", "ungreat",
                                   "lithograph", "exploiter",
                                   "physiologian", "by",
                                   "hellbender", "Filipendula",
                                   "undeterring", "antiscolic",
                                   "pentagamist", "hypoid",
                                   "cacuminal", "sertularian",
                                   "schoolmasterism", "nonuple",
                                   "gallybeggar", "phytonic",
                                   "swearingly", "nebular",
                                   "Confervales", "thermochemically",
                                   "characinoid", "cocksuredom",
                                   "fallacious", "feasibleness",
                                   "debromination", "playfellowship",
                                   "tramplike", "testa",
                                   "participatingly", "unaccessible",
                                   "bromate", "experientialist",
                                   "roughcast", "docimastical",
                                   "choralcelo", "blightbird",
                                   "peptonate", "sombreroed",
                                   "unschematized", "antiabolitionist",
                                   "besagne", "mastication",
                                   "bromic", "sviatonosite",
                                   "cattimandoo", "metaphrastical",
                                   "endotheliomyoma", "hysterolysis",
                                   "unfulminated", "Hester",
                                   "oblongly", "blurredness",
                                   "authorling", "chasmy",
                                   "Scorpaenidae", "toxihaemia",
                                   "Dictograph", "Quakerishly",
                                   "deaf", "timbermonger",
                                   "strammel", "Thraupidae",
                                   "seditious", "plerome",
                                   "Arneb", "eristically",
                                   "serpentinic", "glaumrie",
                                   "socioromantic", "apocalypst",
                                   "tartrous", "Bassaris",
                                   "angiolymphoma", "horsefly",
                                   "kenno", "astronomize",
                                   "euphemious", "arsenide",
                                   "untongued", "parabolicness",
                                   "uvanite", "helpless",
                                   "gemmeous", "stormy",
                                   "templar", "erythrodextrin",
                                   "comism", "interfraternal",
                                   "preparative", "parastas",
                                   "frontoorbital", "Ophiosaurus",
                                   "diopside", "serosanguineous",
                                   "ununiformly", "karyological",
                                   "collegian", "allotropic",
                                   "depravity", "amylogenesis",
                                   "reformatory", "epidymides",
                                   "pleurotropous", "trillium",
                                   "dastardliness", "coadvice",
                                   "embryotic", "benthonic",
                                   "pomiferous", "figureheadship",
                                   "Megaluridae", "Harpa",
                                   "frenal", "commotion",
                                   "abthainry", "cobeliever",
                                   "manilla", "spiciferous",
                                   "nativeness", "obispo",
                                   "monilioid", "biopsic",
                                   "valvula", "enterostomy",
                                   "planosubulate", "pterostigma",
                                   "lifter", "triradiated",
                                   "venialness", "tum",
                                   "archistome", "tautness",
                                   "unswanlike", "antivenin",
                                   "Lentibulariaceae", "Triphora",
                                   "angiopathy", "anta",
                                   "Dawsonia", "becomma",
                                   "Yannigan", "winterproof",
                                   "antalgol", "harr",
                                   "underogating", "ineunt",
                                   "cornberry", "flippantness",
                                   "scyphostoma", "approbation",
                                   "Ghent", "Macraucheniidae",
                                   "scabbiness", "unanatomized",
                                   "photoelasticity", "eurythermal",
                                   "enation", "prepavement",
                                   "flushgate", "subsequentially",
                                   "Edo", "antihero",
                                   "Isokontae", "unforkedness",
                                   "porriginous", "daytime",
                                   "nonexecutive", "trisilicic",
                                   "morphiomania", "paranephros",
                                   "botchedly", "impugnation",
                                   "Dodecatheon", "obolus",
                                   "unburnt", "provedore",
                                   "Aktistetae", "superindifference",
                                   "Alethea", "Joachimite",
                                   "cyanophilous", "chorograph",
                                   "brooky", "figured",
                                   "periclitation", "quintette",
                                   "hondo", "ornithodelphous",
                                   "unefficient", "pondside",
                                   "bogydom", "laurinoxylon",
                                   "Shiah", "unharmed",
                                   "cartful", "noncrystallized",
                                   "abusiveness", "cromlech",
                                   "japanned", "rizzomed",
                                   "underskin", "adscendent",
                                   "allectory", "gelatinousness",
                                   "volcano", "uncompromisingly",
                                   "cubit", "idiotize",
                                   "unfurbelowed", "undinted",
                                   "magnetooptics", "Savitar",
                                   "diwata", "ramosopalmate",
                                   "Pishquow", "tomorn",
                                   "apopenptic", "Haversian",
                                   "Hysterocarpus", "ten",
                                   "outhue", "Bertat",
                                   "mechanist", "asparaginic",
                                   "velaric", "tonsure",
                                   "bubble", "Pyrales",
                                   "regardful", "glyphography",
                                   "calabazilla", "shellworker",
                                   "stradametrical", "havoc",
                                   "theologicopolitical", "sawdust",
                                   "diatomaceous", "jajman",
                                   "temporomastoid", "Serrifera",
                                   "Ochnaceae", "aspersor",
                                   "trailmaking", "Bishareen",
                                   "digitule", "octogynous",
                                   "epididymitis", "smokefarthings",
                                   "bacillite", "overcrown",
                                   "mangonism", "sirrah",
                                   "undecorated", "psychofugal",
                                   "bismuthiferous", "rechar",
                                   "Lemuridae", "frameable",
                                   "thiodiazole", "Scanic",
                                   "sportswomanship", "interruptedness",
                                   "admissory", "osteopaedion",
                                   "tingly", "tomorrowness",
                                   "ethnocracy", "trabecular",
                                   "vitally", "fossilism",
                                   "adz", "metopon",
                                   "prefatorial", "expiscate",
                                   "diathermacy", "chronist",
                                   "nigh", "generalizable",
                                   "hysterogen", "aurothiosulphuric",
                                   "whitlowwort", "downthrust",
                                   "Protestantize", "monander",
                                   "Itea", "chronographic",
                                   "silicize", "Dunlop",
                                   "eer", "componental",
                                   "spot", "pamphlet",
                                   "antineuritic", "paradisean",
                                   "interruptor", "debellator",
                                   "overcultured", "Florissant",
                                   "hyocholic", "pneumatotherapy",
                                   "tailoress", "rave",
                                   "unpeople", "Sebastian",
                                   "thermanesthesia", "Coniferae",
                                   "swacking", "posterishness",
                                   "ethmopalatal", "whittle",
                                   "analgize", "scabbardless",
                                   "naught", "symbiogenetically",
                                   "trip", "parodist",
                                   "columniform", "trunnel",
                                   "yawler", "goodwill",
                                   "pseudohalogen", "swangy",
                                   "cervisial", "mediateness",
                                   "genii", "imprescribable",
                                   "pony", "consumptional",
                                   "carposporangial", "poleax",
                                   "bestill", "subfebrile",
                                   "sapphiric", "arrowworm",
                                   "qualminess", "ultraobscure",
                                   "thorite", "Fouquieria",
                                   "Bermudian", "prescriber",
                                   "elemicin", "warlike",
                                   "semiangle", "rotular",
                                   "misthread", "returnability",
                                   "seraphism", "precostal",
                                   "quarried", "Babylonism",
                                   "sangaree", "seelful",
                                   "placatory", "pachydermous",
                                   "bozal", "galbulus",
                                   "spermaphyte", "cumbrousness",
                                   "pope", "signifier",
                                   "Endomycetaceae", "shallowish",
                                   "sequacity", "periarthritis",
                                   "bathysphere", "pentosuria",
                                   "Dadaism", "spookdom",
                                   "Consolamentum", "afterpressure",
                                   "mutter", "louse",
                                   "ovoviviparous", "corbel",
                                   "metastoma", "biventer",
                                   "Hydrangea", "hogmace",
                                   "seizing", "nonsuppressed",
                                   "oratorize", "uncarefully",
                                   "benzothiofuran", "penult",
                                   "balanocele", "macropterous",
                                   "dishpan", "marten",
                                   "absvolt", "jirble",
                                   "parmelioid", "airfreighter",
                                   "acocotl", "archesporial",
                                   "hypoplastral", "preoral",
                                   "quailberry", "cinque",
                                   "terrestrially", "stroking",
                                   "limpet", "moodishness",
                                   "canicule", "archididascalian",
                                   "pompiloid", "overstaid",
                                   "introducer", "Italical",
                                   "Christianopaganism", "prescriptible",
                                   "subofficer", "danseuse",
                                   "cloy", "saguran",
                                   "frictionlessly", "deindividualization",
                                   "Bulanda", "ventricous",
                                   "subfoliar", "basto",
                                   "scapuloradial", "suspend",
                                   "stiffish", "Sphenodontidae",
                                   "eternal", "verbid",
                                   "mammonish", "upcushion",
                                   "barkometer", "concretion",
                                   "preagitate", "incomprehensible",
                                   "tristich", "visceral",
                                   "hemimelus", "patroller",
                                   "stentorophonic", "pinulus",
                                   "kerykeion", "brutism",
                                   "monstership", "merciful",
                                   "overinstruct", "defensibly",
                                   "bettermost", "splenauxe",
                                   "Mormyrus", "unreprimanded",
                                   "taver", "ell",
                                   "proacquittal", "infestation",
                                   "overwoven", "Lincolnlike",
                                   "chacona", "Tamil",
                                   "classificational", "lebensraum",
                                   "reeveland", "intuition",
                                   "Whilkut", "focaloid",
                                   "Eleusinian", "micromembrane",
                                   "byroad", "nonrepetition",
                                   "bacterioblast", "brag",
                                   "ribaldrous", "phytoma",
                                   "counteralliance", "pelvimetry",
                                   "pelf", "relaster",
                                   "thermoresistant", "aneurism",
                                   "molossic", "euphonym",
                                   "upswell", "ladhood",
                                   "phallaceous", "inertly",
                                   "gunshop", "stereotypography",
                                   "laryngic", "refasten",
                                   "twinling", "oflete",
                                   "hepatorrhaphy", "electrotechnics",
                                   "cockal", "guitarist",
                                   "topsail", "Cimmerianism",
                                   "larklike", "Llandovery",
                                   "pyrocatechol", "immatchable",
                                   "chooser", "metrocratic",
                                   "craglike", "quadrennial",
                                   "nonpoisonous", "undercolored",
                                   "knob", "ultratense",
                                   "balladmonger", "slait",
                                   "sialadenitis", "bucketer",
                                   "magnificently", "unstipulated",
                                   "unscourged", "unsupercilious",
                                   "packsack", "pansophism",
                                   "soorkee", "percent",
                                   "subirrigate", "champer",
                                   "metapolitics", "spherulitic",
                                   "involatile", "metaphonical",
                                   "stachyuraceous", "speckedness",
                                   "bespin", "proboscidiform",
                                   "gul", "squit",
                                   "yeelaman", "peristeropode",
                                   "opacousness", "shibuichi",
                                   "retinize", "yote",
                                   "misexposition", "devilwise",
                                   "pumpkinification", "vinny",
                                   "bonze", "glossing",
                                   "decardinalize", "transcortical",
                                   "serphoid", "deepmost",
                                   "guanajuatite", "wemless",
                                   "arval", "lammy",
                                   "Effie", "Saponaria",
                                   "tetrahedral", "prolificy",
                                   "excerpt", "dunkadoo",
                                   "Spencerism", "insatiately",
                                   "Gilaki", "oratorship",
                                   "arduousness", "unbashfulness",
                                   "Pithecolobium", "unisexuality",
                                   "veterinarian", "detractive",
                                   "liquidity", "acidophile",
                                   "proauction", "sural",
                                   "totaquina", "Vichyite",
                                   "uninhabitedness", "allegedly",
                                   "Gothish", "manny",
                                   "Inger", "flutist",
                                   "ticktick", "Ludgatian",
                                   "homotransplant", "orthopedical",
                                   "diminutively", "monogoneutic",
                                   "Kenipsim", "sarcologist",
                                   "drome", "stronghearted",
                                   "Fameuse", "Swaziland",
                                   "alen", "chilblain",
                                   "beatable", "agglomeratic",
                                   "constitutor", "tendomucoid",
                                   "porencephalous", "arteriasis",
                                   "boser", "tantivy",
                                   "rede", "lineamental",
                                   "uncontradictableness", "homeotypical",
                                   "masa", "folious",
                                   "dosseret", "neurodegenerative",
                                   "subtransverse", "Chiasmodontidae",
                                   "palaeotheriodont", "unstressedly",
                                   "chalcites", "piquantness",
                                   "lampyrine", "Aplacentalia",
                                   "projecting", "elastivity",
                                   "isopelletierin", "bladderwort",
                                   "strander", "almud",
                                   "iniquitously", "theologal",
                                   "bugre", "chargeably",
                                   "imperceptivity", "meriquinoidal",
                                   "mesophyte", "divinator",
                                   "perfunctory", "counterappellant",
                                   "synovial", "charioteer",
                                   "crystallographical", "comprovincial",
                                   "infrastapedial", "pleasurehood",
                                   "inventurous", "ultrasystematic",
                                   "subangulated", "supraoesophageal",
                                   "Vaishnavism", "transude",
                                   "chrysochrous", "ungrave",
                                   "reconciliable", "uninterpleaded",
                                   "erlking", "wherefrom",
                                   "aprosopia", "antiadiaphorist",
                                   "metoxazine", "incalculable",
                                   "umbellic", "predebit",
                                   "foursquare", "unimmortal",
                                   "nonmanufacture", "slangy",
                                   "predisputant", "familist",
                                   "preaffiliate", "friarhood",
                                   "corelysis", "zoonitic",
                                   "halloo", "paunchy",
                                   "neuromimesis", "aconitine",
                                   "hackneyed", "unfeeble",
                                   "cubby", "autoschediastical",
                                   "naprapath", "lyrebird",
                                   "inexistency", "leucophoenicite",
                                   "ferrogoslarite", "reperuse",
                                   "uncombable", "tambo",
                                   "propodiale", "diplomatize",
                                   "Russifier", "clanned",
                                   "corona", "michigan",
                                   "nonutilitarian", "transcorporeal",
                                   "bought", "Cercosporella",
                                   "stapedius", "glandularly",
                                   "pictorially", "weism",
                                   "disilane", "rainproof",
                                   "Caphtor", "scrubbed",
                                   "oinomancy", "pseudoxanthine",
                                   "nonlustrous", "redesertion",
                                   "Oryzorictinae", "gala",
                                   "Mycogone", "reappreciate",
                                   "cyanoguanidine", "seeingness",
                                   "breadwinner", "noreast",
                                   "furacious", "epauliere",
                                   "omniscribent", "Passiflorales",
                                   "uninductive", "inductivity",
                                   "Orbitolina", "Semecarpus",
                                   "migrainoid", "steprelationship",
                                   "phlogisticate", "mesymnion",
                                   "sloped", "edificator",
                                   "beneficent", "culm",
                                   "paleornithology", "unurban",
                                   "throbless", "amplexifoliate",
                                   "sesquiquintile", "sapience",
                                   "astucious", "dithery",
                                   "boor", "ambitus",
                                   "scotching", "uloid",
                                   "uncompromisingness", "hoove",
                                   "waird", "marshiness",
                                   "Jerusalem", "mericarp",
                                   "unevoked", "benzoperoxide",
                                   "outguess", "pyxie",
                                   "hymnic", "euphemize",
                                   "mendacity", "erythremia",
                                   "rosaniline", "unchatteled",
                                   "lienteria", "Bushongo",
                                   "dialoguer", "unrepealably",
                                   "rivethead", "antideflation",
                                   "vinegarish", "manganosiderite",
                                   "doubtingness", "ovopyriform",
                                   "Cephalodiscus", "Muscicapa",
                                   "Animalivora", "angina",
                                   "planispheric", "ipomoein",
                                   "cuproiodargyrite", "sandbox",
                                   "scrat", "Munnopsidae",
                                   "shola", "pentafid",
                                   "overstudiousness", "times",
                                   "nonprofession", "appetible",
                                   "valvulotomy", "goladar",
                                   "uniarticular", "oxyterpene",
                                   "unlapsing", "omega",
                                   "trophonema", "seminonflammable",
                                   "circumzenithal", "starer",
                                   "depthwise", "liberatress",
                                   "unleavened", "unrevolting",
                                   "groundneedle", "topline",
                                   "wandoo", "umangite",
                                   "ordinant", "unachievable",
                                   "oversand", "snare",
                                   "avengeful", "unexplicit",
                                   "mustafina", "sonable",
                                   "rehabilitative", "eulogization",
                                   "papery", "technopsychology",
                                   "impressor", "cresylite",
                                   "entame", "transudatory",
                                   "scotale", "pachydermatoid",
                                   "imaginary", "yeat",
                                   "slipped", "stewardship",
                                   "adatom", "cockstone",
                                   "skyshine", "heavenful",
                                   "comparability", "exprobratory",
                                   "dermorhynchous", "parquet",
                                   "cretaceous", "vesperal",
                                   "raphis", "undangered",
                                   "Glecoma", "engrain",
                                   "counteractively", "Zuludom",
                                   "orchiocatabasis", "Auriculariales",
                                   "warriorwise", "extraorganismal",
                                   "overbuilt", "alveolite",
                                   "tetchy", "terrificness",
                                   "widdle", "unpremonished",
                                   "rebilling", "sequestrum",
                                   "equiconvex", "heliocentricism",
                                   "catabaptist", "okonite",
                                   "propheticism", "helminthagogic",
                                   "calycular", "giantly",
                                   "wingable", "golem",
                                   "unprovided", "commandingness",
                                   "greave", "haply",
                                   "doina", "depressingly",
                                   "subdentate", "impairment",
                                   "decidable", "neurotrophic",
                                   "unpredict", "bicorporeal",
                                   "pendulant", "flatman",
                                   "intrabred", "toplike",
                                   "Prosobranchiata", "farrantly",
                                   "toxoplasmosis", "gorilloid",
                                   "dipsomaniacal", "aquiline",
                                   "atlantite", "ascitic",
                                   "perculsive", "prospectiveness",
                                   "saponaceous", "centrifugalization",
                                   "dinical", "infravaginal",
                                   "beadroll", "affaite",
                                   "Helvidian", "tickleproof",
                                   "abstractionism", "enhedge",
                                   "outwealth", "overcontribute",
                                   "coldfinch", "gymnastic",
                                   "Pincian", "Munychian",
                                   "codisjunct", "quad",
                                   "coracomandibular", "phoenicochroite",
                                   "amender", "selectivity",
                                   "putative", "semantician",
                                   "lophotrichic", "Spatangoidea",
                                   "saccharogenic", "inferent",
                                   "Triconodonta", "arrendation",
                                   "sheepskin", "taurocolla",
                                   "bunghole", "Machiavel",
                                   "triakistetrahedral", "dehairer",
                                   "prezygapophysial", "cylindric",
                                   "pneumonalgia", "sleigher",
                                   "emir", "Socraticism",
                                   "licitness", "massedly",
                                   "instructiveness", "sturdied",
                                   "redecrease", "starosta",
                                   "evictor", "orgiastic",
                                   "squdge", "meloplasty",
                                   "Tsonecan", "repealableness",
                                   "swoony", "myesthesia",
                                   "molecule", "autobiographist",
                                   "reciprocation", "refective",
                                   "unobservantness", "tricae",
                                   "ungouged", "floatability",
                                   "Mesua", "fetlocked",
                                   "chordacentrum", "sedentariness",
                                   "various", "laubanite",
                                   "nectopod", "zenick",
                                   "sequentially", "analgic",
                                   "biodynamics", "posttraumatic",
                                   "nummi", "pyroacetic",
                                   "bot", "redescend",
                                   "dispermy", "undiffusive",
                                   "circular", "trillion",
                                   "Uraniidae", "ploration",
                                   "discipular", "potentness",
                                   "sud", "Hu",
                                   "Eryon", "plugger",
                                   "subdrainage", "jharal",
                                   "abscission", "supermarket",
                                   "countergabion", "glacierist",
                                   "lithotresis", "minniebush",
                                   "zanyism", "eucalypteol",
                                   "sterilely", "unrealize",
                                   "unpatched", "hypochondriacism",
                                   "critically", "cheesecutter",
                                  };

    }
}
