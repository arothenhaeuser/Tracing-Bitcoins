using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    /// <summary>
    /// Feature Extractor: Extracts the hour of the day from the BlockTime. (2018-01-01 12:34:56 -> 12)
    /// Outputs a 24 entries vector indicating which hours of the day have been observed.
    /// Example:
    /// hotd:   0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20  21  22  23  24
    /// feat:   1   0   0   0   0   0   0   1   1   1   0   0   0   0   1   0   1   1   1   0   0   0   0   0   0
    /// </summary>
    public class TimeSlots : Clustering
    {
        private Dictionary<string, BitArray> _result;
        public TimeSlots()
        {
            _result = new Dictionary<string, BitArray>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result[addr1];
            var v2 = _result[addr2];
            var numerator = v1.Xor(v2).OfType<bool>().Count(x => x);
            var denominator = v1.OfType<bool>().Count(x => x) + v2.OfType<bool>().Count(x => x);
            var res = (double)numerator / denominator;
            return Double.IsNaN(res) ? 0 : res;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            using (var mainDB = new ODatabase(mainOptions))
            {
                foreach (var address in addresses)
                {
                    var kvp = mainDB.Query($"SELECT list($hour) as hour FROM Link LET $hour = outV().BlockTime.format('k').asLong() WHERE tAddr = '{address}'").Select(x => new KeyValuePair<string, BitArray>(address, ToBitArray(x.GetField<List<long>>("hour")))).First();
                    try
                    {
                        _result.Add(kvp.Key, kvp.Value);
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(address + " not in DB?");
                    }
                    Console.WriteLine(address + " done.");
                }
            }
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        private BitArray ToBitArray(List<long> list)
        {
            var dist = list.Distinct().Select(x => (int)x);
            var ret = new BitArray(24);
            foreach(var value in dist)
            {
                ret.Set(value - 1, true);
            }
            return ret;
        }
    }
}
