using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web.Script.Serialization;

namespace fd.Coins.Core.Clustering.Intrinsic
{
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
            return (double)numerator / denominator;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            using (var mainDB = new ODatabase(mainOptions))
            {
                _result = mainDB.Query($"SELECT list($hour) as hour, tAddr as address FROM Link LET $hour = outV().BlockTime.format('k').asLong() WHERE tAddr in [{string.Join(",", addresses.Select(x => "'" + x + "'"))}] GROUP BY tAddr").ToDictionary(x => x.GetField<string>("address"), y => ToBitArray(y.GetField<List<long>>("hour")));
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
