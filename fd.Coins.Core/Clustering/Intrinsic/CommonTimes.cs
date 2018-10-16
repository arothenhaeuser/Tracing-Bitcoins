using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class CommonTimes : Clustering
    {
        private Dictionary<string, int[]> _result;
        public CommonTimes()
        {
            _result = new Dictionary<string, int[]>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result[addr1];
            var v2 = _result[addr2];
            var numerator = v1.Select((x, i) => Math.Pow(x - v2[i], 2)).Sum();
            var denominator = v1.Select(x => Math.Pow(x, 2)).Sum() + v2.Select(x => Math.Pow(x, 2)).Sum();
            var res = numerator / denominator;
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
                _result = mainDB.Query($"SELECT list($hour) as hour, tAddr as address FROM Link LET $hour = outV().BlockTime.format('k').asLong() WHERE tAddr in [{string.Join(",", addresses.Select(x => "'" + x + "'"))}] GROUP BY tAddr").ToDictionary(x => x.GetField<string>("address"), y => ToFeatureVector(y.GetField<List<long>>("hour")));
            }
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        private int[] ToFeatureVector(List<long> samples)
        {
            var fv = new int[24];
            foreach (var sample in samples)
            {
                fv[(int)sample - 1]++;
            }
            return fv;
        }
    }
}
