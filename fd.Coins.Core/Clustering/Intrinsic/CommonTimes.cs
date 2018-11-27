using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    /// <summary>
    /// Feature Extractor: Extracts the hour of the day from the BlockTime. (2018-01-01 12:34:56 -> 12)
    /// Outputs a 24 entries vector indicating how often each hour of the day has been observed.
    /// Example:
    /// hotd:   0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20  21  22  23  24
    /// feat:   1   0   0   0   0   0   0   2   9   3   0   0   0   0   1   0   3   3   1   0   0   0   0   0   0
    /// </summary>
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
            //var res = v1.Select((x, i) => Math.Sqrt(Math.Pow(x - v2[i], 2)) / Math.Sqrt(Math.Pow(x + v2[i], 2))).Sum();
            var numerator = Math.Sqrt(v1.Select((x, i) => Math.Pow(x - v2[i], 2)).Sum());
            var denominator = v1.Sum() + v2.Sum();
            var res = numerator / denominator;
            return double.IsNaN(res) ? 0 : res;
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
                    var kvp = mainDB.Query($"SELECT list($hour) as hour FROM Link LET $hour = outV().BlockTime.format('k').asLong() WHERE tAddr = '{address}'").Select(x => new KeyValuePair<string, int[]>(address, ToFeatureVector(x.GetField<List<long>>("hour")))).First();

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
