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
    /// Feature Extractor: Extracts the day of the week from the BlockTime. (2018-01-01 12:34:56 -> Mo)
    /// Outputs a 7 entries vector indicating which days of the week have been observed.
    /// Example:
    /// dotw:   0   1   2   3   4   5   6   7
    /// feat:   0   0   0   1   0   0   0   1
    /// </summary>
    public class DayOfWeek : Clustering
    {
        private Dictionary<string, BitArray> _result;

        public DayOfWeek()
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
                    var kvp = mainDB.Query($"SELECT list($day) as day FROM (SELECT * FROM Link WHERE tAddr = '{address}' LIMIT 500) LET $day = outV().BlockTime.format('E')").Select(x => new KeyValuePair<string, BitArray>(address, ToBitArray(x.GetField<List<string>>("day")))).First();
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

        private BitArray ToBitArray(List<string> list)
        {
            var dist = list.Distinct();
            var ret = new BitArray(7);
            foreach (var value in dist)
            {
                ret.Set(ToInt(value), true);
            }
            return ret;
        }

        private void AddToResult(Dictionary<string, List<string>> query)
        {
            foreach (var kvp in query)
            {
                foreach (var address in kvp.Value)
                {
                    if (!string.IsNullOrEmpty(address))
                    {
                        var slots = new BitArray(7);
                        slots.Set(ToInt(kvp.Key), true);
                        try
                        {
                            _result.Add(address, slots);
                        }
                        catch (ArgumentException)
                        {
                            _result[address] = _result[address].Or(slots);
                        }
                    }
                }
            }
        }

        private int ToInt(string dayOfMonth)
        {
            switch (dayOfMonth)
            {
                case "Mo":
                    return 0;
                case "Di":
                    return 1;
                case "Mi":
                    return 2;
                case "Do":
                    return 3;
                case "Fr":
                    return 4;
                case "Sa":
                    return 5;
                case "So":
                    return 6;
                default:
                    return -1;
            }
        }
    }
}
