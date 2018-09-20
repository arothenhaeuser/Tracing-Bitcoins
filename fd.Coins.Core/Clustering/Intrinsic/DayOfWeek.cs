﻿using Orient.Client;
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
    public class DayOfWeek : Clustering
    {
        private Dictionary<string, BitArray> _result;
        public DayOfWeek()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "DayOfWeek";
            _options.DatabaseType = ODatabaseType.Graph;
            _options.HostName = "localhost";
            _options.Password = "admin";
            _options.Port = 2424;
            _options.UserName = "admin";

            _dimension = 1;

            _result = new Dictionary<string, BitArray>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result[addr1];
            var v2 = _result[addr2];
            return Math.Sqrt(v1.Xor(v2).OfType<bool>().Count(x => x));
        }

        public override void FromFile(string path)
        {
            _result = new JavaScriptSerializer().Deserialize<Dictionary<string, BitArray>>(File.ReadAllText(Path.Combine(path, _options.DatabaseName + ".txt")));
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            using (var mainDB = new ODatabase(mainOptions))
            {
                _result = mainDB.Query($"SELECT list($day) as day, tAddr as address FROM Link LET $day = outV().BlockTime.format('E') WHERE tAddr in [{string.Join(",", addresses.Select(x => "'" + x + "'"))}] GROUP BY tAddr").ToDictionary(x => x.GetField<string>("address"), y => ToBitArray(y.GetField<List<string>>("day")));
            }
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        private BitArray ToBitArray(List<string> list)
        {
            var dist = list.Distinct();
            var ret = new BitArray(7);
            foreach(var value in dist)
            {
                ret.Set(ToInt(value), true);
            }
            return ret;
        }

        public override void ToFile(string path)
        {
            Directory.CreateDirectory(path);
            File.WriteAllText(Path.Combine(path, _options.DatabaseName + ".txt"), new JavaScriptSerializer().Serialize(_result));
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
