using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web.Script.Serialization;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class TimeSlots : Clustering
    {
        private Dictionary<string, BitArray> _result;
        public TimeSlots()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "TimeSlots";
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
            using (var mainDB = new ODatabase(mainOptions))
            {
                var timeSlots = mainDB.Query($"SELECT $hour as hour, list(tAddr) as addresses FROM Link LET $hour = outV().BlockTime.format('k').asLong() WHERE tAddr in [{string.Join(",", addresses.Select(x => "'" + x + "'"))}] GROUP BY $hour").ToDictionary(x => x.GetField<long>("hour"), y => y.GetField<List<string>>("addresses"));
                AddToResult(timeSlots);
            }
        }

        public override void ToFile(string path)
        {
            Directory.CreateDirectory(path);
            File.WriteAllText(Path.Combine(path, _options.DatabaseName + ".txt"), new JavaScriptSerializer().Serialize(_result));
        }

        private void AddToResult(Dictionary<long, List<string>> query)
        {
            foreach (var kvp in query)
            {
                foreach (var address in kvp.Value)
                {
                    if (!string.IsNullOrEmpty(address))
                    {
                        var slots = new BitArray(24);
                        slots.Set(Convert.ToInt32(kvp.Key) - 1, true);
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
    }
}
