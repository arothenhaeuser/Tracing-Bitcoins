using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web.Script.Serialization;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class TotalAmounts : Clustering
    {
        private Dictionary<string, double> _result;
        public TotalAmounts()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "TotalAmounts";
            _options.DatabaseType = ODatabaseType.Graph;
            _options.HostName = "localhost";
            _options.Password = "admin";
            _options.Port = 2424;
            _options.UserName = "admin";

            _dimension = 0;

            _result = new Dictionary<string, double>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result[addr1];
            var v2 = _result[addr2];
            return Math.Abs(v1 - v2)/(v1 + v2);
        }

        public override void FromFile(string path)
        {
            _result = new JavaScriptSerializer().Deserialize<Dictionary<string, double>>(File.ReadAllText(Path.Combine(path, _options.DatabaseName + ".txt")));
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                _result = mainDB.Query($"SELECT avg(inV().inE().amount).asLong() as total, tAddr as address FROM Link WHERE tAddr IN [{string.Join(",", addresses.Select(x => "'" + x + "'"))}] GROUP BY tAddr").ToDictionary(x => x.GetField<string>("address"), y => (double)y.GetField<long>("total").RoundToSignificant());
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
                    try
                    {
                        _result.Add(address, kvp.Key);
                    }
                    catch (ArgumentException)
                    {
                        _result[address] = (_result[address] + kvp.Key) / 2;
                    }
                }
            }
        }
    }
}
