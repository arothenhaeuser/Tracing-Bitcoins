using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class TimeSlots : Clustering
    {
        ConnectionOptions _options;
        public TimeSlots()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "TimeSlots";
            _options.DatabaseType = ODatabaseType.Graph;
            _options.HostName = "localhost";
            _options.Password = "admin";
            _options.Port = 2424;
            _options.UserName = "admin";

            Recreate();
        }
        public void Run(ConnectionOptions mainOptions)
        {
            using(var mainDB = new ODatabase(mainOptions))
            {
                var timeSlots = mainDB.Command($"SELECT $timeSlot AS timeSlot, list(inE().tAddr) AS addresses FROM Transaction LET $timeSlot = BlockTime.format('H') GROUP BY $timeSlot").ToList().Select(x => new KeyValuePair<Int32, List<string>>(x.GetField<Int32>("timeSlot"), x.GetField<List<string>>("addresses"))).ToDictionary(x => x.Key, y => y.Value);
            }
        }

        public void ToFile(string path)
        {
            using (var resultDB = new ODatabase(_options))
            {
                // get the root of each connected component in the graph
                var roots = resultDB.Command("SELECT distinct(traversedElement(0)) AS root FROM (TRAVERSE * FROM V)").ToList().Select(x => x.GetField<ORID>("root"));
                // traverse from each root to get addresses of each connected component as list
                var addrClusters = new List<IEnumerable<string>>();
                foreach (var root in roots)
                {
                    var cluster = resultDB.Command($"TRAVERSE * FROM {root.RID}").ToList().Select(x => x.GetField<string>("Address")).Where(x => !string.IsNullOrEmpty(x));
                    if (cluster.Count() > 1)
                        addrClusters.Add(cluster);
                }
                Directory.CreateDirectory(path);
                File.WriteAllLines(Path.Combine(path, _options.DatabaseName + ".txt"), addrClusters.Select(x => string.Join("\t", x)));
            }
        }

        private void Recreate()
        {
            // check if an old version of the resulting graph already exists and delete it
            var server = new OServer("localhost", 2424, "root", "root");
            if (server.DatabaseExist(_options.DatabaseName, OStorageType.PLocal))
            {
                server.DropDatabase(_options.DatabaseName, OStorageType.PLocal);
            }
            // create a new graph for the address clusters
            server.CreateDatabase(_options.DatabaseName, _options.DatabaseType, OStorageType.PLocal);
            using (var db = new ODatabase(_options))
            {
                db.Command("CREATE CLASS Node EXTENDS V");
                db.Command("CREATE PROPERTY Node.Address STRING");
                db.Command("CREATE INDEX IndexForAddress ON Node (Address) UNIQUE_HASH_INDEX");
            }
        }
    }
}
