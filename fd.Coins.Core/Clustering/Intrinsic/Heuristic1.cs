using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    // heuristic #1: all addresses that are input to the same transaction are considered to belong to the same owner
    public class Heuristic1 : Clustering
    {
        private List<List<string>> _result;
        private ConnectionOptions _options;
        public Heuristic1()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "FistfulH1";
            _options.DatabaseType = ODatabaseType.Graph;
            _options.HostName = "localhost";
            _options.Password = "admin";
            _options.Port = 2424;
            _options.UserName = "admin";

            _result = new List<List<string>>();

            //Recreate();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result.FirstOrDefault(x => x.Contains(addr1));
            var v2 = _result.FirstOrDefault(x => x.Contains(addr2));
            return (v1 != null && v2 != null && v1.SequenceEqual(v2)) ? 0.0 : 1.0;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            var inGroups = new List<List<string>>();
            using (var mainDB = new ODatabase(mainOptions))
            {
                inGroups = mainDB.Query($"SELECT inV().inE().tAddr AS address FROM Link WHERE tAddr IN [{string.Join(",", addresses.Select(x => "'" + x + "'"))}]").Select(x => x.GetField<List<string>>("address").Where(y => !string.IsNullOrEmpty(y)).Distinct().ToList()).Where(x => x.Count > 1).Distinct().ToList();
                //using (var resultDB = new ODatabase(_options))
                //{
                //    resultDB.DatabaseProperties.ORID = new ORID();
                //    // get distinct addresses from groups to create nodes
                //    var nodes = inGroups.SelectMany(x => x).Distinct();
                //    foreach (var node in nodes)
                //    {
                //        var record = new OVertex();
                //        record.OClassName = "Node";
                //        record.SetField("Address", node);
                //        resultDB.Transaction.Add(record);
                //    }
                //    resultDB.Transaction.Commit();
                //    // connect nodes of each group
                //    var pairs = inGroups.SelectMany(c => c.SelectMany(x => c, (x, y) => Tuple.Create( x, y ))).Where(p => Comparer<string>.Default.Compare(p.Item1, p.Item2) < 0).Distinct().OrderBy(x => x.Item1);
                //    foreach (var pair in pairs)
                //    {
                //        var n1 = resultDB.Select().From("Node").Where("Address").Equals(pair.Item1).ToList<OVertex>().FirstOrDefault();
                //        var n2 = resultDB.Select().From("Node").Where("Address").Equals(pair.Item2).ToList<OVertex>().FirstOrDefault();
                //        var record = new OEdge();
                //        record.OClassName = _options.DatabaseName;
                //        resultDB.Transaction.AddEdge(record, n1, n2);
                //    }
                //    resultDB.Transaction.Commit();
                //}
            }
            //using (var resultDB = new ODatabase(_options))
            //{
            //    // get the root of each connected component in the graph
            //    var roots = resultDB.Command("SELECT distinct(traversedElement(0)) AS root FROM (TRAVERSE * FROM V)").ToList().Select(x => x.GetField<ORID>("root"));
            //    // traverse from each root to get addresses of each connected component as list
            //    foreach (var root in roots)
            //    {
            //        var cluster = resultDB.Command($"TRAVERSE * FROM {root.RID}").ToList().Select(x => x.GetField<string>("Address")).Where(x => !string.IsNullOrEmpty(x)).ToList();
            //        if (cluster.Count() > 1)
            //            _result.Add(cluster);
            //    }
            //}
            var cc = new ClusteringCollapser();
            cc.Collapse(inGroups);
            _result = cc.Clustering;
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        private List<string> Sort(List<string> items)
        {
            items.Sort();
            return items;
        }

        public void Recreate()
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
                db.Command($"CREATE CLASS {_options.DatabaseName} EXTENDS E");
                db.Command("CREATE INDEX IndexForAddress ON Node (Address) UNIQUE_HASH_INDEX");
            }
        }
    }
}
