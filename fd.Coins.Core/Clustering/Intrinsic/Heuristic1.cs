using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web.Script.Serialization;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    // heuristic #1: all addresses that are input to the same transaction are considered to belong to the same owner
    public class Heuristic1 : Clustering
    {
        private List<List<string>> _result;
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

            Recreate();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result.SelectMany(x => x.Where(y => y.Contains(addr1)));
            var v2 = _result.SelectMany(x => x.Where(y => y.Contains(addr2)));
            return v1.SequenceEqual(v2) ? 0.0 : 1.0;
        }

        public override void FromFile(string path)
        {
            _result = new JavaScriptSerializer().Deserialize<List<List<string>>>(File.ReadAllText(Path.Combine(path, _options.DatabaseName + ".txt")));
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            using (var mainDB = new ODatabase(mainOptions))
            {
                Console.WriteLine($"\tGetting groups...");
                var inGroups = mainDB.Query($"SELECT inV().inE().tAddr AS address FROM Link WHERE tAddr IN [{string.Join(",", addresses.Select(x => "'" + x + "'"))}]").Select(x => x.GetField<List<string>>("address").Where(y => !string.IsNullOrEmpty(y)).Distinct().ToList()).Where(x => x.Count > 1).Distinct();
                using (var resultDB = new ODatabase(_options))
                {
                    resultDB.DatabaseProperties.ORID = new ORID();
                    foreach (var group in inGroups.Select(x => x.Distinct()))
                    {
                        var orids = new Dictionary<string, ORID>();
                        foreach (var address in group)
                        {
                            var node = new OVertex();
                            node.OClassName = "Node";
                            node.SetField("Address", address);
                            resultDB.Transaction.Add(node);
                            orids.Add(address, node.ORID);
                        }
                        for(var i = 0; i < group.Count() - 1; i++)
                        {
                            var link = new OEdge();
                            link.OClassName = "Link";
                            var from = resultDB.Transaction.GetPendingObject<OVertex>(orids[group.ElementAt(i)]);
                            var to = resultDB.Transaction.GetPendingObject<OVertex>(orids[group.ElementAt(i + 1)]);
                            resultDB.Transaction.AddEdgeIfNotExists(link, from, to);
                        }
                    }
                    resultDB.Transaction.Commit();
                }
                //foreach (var group in inGroups.Where(x => x.Count > 1))
                //{
                //    Console.WriteLine($"\t\tInserting group...");
                //    using (var resultDB = new ODatabase(_options))
                //    {
                //        for (var i = 0; i < group.Count - 1; i++)
                //        {
                //            Utils.RetryOnConcurrentFail(3, () =>
                //            {
                //                var tx = resultDB.Transaction;
                //                OVertex cur = null, next = null;
                //                try
                //                {
                //                    cur = resultDB.Select().From("Node").Where("Address").Equals(group[i])?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", group[i]).Run();
                //                    next = resultDB.Select().From("Node").Where("Address").Equals(group[i + 1])?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", group[i + 1]).Run();
                //                    tx.AddOrUpdate(cur);
                //                    tx.AddOrUpdate(next);
                //                    tx.AddEdge(new OEdge() { OClassName = _options.DatabaseName }, cur, next);
                //                    tx.Commit();
                //                }
                //                catch
                //                {
                //                    tx.Reset();
                //                    throw;
                //                }
                //                return true;
                //            });
                //        }
                //    }
                //}
            }
            Console.WriteLine($"\tParsing connected components...");
            using (var resultDB = new ODatabase(_options))
            {
                // get the root of each connected component in the graph
                var roots = resultDB.Command("SELECT distinct(traversedElement(0)) AS root FROM (TRAVERSE * FROM V)").ToList().Select(x => x.GetField<ORID>("root"));
                // traverse from each root to get addresses of each connected component as list
                foreach (var root in roots)
                {
                    var cluster = resultDB.Command($"TRAVERSE * FROM {root.RID}").ToList().Select(x => x.GetField<string>("Address")).Where(x => !string.IsNullOrEmpty(x)).ToList();
                    if (cluster.Count() > 1)
                        _result.Add(cluster);
                }
            }
            sw.Stop();
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        public override void ToFile(string path)
        {
            Directory.CreateDirectory(path);
            File.WriteAllText(Path.Combine(path, _options.DatabaseName + ".txt"), new JavaScriptSerializer().Serialize(_result));
        }
    }
}
