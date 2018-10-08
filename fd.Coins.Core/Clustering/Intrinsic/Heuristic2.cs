using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web.Script.Serialization;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    // heuristic #2: The change address of each transaction is considered to belong to the same user as the inputs. Change addresses are identified by checking the corresponding output against four conditions:
    // 1. This is the only output referencing this address
    // 2. This output does not belong to a coinbase transaction
    // 3. The address is not referenced in one of the transactions inputs (prevOut)
    // 4. Only one output of this transaction matches the above (ambiguity)
    public class Heuristic2 : Clustering
    {
        private List<List<string>> _result;
        private ConnectionOptions _options;

        public Heuristic2()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "FistfulH2";
            _options.DatabaseType = ODatabaseType.Graph;
            _options.HostName = "localhost";
            _options.Password = "admin";
            _options.Port = 2424;
            _options.UserName = "admin";

            _result = new List<List<string>>();

            Recreate();
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            using (var mainDB = new ODatabase(mainOptions))
            {
                var records = mainDB.Query($"SELECT tx, list(tx.inE().tAddr) as source, list(tx.outE().tAddr) as target FROM (SELECT inV() as tx FROM Link WHERE tAddr in [{string.Join(",", addresses.Select(x => "'" + x + "'"))}]) WHERE tx.Coinbase = false AND tx.Unlinked = false GROUP BY tx").ToDictionary(x => x.GetField<ORID>("tx").ToString(), y => new List<List<string>>() { y.GetField<List<string>>("source"), y.GetField<List<string>>("target") });
                var groups = new List<List<string>>();
                foreach (var record in records)
                {
                    // can we identify a return address?
                    var addr = GetChangeAddress(record, mainOptions);
                    if (addr != null)
                    {
                        // add a new group
                        var group = record.Value.First();
                        group.Add(addr);
                        groups.Add(group);
                    }
                }
                using (var resultDB = new ODatabase(_options))
                {
                    resultDB.DatabaseProperties.ORID = new ORID();
                    // get distinct addresses from groups to create nodes
                    var nodes = groups.SelectMany(x => x).Distinct();
                    foreach (var node in nodes)
                    {
                        var record = new OVertex();
                        record.OClassName = "Node";
                        record.SetField("Address", node);
                        resultDB.Transaction.Add(record);
                    }
                    resultDB.Transaction.Commit();
                    // connect nodes of each group
                    var pairs = groups.SelectMany(c => c.SelectMany(x => c, (x, y) => Tuple.Create(x, y))).Where(p => Comparer<string>.Default.Compare(p.Item1, p.Item2) < 0).Distinct().OrderBy(x => x.Item1);
                    foreach (var pair in pairs)
                    {
                        var n1 = resultDB.Select().From("Node").Where("Address").Equals(pair.Item1).ToList<OVertex>().FirstOrDefault();
                        var n2 = resultDB.Select().From("Node").Where("Address").Equals(pair.Item2).ToList<OVertex>().FirstOrDefault();
                        var record = new OEdge();
                        record.OClassName = _options.DatabaseName;
                        resultDB.Transaction.AddEdge(record, n1, n2);
                    }
                    resultDB.Transaction.Commit();
                }
            }
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

        private string GetChangeAddress(KeyValuePair<string, List<List<string>>> kvp, ConnectionOptions mainOptions)
        {
            string changeAddress = null;
            var ambiguous = false;
            using (var mainDB = new ODatabase(mainOptions))
            {
                var inAddresses = kvp.Value.First();
                var outAddresses = kvp.Value.Last();
                foreach (var outAddress in outAddresses)
                {
                    // address is no self-change address
                    if (inAddresses.Contains(outAddress))
                    {
                        continue;
                    }
                    // address is not reused
                    if (mainDB.Command($"SELECT count(*) FROM Link WHERE tAddr = '{outAddress}'").ToSingle().GetField<Int64>("count") != 1)
                    {
                        continue;
                    }
                    if (changeAddress == null)
                    {
                        changeAddress = outAddress;
                    }
                    else
                    {
                        ambiguous = true;
                        break;
                    }
                }
                return ambiguous ? null : changeAddress;
            }
        }

        private static string[] GetOutputStrings(ODocument node)
        {
            var list = new List<string>();
            var index = 0;
            while (node.ContainsKey($"OUTPUT{index}"))
            {
                list.Add(node.GetField<string>($"OUTPUT{index++}"));
            }
            return list.ToArray();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result.SelectMany(x => x.Where(y => y.Contains(addr1)));
            var v2 = _result.SelectMany(x => x.Where(y => y.Contains(addr2)));
            return (v1.Any() && v1.SequenceEqual(v2)) ? 0.0 : 1.0;
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
