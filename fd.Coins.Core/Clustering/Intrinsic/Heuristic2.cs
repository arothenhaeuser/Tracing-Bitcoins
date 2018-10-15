using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

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

            //Recreate();
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} running...");
            var sw = new Stopwatch();
            sw.Start();
            var groups = new List<List<string>>();
            using (var mainDB = new ODatabase(mainOptions))
            {
                var records = mainDB.Query($"SELECT tx, list(tx.inE().tAddr) as source, list(tx.outE().tAddr) as target FROM (SELECT inV() as tx FROM Link WHERE tAddr in [{string.Join(",", addresses.Select(x => "'" + x + "'"))}]) WHERE tx.Coinbase = false AND tx.Unlinked = false GROUP BY tx").ToDictionary(x => x.GetField<ORID>("tx").ToString(), y => new List<List<string>>() { y.GetField<List<string>>("source"), y.GetField<List<string>>("target") });
                foreach (var record in records)
                {
                    // can we identify a return address?
                    var addr = GetChangeAddress(record, mainOptions);
                    if (addr != null)
                    {
                        // add a new group
                        var group = record.Value.First();
                        group.Add(addr);
                        group.RemoveAll(x => string.IsNullOrEmpty(x));
                        groups.Add(group);
                    }
                }
            }
            var cc = new ClusteringCollapser();
            cc.Collapse(groups);
            _result = cc.Clustering;
            // DEBUG
            Console.WriteLine($"{GetType().Name}.{MethodBase.GetCurrentMethod().Name} done. {sw.Elapsed}");
        }

        private string GetChangeAddress(KeyValuePair<string, List<List<string>>> kvp, ConnectionOptions mainOptions)
        {
            string changeAddress = null;
            var ambiguous = false;
            using (var mainDB = new ODatabase(mainOptions))
            {
                var inAddresses = kvp.Value.First().Distinct().ToList();
                var outAddresses = kvp.Value.Last().Distinct().ToList();
                if(inAddresses.Count == 0 || outAddresses.Count == 0)
                {
                    return null;
                }
                var occurrences = mainDB.Command($"SELECT tAddr AS address, count(tAddr) AS count FROM Link WHERE tAddr IN [{string.Join(",", outAddresses.Select(x => $"'{x}'"))}] GROUP BY tAddr").ToList().ToDictionary(x => x.GetField<string>("address"), y => (int)y.GetField<long>("count"));
                foreach (var outAddress in outAddresses)
                {
                    // address is no self-change address
                    if (inAddresses.Contains(outAddress))
                    {
                        continue;
                    }
                    // address is not reused
                    if (occurrences[outAddress] != 1)
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
            var v1 = _result.FirstOrDefault(x => x.Contains(addr1));
            var v2 = _result.FirstOrDefault(x => x.Contains(addr2));
            return (v1 != null && v2 != null && v1.SequenceEqual(v2)) ? 0.0 : 1.0;
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
