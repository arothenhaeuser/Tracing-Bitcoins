using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    // heuristic #2: The change address of each transaction is considered to belong to the same user as the inputs. Change addresses are identified by checking the corresponding output against four conditions:
    // 1. This is the only output referencing this address
    // 2. This output does not belong to a coinbase transaction
    // 3. The address is not referenced in one of the transactions inputs (prevOut)
    // 4. Only one output of this transaction matches the above (ambiguity)
    public class Heuristic2 : Clustering
    {
        public Heuristic2()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "FistfulH2";
            _options.DatabaseType = ODatabaseType.Graph;
            _options.HostName = "localhost";
            _options.Password = "admin";
            _options.Port = 2424;
            _options.UserName = "admin";

            Recreate();
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<ORID> rids)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                var records = mainDB.Command($"SELECT * FROM [{string.Join(",", rids.Select(x => x.RID))}]").ToList();
                foreach(var record in records)
                {
                    using (var resultDB = new ODatabase(_options))
                    {
                        // can we identify a return address?
                        var addr = GetChangeAddress(record);
                        if (addr != null)
                        {
                            Utils.RetryOnConcurrentFail(3, () =>
                            {
                                using (var mainDBp = new ODatabase(mainOptions))
                                {
                                    var tx = resultDB.Transaction;
                                    try
                                    {
                                        var returnNode = resultDB.Select().From("Node").Where("Address").Equals(addr)?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", addr).Run();
                                        tx.AddOrUpdate(returnNode);
                                        var sourceAddresses = mainDBp.Command($"SELECT inE().tAddr AS address FROM {record.ORID}").ToSingle().GetField<List<string>>("address").ToList();
                                        foreach (var address in sourceAddresses)
                                        {
                                            var sourceNode = resultDB.Select().From("Node").Where("Address").Equals(address)?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", address).Run();
                                            tx.AddOrUpdate(sourceNode);
                                            tx.AddEdge(new OEdge() { OClassName = _options.DatabaseName }, returnNode, sourceNode);
                                        }
                                        tx.Commit();
                                    }
                                    catch
                                    {
                                        tx.Reset();
                                        return false;
                                    }
                                    return true;
                                }
                            });
                        }
                    }
                }
            }
        }

        private static string GetChangeAddress(ODocument node)
        {
            var outputs = GetOutputStrings(node);
            
            var ambigous = false;
            string changeAddress = null;
            if (outputs.Length > 1)
            {
                if (!node.GetField<bool>("Coinbase") && !node.GetField<bool>("Unlinked"))
                {
                    foreach (var output in outputs)
                    {
                        var outputAddress = output.Split(':')[1];
                        using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
                        {
                            var inputAddresses = db.Command($"SELECT tAddr FROM (SELECT expand(inE()) FROM {node.ORID})").ToList().Select(x => x.GetField<string>("tAddr"));
                            if (!inputAddresses.Contains(outputAddress))
                            {
                                if (db.Command($"SELECT count(*) FROM Link WHERE tAddr = '{outputAddress}'").ToSingle().GetField<Int64>("count") == 1) // PERFORMANCE!
                                {
                                    if (changeAddress == null)
                                    {
                                        changeAddress = outputAddress;
                                    }
                                    else
                                    {
                                        ambigous = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (ambigous || changeAddress == null)
            {
                return null;
            }
            else
            {
                return changeAddress;
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
    }
}
