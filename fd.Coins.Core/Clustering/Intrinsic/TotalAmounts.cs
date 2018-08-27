using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class TotalAmounts : Clustering
    {
        public TotalAmounts()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "TotalAmounts";
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
                var totalAmounts = mainDB.Command($"SELECT sum(inE().amount).asLong() as total, inE().tAddr as addresses FROM (SELECT * FROM [{string.Join(",", rids.Select(x => x.RID))}] WHERE Coinbase = false AND Unlinked = false) GROUP BY total").ToList().Select(x => new KeyValuePair<long, List<string>>(x.GetField<long>("total"),x.GetField<List<string>>("addresses"))).ToDictionary(x => x.Key, y => y.Value.Distinct().ToList());
                Console.WriteLine($"TotalAmounts:\n{string.Join("\n", totalAmounts.Select(x => x.Key + ":" + string.Join(",", x.Value)))}");
                Console.WriteLine("==========");
                Parallel.ForEach(totalAmounts.Select(x => x.Value), (addresses) =>
                {
                    using (var resultDB = new ODatabase(_options))
                    {
                        for (var i = 0; i < addresses.Count - 1; i++)
                        {
                            try
                            {
                                Utils.RetryOnConcurrentFail(3, () =>
                                {
                                    var tx = resultDB.Transaction;
                                    try
                                    {
                                        var cur = resultDB.Select().From("Node").Where("Address").Equals(addresses[i])?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", addresses[i]).Run();
                                        var next = resultDB.Select().From("Node").Where("Address").Equals(addresses[i + 1])?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", addresses[i + 1]).Run();
                                        tx.AddOrUpdate(cur);
                                        tx.AddOrUpdate(next);
                                        tx.AddEdge(new OEdge() { OClassName = _options.DatabaseName }, cur, next);
                                        tx.Commit();
                                    }
                                    catch (Exception e)
                                    {
                                        tx.Reset();
                                        return false;
                                    }
                                    return true;
                                });
                            }
                            catch (InvalidOperationException e)
                            {
                                File.AppendAllText("log.txt", $"Cluster [{string.Join(",", addresses)}] could not be created.\n");
                            }
                        }
                    }
                });
            }
        }

        private Tuple<Int64, List<string>> ToTuple(ODocument x)
        {
            long total = 0;
            try
            {
                total = (Int64)(x["total"]);
            }
            catch (Exception e) { }
            return Tuple.Create(total, x.GetField<List<string>>("addresses"));
        }
    }
}
