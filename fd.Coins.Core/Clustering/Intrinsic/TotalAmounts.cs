using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        public override void Run(ConnectionOptions mainOptions)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                var totalAmounts = mainDB.Command($"SELECT sum(inE().amount) as total, inE().tAddr as addresses FROM (SELECT * FROM Transaction WHERE Coinbase = false AND Unlinked = false) GROUP BY @rid").ToList().Select(x => ToTuple(x));
                Parallel.ForEach(totalAmounts.Select(x => x.Item2), (addresses) =>
                {
                    using (var resultDB = new ODatabase(_options))
                    {
                        for (var i = 0; i < addresses.Count - 1; i++)
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
                                catch
                                {
                                    tx.Reset();
                                    return false;
                                }
                                return true;
                            });
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
            catch (Exception e)
            {
                Console.WriteLine(x.ContainsKey("total"));
                Console.WriteLine($"{x["total"]} is of type {x["total"].GetType()}. Casting to {total.GetType()} returned: {total}.");//
            }
            return Tuple.Create(total, x.GetField<List<string>>("addresses"));
        }
    }
}
