using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;

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

            _dimension = 0;

            _result = new Dictionary<string, long>();
        }
        public override void Run(ConnectionOptions mainOptions, IEnumerable<ORID> rids)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                var totalAmounts = mainDB.Command($"SELECT sum(inE().amount).asLong() as total, inE().tAddr as addresses FROM (SELECT * FROM [{string.Join(",", rids.Select(x => x.RID))}] WHERE Coinbase = false AND Unlinked = false) GROUP BY @rid").ToList().Select(x => new KeyValuePair<long, List<string>>(x.GetField<long>("total").RoundToSignificant(), x.GetField<List<string>>("addresses"))).GroupBy(x => x.Key).ToDictionary(x => x.Key, y => y.SelectMany(z => z.Value).Distinct().ToList());
                _result = totalAmounts.ToAverage();
                //foreach (var cluster in totalAmounts.Where(x => x.Value.Count > 1))
                //{
                //    var addresses = cluster.Value;
                //    var amount = cluster.Key;
                //    using (var resultDB = new ODatabase(_options))
                //    {
                //        OVertex root = null;
                //        for (var i = 0; i < addresses.Count; i++)
                //        {
                //            Utils.RetryOnConcurrentFail(3, () =>
                //            {
                //                var tx = resultDB.Transaction;
                //                try
                //                {
                //                    root = resultDB.Select().From("Node").Where("Amount").Equals(amount)?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Amount", amount).Set("Address", amount.GetHashCode().ToString()).Run();
                //                    tx.AddOrUpdate(root);
                //                    var cur = resultDB.Select().From("Node").Where("Address").Equals(addresses[i])?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", addresses[i]).Run();
                //                    tx.AddOrUpdate(cur);
                //                    tx.AddEdge(new OEdge() { OClassName = _options.DatabaseName }, root, cur);
                //                    tx.Commit();
                //                }
                //                catch (Exception e)
                //                {
                //                    tx.Reset();
                //                    return false;
                //                }
                //                return true;
                //            });
                //        }
                //    }
                //}
            }
        }

        protected override void AddToResult<TKey, TValue>(Dictionary<TKey, TValue> query)
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
                        _result[address] = ((long)(_result[address as string]) + Convert.ToInt64(kvp.Key)) / 2;
                    }
                }
            }
        }
    }
}
