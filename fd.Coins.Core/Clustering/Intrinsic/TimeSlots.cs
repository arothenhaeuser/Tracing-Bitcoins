using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Core.Clustering.Intrinsic
{
    public class TimeSlots : Clustering
    {
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
        public override void Run(ConnectionOptions mainOptions)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                var timeSlots = mainDB.Command($"SELECT $timeSlot.asLong() AS timeSlot, list(inE().tAddr) AS addresses FROM Transaction LET $timeSlot = BlockTime.format('H') GROUP BY $timeSlot").ToList().Select(x => new KeyValuePair<Int64, List<string>>(x.GetField<Int64>("timeSlot"), x.GetField<List<string>>("addresses"))).ToDictionary(x => x.Key, y => y.Value);
                Parallel.ForEach(timeSlots.Select(x => x.Value), (addresses) =>
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
                                    resultDB.Command($"CREATE EDGE {_options.DatabaseName} FROM {cur.ORID} TO {next.ORID}");
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
    }
}
