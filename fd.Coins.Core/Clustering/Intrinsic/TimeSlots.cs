using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.Linq;

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
        public override void Run(ConnectionOptions mainOptions, IEnumerable<ORID> rids)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                var timeSlots = mainDB.Command($"SELECT $timeSlot.asLong() AS timeSlot, list(inE().tAddr) AS addresses FROM [{string.Join(",", rids.Select(x => x.RID))}] LET $timeSlot = BlockTime.format('H') GROUP BY $timeSlot").ToList().Select(x => new KeyValuePair<long, List<string>>(x.GetField<Int64>("timeSlot"), x.GetField<List<string>>("addresses"))).ToDictionary(x => x.Key, y => y.Value.Distinct().ToList());
                foreach(var cluster in timeSlots)
                {
                    var addresses = cluster.Value;
                    var time = cluster.Key;
                    using (var resultDB = new ODatabase(_options))
                    {
                        OVertex root = null;
                        for (var i = 0; i < addresses.Count; i++)
                        {
                            Utils.RetryOnConcurrentFail(3, () =>
                            {
                                var tx = resultDB.Transaction;
                                try
                                {
                                    root = resultDB.Select().From("Node").Where("Time").Equals(time)?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Time", time).Set("Address", time.GetHashCode().ToString()).Run();
                                    tx.AddOrUpdate(root);
                                    var cur = resultDB.Select().From("Node").Where("Address").Equals(addresses[i])?.ToList<OVertex>().FirstOrDefault() ?? resultDB.Create.Vertex("Node").Set("Address", addresses[i]).Run();
                                    tx.AddOrUpdate(cur);
                                    tx.AddEdge(new OEdge() { OClassName = _options.DatabaseName }, root, cur);
                                    tx.Commit();
                                }
                                catch(Exception e)
                                {
                                    tx.Reset();
                                    return false;
                                }
                                return true;
                            });
                        }
                    }
                }
            }
        }
    }
}
