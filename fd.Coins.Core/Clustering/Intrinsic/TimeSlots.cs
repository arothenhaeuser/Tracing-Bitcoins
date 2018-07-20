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
                                Utils.RetryOnConcurrentFail(3, () =>
                                {
                                    var cur = resultDB.Command($"UPDATE Node SET Address = '{addresses[i]}' UPSERT RETURN AFTER $current WHERE Address = '{addresses[i]}'").ToSingle();
                                    var next = resultDB.Command($"UPDATE Node SET Address = '{addresses[i + 1]}' UPSERT RETURN AFTER $current WHERE Address = '{addresses[i + 1]}'").ToSingle();
                                    resultDB.Command($"CREATE EDGE {_options.DatabaseName} FROM {cur.ORID} TO {next.ORID}");
                                    return true;
                                });
                        }
                    });
            }
        }
    }
}
