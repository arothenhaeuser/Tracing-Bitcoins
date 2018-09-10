using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections;
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

            _dimension = 1;
            _result = new Dictionary<string, BitArray>();
        }
        public override void Run(ConnectionOptions mainOptions, IEnumerable<ORID> rids)
        {
            using (var mainDB = new ODatabase(mainOptions))
            {
                var timeSlots = mainDB.Query($"SELECT $timeSlot.asLong() AS timeSlot, list(inE().tAddr) AS addresses FROM [{string.Join(",", rids.Select(x => x.RID))}] LET $timeSlot = BlockTime.format('k') GROUP BY $timeSlot").ToDictionary(x => x.GetField<long>("timeSlot"), y => y.GetField<List<string>>("addresses"));
                AddToResult(timeSlots);
            }
        }

        protected override void AddToResult<TKey, TValue>(Dictionary<TKey, TValue> query)
        {
            foreach (var kvp in query)
            {
                foreach (var address in kvp.Value)
                {
                    var slots = new BitArray(24);
                    slots.Set(Convert.ToInt32(kvp.Key) - 1, true);
                    try
                    {
                        _result.Add(address, slots);
                    }
                    catch (ArgumentException)
                    {
                        _result[address] = (_result[address] as BitArray).Or(slots);
                    }
                }
            }
        }
    }
}
