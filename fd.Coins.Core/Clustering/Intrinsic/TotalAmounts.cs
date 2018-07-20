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
                var totalAmounts = mainDB.Command($"SELECT sum(inE().amount) as total, inE().tAddr as addresses FROM (SELECT * FROM Transaction WHERE Coinbase = false AND Unlinked = false LIMIT 10000) GROUP BY @rid").ToList().Select(x => Tuple.Create(x.GetField<Int64>("total"), x.GetField<List<string>>("addresses")));
                Parallel.ForEach(totalAmounts.Select(x => x.Item2), (addresses) =>
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
