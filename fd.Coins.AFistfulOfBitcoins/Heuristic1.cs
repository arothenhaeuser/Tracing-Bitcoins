﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using fd.Coins.Core.Clustering;
using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;

namespace fd.Coins.AFistfulOfBitcoins
{
    // heuristic #1: all addresses that are input to the same transaction are considered to belong to the same owner
    public class Heuristic1 : Clustering
    {
        public Heuristic1()
        {
            _options = new ConnectionOptions();
            _options.DatabaseName = "Fistful_H1";
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
                var inGroups = mainDB.Command($"SELECT inE().tAddr AS address FROM [{string.Join(",", rids.Select(x => x.RID))}]").ToList().Select(x => x.GetField<List<string>>("address")).ToList();
                Parallel.ForEach(inGroups.Where(x => x.Count > 1), (addresses) =>
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

        //private void PrepareDatabase()
        //{
        //    using (var txdb = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
        //    {
        //        try
        //        {
        //            txdb.Command("CREATE PROPERTY Transaction.cIn IF NOT EXISTS LONG");
        //            if (!txdb.Command("SELECT expand(indexes.name) FROM metadata:indexmanager").ToList().Select(x => x.GetField<string>("value")).Contains("IndexForCIn"))
        //            {
        //                Task.Run(() =>
        //                {
        //                    txdb.Command("CREATE INDEX IndexForCIn ON Transaction (cIn) NOTUNIQUE");
        //                });
        //                txdb.Command("UPDATE Transaction SET cIn = inE().size()");
        //            }
        //        }
        //        catch
        //        {
        //            throw;
        //        }
        //    }
        //}
    }
}
