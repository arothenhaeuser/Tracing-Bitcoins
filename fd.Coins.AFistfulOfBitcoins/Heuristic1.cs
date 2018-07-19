using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Orient.Client;

namespace fd.Coins.AFistfulOfBitcoins
{
    // heuristic #1: all addresses that are input to the same transaction are considered to belong to the same owner
    public class Heuristic1
    {
        public void Run()
        {
            using (var txdb = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
            {
                var skip = new ORID();
                var limit = 5000;
                Console.WriteLine($"{DateTime.Now}: Retrieving initial chunk.");
                var clusters = txdb.Command($"SELECT @rid AS rid, inE().tAddr AS address FROM Transaction WHERE @rid > {skip.RID} LIMIT {limit}").ToList().Select(x => Tuple.Create(x.GetField<ORID>("rid"), x.GetField<List<string>>("address"))).Select(x => Tuple.Create(x.Item1, x.Item2.Distinct().ToList())).ToList();
                while (clusters.Count == limit)
                {
                    skip = new ORID(clusters.Last().Item1);
                    Console.WriteLine($"{DateTime.Now}: Processing {clusters.Where(x => x.Item2.Count > 1).Count()} chunks with more than one input.");
                    Parallel.ForEach(clusters.Select(x => x.Item2).Where(x => x.Count > 1), (cluster) =>
                    {
                        Utils.RetryOnConcurrentFail(3, () =>
                        {
                            using (var addrdb = new ODatabase("localhost", 2424, "addressclusters", ODatabaseType.Graph, "admin", "admin"))
                            {
                                for (var i = 0; i < cluster.Count - 1; i++)
                                {
                                    var cur = addrdb.Command($"UPDATE Node SET Address = '{cluster[i]}' UPSERT RETURN AFTER $current WHERE Address = '{cluster[i]}'").ToSingle();
                                    var next = addrdb.Command($"UPDATE Node SET Address = '{cluster[i + 1]}' UPSERT RETURN AFTER $current WHERE Address = '{cluster[i + 1]}'").ToSingle();
                                    var con = addrdb.Command($"CREATE EDGE Fistful From {cur.ORID} TO {next.ORID} SET Tag = 'H1' RETRY 3");
                                }
                            }
                            return true;
                        });
                    });
                    var sw = new Stopwatch();
                    sw.Start();
                    clusters = txdb.Command($"SELECT @rid AS rid, inE().tAddr AS address FROM Transaction WHERE @rid > {skip.RID} LIMIT {limit}").ToList().Select(x => Tuple.Create(x.GetField<ORID>("rid"), x.GetField<List<string>>("address"))).Select(x => Tuple.Create(x.Item1, x.Item2.Distinct().ToList())).ToList();
                    sw.Stop();
                    Console.WriteLine($"{DateTime.Now}: Retrieving new chunk of clusters took {sw.Elapsed}.");
                }
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
