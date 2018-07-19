using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Orient.Client;

namespace fd.Coins.AFistfulOfBitcoins
{
    public class Heuristic1
    {
        public void Run()
        {
            PrepareDatabase();
            using (var addrdb = new ODatabase("localhost", 2424, "addressclusters", ODatabaseType.Graph, "admin", "admin"))
            using (var txdb = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
            {
                var skip = new ORID();
                var limit = 20000;
                var rids = txdb.Command($"SELECT inE().tAddr AS address, $current AS tx FROM (SELECT * FROM Transaction WHERE @rid > {skip} LIMIT {limit} TIMEOUT 20000 RETURN) WHERE cIn > 0").ToList().Select(x => (x.GetField<List<string>>("address"), x.GetField<ORID>("tx"))).ToList();
                while (rids.Count > 0)
                {
                    skip = new ORID(rids.Last().Item2);
                    foreach (var cluster in rids.Select(x => x.Item1))
                    {
                        var attempts = 0;
                        while (attempts < 3)
                        {
                            try
                            {
                                for (var i = 0; i < cluster.Count - 1; i++)
                                {
                                    var cur = addrdb.Command($"UPDATE Node SET Address = '{cluster[i]}' UPSERT RETURN AFTER $current WHERE Address = '{cluster[i]}'").ToSingle();
                                    var next = addrdb.Command($"UPDATE Node SET Address = '{cluster[i + 1]}' UPSERT RETURN AFTER $current WHERE Address = '{cluster[i + 1]}'").ToSingle();
                                    var con = addrdb.Create.Edge("Fistful").From(cur).To(next).Set("Tag", "H1").Run();
                                }
                                break;
                            }
                            catch
                            {
                                attempts++;
                            }
                        }
                    }
                    var sw = new Stopwatch();
                    sw.Start();
                    rids = txdb.Command($"SELECT inE().tAddr AS address, $current AS tx FROM (SELECT * FROM Transaction WHERE @rid > {skip} LIMIT {limit} TIMEOUT 20000 RETURN) WHERE cIn > 0").ToList().Select(x => (x.GetField<List<string>>("address"), x.GetField<ORID>("tx"))).ToList();
                    Console.WriteLine(sw.Elapsed);
                }
            }
        }

        private void PrepareDatabase()
        {
            using (var txdb = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
            {
                try
                {
                    txdb.Command("CREATE PROPERTY Transaction.cIn IF NOT EXISTS LONG");
                    if (!txdb.Command("SELECT expand(indexes.name) FROM metadata:indexmanager").ToList().Select(x => x.GetField<string>("value")).Contains("IndexForCIn"))
                    {
                        Task.Run(() =>
                        {
                            txdb.Command("CREATE INDEX IndexForCIn ON Transaction (cIn) NOTUNIQUE");
                        });
                    }
                    txdb.Command("UPDATE Transaction SET cIn = inE().size()");
                }
                catch
                {
                    throw;
                }
            }
        }
    }
}
