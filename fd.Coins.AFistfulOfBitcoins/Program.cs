﻿using Orient.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.AFistfulOfBitcoins
{
    class Program
    {
        static void Main(string[] args)
        {
            Utils.ResetPreviousRun();

            using (var txdb = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
            using (var addrdb = new ODatabase("localhost", 2424, "addressclusters", ODatabaseType.Graph, "admin", "admin"))
            {
                var h1 = new Heuristic1();
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                h1.Run();
                stopwatch.Stop();
                Console.WriteLine($"Heuristic 1 took {stopwatch.Elapsed}.");
                // heuristic #2: The change address of each transaction is considered to belong to the same user as the inputs. Change addresses are identified by checking the corresponding output against four conditions:
                // 1. This is the only output referencing this address
                // 2. This output does not belong to a coinbase transaction
                // 3. The address is not referenced in one of the transactions inputs (prevOut)
                // 4. Only one output of this transaction matches the above (ambiguity)
                var stopwatch2 = new Stopwatch();
                stopwatch2.Start();
                // get the total count of all transactions
                var totalCount = txdb.Command("SELECT count(*) FROM Transaction").ToSingle().GetField<Int64>("count");
                // set the counter of already processed transactions
                var skip = 0;
                // limit the number of transactions to process to 1000 since OrientDB does not allow more than 1000 concurrent connections
                var limit = 1000;
                // proceed to process transactions until all are done
                while (skip < totalCount)
                {
                    // get the transactions to process in this loop
                    var some = txdb.Command($"SELECT * FROM Transaction SKIP {skip} LIMIT {limit} TIMEOUT 25000 RETURN").ToList();
                    // increase skip counter for next loop
                    skip += some.Count;

                    Console.WriteLine($"{skip} transactions checked...");
                    Parallel.ForEach(some, (node) =>
                    {
                        // can we identify a return address?
                        var addr = GetChangeAddress(node);
                        if (addr != null)
                        {
                            RetryOnConcurrentFail(3, () =>
                            {
                                // get or insert the return address's node
                                var returnNode = addrdb.Command($"UPDATE Node SET Address = '{addr}' UPSERT RETURN AFTER $current WHERE Address = '{addr}'").ToSingle();
                                // get the source addresses as target for new links
                                var sourceAddr = txdb.Command($"SELECT inE().tAddr AS addr FROM {node.ORID}").ToSingle().GetField<List<string>>("addr").First();
                                // get or insert the source address's node
                                var sourceNode = addrdb.Command($"UPDATE Node SET Address = '{sourceAddr}' UPSERT RETURN AFTER $current WHERE Address = '{sourceAddr}'").ToSingle();
                                // establish a link between the return address node and the source address node
                                addrdb.Create.Edge("Fistful").From(returnNode).To(sourceNode).Set("Tag", "H2").Run();
                                return true;
                            });
                        }
                    });
                }

                stopwatch2.Stop();
                Console.WriteLine($"Heuristic 2 took {stopwatch2.Elapsed}.");

                // get the root of each connected component in the graph
                var roots = addrdb.Command("SELECT distinct(traversedElement(0)) AS root FROM (TRAVERSE * FROM V)").ToList().Select(x => x.GetField<ORID>("root"));
                // traverse from each root to get addresses of each connected component as list
                var addrClusters = new List<IEnumerable<string>>();
                foreach (var root in roots)
                {
                    var cluster = addrdb.Command($"TRAVERSE * FROM {root.RID}").ToList().Select(x => x.GetField<string>("Address")).Where(x => !string.IsNullOrEmpty(x));
                    if (cluster.Count() > 1)
                        addrClusters.Add(cluster);
                }
                File.WriteAllLines("Clusters.txt", addrClusters.Select(x => string.Join("\t", x)));
                Console.WriteLine($"{addrClusters.Count} clusters detected, containing {addrClusters.Sum(x => x.Count())} addresses.");
                Console.WriteLine("Finished");
                Console.ReadLine();
            }
        }

        private static void RetryOnConcurrentFail(int attempts, Func<bool> p)
        {
            var count = attempts;
            while (count > 0)
            {
                try
                {
                    if (p.Invoke())
                        return;
                }
                catch
                {
                    count--;
                }
            }
        }

        private static string[] GetOutputStrings(ODocument node)
        {
            var list = new List<string>();
            var index = 0;
            while (node.ContainsKey($"OUTPUT{index}"))
            {
                list.Add(node.GetField<string>($"OUTPUT{index++}"));
            }
            return list.ToArray();
        }

        private static string GetChangeAddress(ODocument node)
        {
            var outputs = GetOutputStrings(node);
            var ambigous = false;
            string changeAddress = null;
            if (outputs.Length > 1)
            {
                if (!node.GetField<bool>("Coinbase") && node.GetField<Int64>("cIn") > 0)
                {
                    foreach (var output in outputs)
                    {
                        var outputAddress = output.Split(':')[1];
                        using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "root", "root"))
                        {
                            var inputAddresses = db.Command($"SELECT tAddr FROM (SELECT expand(inE()) FROM {node.ORID})").ToList().Select(x => x.GetField<string>("tAddr"));
                            if (!inputAddresses.Contains(outputAddress))
                            {
                                if (db.Command($"SELECT count(*) FROM Link WHERE tAddr = '{outputAddress}'").ToSingle().GetField<Int64>("count") == 1)
                                {
                                    if (changeAddress == null)
                                    {
                                        changeAddress = outputAddress;
                                    }
                                    else
                                    {
                                        ambigous = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (ambigous || changeAddress == null)
            {
                return null;
            }
            else
            {
                return changeAddress;
            }
        }
    }
}
