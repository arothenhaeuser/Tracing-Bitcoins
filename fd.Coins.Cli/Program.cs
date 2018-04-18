using fd.Coins.Core;
using Microsoft.Isam.Esent.Collections.Generic;
using NBitcoin;
using NBitcoin.JsonConverters;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace fd.Coins.Cli
{
    class Program
    {
        static void Main(string[] args)
        {
            var provider = new BlockProvider();
            provider.Start();
            Console.Read();
            provider.Stop();
            //var provider = new BlockProvider();
            //var blocks = provider.GetBlocks(300000, 300100);
            //var hashes = new List<string>();
            //foreach (var block in blocks)
            //{
            //    hashes.AddRange(block.Transactions.Select(x => x.GetHash().ToString()));
            //}

            //var persDict = new PersistentDictionary<string, string>("test");
            //var mySqlRepo = new KeyValueRepository(
            //    "test",
            //    ConfigurationManager.ConnectionStrings["BitcoinMySQL"].ConnectionString);

            //var sw1 = new Stopwatch();
            //var sw2 = new Stopwatch();
            //var sb = new StringBuilder();

            //// INSERT
            //sw1.Start();
            //foreach (var block in blocks)
            //{
            //    foreach (var tx in block.Transactions)
            //    {
            //        persDict.Add(tx.GetHash().ToString(), Serializer.ToString(tx));
            //    }
            //}
            //sw1.Stop();

            //sw2.Start();
            //foreach (var block in blocks)
            //{
            //    var ret = mySqlRepo.AddRange(block.Transactions.Select(tx => new MyTestClass2(tx.GetHash().ToString(), Serializer.ToString(tx))));
            //}
            //sw2.Stop();

            //sb.AppendLine($"PersDict: Insertion of {persDict.Count} entries took {sw1.Elapsed}");
            //sb.AppendLine($"MySQL: Insertion of {mySqlRepo.Count} entries took {sw2.Elapsed}");
            //// END INSERT

            //var random = new Random();

            //// INDEX ACCESS
            //var iterations = 10000;
            //sw1.Restart();
            //for (int i = 0; i < iterations; i++)
            //{
            //    var r = random.Next(0, hashes.Count);
            //    var data = persDict[hashes[r]];
            //    Debug.Assert(
            //        Serializer.ToObject<Transaction>(data)
            //            .GetHash()
            //            .ToString()
            //            .Length > 0);
            //}
            //sw1.Stop();

            //sw2.Restart();
            //for (int i = 0; i < 10000; i++)
            //{
            //    var r = random.Next(0, hashes.Count);
            //    var data = mySqlRepo.Find(hashes[r]).Value;
            //    Debug.Assert(
            //        Serializer.ToObject<Transaction>(data)
            //            .GetHash()
            //            .ToString()
            //            .Length > 0);
            //}
            //sw2.Stop();
            //sb.AppendLine($"PersDict: Index access on {iterations} entries took {sw1.Elapsed}");
            //sb.AppendLine($"MySQL: Index access on {iterations} entries took {sw2.Elapsed}");
            //// END INDEX ACCESS

            //// UPDATE
            //sw1.Restart();
            //foreach (var hash in hashes)
            //{
            //    persDict[hash] = "";
            //}
            //sw1.Stop();

            //sw2.Restart();
            //mySqlRepo.Update(hashes.Select(x => new MyTestClass2(x, "")));
            //sw2.Stop();
            //sb.AppendLine($"PersDict: Updating all entries took {sw1.Elapsed}");
            //sb.AppendLine($"MySQL: Updating all entries took {sw2.Elapsed}");
            //// END UPDATE

            //// CHECK ALL
            //sw1.Restart();
            //Debug.Assert(persDict.All(x => x.Value.ToString().Length == 0));
            //sw1.Stop();
            //sw2.Restart();
            //Debug.Assert(mySqlRepo.FindAll().All(x => x.Value.ToString().Length == 0));
            //sw2.Stop();
            //sb.AppendLine($"PersDict: Verifying all entries took {sw1.Elapsed}");
            //sb.AppendLine($"MySQL: Verifying all entries took {sw2.Elapsed}");
            //// END CHECK ALL

            //Console.WriteLine($"MySql: {mySqlRepo.Count} entries; Dictionary: {persDict.Count} entries; Hashes: {hashes.Count};");
            //Console.WriteLine(sb.ToString());
            //Console.Read();
        }
    }
}
