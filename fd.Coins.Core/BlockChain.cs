using Microsoft.Msagl.Drawing;
using Microsoft.Msagl.GraphViewerGdi;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using NBitcoin;
using NBitcoin.BitcoinCore;
using NBitcoin.Protocol;
using NBitcoin.Protocol.Behaviors;
using NBitcoin.SPV;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace fd.Coins.Core
{
    public class BlockChain
    {
        private TimeSpan txTimer, dictTimer, chainTimer, testTimer;
        private Graph graph;

        private NodeConnectionParameters _ConnectionParameters;
        private NodesGroup _Group;
        private ConcurrentDictionary<uint256, Block> _Blocks;
        private Dictionary<uint256, ChainedTransaction> _chainedTransactions = new Dictionary<uint256, ChainedTransaction>();
        private Dictionary<uint256, List<uint256>> _nextTxs = new Dictionary<uint256, List<uint256>>();
        private Dictionary<uint256, List<uint256>> _prevTxs = new Dictionary<uint256, List<uint256>>();

        private object _Saving;
        private bool _Disposed;

        public void DownloadBlockChain()
        {
            _Saving = new object();
            _Disposed = false;

            StartConnecting();

            var store = new BlockStore(@"C:\Users\Andreas Rothenhäuser\AppData\Roaming\Bitcoin\blocks", Network.Main);
            var index = new IndexedBlockStore(new InMemoryNoSqlRepository(), store);
            index.ReIndex();

            Console.Read();

            string connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            IMongoDatabase database = client.GetDatabase("test");
            var txCollection = database.GetCollection<BsonDocument>("txs");


            var sw = new Stopwatch();
            sw.Start();
            // equals to database of all transactions
            foreach (var block in GetChain().ToEnumerable(false).Skip(400000).Take(50))
            {
                foreach (var transaction in index.Get(block.HashBlock).Transactions)
                {
                    txCollection.InsertOne(BsonDocument.Parse(transaction.ToString()));
                }
            }
            txTimer = Read(sw);
            Console.WriteLine(txTimer);



            // fill dictionaries
            //foreach (var transaction in txCollection.Find(p => true).ToList())
            //{
            //    var hash = transaction.GetHash();
            //    var ret = new List<uint256>();
            //    foreach (var input in transaction.Inputs)
            //    {
            //        if (!transaction.IsCoinBase)
            //        {
            //            if (_transactions.ContainsKey(input.PrevOut.Hash))
            //            {
            //                ret.Add(input.PrevOut.Hash);
            //                if (_nextTxs.ContainsKey(input.PrevOut.Hash))
            //                {
            //                    _nextTxs[input.PrevOut.Hash]
            //                        .Add(transaction.GetHash());
            //                }
            //                else
            //                {
            //                    _nextTxs.Add(
            //                        input.PrevOut.Hash,
            //                        new List<uint256>()
            //                        {
            //                            transaction.GetHash()
            //                        });
            //                }
            //            }
            //        }
            //    }
            //    _prevTxs[hash] = ret;
            //}
            //dictTimer = Read(sw);
            //Console.WriteLine(dictTimer);
            //Console.Read();

            //// build the chained transactions
            //foreach (var transaction in _transactions.Values)
            //{
            //    var hash = transaction.GetHash();
            //    var next = new List<uint256>();
            //    var prev = new List<uint256>();
            //    _nextTxs.TryGetValue(hash, out next);
            //    _prevTxs.TryGetValue(hash, out prev);
            //    var cTx = new ChainedTransaction(transaction);
            //    cTx.NextIds = next?.Select(x => x.ToString()).ToArray();
            //    cTx.PrevIds = prev?.Select(x => x.ToString()).ToArray();
            //    _chainedTransactions.Add(transaction.GetHash(), cTx);
            //}
            //chainTimer = Read(sw);
            //var repo = new Repository<ChainedTransaction>("txs", "Server=localhost;Database=coin_test;User Id=Bitcoin;Password=bitcoin;");
            //repo.AddRange(_chainedTransactions.Values);

            //int iterations = 1;
            //var rand = new Random();
            //graph = new Graph("Graph");
            //for (int i = 0; i < iterations; i++)
            //{
            //    var pick = rand.Next(_chainedTransactions.Count-1);
            //    var test = _chainedTransactions.ElementAt(pick);
            //    File.AppendAllText("log.txt", string.Format("Randomly picked at {0}:\n", pick));
            //    PrintNext(test.Key, 0);
            //    File.AppendAllText("log.txt", "\n\n");
            //}

            //GraphRenderer renderer = new GraphRenderer(graph);
            //renderer.CalculateLayout();
            //int width = 9000;
            //Bitmap bitmap = new Bitmap(width, (int)(graph.Height * (width / graph.Width)), PixelFormat.Format32bppPArgb);
            //renderer.Render(bitmap);
            //bitmap.Save("test.png");
            //var form = new Form();
            //var viewer = new GViewer();
            //viewer.Graph = graph;
            //form.SuspendLayout();
            //viewer.Dock = DockStyle.Fill;
            //form.Controls.Add(viewer);
            //form.ResumeLayout();
            //form.ShowDialog();
            //testTimer = Read(sw);
        }

        private TimeSpan Read(Stopwatch sw)
        {
            sw.Stop();
            var elapsed = sw.Elapsed;
            sw.Restart();
            return elapsed;
        }

        //private void PrintNext(uint256 input, int indent)
        //{
        //    if (indent > 5)
        //        return;
        //    File.AppendAllText("log.txt", new string(' ', indent) + input.ToString() + "\n");
        //    var currentTx = _chainedTransactions[input];
        //    if (currentTx.NextIds != null)
        //    {
        //        foreach (var nextId in _chainedTransactions[input].NextIds)
        //        {
        //            var currInAddr = currentTx.InputAddresses(_transactions.Excerpt(currentTx.PrevIds.Select(x => new uint256(x)).ToArray()));
        //            var currOutAddr = currentTx.OutputAddresses();
        //            var nextInAddr = _chainedTransactions[new uint256(nextId)].InputAddresses(_transactions.Excerpt(_chainedTransactions[new uint256(nextId)].PrevIds.Select(x => new uint256(x)).ToArray()));
        //            var nextOutAddr = _chainedTransactions[new uint256(nextId)].OutputAddresses();
        //            graph.AddEdge(
        //                string.Format("Tx: {0}\nIn: {1}\nOut: {2}", input.ToString(), string.Join(",", currInAddr), string.Join(",", currOutAddr)),
        //                string.Format("Tx: {0}\nIn: {1}\nOut: {2}", nextId.ToString().ToString(), string.Join(",", nextInAddr), string.Join(",", nextOutAddr)));
        //            PrintNext(new uint256(nextId), indent + 1);
        //        }
        //    }
        //}

        private async void PeriodicDownloadNewBlocks()
        {
            while (!_Disposed)
            {
                if (_Blocks == null)
                {
                    await Task.Delay(30000);
                }
                if (_Blocks?.Count < GetChain().Height)
                {
                    await Task.Factory.StartNew(() =>
                    {
                        var nodesEnumerator = _Group.ConnectedNodes.GetEnumerator();
                        while (nodesEnumerator.MoveNext())
                        {
                            try
                            {
                                foreach (var block in nodesEnumerator.Current.GetBlocks(GetChain().Tip.HashBlock))
                                {
                                    _Blocks.AddOrUpdate(block.GetHash(), block, (i, b) => b);
                                }
                            }
                            catch (InvalidOperationException)
                            {
                                continue;
                            }
                        }
                    });
                }
                await Task.Delay(TimeSpan.FromMinutes(2));
            }
        }

        private async void StartConnecting()
        {
            await Task.Factory.StartNew(() =>
            {
                var parameters = new NodeConnectionParameters();
                parameters.TemplateBehaviors.Add(new AddressManagerBehavior(GetAddressManager())); //So we find nodes faster
                parameters.TemplateBehaviors.Add(new ChainBehavior(GetChain())); //So we don't have to load the chain each time we start
                parameters.TemplateBehaviors.Add(new TrackerBehavior(GetTracker())); //Tracker knows which scriptPubKey and outpoints to track, it monitors all your wallets at the same
                if (!_Disposed)
                {
                    _Group = new NodesGroup(Network.Main, parameters, new NodeRequirement()
                    {
                        RequiredServices = NodeServices.Network
                    });
                    _Group.MaximumNodeConnection = 8;
                    _Group.Connect();
                    _ConnectionParameters = parameters;
                }
            });

            PeriodicSave();
            PeriodicUiUpdate();
            PeriodicKick();
            // PeriodicDownloadNewBlocks();
        }

        private async void PeriodicKick()
        {
            while (!_Disposed)
            {
                await Task.Delay(TimeSpan.FromMinutes(10));
                _Group.Purge("For privacy concerns, will renew bloom filters on fresh nodes");
            }
        }

        private async void PeriodicUiUpdate()
        {
            while (!_Disposed)
            {
                await Task.Delay(2000);
                //Console.Clear();
                //Console.WriteLine("Current height: {0}", GetChain().Height);
                //Console.WriteLine("Current connections: {0}", _Group.ConnectedNodes.Count);
                //Console.WriteLine("Current blocks: {0}", GetBlocks().Count);
                //if (!txTimer.Equals(new TimeSpan()))
                //{
                //    Console.WriteLine("Parsing transactions took: {0}", txTimer.ToString());
                //}
                //if (!dictTimer.Equals(new TimeSpan()))
                //{
                //    Console.WriteLine("Compiling dictionaries took: {0}", dictTimer.ToString());
                //}
                //if (!chainTimer.Equals(new TimeSpan()))
                //{
                //    Console.WriteLine("Building transaction-chain took: {0}", chainTimer.ToString());
                //}
                //if (!testTimer.Equals(new TimeSpan()))
                //{
                //    Console.WriteLine("Yielding test took: {0}", testTimer.ToString());
                //}
            }
        }

        private async void PeriodicSave()
        {
            while (!_Disposed)
            {
                await Task.Delay(30000);
                SaveAsync();
            }
        }


        private void SaveAsync()
        {
            var unused = Task.Factory.StartNew(() =>
            {
                lock (_Saving)
                {
                    GetAddressManager().SavePeerFile(AddrmanFile(), Network.Main);
                    using (var fs = File.Open(ChainFile(), FileMode.Create))
                    {
                        GetChain().WriteTo(fs);
                    }
                    using (var fs = File.Open(TrackerFile(), FileMode.Create))
                    {
                        GetTracker().Save(fs);
                    }
                }
            });
        }

        private ConcurrentDictionary<uint256, Block> GetBlocks()
        {
            if (_Blocks != null)
            {
                return _Blocks;
            }
            _Blocks = new ConcurrentDictionary<uint256, Block>();
            return _Blocks;
        }

        private ConcurrentChain GetChain()
        {
            if (_ConnectionParameters != null)
            {
                return _ConnectionParameters.TemplateBehaviors.Find<ChainBehavior>().Chain;
            }
            var chain = new ConcurrentChain(Network.Main);
            try
            {
                lock (_Saving)
                {
                    chain.Load(File.ReadAllBytes(ChainFile()));
                }
            }
            catch
            {
            }
            return chain;
        }

        private AddressManager GetAddressManager()
        {
            if (_ConnectionParameters != null)
            {
                return _ConnectionParameters.TemplateBehaviors.Find<AddressManagerBehavior>().AddressManager;
            }
            try
            {
                lock (_Saving)
                {
                    return AddressManager.LoadPeerFile(AddrmanFile());
                }
            }
            catch
            {
                return new AddressManager();
            }

        }

        private Tracker GetTracker()
        {
            if (_ConnectionParameters != null)
            {
                return _ConnectionParameters.TemplateBehaviors.Find<TrackerBehavior>().Tracker;
            }
            try
            {
                lock (_Saving)
                {
                    using (var fs = File.OpenRead(TrackerFile()))
                    {
                        return Tracker.Load(fs);
                    }
                }
            }
            catch
            {
            }
            return new Tracker();
        }

        private string TrackerFile()
        {
            return "tracker.dat";
        }

        private static string ChainFile()
        {
            return "chain.dat";
        }

        private static string AddrmanFile()
        {
            return "addrman.dat";
        }
    }
}
