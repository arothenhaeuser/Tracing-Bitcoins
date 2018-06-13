using Microsoft.Isam.Esent.Collections.Generic;
using NBitcoin;
using NBitcoin.Protocol;
using Orient.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Core.NetworkConnector
{
    public class BlockProvider
    {
        private bool _disposed;

        private BitcoinNetworkConnector _network;
        private Node _localClient;

        private int _height;
        public int Height
        {
            get
            {
                return _height;
            }
        }

        private long _linkState;
        public long LinkState
        {
            get
            {
                return _linkState;
            }
        }

        public BlockProvider()
        {
            _network = new BitcoinNetworkConnector();
            if (File.Exists("state.log"))
            {
                _height = int.Parse(File.ReadAllLines("state.log").ToList()[0]);
                _linkState = long.Parse(File.ReadLines("state.log").ToList()[1]);
            }
            else
            {
                _height = 0;  // genesis block
                _linkState = 0;
            }
            Connect();
        }


        private async void PeriodicReport()
        {
            while (!_disposed)
            {
                Console.Clear();
                Console.WriteLine(_height);
                await Task.Delay(5000);
            }
            Console.Clear();
        }

        //private async void PeriodicLinkData()
        //{
        //    while (!_disposed)
        //    {
        //        using(var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
        //        {
        //            var unlinked = db.Command($"SELECT FROM Transaction WHERE unlinked = True").ToList();
        //            foreach(var node in unlinked)
        //        }
        //    }
        //    await Task.Delay(60000);
        //}

        private async void PeriodicLoadData()
        {
            while (!_disposed)
            {
                // get new blocks
                if (_network.BlockChain.Height > _height)
                {
                    var hashes =
                        _network.BlockChain
                        .ToEnumerable(false)
                        .Skip(_height)
                        .Take(50000)
                        .Select(x => x.HashBlock);
                    try
                    {
                        foreach (var block in _localClient.GetBlocks(hashes))
                        {
                            if (ProcessBlock(block))
                            {
                                _height++;
                            }
                            else
                            {
                                throw new InvalidOperationException($"Processing of block failed. ({block.GetHash()})");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        File.AppendAllText("err.log", DateTime.Now + ":\t" + e.ToString() + "\n");
                        Save();
                        return;
                    }
                }
                await Task.Delay(60000);
            }
        }
        private bool ProcessBlock(Block block)
        {
            var blockTime = block.Header.BlockTime.LocalDateTime;
            try
            {
                foreach (var tx in block.Transactions)
                {
                    var vTx = AddVertex(tx, blockTime);
                    //AddEdges(tx, vTx);
                }
            }
            catch(OException oe)
            {
                return true; // duplicate txids occured in the early years of bitcoin
            }
            catch(Exception e)
            {
                throw;
            }
            return true;
        }
        public OVertex AddVertex(Transaction tx, DateTime blockTime)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                var vertex = db.Create.Vertex("Transaction")
                    .Set("Hash", tx.GetHash().ToString())
                    .Set("BlockTime", blockTime)
                    .Run();
                for(var i=0; i < tx.Inputs.Count; i++)
                {
                    var input = tx.Inputs[i];
                    vertex.SetField($"INPUT{i}", $"{input.PrevOut.Hash}:{input.PrevOut.N}");
                }
                for(var i=0; i<tx.Outputs.Count; i++)
                {
                    var output = tx.Outputs[i];
                    vertex.SetField($"OUTPUT{i}", $"{i}:{GetAddress(output.ScriptPubKey)}:{output.Value.Satoshi}");
                }
                db.Update(vertex).Run();
                //_vertices.AddOrReplace(tx.GetHash().ToString(), vertex.ORID.ToString());
                return vertex;
            }
        }
        public void AddEdges(Transaction tx, OVertex vTx)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                for (var i = 0; i < tx.Inputs.Count; i++)
                {
                    var prevOrid = string.Empty;
                    if (string.IsNullOrEmpty(prevOrid))
                    {
                        prevOrid = db.Select().From("Transaction").Where("Hash").Equals(tx.Inputs[i].PrevOut.Hash.ToString()).ToList<OVertex>().FirstOrCoinbase(db, tx.TotalOut.Satoshi).ORID.ToString();
                    }
                    var source = db.Select().From(prevOrid).ToList<OVertex>().First();
                    var edge = db.Create.Edge("Link")
                        .From(source)
                        .To(vTx)
                        .Set("sTx", tx.Inputs[i].PrevOut.Hash.ToString())
                        .Set("sN", tx.Inputs[i].PrevOut.N)
                        .Set("tTx", tx.GetHash().ToString())
                        .Run();
                    if (!source.IsCoinBase())
                    {
                        var candidates = new List<OEdge>();
                        OEdge eFuture = null;
                        foreach(var orid in source.GetField<List<ORID>>("out_Link"))
                        {
                            eFuture = db.Select().From(orid).ToList<OEdge>().First();
                            if (eFuture.GetField<long>("sN").Equals(tx.Inputs[i].PrevOut.N))
                            {
                                break;
                            }
                        }
                        if (eFuture != null)
                        {
                            edge.SetField("tAddr", eFuture.GetField<string>("tAddr"));
                            edge.SetField("amount", eFuture.GetField<long>("amount"));
                            db.Update(edge).Run();
                            var vFuture = db.Select().From(eFuture.InV).ToList<OVertex>().SingleOrDefault();
                            if (vFuture != null)
                            {
                                db.Delete.Vertex(vFuture).Run();
                            }
                        }
                    }
                }
                for (int i = 0; i < tx.Outputs.Count; i++)
                {
                    db.Create.Edge("Link")
                        .From(vTx)
                        .To(db.Create.Vertex("Transaction").Set("Hash", "future").Run())
                        .Set("sTx", tx.GetHash().ToString())
                        .Set<long>("sN", i)
                        .Set("tTx", "future")
                        .Set("tAddr", GetAddress(tx.Outputs[i].ScriptPubKey))
                        .Set("amount", tx.Outputs[i].Value.Satoshi)
                        .Run();
                }
            }
        }

        public string GetAddress(Script scriptPubKey)
        {
            var add = scriptPubKey.GetDestinationAddress(Network.Main)?.ToString();
            if (string.IsNullOrEmpty(add))
            {
                var keys = scriptPubKey.GetDestinationPublicKeys();
                add = string.Join(",", keys.Select(x => x.GetAddress(Network.Main)));
            }
            return add;
        }

        public IEnumerable<Block> GetBlocks(int fromHeight, int toHeight)
        {
            while (_network.CurrentHeight < toHeight)
            {
                Task.Delay(1000);
            }
            var hashes = _network.BlockChain.ToEnumerable(false).Skip(fromHeight).Take(toHeight - fromHeight).Select(x => x.HashBlock);
            return _localClient.GetBlocks(hashes);
        }

        private void Connect()
        {
            _network.Connect();
            _localClient = Node.ConnectToLocal(Network.Main, new NodeConnectionParameters());
            _localClient.VersionHandshake();
        }

        private void Save()
        {
            File.WriteAllLines("state.log", new string[] { _height.ToString(), _linkState.ToString() });
        }

        public void Start()
        {
            CreateDatabaseIfNotExists("localhost", 2424, "root", "root", "txgraph");
            PeriodicReport();
            PeriodicLoadData();
        }

        public void Stop()
        {
            _disposed = true;
            _network.Disconnect();
            _localClient.Disconnect();
            Save();
        }
        private bool CreateDatabaseIfNotExists(string hostname, int port, string user, string password, string database)
        {
            using (var server = new OServer(hostname, port, user, password))
            {
                if (!server.DatabaseExist(database, OStorageType.PLocal))
                {
                    var created = server.CreateDatabase(database, ODatabaseType.Graph, OStorageType.PLocal);
                    using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
                    {
                        db.Command("CREATE CLASS Transaction EXTENDS V");
                        db.Command("CREATE PROPERTY Transaction.Hash STRING");
                        db.Command("CREATE INDEX IndexForHash ON Transaction (Hash) UNIQUE_HASH_INDEX");
                        db.Command("CREATE PROPERTY Transaction.BlockTime DATETIME");
                        db.Command("ALTER PROPERTY Transaction.unlinked DEFAULT True");
                        db.Command("CREATE PROPERTY Transaction.unlinked BOOLEAN");
                        db.Command("CREATE CLASS Link EXTENDS E");
                        db.Command("CREATE PROPERTY Link.sTx STRING");
                        db.Command("CREATE PROPERTY Link.sN LONG");
                        db.Command("CREATE PROPERTY Link.tTx STRING");
                        db.Command("CREATE PROPERTY Link.tAddr STRING");
                        db.Command("CREATE PROPERTY Link.amount LONG");
                    }
                    return created;
                }
                else
                {
                    return true;
                }
            }
        }
    }
}
