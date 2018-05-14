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
        private PersistentDictionary<string, string> _utxos { get; set; }

        public int Height
        {
            get
            {
                return _height;
            }
        }
        public TransactionRepository Transactions { get; set; }

        public BlockProvider()
        {
            _network = new BitcoinNetworkConnector();
            if (File.Exists("state.log"))
            {
                _height = int.Parse(File.ReadAllText("state.log"));
            }
            else
            {
                _height = 0;  // genesis block
            }
            _utxos = KeyValueStoreProvider.Instance.GetDatabase("UTXOs");
            Transactions = new TransactionRepository(
                    ConfigurationManager.ConnectionStrings["BitcoinMySQL"].ConnectionString,
                    "transactions");
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
                        File.AppendAllText("err.log", DateTime.Now + ":\t" + e.Message + "\n");
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
                    AddVertices(tx, blockTime);
                }
                foreach (var tx in block.Transactions)
                {
                    AddEdges(tx);
                }
            }
            catch(Exception e)
            {
                return false;
            }
            return true;
        }
        private void AddVertices(Transaction tx, DateTime blockTime)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                var coinbase = db.Create.Vertex<OVertex>().Set("hash", "coinbase").Run();
                var current = db.Create.Vertex<OVertex>().Set("hash", tx.GetHash().ToString()).Set("blockTime", DateTime.Now).Run();
            }
        }
        private void AddEdges(Transaction tx)
        {
            using (var db = new ODatabase("localhost", 2424, "txgraph", ODatabaseType.Graph, "admin", "admin"))
            {
                for (var i = 0; i < tx.Inputs.Count; i++)
                {
                    db.Create.Edge<OEdge>()
                        .From(db.Select().From("V")
                            .Where("hash").Equals(tx.Inputs[i].PrevOut.Hash.ToString())
                                .ToList<OVertex>().FirstOrCoinbase("coinbase", db))
                        .To(db.Select().From("V")
                            .Where("hash").Equals(tx.GetHash().ToString()))
                        .Set("tTx", tx.GetHash().ToString())
                        .Set("sTx", tx.Inputs[i].PrevOut.Hash.ToString())
                        .Set("sN", tx.Inputs[i].PrevOut.N)
                        .Run();
                }
                for (var i = 0; i < tx.Outputs.Count; i++)
                {
                    var edge = db.Select().From("E")
                        .Where("sTx").Equals(tx.GetHash().ToString())
                        .And("sN").Equals(i)
                        .ToList<OEdge>().FirstOrDefault();
                    if (edge != null)
                    {
                        edge.SetField("tAdd", GetAddress(tx.Outputs[i].ScriptPubKey));
                        edge.SetField("val", tx.Outputs[i].Value.Satoshi);
                        db.Update(edge).Run();
                    }
                }
            }
        }
        private bool ProcessNewBlock(Block block)
        {
            var success = true;
            var blockTime = block.Header.BlockTime.LocalDateTime;
            // add outputs to dict
            foreach (var tx in block.Transactions)
            {
                for (var i = 0; i < tx.Outputs.Count; i++)
                {
                    try
                    {
                        _utxos.Add(
                        $"{tx.GetHash().ToString()},{i.ToString()}",
                        new TxOutput(
                            i,
                            tx.GetHash().ToString(),
                            GetAddress(tx.Outputs[i].ScriptPubKey),
                            tx.Outputs[i].Value.Satoshi)
                            .ToString());
                    }
                    catch (Exception e)
                    {
                        if (!(e is ArgumentException))
                        {
                            success = false;
                            File.AppendAllText("err.log", DateTime.Now + ":\t" + e.Message + "\n");
                        }
                    }
                }
            }

            // adding to db
            success = Transactions.AddRange(
                block.Transactions.Select(
                    x => new TransactionEntity(
                        x.GetHash().ToString(),
                        blockTime,
                        ConvertInputs(x),
                        x.Outputs.Select(
                            (z, i) => new TxOutput()
                            {
                                Amount = z.Value.Satoshi,
                                Hash = x.GetHash().ToString(),
                                Position = i,
                                TargetAddress = GetAddress(z.ScriptPubKey)
                            }))).ToList()) && success;
            //clean up UTXOs
            foreach (var key in block.Transactions.Where(x => !x.IsCoinBase).SelectMany(x => x.Inputs.Select(y => $"{y.PrevOut.Hash},{y.PrevOut.N}")).ToList())
            {
                try
                {
                    _utxos.Remove(key);
                }
                catch (Exception e)
                {
                    success = false;
                    File.AppendAllText("err.log", DateTime.Now + ":\t" + e.Message + "\n");
                }
            }
            return success;
        }

        private string GetAddress(Script scriptPubKey)
        {
            var add = scriptPubKey.GetDestinationAddress(Network.Main)?.ToString();
            if (string.IsNullOrEmpty(add))
            {
                var keys = scriptPubKey.GetDestinationPublicKeys();
                add = string.Join(",", keys.Select(x => x.GetAddress(Network.Main)));
            }
            return add;
        }

        private IEnumerable<TxInput> ConvertInputs(Transaction tx)
        {
            var inputs = tx.Inputs;
            if (tx.IsCoinBase)
            {
                return new List<TxInput>() { new TxInput(0, tx.GetHash().ToString(), "coinbase", tx.TotalOut) };
            }
            else
            {
                return inputs.Select((x, i) => new TxInput(tx.GetHash().ToString(), i, _utxos[$"{x.PrevOut.Hash},{x.PrevOut.N}"])); // lookup outputs pointing towards me
            }
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
            File.WriteAllText("state.log", _height.ToString());
            _utxos.Flush();
        }

        public void Start()
        {
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
    }
}
