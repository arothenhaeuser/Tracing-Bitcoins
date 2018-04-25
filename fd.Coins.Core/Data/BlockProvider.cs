using Microsoft.Isam.Esent.Collections.Generic;
using NBitcoin;
using NBitcoin.Protocol;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Core.NetworkConnector
{
    public class BlockProvider
    {
        // these two exist in the blockchain two times
        public static string[] KNOWN_DUPLICATE_TRANSACTION_HASHES
        {
            get
            {
                return new string[]
                {
                    "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599",
                    "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"
                };
            }
        }

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
                Save();
                await Task.Delay(2000);
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

                    foreach (var block in _localClient.GetBlocks(hashes))
                    {
                        if (ProcessNewBlock(block))
                        {
                            _height++;
                        }
                        else
                        {
                            throw new InvalidOperationException($"Processing of block failed. ({block.GetHash()})");
                        }
                    }
                }
                await Task.Delay(60000);
            }
        }

        private bool ProcessNewBlock(Block block)
        {
            var success = true;
            var blockTime = block.Header.BlockTime.ToString("yyyy-MM-dd hh:mm:ss");
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
                            }),
                        x.ToBytes())).ToList()) && success;
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
        }

        public void Start()
        {
            PeriodicReport();
            PeriodicLoadData();
        }

        public void Stop()
        {
            _network.Disconnect();
            _localClient.Disconnect();
            _disposed = true;
            Save();
        }
    }
}
