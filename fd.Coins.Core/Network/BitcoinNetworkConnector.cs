using NBitcoin;
using NBitcoin.Protocol;
using NBitcoin.Protocol.Behaviors;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Core.NetworkConnector
{
    /// <summary>
    /// Handles the connection to the Bitcoin network.
    /// </summary>
    public class BitcoinNetworkConnector
    {
        // Parameters describing the network connection.
        private NodeConnectionParameters _connectionParameters;
        // The connected peers.
        private NodesGroup _peers;
        // Set to true, if you want to dismiss all connections.
        private bool _disposed;
        // Lock used for file access.
        private object _saving => new object();
        /// <summary>
        /// The local blockchain instance (without transaction data).
        /// </summary>
        public ConcurrentChain BlockChain
        {
            get
            {
                return GetChain();
            }
        }
        /// <summary>
        /// The height of the local blockchain instance.
        /// </summary>
        public int CurrentHeight { get; private set; }
        /// <summary>
        /// The number of connected peers.
        /// </summary>
        public int ConnectedNodes { get; private set; }
        /// <summary>
        /// Starts and manages a connection to multiple peers in the Bitcoin network.
        /// </summary>
        public void Connect()
        {
            var parameters = new NodeConnectionParameters();
            parameters.TemplateBehaviors.Add(new AddressManagerBehavior(GetAddressManager()));
            parameters.TemplateBehaviors.Add(new ChainBehavior(GetChain()));

            _peers = new NodesGroup(Network.Main, parameters, new NodeRequirement()
            {
                RequiredServices = NodeServices.Network
            });
            _peers.MaximumNodeConnection = 8;
            _peers.Connect();
            _connectionParameters = parameters;

            PeriodicSave();
            PeriodicReconnect();
            PeriodicUpdate();
        }
        /// <summary>
        /// Get the hashes of a range of blocks.
        /// </summary>
        /// <param name="from">Height of first block in range.</param>
        /// <param name="to">Height of last block in range.</param>
        /// <returns>Array of block hashes</returns>
        public uint256[] GetHashes(int from, int to)
        {
            if (from > to)
            {
                var tmp = from;
                from = to;
                to = tmp;
            }
            return BlockChain
                .ToEnumerable(false)
                .Where(x => x.Height >= from && x.Height <= to)
                .Select(x => x.HashBlock)
                .ToArray();
        }
        /// <summary>
        /// Periodically updates CurrentHeight and ConnectedNodes.
        /// </summary>
        private async void PeriodicUpdate()
        {
            while (!_disposed)
            {
                CurrentHeight = GetChain().Height;
                ConnectedNodes = _peers.ConnectedNodes.Count;
                await Task.Delay(2000);
            }
        }
        /// <summary>
        /// Periodically establishes new peer connections.
        /// </summary>
        private async void PeriodicReconnect()
        {
            while (!_disposed)
            {
                Reconnect();
                await Task.Delay(TimeSpan.FromMinutes(10));
            }
        }
        /// <summary>
        /// Periodically saves all connection data.
        /// </summary>
        private async void PeriodicSave()
        {
            while (!_disposed)
            {
                Save();
                await Task.Delay(30000);
            }
        }
        /// <summary>
        /// Disconnects from the network and saves all connection data.
        /// </summary>
        public void Disconnect()
        {
            Save();
            _peers.Disconnect();
            _disposed = true;
        }
        /// <summary>
        /// Reconnects to the network.
        /// </summary>
        private void Reconnect()
        {
            _peers.Purge("Reconnect");
        }
        /// <summary>
        /// Saves all relevant connection info to file.
        /// </summary>
        private void Save()
        {
            try
            {
                lock (_saving)
                {
                    using (var fs = new FileStream(ChainFile(), FileMode.Create))
                    {
                        GetChain().WriteTo(fs);
                    }
                }
                lock (_saving)
                {
                    GetAddressManager().SavePeerFile(AddrmanFile(), Network.Main);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed saving.");
                Console.WriteLine(e);
            }
        }
        /// <summary>
        /// Get the local blockchain instance.
        /// </summary>
        /// <returns></returns>
        private ConcurrentChain GetChain()
        {
            if (_connectionParameters != null)
            {
                return _connectionParameters.TemplateBehaviors.Find<ChainBehavior>().Chain;
            }
            var chain = new ConcurrentChain(Network.Main);
            try
            {
                lock (_saving)
                {
                    chain.Load(File.ReadAllBytes(ChainFile()), Network.Main);
                }
            }
            catch
            {
            }
            return chain;
        }
        /// <summary>
        /// The file name of the local blockchain instance.
        /// </summary>
        /// <returns></returns>
        private static string ChainFile()
        {
            return "chain.dat";
        }
        /// <summary>
        /// Get the address manager for peer connections.
        /// </summary>
        /// <returns>The address manager</returns>
        private AddressManager GetAddressManager()
        {
            if (_connectionParameters != null)
            {
                return _connectionParameters.TemplateBehaviors.Find<AddressManagerBehavior>().AddressManager;
            }
            try
            {
                lock (_saving)
                {
                    return AddressManager.LoadPeerFile(AddrmanFile());
                }
            }
            catch
            {
                return new AddressManager();
            }
        }
        /// <summary>
        /// The file name of the address manager.
        /// </summary>
        /// <returns></returns>
        private static string AddrmanFile()
        {
            return "addrman.dat";
        }
    }
}
