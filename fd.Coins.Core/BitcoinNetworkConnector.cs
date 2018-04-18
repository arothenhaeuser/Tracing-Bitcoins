using NBitcoin;
using NBitcoin.Protocol;
using NBitcoin.Protocol.Behaviors;
using NBitcoin.SPV;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Coins.Core
{
    public class BitcoinNetworkConnector
    {
        private NodeConnectionParameters _connectionParameters;

        private NodesGroup _peers;

        private bool _disposed;

        private object _saving => new object();

        public ConcurrentChain BlockChain
        {
            get
            {
                return GetChain();
            }
        }

        public int CurrentHeight { get; private set; }
        public int ConnectedNodes { get; private set; }

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

        private async void PeriodicUpdate()
        {
            while (!_disposed)
            {
                CurrentHeight = GetChain().Height;
                ConnectedNodes = _peers.ConnectedNodes.Count;
                await Task.Delay(2000);
            }
        }

        private async void PeriodicReconnect()
        {
            while (!_disposed)
            {
                Reconnect();
                await Task.Delay(TimeSpan.FromMinutes(10));
            }
        }

        private async void PeriodicSave()
        {
            while (!_disposed)
            {
                Save();
                await Task.Delay(30000);
            }
        }

        public void Disconnect()
        {
            Save();
            _peers.Disconnect();
            _disposed = true;
        }

        private void Reconnect()
        {
            _peers.Purge("Reconnect");
        }

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
                    chain.Load(File.ReadAllBytes(ChainFile()));
                }
            }
            catch
            {
            }
            return chain;
        }

        private Tracker GetTracker()
        {
            if (_connectionParameters != null)
            {
                return _connectionParameters.TemplateBehaviors.Find<TrackerBehavior>().Tracker;
            }
            try
            {
                lock (_saving)
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

        private static string AddrmanFile()
        {
            return "addrman.dat";
        }
    }
}
