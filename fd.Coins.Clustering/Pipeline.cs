using OrientDB_Net.binary.Innov8tive.API;
using System.Collections.Generic;

namespace fd.Coins.Clustering
{
    public class Pipeline
    {
        List<Core.Clustering.Clustering> _algos;

        public Pipeline()
        {
            _algos = new List<Core.Clustering.Clustering>();
        }

        public void Add(Core.Clustering.Clustering algo)
        {
            _algos.Add(algo);
        }

        public void Process(ConnectionOptions sourceDatabase, List<string> candidateAddresses)
        {
            foreach(var algo in _algos)
            {
                algo.Run(sourceDatabase, candidateAddresses);
            }
        }

        public double Distance(string addr1, string addr2)
        {
            double distance = 0;
            foreach (var algo in _algos)
            {
                distance += algo.Distance(addr1, addr2);
            }
            return distance / _algos.Count;
        }
    }
}
