using OrientDB_Net.binary.Innov8tive.API;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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
            Parallel.ForEach(_algos, (algo) =>
            {
                algo.Run(sourceDatabase, candidateAddresses);
            });
        }

        public double Distance(string addr1, string addr2)
        {
            return _algos.AsParallel().Sum(x => x.Distance(addr1, addr2)) / _algos.Count;
        }
    }
}
