using OrientDB_Net.binary.Innov8tive.API;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace fd.Coins.Clustering
{
    public class Pipeline
    {
        List<Core.Clustering.Extractor> _algos;

        public Pipeline()
        {
            _algos = new List<Core.Clustering.Extractor>();
        }

        public void Add(Core.Clustering.Extractor algo)
        {
            _algos.Add(algo);
        }

        public void Process(ConnectionOptions sourceDatabase, List<string> candidateAddresses)
        {
            //Parallel.ForEach(_algos, (algo) =>
            foreach(var algo in _algos)
            {
                algo.Run(sourceDatabase, candidateAddresses);
            };
        }

        public double Distance(string addr1, string addr2)
        {
            return _algos.Sum(x => x.Distance(addr1, addr2)) / _algos.Count;
        }
    }
}
