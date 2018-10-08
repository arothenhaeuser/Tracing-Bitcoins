using OrientDB_Net.binary.Innov8tive.API;
using System.Collections.Generic;

namespace fd.Coins.Core.Clustering
{
    public abstract class Clustering
    {
        public abstract void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses);
        public abstract double Distance(string addr1, string addr2);
    }
}
