using OrientDB_Net.binary.Innov8tive.API;
using System.Collections.Generic;

namespace fd.Coins.Core.Clustering
{
    /// <summary>
    /// All FeatureExtractors must implement this in order to fit the Pipeline.
    /// </summary>
    public abstract class Extractor
    {
        public abstract void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses);
        public abstract double Distance(string addr1, string addr2);
    }
}
