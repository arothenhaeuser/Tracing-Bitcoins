using Aglomera;
using System.Collections.Generic;

namespace fd.Coins.Clustering
{
    class AddressDissimilarityMetric : IDissimilarityMetric<string>
    {
        private Pipeline _pipeline;
        private CachedDissimilarityMetric<string> _cachedDissimilarity;

        public AddressDissimilarityMetric(Pipeline pipeline, List<string> addresses)
        {
            _pipeline = pipeline;
            _cachedDissimilarity = new CachedDissimilarityMetric<string>(this, new HashSet<string>(addresses));
        }

        public double Calculate(string address1, string address2)
        {
            try
            {
                return _cachedDissimilarity.Calculate(address1, address2);
            }
            catch
            {
                return _pipeline.Distance(address1, address2);
            }
        }
    }
}
