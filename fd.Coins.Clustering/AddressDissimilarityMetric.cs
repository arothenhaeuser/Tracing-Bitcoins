using Aglomera;

namespace fd.Coins.Clustering
{
    class AddressDissimilarityMetric : IDissimilarityMetric<string>
    {
        private Pipeline _pipeline;

        public AddressDissimilarityMetric(Pipeline pipeline)
        {
            _pipeline = pipeline;
        }

        public double Calculate(string address1, string address2)
        {
            return _pipeline.Distance(address1, address2);
        }
    }
}
