using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Core.Clustering
{
    public class ClusteringCollapser
    {
        private Dictionary<string, int> _inventory;
        private List<List<string>> _collapsed;

        public ClusteringCollapser()
        {
            _inventory = new Dictionary<string, int>();
            _collapsed = new List<List<string>>();
        }

        public void Collapse(List<List<string>> clustering)
        {
            foreach (var cluster in clustering)
            {
                Update(cluster);
            }
        }

        private void Update(List<string> cluster)
        {
            var seen = cluster.Where(x => _inventory.ContainsKey(x)).ToList();
            var unseen = cluster.Except(seen).ToList();
            if (seen.Count == 0)
            {
                _collapsed.Add(unseen);
                UpdateInventory(unseen, _collapsed.Count - 1);
            }
            else if (unseen.Count == 0)
            {
                return;
            }
            else
            {
                var firstIndex = _inventory[seen.First()];
                var indexList = seen.Select(x => _inventory[x]).Distinct().OrderByDescending(x => x).ToList();
                indexList.Remove(firstIndex);
                foreach (var index in indexList)
                {
                    foreach (var key in _collapsed[index])
                    {
                        _inventory[key] = firstIndex;
                    }
                    _collapsed[firstIndex] = _collapsed[firstIndex].Union(_collapsed[index]).ToList();
                }
                foreach (var index in indexList)
                {
                    Remove(index);
                }
                _collapsed[firstIndex].AddRange(unseen);
                UpdateInventory(unseen, firstIndex);
            }
        }

        private void Remove(int index)
        {
            _collapsed.RemoveAt(index);
            foreach (var key in _inventory.Keys.Where(x => _inventory[x] > index).ToList())
            {
                _inventory[key]--;
            }
        }

        private void UpdateInventory(List<string> items, int index)
        {
            foreach (var item in items)
            {
                _inventory.Add(item, index);
            }
        }

        public List<List<string>> Clustering
        {
            get
            {
                return _collapsed;
            }
        }
    }
}
