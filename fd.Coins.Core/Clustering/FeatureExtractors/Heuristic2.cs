using Orient.Client;
using OrientDB_Net.binary.Innov8tive.API;
using System.Collections.Generic;
using System.Linq;

namespace fd.Coins.Core.Clustering.FeatureExtractors
{
    // heuristic #2: The change address of each transaction is considered to belong to the same user as the inputs. Change addresses are identified by checking the corresponding output against four conditions:
    // 1. This is the only output referencing this address
    // 2. This output does not belong to a coinbase transaction
    // 3. The address is not referenced in one of the transactions inputs (prevOut)
    // 4. Only one output of this transaction matches the above (ambiguity)
    public class Heuristic2 : Extractor
    {
        private List<List<string>> _result;

        public Heuristic2()
        {
            _result = new List<List<string>>();
        }

        public override double Distance(string addr1, string addr2)
        {
            var v1 = _result.FirstOrDefault(x => x.Contains(addr1));
            var v2 = _result.FirstOrDefault(x => x.Contains(addr2));
            return (v1 != null && v2 != null && v1.SequenceEqual(v2)) ? 0.0 : 1.0;
        }

        public override void Run(ConnectionOptions mainOptions, IEnumerable<string> addresses)
        {
            var groups = new List<List<string>>();
            using (var mainDB = new ODatabase(mainOptions))
            {
                foreach (var address in addresses)
                {
                    var record = mainDB.Query($"SELECT $i as inputs, $o as outputs FROM (SELECT expand(inV) FROM (SELECT inV() FROM Link WHERE tAddr = '{address}' LIMIT 10000)) LET $i = set(inE().tAddr), $o = set(outE().tAddr) WHERE Coinbase = false").Select(x => new KeyValuePair<string, List<List<string>>>(x.GetField<ORID>("tx").ToString(), new List<List<string>>() { x.GetField<List<string>>("source"), x.GetField<List<string>>("target") })).First();
                    // clean the record
                    record.Value[0] = record.Value[0].Distinct().ToList();
                    record.Value[1] = record.Value[0].Distinct().ToList();
                    record.Value[0].RemoveAll(x => x == null);
                    record.Value[1].RemoveAll(x => x == null);
                    // can we identify a return address?
                    var occurrences = new Dictionary<string, int>();
                    foreach(var outAddr in record.Value[1])
                    {
                        var count = mainDB.Query($"SELECT COUNT(*) FROM Link WHERE tAddr = '{outAddr}'").SingleOrDefault()?.GetField<long>("count");
                        if(count != null)
                        {
                            occurrences.Add(outAddr, (int)count);
                        }
                    }
                        var addr = GetChangeAddress(record, occurrences, mainOptions);
                        if (addr != null)
                        {
                            // add a new group
                            var group = record.Value.First();
                            group.Add(addr);
                            group.RemoveAll(x => string.IsNullOrEmpty(x));
                            groups.Add(group);
                        }
                }
            }
            var cc = new ClusteringCollapser();
            cc.Collapse(groups);
            _result = cc.Clustering;
        }

        private string GetChangeAddress(KeyValuePair<string, List<List<string>>> kvp, Dictionary<string, int> occurrences, ConnectionOptions mainOptions)
        {
            string changeAddress = null;
            var ambiguous = false;
            using (var mainDB = new ODatabase(mainOptions))
            {
                var inAddresses = kvp.Value[0];
                var outAddresses = kvp.Value[1];
                if (inAddresses.Count == 0 || outAddresses.Count == 0)
                {
                    return null;
                }
                foreach (var outAddress in outAddresses)
                {
                    // address is no self-change address
                    if (inAddresses.Contains(outAddress))
                    {
                        continue;
                    }
                    // address is not reused
                    if (occurrences[outAddress] != 1)
                    {
                        continue;
                    }
                    if (changeAddress == null)
                    {
                        changeAddress = outAddress;
                    }
                    else
                    {
                        ambiguous = true;
                        break;
                    }
                }
                return ambiguous ? null : changeAddress;
            }
        }
    }
}
