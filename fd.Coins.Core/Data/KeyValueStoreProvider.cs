using Microsoft.Isam.Esent.Collections.Generic;
using System.Collections.Generic;

namespace fd.Coins.Core.NetworkConnector
{
    public class KeyValueStoreProvider
    {
        private static KeyValueStoreProvider _instance;

        private Dictionary<string, PersistentDictionary<string, string>> _databases;

        private KeyValueStoreProvider()
        {
            _databases = new Dictionary<string, PersistentDictionary<string, string>>();
        }

        public static KeyValueStoreProvider Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new KeyValueStoreProvider();
                }
                return _instance;
            }
        }

        public PersistentDictionary<string, string> GetDatabase(string name)
        {
            PersistentDictionary<string, string> dict;
            if(_databases.TryGetValue(name, out dict))
            {
                return dict;
            }
            else
            {
                dict = new PersistentDictionary<string, string>(name);
                _databases.Add(name, dict);
                return dict;
            }
        }
    }
}
