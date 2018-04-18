using Microsoft.Isam.Esent.Collections.Generic;
using System.Collections.Generic;

namespace fd.Coins
{
    public class DatabaseConnectionProvider
    {
        private static DatabaseConnectionProvider _instance;

        private Dictionary<string, PersistentDictionary<string, string>> _databases;

        private DatabaseConnectionProvider()
        {
            _databases = new Dictionary<string, PersistentDictionary<string, string>>();
        }

        public static DatabaseConnectionProvider Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new DatabaseConnectionProvider();
                }
                return _instance;
            }
        }

        public PersistentDictionary<string, string> GetDatabase(string name)
        {
            PersistentDictionary<string, string> dict;
            if (_databases.TryGetValue(name, out dict))
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
