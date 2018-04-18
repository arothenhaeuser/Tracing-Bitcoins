using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace fd.Coins.Core
{
    public static class Extensions
    {
        public static Dictionary<TKey, TValue> Excerpt<TKey, TValue>(this Dictionary<TKey, TValue> self, TKey[] keys)
        {
            return self.Where(x => keys.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value);
        }

        public static PropertyInfo GetPrimaryKey(this Type self)
        {
            foreach (var prop in self.GetProperties())
            {
                if (
                    prop.GetCustomAttributes(true)
                    .Any(x => x.GetType() == typeof(PrimaryKey)))
                {
                    return prop;
                }
            }
            throw new ArgumentException("The provided item has no primary key.");
        }

        public static object SafeGetValue(this PropertyInfo self, object obj)
        {
            var value = self.GetValue(obj);
            if (value.GetType() == typeof(string))
            {
                return $"\"{value}\"";
            }
            if (value.GetType() == typeof(string[]))
            {
                return $"\"{string.Join(",", value)}\"";
            }
            return value == null ? "NULL" : value;
        }
    }
}
