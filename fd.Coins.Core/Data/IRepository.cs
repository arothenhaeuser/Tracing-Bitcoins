using System.Collections.Generic;

namespace fd.Coins.Core.NetworkConnector
{
    public interface IRepository<T>
    {
        bool Add(T item);
        bool AddRange(IEnumerable<T> items);
        IEnumerable<T> GetAll();
        T GetById(string id);
        bool Remove(T item);
        bool Remove(IEnumerable<T> items);
        bool Update(T item);
        bool Update(IEnumerable<T> items);
    }
}
