using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace fd.Coins.Core
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class PrimaryKey : Attribute
    {
        public bool PK { get; set; }
        public PrimaryKey()
        {
            PK = true;
        }
    }
}
