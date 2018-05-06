using Massive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test.Repository
{
    internal class CustomerRepository : DynamicModel
    {
        public CustomerRepository() : base("default", "Customers", "CustomerID")
        {
        }
    }
}