using Massive;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test.Repository
{
    internal class ProductRepository : DynamicModel
    {
        public ProductRepository() : base("default", "Products", "ProductID")
        {
        }
    }
}