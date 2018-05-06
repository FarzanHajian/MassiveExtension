using Massive;
using Slapper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Transactions;
using Test.Model;
using Test.Repository;
using static Slapper.AutoMapper;

namespace Test
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //CustomerLoadTestPoco();
            //CustomerLoadTestDynamic();

            LoadProductsWithparentsPoco();
            LoadProductsWithParentsDynamic();

            //BatchInsertProduct();

            Console.ReadLine();
        }

        private static void CustomerLoadTestPoco()
        {
            var customers = new CustomerRepository();
            IEnumerable<Customer> data;

            Stopwatch sw = Stopwatch.StartNew();
            for (int i = 0; i < 1_000; i++)
            {
                data = customers.All<Customer>();
            }
            sw.Stop();

            Console.WriteLine($"{nameof(CustomerLoadTestPoco)} => {sw.ElapsedMilliseconds:N0}");
        }

        private static void CustomerLoadTestDynamic()
        {
            var customers = new CustomerRepository();
            IEnumerable<dynamic> data;

            Stopwatch sw = Stopwatch.StartNew();
            for (int i = 0; i < 1_000; i++)
            {
                data = customers.All();
            }
            sw.Stop();

            Console.WriteLine($"{nameof(CustomerLoadTestDynamic)} => {sw.ElapsedMilliseconds:N0}");
        }

        private static void LoadProductsWithparentsPoco()
        {
            var products = new ProductRepository();
            IEnumerable<Product> data;

            Stopwatch sw = Stopwatch.StartNew();
            for (int i = 0; i < 1_000; i++)
            {
                data = products.QueryAndLink<Product>(
                    @"select p.*, null as ___, s.*, null as ___, c.*
                        from Products p
                        inner join Suppliers s on s.SupplierID=p.SupplierID
                        inner join Categories c on c.CategoryID=p.CategoryID",
                    (m, linkeds) => { m.Supplier = (Supplier)linkeds[0]; m.Category = (Category)linkeds[1]; },
                    new Type[] { typeof(Supplier), typeof(Category) },
                    true
                ).ToList();
            }
            sw.Stop();

            Console.WriteLine($"{nameof(LoadProductsWithparentsPoco)} => {sw.ElapsedMilliseconds:N0}");
        }

        private static void LoadProductsWithParentsDynamic()
        {
            var products = new ProductRepository();
            IEnumerable<dynamic> data;

            Stopwatch sw = Stopwatch.StartNew();
            for (int i = 0; i < 1_000; i++)
            {
                data = products.QueryAndLink(
                    @"select p.*, null as ___, s.*, null as ___, c.*
                        from Products p
                        inner join Suppliers s on s.SupplierID=p.SupplierID
                        inner join Categories c on c.CategoryID=p.CategoryID",
                    (m, linkeds) => { m.Supplier = linkeds[0]; m.Category = linkeds[1]; },
                    true
                ).ToList();
            }
            sw.Stop();

            Console.WriteLine($"{nameof(LoadProductsWithParentsDynamic)} => {sw.ElapsedMilliseconds:N0}");
        }

        private static void BatchInsertProduct()
        {
            int count = 100;
            Random rnd = new Random(DateTime.Now.Millisecond);
            dynamic[] data = new dynamic[count];
            for (int i = 0; i < count; i++)
            {
                data[i] =
                    new
                    {
                        CategoryID = rnd.Next(1, 8),
                        Discontinued = false,
                        ProductName = $"PRODUCT {rnd.Next(1, 1_000_000)}",
                        QuantityPerUnit = $"{rnd.Next(1, 12)} Boxes",
                        ReorderLevel = (short)(rnd.Next(1, 6) * 5),
                        SupplierID = rnd.Next(1, 6),
                        UnitPrice = Math.Round((decimal)(rnd.Next(1000, 5000) / 100), 2),
                        UnitsInStock = (short)rnd.Next(100),
                        UnitsOnOrder = (short)rnd.Next(100)
                    };
            }

            var products = new ProductRepository();
            Stopwatch sw = Stopwatch.StartNew();
            using (TransactionScope trans = new TransactionScope())
            {
                products.InsertBatch(data);
                trans.Complete();
            }
            sw.Stop();

            Console.WriteLine($"{nameof(BatchInsertProduct)} => {sw.ElapsedMilliseconds:N0}");
        }
    }
}