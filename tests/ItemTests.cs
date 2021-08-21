using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace metastrings
{
    [TestClass]
    public class ItemTests
    {
        [TestMethod]
        public void TestEnsureItem()
        {
            using (var ctxt = TestUtils.GetCtxt())
            {
                int tableId = Tables.GetIdAsync(ctxt, "foo").Result;
                long valueId = Values.GetIdAsync(ctxt, "bar").Result;

                long id0 = Items.GetIdAsync(ctxt, tableId, valueId).Result;
                long id1 = Items.GetIdAsync(ctxt, tableId, valueId).Result;

                Assert.AreEqual(id0, id1);
            }
        }

        [TestMethod]
        public void TestItemData()
        {
            using (var ctxt = TestUtils.GetCtxt())
            {
                int tableId = Tables.GetIdAsync(ctxt, "blet").Result;
                long itemId = Items.GetIdAsync(ctxt, tableId, Values.GetIdAsync(ctxt, "monkey").Result).Result;

                {
                    var itemData = new Dictionary<int, long>();
                    itemData[Names.GetIdAsync(ctxt, tableId, "foo").Result] = Values.GetIdAsync(ctxt, "bar").Result;
                    itemData[Names.GetIdAsync(ctxt, tableId, "something").Result] = Values.GetIdAsync(ctxt, "else").Result;
                    Items.SetItemData(ctxt, itemId, itemData);
                    ctxt.ProcessPostOpsAsync().Wait();

                    var metadata = Items.GetItemDataAsync(ctxt, itemId).Result;
                    Console.WriteLine($"metadata1 Dict contents ({metadata.Count}):");
                    Console.WriteLine(string.Join("\n", metadata.Select(kvp => $"{kvp.Key}: {kvp.Value}")));

                    foreach (var kvp in metadata)
                    {
                        string nameVal = Names.GetNameAsync(ctxt, kvp.Key).Result.name;
                        string valueVal = Values.GetValueAsync(ctxt, kvp.Value).Result.ToString();
                        if (nameVal == "foo")
                            Assert.AreEqual("bar", valueVal);
                        else if (nameVal == "something")
                            Assert.AreEqual("else", valueVal);
                        else
                            Assert.Fail("Name not recognized: " + nameVal);
                    }
                }

                {
                    var itemData = new Dictionary<int, long>();
                    itemData[Names.GetIdAsync(ctxt, tableId, "foo").Result] = Values.GetIdAsync(ctxt, "blet").Result;
                    itemData[Names.GetIdAsync(ctxt, tableId, "something").Result] = Values.GetIdAsync(ctxt, "monkey").Result;
                    Items.SetItemData(ctxt, itemId, itemData);
                    ctxt.ProcessPostOpsAsync().Wait();

                    var metadata = Items.GetItemDataAsync(ctxt, itemId).Result;
                    Console.WriteLine($"metadata2 Dict contents ({metadata.Count}):");
                    Console.WriteLine(string.Join("\n", metadata.Select(kvp => $"{kvp.Key}: {kvp.Value}")));

                    foreach (var kvp in metadata)
                    {
                        string nameVal = Names.GetNameAsync(ctxt, kvp.Key).Result.name;
                        string valueVal = Values.GetValueAsync(ctxt, kvp.Value).Result.ToString();
                        if (nameVal == "foo")
                            Assert.AreEqual("blet", valueVal);
                        else if (nameVal == "something")
                            Assert.AreEqual("monkey", valueVal);
                        else
                            Assert.Fail("Name not recognized: " + nameVal);
                    }
                }
            }
        }

        [TestMethod]
        public void TestLongStringItems()
        {
            using (var ctxt = TestUtils.GetCtxt())
            {
                string longStr;

                ctxt.Cmd.CreateTableAsync("blet", isNumeric: false).Wait();

                Define define = new Define("blet", "monkey");
                ctxt.Cmd.DefineAsync(define).Wait();

                long monkeyId = ctxt.GetRowIdAsync("blet", "monkey").Result;

                LongStringOp get = new LongStringOp() { table = "blet", fieldName = "foo", itemId = monkeyId };
                longStr = ctxt.Cmd.GetLongStringAsync(get).Result;
                Assert.IsNull(longStr);

                LongStringPut put = new LongStringPut() { table = "blet", fieldName = "foo", itemId = monkeyId, longString = "bar" };
                ctxt.Cmd.PutLongStringAsync(put).Wait();

                longStr = ctxt.Cmd.GetLongStringAsync(get).Result;
                Assert.AreEqual("bar", longStr);

                LongStringOp del = new LongStringOp() { table = "blet", fieldName = "foo", itemId = monkeyId };
                ctxt.Cmd.DeleteLongStringAsync(del).Wait();
                longStr = ctxt.Cmd.GetLongStringAsync(get).Result;
                Assert.IsNull(longStr);
            }
        }
    }
}
