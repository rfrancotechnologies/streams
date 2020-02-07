using System;
using System.Collections.Generic;
using Com.RFranco.Streams.State.Zookeeper;

namespace Com.RFranco.Example.Streams.State.Zookeeper
{
    class Program
    {
        static void Main(string[] args)
        {
            //  Requires a Zookeeper
            var zkStorage = new ZookeeperStateStorage(new ZookeeperClientOptions
            {
                ConnectionString = "localhost:2181",
                SessionTimeoutMilliseconds = 24*60*60*1000,
                Namespace = "test1",
                Name = "testState"
            });

            Console.WriteLine("Checking previous state (it must be null)");
            var state = zkStorage.GetValue();
            if (state != null) throw new Exception($"Invalid state founded: {state.ToString()}");

            state = new DummyState()
            {
                Description = "New description",
                Offset = 1,
                Type = Kind.PUBLIC,
                Items = new List<Item> { new Item { Code = "AA", Amount = 200.10 }, new Item { Code = "BB", Amount = 0 } }
            };

            zkStorage.Update(state);
            state = zkStorage.GetValue();
            Console.WriteLine($"state {state.ToString()}");

            zkStorage.Clear();
            state = zkStorage.GetValue();
            if (state != null) throw new Exception($"Invalid state founded: {state.ToString()}");

            zkStorage.Close();
        }
    }

    public enum Kind
    {
        PUBLIC, PRIVATE
    }

    [Serializable]
    public class DummyState
    {
        public long Offset { get; set; }
        public string Description { get; set; }

        public List<Item> Items { get; set; }

        public Kind Type { get; set; }

        public override string ToString()
        {
            return "{\"Offset\": " + Offset
                + ",\"Description\": " + Description
                + ",\"Type\": " + Type.ToString()
                + ",\"Items\": " + Items.Count
                + "}";
        }

    }

    [Serializable]
    public class Item
    {
        public string Code { get; set; }

        public double Amount { get; set; }
    }
}
