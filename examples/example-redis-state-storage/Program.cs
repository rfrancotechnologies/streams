using System;
using System.Collections.Generic;
using Com.RFranco.Streams.State.Redis;

namespace Com.RFranco.Example.Streams.State.Redis
{
    class Program
    {
        static void Main(string[] args)
        {
            //  Requires a redis
            var redisStorage = new RedisStateStorage(new RedisConfiguration
            {
                Nodes = "localhost:6379",
                //TTL = 24*60*60*1000,
                Key = "testState1"
            });

            Console.WriteLine("Checking previous state (it must be null)");
            var state = redisStorage.GetValue();
            if (state != null) throw new Exception($"Invalid state founded: {state.ToString()}");

            state = new DummyState()
            {
                Description = "New description",
                Offset = 1,
                Type = Kind.PUBLIC,
                Items = new List<Item> { new Item { Code = "AA", Amount = 200.10 }, new Item { Code = "BB", Amount = 0 } }
            };

            redisStorage.Update(state);
            state = redisStorage.GetValue();
            Console.WriteLine($"state {state.ToString()}");

            redisStorage.Clear();
            state = redisStorage.GetValue();
            if (state != null) throw new Exception($"Invalid state founded: {state.ToString()}");

            redisStorage.Close();

            var redisStorage2 = new RedisStateStorage(new RedisConfiguration
            {
                Nodes = "localhost:6379",
                TTL = 1,
                Key = "testState2"
            });
            Console.WriteLine("Checking previous state (it must be null)");
            long? state2 = redisStorage2.GetValue() as long?;
            if (state2 != null) throw new Exception($"Invalid state founded: {state2.ToString()}");

            state2 = 12;

            redisStorage2.Update(state2.Value);
            Console.WriteLine($"state {redisStorage2.GetValue()}");

            state2 = redisStorage2.GetValue() as long?;

            redisStorage2.Clear();
            state = redisStorage2.GetValue();
            if (state != null) throw new Exception($"Invalid state founded: {state2.ToString()}");

            redisStorage.Close();


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
