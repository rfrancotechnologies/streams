using System;
using System.Collections.Generic;
using Com.RFranco.Streams.State.Redis;

namespace Com.RFranco.Example.Streams.State.Redis
{
    class Program
    {
        static void Main(string[] args)
        {
            string key = "changetracking-PlayerChangeStatusTracking";
            //  Requires a redis
            var redisStorage = new RedisStateStorage(new RedisConfiguration
            {
                Nodes = "localhost:6379"
            });

            Console.WriteLine("Checking previous state (it must be null)");
            var state = redisStorage.GetValue(key);
            //if (state != null) throw new Exception($"Invalid state founded: {state.ToString()}");

            state = new DummyState
            {
                Description = "Description",
                Offset = 100
            };

            redisStorage.Update(key, state);
            var state2 = redisStorage.GetValue(key);
            Console.WriteLine($"state {state2.ToString()}");

            redisStorage.Clear(key);
            state2 = redisStorage.GetValue(key);
            if (state2 != null) throw new Exception($"Invalid state founded: {state2.ToString()}");

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
