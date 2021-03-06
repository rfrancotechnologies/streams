using System;
using System.Threading;
using StackExchange.Redis;

namespace Com.RFranco.Streams.State.Redis
{

    /// <summary>
    /// State implementation based on Redis
    /// </summary>
    public class RedisStateStorage : StateStorage
    {
        private readonly Lazy<ConnectionMultiplexer> Redis;

        private readonly RedisConfiguration RedisConfiguration;

        public RedisStateStorage(RedisConfiguration redisConfiguration)
        {
            RedisConfiguration = redisConfiguration;
            Redis = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(redisConfiguration.Nodes), LazyThreadSafetyMode.PublicationOnly);
        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <param name="key">Key to identify the state</param>
        /// <returns></returns>
        public override object GetValue(string key)
        {
            IDatabase db = Redis.Value.GetDatabase();
            var state = db.StringGet(key);
            if (state.IsNull) return null;
            else
                try
                {
                    return Deserialize(state);
                }
                catch (Exception)
                {
                    return state.ToString();
                }

        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="key">Key to identify the state</param>
        /// <param name="newState">The new value of the state</param>
        public override void Update(string key, object newState)
        {
            IDatabase db = Redis.Value.GetDatabase();
            TimeSpan? expiration = null;
            if (RedisConfiguration.TTL > 0)
                expiration = TimeSpan.FromSeconds(RedisConfiguration.TTL);
            if (newState.GetType().IsPrimitive)
                db.StringSet(key, newState.ToString() , expiry: expiration);
            else
                db.StringSet(key, Serialize(newState), expiry: expiration);
        }

        /// <summary>
        /// Close Redis instance
        /// </summary>
        public override void Close()
        {
            Redis.Value.Dispose();
        }

        /// <summary>
        /// Clear the key / value used  to store the state
        /// </summary>
        /// <param name="key">Key to identify the state</param>
        public override void Clear(string key)
        {
            IDatabase db = Redis.Value.GetDatabase();
            db.KeyDelete(key);
        }
    }
}