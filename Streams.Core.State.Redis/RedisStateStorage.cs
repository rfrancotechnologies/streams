using System;
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
            Redis =  new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(redisConfiguration.Nodes));
        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <returns></returns>
        public override object GetValue()
        {
            IDatabase db = Redis.Value.GetDatabase();
            var state = db.StringGet(RedisConfiguration.Key);
            if(state.IsNull) return null;
            else  return Deserialize(state);
        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="newState">The new value of the state</param>
        public override void Update(object newState)
        {
            IDatabase db = Redis.Value.GetDatabase();
            TimeSpan? expiration = null;
            if(RedisConfiguration.TTL > 0 )
                expiration = TimeSpan.FromSeconds(RedisConfiguration.TTL);
            db.StringSet(RedisConfiguration.Key, Serialize(newState), expiry: expiration);
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
        public override void Clear()
        {
            IDatabase db = Redis.Value.GetDatabase();
            db.KeyDelete(RedisConfiguration.Key);
        }
    }
}