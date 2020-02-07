using System;
using System.Text;
using RocksDbSharp;

namespace Com.RFranco.Streams.State.RocksDB
{

    /// <summary>
    /// State implementation based on RocksDBS
    /// </summary>
    public class RocksDBStateStorage : StateStorage
    {
        /// <summary>
        /// Rocks database instance
        /// </summary>
        private RocksDb Database;

        /// <summary>
        /// Key used to store the state value
        /// </summary>
        private byte[] Key;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns></returns>
        public RocksDBStateStorage(String path, string key) : this(path, key, new DbOptions().SetCreateIfMissing(true))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State desciptor</param>
        /// <param name="options">RocksDB options</param>
        /// <returns></returns>
        public RocksDBStateStorage(string path, string key, DbOptions options)
        {
            Database = RocksDb.Open(options, path);
            Key = Encoding.UTF8.GetBytes(key);
        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <returns></returns>
        public override object GetValue()
        {

            object State = null;

            try
            {
                State = Deserialize(Database.Get(Key));

            }
            catch (Exception) { }

            return State;
        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="newState">The new value of the state</param>
        public override void Update(object newState)
        {
            Database.Put(Key, Serialize(newState));
        }

        /// <summary>
        /// Close RocksDB instance
        /// </summary>
        public override void Close()
        {
            Database.Dispose();
        }

        /// <summary>
        /// Clear the key / value used  to store the state
        /// </summary>
        public override void Clear()
        {
            Database.Remove(Key);
        }
    }
}