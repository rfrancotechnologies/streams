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
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns></returns>
        public RocksDBStateStorage(String path) : this(path, new DbOptions().SetCreateIfMissing(true))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State desciptor</param>
        /// <param name="options">RocksDB options</param>
        /// <returns></returns>
        public RocksDBStateStorage(string path, DbOptions options)
        {
            Database = RocksDb.Open(options, path);            
        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <param name="key">Key to identify the state</param>
        /// <returns></returns>
        public override object GetValue(string key)
        {

            object State = null;

            try
            {
                State = Deserialize(Database.Get(Encoding.UTF8.GetBytes(key)));

            }
            catch (Exception) { }

            return State;
        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="key">Key to identify the state</param>
        /// <param name="newState">The new value of the state</param>
        public override void Update(string key, object newState)
        {
            Database.Put(Encoding.UTF8.GetBytes(key), Serialize(newState));
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
        /// <param name="key">Key to identify the state</param>
        public override void Clear(string key)
        {
            Database.Remove(Encoding.UTF8.GetBytes(key));
        }
    }
}