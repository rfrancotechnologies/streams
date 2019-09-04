using System;
using System.Text;
using RocksDbSharp;

namespace Com.RFranco.Streams.State.RocksDB
{

    /// <summary>
    /// State implementation based on RocksDBS
    /// </summary>
    public class RocksDBState<T> : State<T>
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
        public RocksDBState(StateDescriptor<T> descriptor) : this(descriptor, new DbOptions().SetCreateIfMissing(true))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State desciptor</param>
        /// <param name="options">RocksDB options</param>
        /// <returns></returns>
        public RocksDBState(StateDescriptor<T> descriptor, DbOptions options) : base(descriptor)
        {
            Database = RocksDb.Open(options, Descriptor.Namespace);
            Key = Encoding.UTF8.GetBytes(Descriptor.Name);
        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <returns></returns>
        public override T Value()
        {

            T State = Descriptor.DefaultValue;

            try
            {
                State = Descriptor.Serializer.Deserialize(Database.Get(Key));

            }
            catch (Exception) { }

            return State;
        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="newState">The new value of the state</param>
        public override void Update(T newState)
        {
            Database.Put(Key, Descriptor.Serializer.Serialize(newState));
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