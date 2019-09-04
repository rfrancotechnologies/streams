using RocksDbSharp;

namespace Com.RFranco.Streams.State.RocksDB
{
    /// <summary>
    /// RocksDB State factory
    /// </summary>
    /// <typeparam name="T">Type of the state value</typeparam>
    public class RocksDBSateBackend<T> : StateBackend<T>
    {
        /// <summary>
        /// Options to configure RocksDB instance
        /// </summary>
        private DbOptions Options;

        /// <summary>
        /// Constructor
        /// </summary>
        public RocksDBSateBackend() { }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">Options to configure RocksDB instance</param>
        public RocksDBSateBackend(DbOptions options)
        {
            Options = options;
        }

        /// <summary>
        /// Create or retrieve the RocksDB state by its state descriptor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns>RocksDBState instance</returns>
        public override State<T> GetOrCreateState(StateDescriptor<T> descriptor)
        {
            return (Options != null) ? new RocksDBState<T>(descriptor, Options) : new RocksDBState<T>(descriptor);
        }
    }
}