using LiteDB;

namespace Com.RFranco.Streams.State.FileSystem
{
    /// <summary>
    /// FileSystem state factory
    /// </summary>
    /// <typeparam name="T">Type of the state value</typeparam>
    public class FileSystemStateBackend<T> : StateBackend<T>
    {
        /// <summary>
        /// Instance of litedabase used
        /// </summary>
        private LiteDatabase Database;

        /// <summary>
        /// Constructor
        /// </summary>
        public FileSystemStateBackend() {}

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="database">Instance of litedatabase to be used to store the state</param>
        public FileSystemStateBackend(LiteDatabase database) 
        {
            Database = database;
        }
                
        /// <summary>
        /// Create or retrieve the state by its descriptor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns>FileSystem state instance</returns>
        public override State<T> GetOrCreateState(StateDescriptor<T> descriptor)
        {
            return (Database != null) ? new FileSystemState<T>(descriptor, Database) : new FileSystemState<T>(descriptor);
        }
    }
}