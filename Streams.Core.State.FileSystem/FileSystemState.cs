using System.Linq;
using LiteDB;

namespace Com.RFranco.Streams.State.FileSystem
{

    /// <summary>
    /// State implementation based on LiteDatabase
    /// </summary>
    public class FileSystemState<T> : State<T>
    {
        /// <summary>
        /// LiteDatabase instance to store the state
        /// </summary>
        private LiteDatabase Database;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <returns></returns>
        public FileSystemState(StateDescriptor<T> descriptor) : this(descriptor, new LiteDatabase(descriptor.Namespace + ".db"))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="descriptor">State descriptor</param>
        /// <param name="database">LiteDabase instance</param>
        /// <returns></returns>
        public FileSystemState(StateDescriptor<T> descriptor, LiteDatabase database) : base(descriptor)
        {
            Database = database;

        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <returns>State value</returns>
        public override T Value()
        {
            if(! Database.CollectionExists(Descriptor.Name)) return Descriptor.DefaultValue;
            return Database.GetCollection<T>(Descriptor.Name).FindAll().First();            
        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="newState">The new state value</param>
        public override void Update(T newState)
        {
            var stateCollection = Database.GetCollection<T>(Descriptor.Name);

            if (!stateCollection.Update(newState))
                stateCollection.Insert(newState);
        }

        /// <summary>
        /// Close the database / file
        /// </summary>
        public override void Close()
        {
            Database.Dispose();
        }

        /// <summary>
        /// Drop the collection (data and indexes) used to store the state value
        /// </summary>
        public override void Clear()
        {
            Database.DropCollection(Descriptor.Name);
        }
    }
}