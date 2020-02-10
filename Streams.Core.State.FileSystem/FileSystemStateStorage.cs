using System.Linq;
using LiteDB;

namespace Com.RFranco.Streams.State.FileSystem
{

    /// <summary>
    /// State implementation based on LiteDatabase
    /// </summary>
    public class FileSystemStateStorage : StateStorage
    {
        /// <summary>
        /// LiteDatabase instance to store the state
        /// </summary>
        private LiteDatabase Database;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName">File Name of the .db file</param>
        /// <param name="collectionName">Collection Name used</param>
        /// <returns></returns>
        public FileSystemStateStorage(string fileName) : this(new LiteDatabase(fileName + ".db"))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="collectionName">Collection name used</param>
        /// <param name="database">LiteDabase instance</param>
        /// <returns></returns>
        public FileSystemStateStorage(LiteDatabase database)
        {
            Database = database;
        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <returns>State value</returns>
        /// <param name="collectionName">Key to identify the state</param>
        public override object GetValue(string collectionName)
        {
            if(! Database.CollectionExists(collectionName)) return null;
            return Database.GetCollection<object>(collectionName).FindAll().First();            
        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="newState">The new state value</param>
        /// <param name="collectionName">Key to identify the state</param>
        public override void Update(string collectionName, object newState)
        {
            var stateCollection = Database.GetCollection<object>(collectionName);

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
        /// <param name="collectionName">Key to identify the state</param>
        public override void Clear(string collectionName)
        {
            Database.DropCollection(collectionName);
        }
    }
}