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
        /// Collection name used
        /// </summary>
        private string CollectionName;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName">File Name of the .db file</param>
        /// <param name="collectionName">Collection Name used</param>
        /// <returns></returns>
        public FileSystemStateStorage(string fileName, string collectionName) : this(collectionName, new LiteDatabase(fileName + ".db"))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="collectionName">Collection name used</param>
        /// <param name="database">LiteDabase instance</param>
        /// <returns></returns>
        public FileSystemStateStorage(string collectionName, LiteDatabase database) 
        {
            CollectionName = collectionName;
            Database = database;
        }

        /// <summary>
        /// Return the state value
        /// </summary>
        /// <returns>State value</returns>
        public override object GetValue()
        {
            if(! Database.CollectionExists(CollectionName)) return null;
            return Database.GetCollection<object>(CollectionName).FindAll().First();            
        }

        /// <summary>
        /// Update the state value
        /// </summary>
        /// <param name="newState">The new state value</param>
        public override void Update(object newState)
        {
            var stateCollection = Database.GetCollection<object>(CollectionName);

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
            Database.DropCollection(CollectionName);
        }
    }
}